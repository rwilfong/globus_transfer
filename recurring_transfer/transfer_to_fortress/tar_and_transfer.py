"""
Globus Nightly Archiver

Workflow:
1. Scans SOURCE_ROOT for files modified yesterday.
2. Groups files by their parent directory.
3. If a folder contains small files, it creates a TAR archive in STAGING_ROOT.
4. If a folder contains large files, it transfers them individually.
5. Submits the batch to Globus.

Usage:
    python nightly_transfer.py

This will submit a transfer request using Globus 
"""

import globus_sdk
import os
import keyring
import logging
import posixpath
import configparser
import tarfile
from collections import defaultdict
from datetime import datetime, timedelta, time

# Setting a file size threshold
# If average file size in a folder is < 50MB, tar it -- this follows HPSS best practices 
SMALL_FILE_THRESHOLD = 50 * 1024 * 1024 

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('nightly_sync.log'),
            logging.StreamHandler() # prints to terminal
        ]
    )
    return logging.getLogger(__name__)

def get_authorizer(service_name, client_id):
    secret = keyring.get_password(service_name, client_id)
    if not secret:
        raise ValueError(f"No secret found in keyring for {service_name}")
    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes="urn:globus:auth:scope:transfer.api.globus.org:all")

def create_tarball(staging_root, rel_path, file_list):
    """
    Creates a tarball locally in the STAGING_ROOT.
    
    Args:
        staging_root: Local path to write tars (e.g. /scratch/negishi/rwilfong/globus_stage)
        rel_path: The relative path of the source dir (e.g. 'project/data/01')
        file_list: List of absolute file paths to include in the tar
        
    Returns:
        abs_tar_path: The full path to the generated tarball.
    """
    # Clean up path to make a filename (replace slashes with underscores for flat staging)
    safe_name = rel_path.replace(os.sep, '_') + ".tar"
    abs_tar_path = os.path.join(staging_root, safe_name)

    # Create the tarball
    # mode='w' is uncompressed. Use 'w:gz' for compression
    try:
        with tarfile.open(abs_tar_path, "w") as tar:
            for file_path in file_list:
                tar.add(file_path, arcname=os.path.basename(file_path))
        return abs_tar_path
    except IOError as e:
        print(f"Error creating tar: {e}")
        return None

def main():
    logger = setup_logging()
    config = configparser.ConfigParser()
    config.read('config_nightly.ini')

    try:
        # Extract from config
        # Paths
        SOURCE_ROOT = os.path.abspath(config['paths']['SOURCE_ROOT'])
        GLOBUS_SOURCE_ROOT = config['paths']['GLOBUS_SOURCE_ROOT']
        
        # Local staging 
        STAGING_ROOT = os.path.abspath(config['paths']['STAGING_ROOT']) 
        # The Globus path to that scratch space (usually same as STAGING_ROOT)
        GLOBUS_STAGING_ROOT = config['paths'].get('GLOBUS_STAGING_ROOT', STAGING_ROOT)

        # Globus IDs
        SERVICE_NAME = config['keyring']['service_name']
        CLIENT_ID = config['globus']['client_id']
        SOURCE_EP = config['globus']['SOURCE_ENDPOINT_ID']
        DEST_EP = config['globus']['DEST_ENDPOINT_ID']
        DEST_ROOT = config['globus']['DEST_BASE_PATH']
    except KeyError as e:
        logger.error(f"Missing config: {e}")
        return

    # Define time window -- yesterday 
    yesterday = datetime.now().date() - timedelta(days=1)
    start_window = datetime.combine(yesterday, time.min)
    end_window = datetime.combine(yesterday, time.max)
    
    logger.info(f"Scanning {SOURCE_ROOT} for changes on {yesterday}...")

    # Scan and group the files
    # Dictionary structure: { 'absolute/path/to/folder': [file1, file2] }
    modified_groups = defaultdict(list)
    
    for root, dirs, files in os.walk(SOURCE_ROOT):
        for f in files:
            file_path = os.path.join(root, f)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if start_window <= mtime <= end_window:
                    modified_groups[root].append(file_path)
            except OSError:
                continue

    if not modified_groups:
        logger.info("No modified files found.")
        return

    # Prep transfer
    try:
        tc = globus_sdk.TransferClient(authorizer=get_authorizer(SERVICE_NAME, CLIENT_ID))
        transfer_data = globus_sdk.TransferData(
            source_endpoint=SOURCE_EP,
            destination_endpoint=DEST_EP,
            label=f"Archive_{yesterday}",
            sync_level="mtime"
        )
    except Exception as e:
        logger.error(f"Globus Init Failed: {e}")
        return

    files_queued = 0
    tars_created = 0

    # Iterate groups and decide (tar vs raw)
    for folder_abs_path, file_list in modified_groups.items():
        rel_path = os.path.relpath(folder_abs_path, SOURCE_ROOT)
        
        tar_name_base = "root_files" if rel_path == '.' else rel_path
        
        # Calculate stats
        total_size = sum(os.path.getsize(f) for f in file_list)
        avg_size = total_size / len(file_list) if file_list else 0
        
        # Tar if files are small
        if avg_size < SMALL_FILE_THRESHOLD:
            logger.info(f"Tarring {rel_path} ({len(file_list)} files, Avg: {avg_size/1024:.1f}KB)")
            
            # Pass our fixed tar_name_base
            tar_local_path = create_tarball(STAGING_ROOT, tar_name_base, file_list)
            
            if tar_local_path:
                rel_tar_name = os.path.basename(tar_local_path)
                g_source = posixpath.join(GLOBUS_STAGING_ROOT, rel_tar_name)
                
                # Dest path: target/path/to/folder.tar (use fixed base)
                g_dest = posixpath.join(DEST_ROOT, tar_name_base + ".tar")
                
                transfer_data.add_item(g_source, g_dest)
                tars_created += 1
                files_queued += 1
        else:
            # Transfer raw files
            logger.info(f"Transferring raw {rel_path} (Avg: {avg_size/1024/1024:.1f}MB)")
            for f in file_list:
                rel_file = os.path.relpath(f, SOURCE_ROOT)
                g_source = posixpath.join(GLOBUS_SOURCE_ROOT, rel_file)
                g_dest = posixpath.join(DEST_ROOT, rel_file)
                transfer_data.add_item(g_source, g_dest)
                files_queued += 1

    # Submit
    if files_queued > 0:
        task = tc.submit_transfer(transfer_data)
        logger.info(f"Task Submitted: {task['task_id']}")
        if tars_created > 0:
            logger.warning(f"Created {tars_created} tarballs in {STAGING_ROOT}. "
                           "Ensure you have a cleanup cron job (find ... -delete) running separately.")
    else:
        logger.info("Nothing to transfer.")

if __name__ == "__main__":
    main()