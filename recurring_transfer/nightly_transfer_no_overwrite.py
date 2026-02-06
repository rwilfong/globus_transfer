"""
Globus Nightly Sync Tool 

This script performs a nightly "incremental mirror" of the given data directory.
It identifies folders modified during the previous calendar day and syncs them to a remote Globus endpoint.

Workflow:
1. Calculates a time window for yesterday (00:00:00-23:59:59)
2. Scans the SOURCE_ROOT for any directories with a modification time (mtime) in that window
3. Submits a recursive, non-destructive transfer via Globus WITH TIMESTAMPED FILENAMES
4. Files are renamed with their modification timestamp to prevent overwrites

This script is best used for being ran early in the morning to collect and transfer the data from the previous day. 

Requires:
- globus_sdk
- keyring

Usage to submit transfer:
    python nightly_transfer_no_overwrite.py

Usage DRY RUN:
    python nightly_transfer_no_overwrite.py --dry-run
"""

import globus_sdk
import os
import keyring
import logging
import posixpath
import configparser
import argparse
from datetime import datetime, timedelta, time

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('nightly_sync.log')]
    )
    return logging.getLogger(__name__)

def get_authorizer(service_name, client_id):
    secret = keyring.get_password(service_name, client_id)
    if not secret:
        raise ValueError(f"Secret not found in keyring for {client_id}")

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)

def add_files_with_timestamps(transfer_data, source_local_root, globus_source_root, 
                             globus_dest_root, logger, start_window, end_window, dry_run):
    files_added = 0
    for root, dirs, files in os.walk(source_local_root):
        for f in files:
            file_path = os.path.join(root, f)
            try:
                mtime_ts = os.path.getmtime(file_path)
                mtime = datetime.fromtimestamp(mtime_ts)
                
                if start_window <= mtime <= end_window:
                    rel_path = os.path.relpath(file_path, source_local_root)
                    rel_dir = os.path.dirname(rel_path)
                    
                    name_part, ext_part = os.path.splitext(f)
                    timestamp_str = mtime.strftime("%Y%m%d_%H%M%S")
                    new_filename = f"{name_part}_{timestamp_str}{ext_part}"
                    
                    g_source = posixpath.join(globus_source_root, rel_path)
                    g_dest = posixpath.join(globus_dest_root, rel_dir, new_filename)
                    
                    if dry_run:
                        logger.info(f"[DRY RUN] Queue: {rel_path} -> {new_filename}")
                    else:
                        transfer_data.add_item(g_source, g_dest)
                    
                    files_added += 1
            except OSError:
                continue
    return files_added

def main():
    # Setup argparse
    parser = argparse.ArgumentParser(description="Globus Nightly Sync with Timestamped Filenames")
    parser.add_argument(
        "--dry-run", 
        action="store_true", 
        help="Scan and log files without submitting the Globus transfer."
    )
    args = parser.parse_args()

    logger = setup_logging()
    config = configparser.ConfigParser()
    
    # Hardcoded ini file 
    if not os.path.exists('config_nightly.ini'):
        logger.error("Configuration file 'config_nightly.ini' not found.")
        return
    config.read('config_nightly.ini')
    
    try:
        SOURCE_ROOT = os.path.abspath(config['paths']['SOURCE_ROOT'])               
        GLOBUS_SOURCE_ROOT = config['paths']['GLOBUS_SOURCE_ROOT'] 
        SERVICE_NAME = config['keyring']['service_name']
        CLIENT_ID = config['globus']['client_id']
        SOURCE_EP = config['globus']['SOURCE_ENDPOINT_ID']
        DEST_EP = config['globus']['DEST_ENDPOINT_ID']
        DEST_ROOT = config['globus']['DEST_BASE_PATH']
    except KeyError as e:
        logger.error(f"Missing config key: {e}")
        return

    yesterday = datetime.now().date() - timedelta(days=1)
    start_window = datetime.combine(yesterday, time.min)
    end_window = datetime.combine(yesterday, time.max)
    
    logger.info(f"Targeting Yesterday: {yesterday}")
    if args.dry_run:
        logger.info("Dry Run!")

    try:
        authorizer = get_authorizer(SERVICE_NAME, CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        return

    transfer_data = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP,
        destination_endpoint=DEST_EP,
        label=f"Daily_Sync_{yesterday}",
        sync_level=None,
        verify_checksum=True
    )

    total_added = add_files_with_timestamps(
        transfer_data, SOURCE_ROOT, GLOBUS_SOURCE_ROOT, 
        DEST_ROOT, logger, start_window, end_window, args.dry_run
    )

    if total_added > 0:
        if args.dry_run:
            logger.info(f"Dry run complete. {total_added} files identified.")
        else:
            task = tc.submit_transfer(transfer_data)
            logger.info(f"Task submitted! ID: {task['task_id']}")
    else:
        logger.info(f"No files modified on {yesterday} were found.")

if __name__ == "__main__":
    main()