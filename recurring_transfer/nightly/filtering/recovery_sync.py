"""
Script to transfer data from steps 2 and 3 of the initial_nightly_filter.py that didn't sync.

This should identify only the data from PROJECT_KEYWORD.

Again, this will go back to June 1, 2025 with the backfill. 
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
        handlers=[logging.StreamHandler(), logging.FileHandler('recovery_sync.log')]
    )
    return logging.getLogger(__name__)

def get_authorizer(service_name, client_id):
    secret = keyring.get_password(service_name, client_id)
    if not secret:
        raise ValueError(f"Secret not found in keyring for {client_id}")

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)

def log_task_id(task_id, description, logger):
    entry = f"{datetime.now().isoformat()} | {description} | task_id={task_id}\n"
    with open("task_ids.log", "a") as f:
        f.write(entry)
    logger.info(f"[{description}] Task ID logged: {task_id}")

def get_latest_mtime(dir_path):
    """
    Finds the most recent modification time among the directory and its contents
    """
    max_mtime = os.path.getmtime(dir_path)
    for root, _, files in os.walk(dir_path):
        for f in files:
            try:
                mtime = os.path.getmtime(os.path.join(root, f))
                if mtime > max_mtime:
                    max_mtime = mtime
            except OSError:
                continue
    return max_mtime

def scan_for_directories(source_roots, start_window, end_window, logger):
    """
    Scans multiple source roots for .d directories modified within the window
    """
    valid_dirs = []
    for src_root in source_roots:
        src_root = src_root.strip()
        if not os.path.exists(src_root):
            continue
        for root, dirs, files in os.walk(src_root):
            d_dirs = [d for d in dirs if d.endswith('.d')]
            for d in d_dirs:
                d_path = os.path.join(root, d)
                try:
                    mtime_ts = get_latest_mtime(d_path)
                    mtime = datetime.fromtimestamp(mtime_ts)
                    if start_window <= mtime <= end_window:
                        rel_path = os.path.relpath(d_path, src_root)
                        valid_dirs.append({
                            'full_path': d_path,
                            'rel_path': rel_path,
                            'parent_root': src_root
                        })
                except OSError:
                    continue
            # Prevent scanning inside .d folders
            dirs[:] = [d for d in dirs if not d.endswith('.d')]
    return valid_dirs

def main():
    parser = argparse.ArgumentParser()
    # set up argparse arguments
    parser.add_argument("--dry-run", action="store_true", help="Scan without submitting transfer.")
    parser.add_argument("--config", default="config_widhalm.ini", help="Path to config file.")
    parser.add_argument("--backfill", action="store_true", help="Use historical backfill dates.")
    args = parser.parse_args()

    logger = setup_logging()
    
    if not os.path.exists(args.config):
        logger.error(f"Configuration file '{args.config}' not found.")
        return

    config = configparser.ConfigParser()
    config.read(args.config)

    try:
        source_roots_str = config['paths']['SOURCE_ROOTS']
        SOURCE_ROOTS     = [s for s in source_roots_str.split(',') if s.strip()]
        SERVICE_NAME     = config['keyring']['service_name']
        CLIENT_ID        = config['globus']['client_id']
        SOURCE_EP        = config['globus']['SOURCE_ENDPOINT_ID']
        GLOBUS_SRC_BASE  = config['paths']['GLOBUS_SOURCE_ROOT'] 
        PROJ_DEST_EP     = config['globus']['PROJECT_DEST_ENDPOINT_ID']
        PROJ_DEST_ROOT   = config['globus']['PROJECT_DEST_PATH']
        TAPE_EP          = config['globus']['TAPE_ENDPOINT_ID']
        TAPE_ROOT        = config['globus']['TAPE_BASE_PATH']
        PROJECT_KEYWORD  = config['settings'].get('PROJECT_KEYWORD', 'widhalm').lower()
        STAGING_DIR      = config['paths']['LOCAL_TAR_STAGING']
        GLOBUS_STAGING   = config['paths']['GLOBUS_TAR_STAGING']
    except KeyError as e:
        logger.error(f"Missing config key: {e}")
        return

    yesterday = datetime.now().date() - timedelta(days=1)
    
    if args.backfill:
        start_window = datetime(2025, 6, 1) 
        logger.info(f"[RECOVERY] Historical backfill sync from {start_window.date()} up to {yesterday}")
    else:
        start_window = datetime.combine(yesterday, time.min)
        logger.info(f"[RECOVERY] Nightly Sync for {yesterday}")
        
    end_window = datetime.combine(yesterday, time.max)

    # Scan phase (Identical to original to find the same targets)
    valid_dirs = scan_for_directories(SOURCE_ROOTS, start_window, end_window, logger)
    
    if not valid_dirs:
        logger.info("No modified '.d' directories found in the target window. Exiting.")
        return

    # Build Transfer Objects
    t_project_raw = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=PROJ_DEST_EP,
        label=f"RECOVERY_Project_{PROJECT_KEYWORD}_Raw_{datetime.now().date()}", 
        preserve_timestamp=True,
        verify_checksum=True
    )

    t_project_tape = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=TAPE_EP,
        label=f"RECOVERY_Project_{PROJECT_KEYWORD}_Tape_{datetime.now().date()}", 
        preserve_timestamp=True,
        verify_checksum=True
    )

    counts = {'project_raw': 0, 'project_tape': 0}

    for item in valid_dirs:
        full_path = item['full_path']
        rel_path = item['rel_path']
        
        # Only process project specific paths
        if PROJECT_KEYWORD in full_path.lower():
            rel_path_posix = rel_path.replace(os.sep, '/')
            g_source_path = posixpath.join(GLOBUS_SRC_BASE, rel_path_posix)

            # Project raw transfer
            g_dest_proj = posixpath.join(PROJ_DEST_ROOT, rel_path_posix)
            if args.dry_run:
                logger.info(f"[DRY RUN] Queueing Project Raw: {rel_path} -> {g_dest_proj}")
            else:
                t_project_raw.add_item(g_source_path, g_dest_proj, recursive=True)
            counts['project_raw'] += 1

            # Project tape transfer using the existing tarballs
            safe_tar_name = rel_path.replace(os.sep, '_') + '.tar'
            local_tar_path = os.path.join(STAGING_DIR, safe_tar_name)
            
            # Verify the tarball actually exists before adding it to Globus
            if os.path.exists(local_tar_path):
                g_dest_tape = posixpath.join(TAPE_ROOT, f"{rel_path_posix}.tar")
                g_source_tape = posixpath.join(GLOBUS_STAGING, safe_tar_name)
                
                if args.dry_run:
                    logger.info(f"[DRY RUN] Queueing EXISTING Tape Tarball: {safe_tar_name} -> {g_dest_tape}")
                else:
                    t_project_tape.add_item(g_source_tape, g_dest_tape)
                counts['project_tape'] += 1
            else:
                logger.warning(f"MISSING TARBALL: Could not find {local_tar_path}. Skipping tape transfer for this file.")

    logger.info(f"Recovery Queue Summary: Project Raw={counts['project_raw']} | Tape={counts['project_tape']}")

    if args.dry_run:
        logger.info("Dry run complete. No transfers submitted.")
        return

    # Submit the transfers 
    if counts['project_raw'] == 0 and counts['project_tape'] == 0:
        logger.info("No project items to transfer. Exiting.")
        return

    try:
        authorizer = get_authorizer(SERVICE_NAME, CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        return

    if counts['project_raw'] > 0:
        try:
            task = tc.submit_transfer(t_project_raw)
            log_task_id(task['task_id'], "RECOVERY_Project_Raw", logger)
        except Exception as e:
            logger.error(f"Project Raw Recovery Transfer failed: {e}")

    if counts['project_tape'] > 0:
        try:
            task = tc.submit_transfer(t_project_tape)
            log_task_id(task['task_id'], "RECOVERY_Project_Tape", logger)
        except Exception as e:
            logger.error(f"Project Tape Recovery Transfer failed: {e}")

if __name__ == "__main__":
    main()