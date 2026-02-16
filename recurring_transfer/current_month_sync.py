"""
Create a sync to get things up to date for the current month.
"""

import globus_sdk
import os
import keyring
import logging
import posixpath
import configparser
import argparse
from datetime import datetime  

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('current_month_sync.log')]
    )
    return logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(
        description="Globus transfer for data created within the current month."
    )
    parser.add_argument(
        "-c", "--config",
        default="config_nightly.ini",
        help="Path to INI configuration file"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without submitting the transfer"
    )
    return parser.parse_args()

def get_authorizer(service_name, client_id):
    secret = keyring.get_password(service_name, client_id)
    if not secret:
        raise ValueError(f"Secret not found in keyring for {client_id}")

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)

def add_files_with_timestamps(
    transfer_data,
    source_local_root,
    globus_source_root,
    globus_dest_root,
    logger,
    start_window,
    end_window,
    dry_run
):
    files_added = 0
    for root, dirs, files in os.walk(source_local_root):
        for f in files:
            file_path = os.path.join(root, f)
            try:
                mtime_ts = os.path.getmtime(file_path)
                mtime = datetime.fromtimestamp(mtime_ts)

                # Window check: Is the file from this month?
                if start_window <= mtime <= end_window:
                    rel_path = os.path.relpath(file_path, source_local_root)
                    rel_dir = os.path.dirname(rel_path)

                    name_part, ext_part = os.path.splitext(f)
                    timestamp_str = mtime.strftime("%Y%m%d_%H%M%S")
                    new_filename = f"{name_part}_{timestamp_str}{ext_part}"

                    g_source = posixpath.join(globus_source_root, rel_path)
                    g_dest = posixpath.join(globus_dest_root, rel_dir, new_filename)

                    if dry_run:
                        logger.info(f"[DRY RUN] Would transfer: {g_source} -> {g_dest}")
                    else:
                        transfer_data.add_item(g_source, g_dest)

                    files_added += 1
            except OSError as e:
                logger.warning(f"Could not process {file_path}: {e}")
    return files_added

def main():
    args = parse_args()
    logger = setup_logging()

    config = configparser.ConfigParser()
    if not os.path.exists(args.config):
        logger.error(f"Configuration file '{args.config}' not found.")
        return
    config.read(args.config)

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

    # Find data from the current month
    now = datetime.now()
    # Start: The 1st day of the current month at 00:00:00
    start_window = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    # End: Right now
    end_window = now

    month_label = start_window.strftime('%B_%Y')
    logger.info(f"Targeting CURRENT month: {month_label}")
    logger.info(f"Window: {start_window} to {end_window}")

    try:
        authorizer = get_authorizer(SERVICE_NAME, CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        return

    transfer_data = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP,
        destination_endpoint=DEST_EP,
        label=f"CurrentMonth_{month_label}",
        sync_level="checksum",
        verify_checksum=True
    )

    total_added = add_files_with_timestamps(
        transfer_data, SOURCE_ROOT, GLOBUS_SOURCE_ROOT, DEST_ROOT,
        logger, start_window, end_window, args.dry_run
    )

    if total_added > 0:
        if args.dry_run:
            logger.info(f"Dry run finished. Found {total_added} files.")
        else:
            task = tc.submit_transfer(transfer_data)
            logger.info(f"Transfer submitted! Task ID: {task['task_id']}")
    else:
        logger.info(f"No files found for {month_label} yet.")

if __name__ == "__main__":
    main()