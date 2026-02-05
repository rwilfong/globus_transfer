"""
Globus Nightly Sync Tool 

This script performs a nightly "incremental mirror" of the given data directory.
It identifies folders modified furing the previous calendar day and syncs them to a remote Globus endpoint.

Workflow:
1. Calculates a time window for yesterday (00:00:00-23:59:59)
2. Scans the SOURCE_ROOT for any directories with a modification time (mtime) in that window
3. Submits a recursive, non-destructive transfer via Globus 
4. Uses 'mtime' sync level to avoid re-transferring unchanged files

This script is best used for being ran early in the morning to collect and transfer the data from the previous day. 

Requires:
- globus_sdk
- keyring

Usage:
    python nightly_transfer.py
"""

import globus_sdk
import os
import keyring
import logging
import posixpath
import configparser
from datetime import datetime, timedelta, time

def setup_logging():
    """
    Configures the logging format and destination.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.FileHandler('nightly_sync.log')] # Option to write to a sync log! 
    )
    return logging.getLogger(__name__)

def get_authorizer(service_name, client_id):
    """
    Retrieves the secret from keyring using Client ID as the username.
    
    Args:
        service_name (str): The name of the keyring service (e.g., 'Globus_MPF').
        client_id (str): The Globus Client UUID.
        
    Returns:
        globus_sdk.ClientCredentialsAuthorizer: The Globus authorizer.
    """
    secret = keyring.get_password(service_name, client_id)
    
    if not secret:
        raise ValueError(
            f"Could not retrieve client secret from keyring for service '{service_name}' "
            f"and user '{client_id}'. Ensure it is set via keyring.set_password()."
        )

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)
def main():
    logger = setup_logging()
    
    # Load the configuration
    config = configparser.ConfigParser()
    # Hard-coded configuration path
    if not os.path.exists('config_nightly.ini'):
        logger.error("Configuration file 'config_nightly.ini' not found.")
        return
    config.read('config_nightly.ini')
    
    try:
        # Keyring & Globus setup
        SERVICE_NAME = config['keyring']['service_name']
        CLIENT_ID = config['globus']['client_id']
        
        # Extract paths and endpoints from the config 
        # Globus and source paths can differ, so we need to define both 
        SOURCE_ROOT = config['paths']['SOURCE_ROOT']               
        GLOBUS_SOURCE_ROOT = config['paths']['GLOBUS_SOURCE_ROOT'] 
        SOURCE_EP = config['globus']['SOURCE_ENDPOINT_ID']
        DEST_EP = config['globus']['DEST_ENDPOINT_ID']
        DEST_ROOT = config['globus']['DEST_BASE_PATH']

    except KeyError as e:
        logger.error(f"Missing expected config key or section: {e}")
        return

    # Identify the time window - 00:00:00-23:59:59 from the previous day 
    today = datetime.now().date()
    yesterday = today - timedelta(days=1)
    start_window = datetime.combine(yesterday, time.min)
    end_window = datetime.combine(yesterday, time.max)
    
    logger.info(f"Starting Sync for Data Date: {yesterday}")

    # Globus authentication 
    try:
        authorizer = get_authorizer(SERVICE_NAME, CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return

    # Initialize the TransferData object
    transfer_data = globus_sdk.TransferData(
        transfer_client=tc,
        source_endpoint=SOURCE_EP,
        destination_endpoint=DEST_EP,
        label=f"Nightly_Sync_{yesterday}",
        sync_level="mtime",
        verify_checksum=True,
        preserve_timestamp=True
    )

    # Scan the directories 
    # Look for folders modified yesterday to mirror structure to destination
    items_added = 0
    for root, dirs, files in os.walk(SOURCE_ROOT):
        for d in dirs:
            dir_path = os.path.join(root, d)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(dir_path))
                
                if start_window <= mtime <= end_window:
                    rel_path = os.path.relpath(dir_path, SOURCE_ROOT)
                    g_source = posixpath.join(GLOBUS_SOURCE_ROOT, rel_path)
                    g_dest = posixpath.join(DEST_ROOT, rel_path)
                    
                    transfer_data.add_item(g_source, g_dest, recursive=True)
                    logger.info(f"Queued for transfer: {rel_path}")
                    items_added += 1
            except OSError:
                continue

    # Submit the transfer task
    if items_added > 0:
        try:
            task = tc.submit_transfer(transfer_data)
            logger.info(f"Task submitted successfully! ID: {task['task_id']}")
        except globus_sdk.TransferAPIError as e:
            logger.error(f"Globus Transfer Submission Failed: {e}")
    else:
        logger.info(f"No data found for date {yesterday}. Sync skipped.")

if __name__ == "__main__":
    main()