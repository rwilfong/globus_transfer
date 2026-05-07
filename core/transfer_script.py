"""
Globus Nightly Sync Utility

This script automates the secure, recurring transfer of files from local source 
directories to a Globus destination endpoint. It is designed to be run as a cron job 
or scheduled task depending on operating system.

Key Features:
- 7-Day Lookback: Scans for any files modified in the last 7 days
- Automatic Sorting: Replicates local directory structures within a 'MM_YYYY' 
  folder at the destination based on the file's modification date
- State Management: Uses a local JSON backlog to track and retry failed transfers
- Concurrency Protection: Implements a PID-based lockfile to prevent overlapping runs
- Dry-Run Mode: Allows testing configuration and file discovery without executing transfers

Prerequisites:
- A `.env` file containing GLOBUS_CLIENT_SECRET
- A `config.ini` file containing endpoint IDs and directory paths
- The `globus_sdk` and `python-dotenv` packages installed

Usage:
    python nightly_sync.py [--config path/to/config.ini] [--dry-run]
"""

import globus_sdk
import os
import logging
import posixpath
import configparser
import argparse
import atexit
import json
from datetime import datetime, timedelta, time
from dotenv import load_dotenv

# Constant configuration
BACKLOG_FILE = "transfer_backlog.json"


# Helper functions for state and concurrency 
def load_backlog():
    """
    Loads pending or previously failed transfers from disk
    
    Returns:
        list: A list of dictionaries containing file metadata. Returns an 
              empty list if the backlog file does not exist or is corrupted
    """
    if os.path.exists(BACKLOG_FILE):
        try:
            with open(BACKLOG_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            # If the file is corrupted or unreadable, start fresh to prevent halting
            return []
    return []

def save_backlog(backlog_list):
    """
    Saves the current list of pending transfers to disk to ensure state 
    is maintained across script executions
    
    Args:
        backlog_list (list): The list of file metadata dictionaries to save
    """
    with open(BACKLOG_FILE, 'w') as f:
        json.dump(backlog_list, f, indent=4)


class LockFile:
    """
    A simple PID-based lock mechanism to prevent the script from running 
    multiple times concurrently 
    (e.g., if a cron job fires while a large transfer is still being calculated)
    """
    def __init__(self, path="nightly_sync.lock"):
        self.path = path

    def acquire(self):
        """
        Attempts to create a lockfile. If one exists, it checks if the 
        Process ID (PID) inside is still actively running
        
        Raises:
            RuntimeError: If another active instance of the script is detected
        """
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r') as f:
                    old_pid = int(f.read().strip())
                
                # os.kill with signal 0 checks if the process is running without actually killing it
                os.kill(old_pid, 0)
                raise RuntimeError(f"Another instance is currently running (PID {old_pid}). Exiting.")
            
            except (ValueError, OSError):
                # If the PID is invalid or the process is dead, clear the stale lock
                os.remove(self.path)

        # Write current PID to the lockfile
        with open(self.path, 'w') as f:
            f.write(str(os.getpid()))
        
        # Ensure the lock is cleaned up when the script exits normally or crashes
        atexit.register(self.release)

    def release(self):
        """
        Removes the lockfile
        """
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass


# Logging and auth helper functions
def setup_logging():
    """
    Configures standard output and file-based logging
    
    Can be updated for the file handler to be a arg

    Returns:
        logging.Logger: Configured logger instance
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), 
                  logging.FileHandler('nightly_sync.log')]
    )
    return logging.getLogger(__name__)

def get_authorizer(client_id):
    """
    Authenticates with Globus using Client Credentials
    Requires a .env file with GLOBUS_CLIENT_SECRET
    
    Args:
        client_id (str): The Globus App Client ID from config.ini
        
    Returns:
        globus_sdk.ClientCredentialsAuthorizer: The configured authorizer object
    """
    load_dotenv()
    secret = os.getenv("GLOBUS_CLIENT_SECRET")
    if not secret:
        raise ValueError("GLOBUS_CLIENT_SECRET not found in .env file. Please create one.")

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)

def log_task_id(task_id, description, logger):
    """
    Logs the Globus Task ID to a dedicated file for auditing and troubleshooting
    
    Args:
        task_id (str): The UUID returned by Globus upon successful submission
        description (str): A human-readable label for the transfer
        logger (logging.Logger): The primary logger instance
    """
    entry = f"{datetime.now().isoformat()} | {description} | task_id={task_id}\n"
    with open("task_ids.log", "a") as f:
        f.write(entry)
    logger.info(f"[{description}] Task ID logged: {task_id}")


# Transfer logic
def scan_for_files(source_roots, start_window, end_window, logger):
    """
    Walks through the defined source directories and identifies files 
    modified within the targeted datetime window
    
    Args:
        source_roots (list): List of root directory paths to scan
        start_window (datetime): The oldest modification date to accept
        end_window (datetime): The newest modification date to accept
        logger (logging.Logger): The primary logger instance
        
    Returns:
        list: A list of dictionaries containing file paths and calculated destination metadata
    """
    valid_items = []
    
    for src_root in source_roots:
        src_root = src_root.strip()
        if not os.path.exists(src_root):
            logger.warning(f"Source root not found, skipping: {src_root}")
            continue

        logger.info(f"Scanning source root: {src_root}")
        
        for root, dirs, files in os.walk(src_root):
            for f in files:
                f_path = os.path.join(root, f)
                
                try:
                    # Check the file's last modified time (mtime)
                    mtime_ts = os.path.getmtime(f_path)
                    mtime = datetime.fromtimestamp(mtime_ts)

                    # If the file was modified within our 7-day lookback window
                    if start_window <= mtime <= end_window:
                        # Get the path relative to the root so we can recreate it remotely
                        rel_path = os.path.relpath(f_path, src_root)
                        
                        # Determine the destination folder string based on file's age
                        month_year_folder = mtime.strftime("%m_%Y")
                        
                        valid_items.append({
                            'full_path': f_path,
                            'rel_path': rel_path,
                            'parent_root': src_root,
                            'date_folder': month_year_folder
                        })
                except OSError as e:
                    logger.error(f"Error accessing {f_path}: {e}")
                    continue
            
    return valid_items

def build_transfers(valid_items, config, logger, dry_run):
    """
    Constructs the Globus TransferData object based on the scanned files
    
    Args:
        valid_items (list): The list of file dictionaries from scan_for_files()
        config (configparser.ConfigParser): Loaded configuration settings
        logger (logging.Logger): The primary logger instance
        dry_run (bool): If True, logs the intent without adding to the transfer payload
        
    Returns:
        tuple: (globus_sdk.TransferData, int) The transfer payload and the total file count
    """
    SOURCE_EP       = config['globus']['SOURCE_ENDPOINT_ID']
    GLOBUS_SRC_BASE = config['paths']['GLOBUS_SOURCE_ROOT'] 
    DEST_EP         = config['globus']['GENERAL_DEST_ENDPOINT_ID']
    DEST_ROOT       = config['globus']['GENERAL_DEST_PATH']

    yesterday = datetime.now().date() - timedelta(days=1)

    # Initialize the transfer payload.
    # sync_level="mtime" ensures we only overwrite destination files if the source is newer
    transfer_data = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=DEST_EP,
        label=f"Nightly_Sync_{yesterday}", 
        preserve_timestamp=True,
        verify_checksum=True,
        sync_level="mtime" 
    )
    
    count = 0

    for item in valid_items:
        rel_path = item['rel_path']
        date_folder = item['date_folder']
        
        # Globus SDK requires POSIX-style paths (forward slashes) for endpoints
        rel_path_posix = rel_path.replace(os.sep, '/')
        g_source_path = posixpath.join(GLOBUS_SRC_BASE, rel_path_posix)
        
        # Construct the final destination path: DEST_ROOT / MM_YYYY / original_folder_hierarchy / file.ext
        g_dest_general = posixpath.join(DEST_ROOT, date_folder, rel_path_posix)
        
        if dry_run:
            logger.info(f"[DRY RUN] Queueing: {rel_path} -> {g_dest_general}")
        else:
            # Add individual files to the payload (recursive=False since we evaluated files, not dirs)
            transfer_data.add_item(g_source_path, g_dest_general, recursive=False)
            
        count += 1

    return transfer_data, count


# Main execution 
def main():
    # Concurrency Check
    lock = LockFile()
    try:
        lock.acquire()
    except RuntimeError as e:
        print(e)
        return

    # Parse arguments
    parser = argparse.ArgumentParser(description="Globus Nightly Data Sync Utility")
    parser.add_argument("--dry-run", action="store_true", help="Scan and log without submitting transfer.")
    parser.add_argument("--config", default="config.ini", help="Path to configuration file.")
    args = parser.parse_args()

    logger = setup_logging()
    
    # Load configuration
    if not os.path.exists(args.config):
        logger.error(f"Configuration file '{args.config}' not found.")
        return

    config = configparser.ConfigParser()
    config.read(args.config)

    try:
        source_roots_str = config['paths']['SOURCE_ROOTS']
        SOURCE_ROOTS = [s for s in source_roots_str.split(',') if s.strip()]
        CLIENT_ID    = config['globus']['client_id']
    except KeyError as e:
        logger.error(f"Missing config key: {e}")
        return

    # State management via backlog
    backlog = load_backlog()

    # Define time window (7-day lookback)
    yesterday   = datetime.now().date() - timedelta(days=1)
    start_date  = datetime.now().date() - timedelta(days=7) 
    
    # Create exact datetime bounds from 00:00:00 to 23:59:59
    start_window = datetime.combine(start_date, time.min)
    end_window   = datetime.combine(yesterday, time.max) 

    logger.info(f"Starting Nightly Sync. Found {len(backlog)} items in backlog.")
    logger.info(f"Scanning for modified files between {start_date} and {yesterday}")

    # Discover files
    new_items = scan_for_files(SOURCE_ROOTS, start_window, end_window, logger)
    
    # Merge newly discovered files with the backlog, using the full path as a unique key
    # to prevent duplicate entries if a transfer failed on a previous night
    existing_paths = {item['full_path'] for item in backlog}
    for item in new_items:
        if item['full_path'] not in existing_paths:
            backlog.append(item)

    # Save immediately to disk in case the script crashes during API authentication
    save_backlog(backlog)

    if not backlog:
        logger.info("No files found to transfer. Exiting.")
        return

    # Build transfer 
    transfer_data, count = build_transfers(backlog, config, logger, args.dry_run)
    logger.info(f"Queue Summary: {count} total files.")

    if args.dry_run:
        logger.info("Dry run complete. No transfers submitted.")
        return

    # Authenticate and submit to Globus
    try:
        authorizer = get_authorizer(CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Authentication failed: {e}")
        return

    submission_failed = False

    if count > 0:
        try:
            task = tc.submit_transfer(transfer_data)
            log_task_id(task['task_id'], "Nightly_Sync", logger)
        except Exception as e:
            logger.error(f"Transfer submission failed: {e}")
            submission_failed = True

    # Post-run cleanup
    # If the API accepted the task, we can safely clear the backlog
    # We rely on Globus's internal retry mechanisms from this point forward
    if not submission_failed:
        logger.info("Transfer submitted successfully. Clearing backlog.")
        save_backlog([])
    else:
        logger.warning("Submission failed. Backlog retained for next run.")

if __name__ == "__main__":
    main()