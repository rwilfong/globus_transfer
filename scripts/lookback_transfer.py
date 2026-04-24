"""
The recurring script for the nightly transfers in Metabolomics that will be ran nightly

This script uses a .env file rather than the keyring. Attempting for more robust scripts

It is a simplified version of lookback_transfer_filter.py because it does not implement any filtering.
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

# Set up a backlog file
BACKLOG_FILE = "transfer_backlog.json"

def load_backlog():
    """
    Loads pending transfers from disk. Returns an empty list if none exist
    """
    if os.path.exists(BACKLOG_FILE):
        try:
            with open(BACKLOG_FILE, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return []
    return []

def save_backlog(backlog_list):
    """
    Saves the current pending transfers to disk.
    """
    with open(BACKLOG_FILE, 'w') as f:
        json.dump(backlog_list, f, indent=4)

# Class utilities 
class LockFile:
    """
    Prevents the script from running multiple times concurrently
    Verifies if the previous Process ID is still active before clearing stale locks
    """
    def __init__(self, path="nightly_sync.lock"):
        self.path = path

    def acquire(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, 'r') as f:
                    old_pid = int(f.read().strip())
                
                os.kill(old_pid, 0)
                raise RuntimeError(f"Another instance is currently running (PID {old_pid}). Exiting.")
            
            except (ValueError, OSError):
                os.remove(self.path)

        with open(self.path, 'w') as f:
            f.write(str(os.getpid()))
        
        atexit.register(self.release)

    def release(self):
        if os.path.exists(self.path):
            try:
                os.remove(self.path)
            except OSError:
                pass

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('nightly_sync.log')]
    )
    return logging.getLogger(__name__)

def get_authorizer(client_id):
    load_dotenv()
    secret = os.getenv("GLOBUS_CLIENT_SECRET")
    if not secret:
        raise ValueError("GLOBUS_CLIENT_SECRET not found in .env file. Please create one.")

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)

def log_task_id(task_id, description, logger):
    entry = f"{datetime.now().isoformat()} | {description} | task_id={task_id}\n"
    with open("task_ids.log", "a") as f:
        f.write(entry)
    logger.info(f"[{description}] Task ID logged: {task_id}")

def get_latest_mtime(dir_path):
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

# Main logic and functions 
def scan_for_directories(source_roots, start_window, end_window, logger):
    valid_dirs = []
    
    for src_root in source_roots:
        src_root = src_root.strip()
        if not os.path.exists(src_root):
            logger.warning(f"Source root not found, skipping: {src_root}")
            continue

        logger.info(f"Scanning source root: {src_root}")
        
        for root, dirs, files in os.walk(src_root):
            d_dirs = [d for d in dirs if d.endswith('.d')]
            
            for d in d_dirs:
                d_path = os.path.join(root, d)
                
                try:
                    mtime_ts = get_latest_mtime(d_path)
                    mtime = datetime.fromtimestamp(mtime_ts)

                    if start_window <= mtime <= end_window:
                        rel_path = os.path.relpath(d_path, src_root)
                        
                        # Calculate the correct folder destination based on the file's actual date
                        month_year_folder = mtime.strftime("%m_%Y")
                        
                        valid_dirs.append({
                            'full_path': d_path,
                            'rel_path': rel_path,
                            'parent_root': src_root,
                            'date_folder': month_year_folder
                        })
                except OSError as e:
                    logger.error(f"Error accessing {d_path}: {e}")
                    continue
            
            # Prevent os.walk from descending into the .d directories
            dirs[:] = [d for d in dirs if not d.endswith('.d')]
            
    return valid_dirs

def build_transfers(valid_dirs, config, logger, dry_run):
    SOURCE_EP       = config['globus']['SOURCE_ENDPOINT_ID']
    GLOBUS_SRC_BASE = config['paths']['GLOBUS_SOURCE_ROOT'] 
    DEST_EP         = config['globus']['GENERAL_DEST_ENDPOINT_ID']
    DEST_ROOT       = config['globus']['GENERAL_DEST_PATH']

    yesterday = datetime.now().date() - timedelta(days=1)

    # Added sync_level="mtime" to all transfers to prevent re-uploading existing files
    transfer_data = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=DEST_EP,
        label=f"All_Raw_Sync_{yesterday}", 
        preserve_timestamp=True,
        verify_checksum=True,
        sync_level="mtime" 
    )
    
    count = 0

    for item in valid_dirs:
        rel_path = item['rel_path']
        date_folder = item['date_folder']
        
        rel_path_posix = rel_path.replace(os.sep, '/')
        g_source_path = posixpath.join(GLOBUS_SRC_BASE, rel_path_posix)
        
        # Uses the specific date folder saved in the item dictionary
        g_dest_general = posixpath.join(DEST_ROOT, date_folder, rel_path_posix)
        
        if dry_run:
            logger.info(f"[DRY RUN] Queueing: {rel_path} -> {g_dest_general}")
        else:
            transfer_data.add_item(g_source_path, g_dest_general, recursive=True)
            
        count += 1

    return transfer_data, count

def main():
    lock = LockFile()
    try:
        lock.acquire()
    except RuntimeError as e:
        print(e)
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Scan without submitting transfer.")
    parser.add_argument("--config", default="config.ini", help="Path to config file.")
    args = parser.parse_args()

    logger = setup_logging()
    
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

    # Load previously failed or queued transfers
    backlog = load_backlog()

    # 7-day lookback logic 
    yesterday   = datetime.now().date() - timedelta(days=1)
    start_date  = datetime.now().date() - timedelta(days=7) 
    
    start_window = datetime.combine(start_date, time.min)
    end_window   = datetime.combine(yesterday, time.max) 

    logger.info(f"Starting Nightly Sync. Found {len(backlog)} items in backlog.")
    logger.info(f"Scanning for modified '.d' files between {start_date} and {yesterday}")

    new_dirs = scan_for_directories(SOURCE_ROOTS, start_window, end_window, logger)
    
    # Merge new directories with the backlog (preventing exact duplicates)
    existing_paths = {item['full_path'] for item in backlog}
    for item in new_dirs:
        if item['full_path'] not in existing_paths:
            backlog.append(item)

    # Save immediately in case the script crashes during processing
    save_backlog(backlog)

    if not backlog:
        logger.info("No directories to transfer. Exiting.")
        return

    transfer_data, count = build_transfers(backlog, config, logger, args.dry_run)

    logger.info(f"Queue Summary: Raw Data Items = {count}")

    if args.dry_run:
        logger.info("Dry run complete. No transfers submitted.")
        return

    try:
        authorizer = get_authorizer(CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        return

    submission_failed = False

    if count > 0:
        try:
            task = tc.submit_transfer(transfer_data)
            log_task_id(task['task_id'], "Raw_Sync", logger)
        except Exception as e:
            logger.error(f"Transfer submission failed: {e}")
            submission_failed = True

    # If everything succeeded, clear the backlog for the next night
    if not submission_failed:
        logger.info("Transfer submitted successfully. Clearing backlog.")
        save_backlog([])
    else:
        logger.warning("Submission failed. Backlog retained for next run.")

if __name__ == "__main__":
    main()