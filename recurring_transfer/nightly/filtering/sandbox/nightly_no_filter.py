"""
The recurring script for the nightly transfers in Metabolomics that will be ran nightly.

This script uses a .env file rather than the keyring for robust background authentication. 
It filters for yesterday's Agilent '.d' folders and sorts them into MM_YYYY destination directories.
"""
import globus_sdk
import os
import logging
import posixpath
import configparser
import argparse
import atexit
from datetime import datetime, timedelta, time
from dotenv import load_dotenv

# Set up lock file
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
                
                # os.kill with signal 0 checks if the process is currently running.
                os.kill(old_pid, 0)
                
                # If we get here, the process is still alive. Do not run.
                raise RuntimeError(f"Another instance is currently running (PID {old_pid}). Exiting.")
            
            except (ValueError, OSError):
                # OSError (ProcessLookupError) means the PID is dead.
                # ValueError means the file was empty or corrupted.
                os.remove(self.path)

        # Create a new lockfile with our current PID
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
    """
    Sets up logging to print to the console and save to a file
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler(), logging.FileHandler('nightly_sync.log')]
    )
    return logging.getLogger(__name__)


def get_authorizer(client_id):
    """
    Retrieves the Globus Confidential App secret securely from a .env file
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
    Saves the Globus Task IDs to a separate file for tracking
    """
    entry = f"{datetime.now().isoformat()} | {description} | task_id={task_id}\n"
    with open("task_ids.log", "a") as f:
        f.write(entry)
    logger.info(f"[{description}] Task ID logged: {task_id}")


def get_latest_mtime(dir_path):
    """
    Finds the most recent modification time among a directory and its contents
    Agilent '.d' files are actually directories that contain relevant metadata for each sample
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
    Scans multiple source roots for Agilent '.d' directories modified exactly 
    within the target timeframe ('yesterday').
    """
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
                        valid_dirs.append({
                            'rel_path': rel_path
                        })
                except OSError as e:
                    logger.error(f"Error accessing {d_path}: {e}")
                    continue
            
            # Modify 'dirs' in-place to prevent scanning inside the .d folders
            dirs[:] = [d for d in dirs if not d.endswith('.d')]
            
    return valid_dirs


def build_transfers(valid_dirs, config, logger, dry_run):
    """
    Builds a single Globus TransferData object for all raw data, dynamically sorting files into MM_YYYY destination folders so it isn't manual
    """
    SOURCE_EP       = config['globus']['SOURCE_ENDPOINT_ID']
    GLOBUS_SRC_BASE = config['paths']['GLOBUS_SOURCE_ROOT'] 
    
    GEN_DEST_EP     = config['globus']['GENERAL_DEST_ENDPOINT_ID']
    GEN_DEST_ROOT   = config['globus']['GENERAL_DEST_PATH']

    # Calculate MM_YYYY based on the data's date (yesterday)
    yesterday = datetime.now().date() - timedelta(days=1)
    month_year_folder = yesterday.strftime("%m_%Y")

    t_general = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=GEN_DEST_EP,
        label=f"Metabolomics_Raw_{yesterday}", 
        verify_checksum=True
    )

    counts = {'general': 0}

    for item in valid_dirs:
        rel_path = item['rel_path']
        
        # Globus always uses forward slashes
        rel_path_posix = rel_path.replace(os.sep, '/')
        g_source_path = posixpath.join(GLOBUS_SRC_BASE, rel_path_posix)

        # Inserts the MM_YYYY folder into the destination path
        g_dest_general = posixpath.join(GEN_DEST_ROOT, month_year_folder, rel_path_posix)
        
        if dry_run:
            logger.info(f"[DRY RUN] {rel_path} -> {g_dest_general}")
        else:
            t_general.add_item(g_source_path, g_dest_general, recursive=True)
        counts['general'] += 1

    return t_general, counts


def main():
    lock = LockFile()
    try:
        lock.acquire()
    except RuntimeError as e:
        print(e)
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Scan without submitting transfer.")
    parser.add_argument("--config", default="config_basic.ini", help="Path to config file.")
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

    yesterday    = datetime.now().date() - timedelta(days=1)
    start_window = datetime.combine(yesterday, time.min)
    end_window   = datetime.combine(yesterday, time.max)

    logger.info(f"Starting Nightly Sync for {yesterday}")

    valid_dirs = scan_for_directories(SOURCE_ROOTS, start_window, end_window, logger)
    
    if not valid_dirs:
        logger.info("No modified '.d' directories found. Exiting.")
        return

    t_general, counts = build_transfers(valid_dirs, config, logger, args.dry_run)

    logger.info(f"Queue Summary: {counts['general']} folders prepared for transfer.")

    if args.dry_run:
        logger.info("Dry run complete. No transfers submitted.")
        return

    # Authenticate and Submit
    try:
        authorizer = get_authorizer(CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
        
        task = tc.submit_transfer(t_general)
        log_task_id(task['task_id'], "Raw_Data_Sync", logger)
        logger.info("Transfer submitted successfully.")
        
    except Exception as e:
        logger.error(f"Transfer failed: {e}")

if __name__ == "__main__":
    main()