"""
The recurring script for the nightly transfers in Metabolomics that will be ran nightly.
"""
import globus_sdk
import os
import keyring
import logging
import posixpath
import configparser
import argparse
import atexit
import tarfile
from datetime import datetime, timedelta, time

# Set up lock file
class LockFile:
    """
    Prevents the script from running multiple times concurrently.
    If a nightly sync takes longer than 24 hours, or gets stuck, we don't 
    want tomorrow's cron job overlapping and causing transfer conflicts.
    """
    def __init__(self, path="nightly_sync.lock"):
        self.path = path

    def acquire(self):
        # Check if a lockfile already exists
        if os.path.exists(self.path):
            # Calculate how old the lock file is
            lock_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(self.path))
            
            # If the lock is older than 6 hours, assume the previous run crashed and clear it
            if lock_age > timedelta(hours=6):
                os.remove(self.path)
            else:
                # Read the Process ID (PID) of the script that created the lock
                try:
                    with open(self.path, 'r') as f:
                        old_pid = int(f.read().strip())
                    # os.kill with signal 0 doesn't actually kill the process; 
                    # it just checks if the process is currently running.
                    os.kill(old_pid, 0)
                    raise RuntimeError(f"Another instance is running (PID {old_pid}). Exiting.")
                except (ValueError, ProcessLookupError):
                    # The PID is no longer running (stale lock), so we can remove it
                    os.remove(self.path)

        # Create a new lockfile with our current PID
        with open(self.path, 'w') as f:
            f.write(str(os.getpid()))
        
        # Ensure the lock is released when the script finishes (even if it crashes)
        atexit.register(self.release)

    def release(self):
        if os.path.exists(self.path):
            os.remove(self.path)


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


def get_authorizer(service_name, client_id):
    """
    Retrieves the Globus Confidential App secret securely from the OS keyring
    and returns a ClientCredentialsAuthorizer. 
    """
    secret = keyring.get_password(service_name, client_id)
    if not secret:
        raise ValueError(f"Secret not found in keyring for {client_id}")

    client = globus_sdk.ConfidentialAppAuthClient(client_id, secret)
    # This scope grants the script permission to use the Transfer API
    scopes = "urn:globus:auth:scope:transfer.api.globus.org:all"
    return globus_sdk.ClientCredentialsAuthorizer(client, scopes=scopes)


def log_task_id(task_id, description, logger):
    """
    Saves the Globus Task IDs to a separate file for easy tracking and auditing
    """
    entry = f"{datetime.now().isoformat()} | {description} | task_id={task_id}\n"
    with open("task_ids.log", "a") as f:
        f.write(entry)
    logger.info(f"[{description}] Task ID logged: {task_id}")

# Prepare file system and tar utilities 
def get_latest_mtime(dir_path):
    """
    Finds the most recent modification time among a directory and its contents.
    Agilent '.d' files are actually directories that contain relevant metadata for each sample. 
    The directory timestamp itself might not update when a sub-file finishes writing, so we check everything inside.
    """
    max_mtime = os.path.getmtime(dir_path)
    for root, _, files in os.walk(dir_path):
        for f in files:
            try:
                mtime = os.path.getmtime(os.path.join(root, f))
                if mtime > max_mtime:
                    max_mtime = mtime
            except OSError:
                continue # Skip files we can't access due to permissions, etc.
    return max_mtime


def make_tarfile(output_filename, source_dir):
    """
    Compresses a directory into a standard .tar file without compression (.gz) for speed
    """
    with tarfile.open(output_filename, "w") as tar:
        # arcname ensures we don't tar the entire absolute path, just the folder name
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def cleanup_old_tarballs(staging_dir, days_old=7):
    """
    Deletes local tarballs older than a given threshold to save disk space. Default is 1 week
    """
    cutoff = datetime.now() - timedelta(days=days_old)
    if not os.path.exists(staging_dir):
        return
    for f in os.listdir(staging_dir):
        if f.endswith('.tar'):
            file_path = os.path.join(staging_dir, f)
            try:
                mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
                if mtime < cutoff:
                    os.remove(file_path)
            except OSError:
                pass

# Scan the directory and build the transfer 
def scan_for_directories(source_roots, start_window, end_window, logger):
    """
    Scans multiple source roots for Agilent '.d' directories modified exactly 
    within the target timeframe (usually 'yesterday').
    """
    valid_dirs = []
    
    for src_root in source_roots:
        src_root = src_root.strip()
        if not os.path.exists(src_root):
            logger.warning(f"Source root not found, skipping: {src_root}")
            continue

        logger.info(f"Scanning source root: {src_root}")
        
        for root, dirs, files in os.walk(src_root):
            # Identify Agilent data directories by the .d extension
            d_dirs = [d for d in dirs if d.endswith('.d')]
            
            for d in d_dirs:
                d_path = os.path.join(root, d)
                
                try:
                    mtime_ts = get_latest_mtime(d_path)
                    mtime = datetime.fromtimestamp(mtime_ts)

                    # Check if the data finished writing during our target window
                    if start_window <= mtime <= end_window:
                        # Calculate relative path so we can mirror the folder structure on the destination
                        rel_path = os.path.relpath(d_path, src_root)
                        valid_dirs.append({
                            'full_path': d_path,
                            'rel_path': rel_path,
                            'parent_root': src_root
                        })
                except OSError as e:
                    logger.error(f"Error accessing {d_path}: {e}")
                    continue
            
            # Modify the 'dirs' list in-place to prevent os.walk from scanning inside 
            # the .d folders we just found. This speeds up scanning significantly~
            dirs[:] = [d for d in dirs if not d.endswith('.d')]
            
    return valid_dirs


def build_transfers(valid_dirs, config, logger, dry_run):
    """
    Takes the list of valid directories and sorts them into 3 Globus TransferData objects:
    1. General Raw (everything)
    2. Project Raw (only data matching the project keyword)
    3. Project Tape (tarred project data for archive)
    """
    
    # Load the configurations file 
    SOURCE_EP       = config['globus']['SOURCE_ENDPOINT_ID']
    GLOBUS_SRC_BASE = config['paths']['GLOBUS_SOURCE_ROOT'] 
    
    GEN_DEST_EP     = config['globus']['GENERAL_DEST_ENDPOINT_ID']
    GEN_DEST_ROOT   = config['globus']['GENERAL_DEST_PATH']
    
    PROJ_DEST_EP    = config['globus']['PROJECT_DEST_ENDPOINT_ID']
    PROJ_DEST_ROOT  = config['globus']['PROJECT_DEST_PATH']
    
    TAPE_EP         = config['globus']['TAPE_ENDPOINT_ID']
    TAPE_ROOT       = config['globus']['TAPE_BASE_PATH']
    
    PROJECT_KEYWORD = config['settings'].get('PROJECT_KEYWORD', 'widhalm').lower()
    
    STAGING_DIR     = config['paths']['LOCAL_TAR_STAGING']
    GLOBUS_STAGING  = config['paths']['GLOBUS_TAR_STAGING']

    # Initialize the globus transferData object
    # verify_checksum=True ensures data integrity after transit
    t_general = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=GEN_DEST_EP,
        label=f"All_Raw_{datetime.now().date()}", 
        verify_checksum=True
    )
    
    t_project_raw = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=PROJ_DEST_EP,
        label=f"Project_{PROJECT_KEYWORD}_Raw_{datetime.now().date()}", 
        verify_checksum=True
    )

    t_project_tape = globus_sdk.TransferData(
        source_endpoint=SOURCE_EP, 
        destination_endpoint=TAPE_EP,
        label=f"Project_{PROJECT_KEYWORD}_Tape_{datetime.now().date()}", 
        verify_checksum=True
    )

    # Prepare local staging area
    os.makedirs(STAGING_DIR, exist_ok=True)
    cleanup_old_tarballs(STAGING_DIR)

    # Keep track of how many items are in each transfer bucket
    counts = {'general': 0, 'project_raw': 0, 'project_tape': 0}

    # Process the directories 
    for item in valid_dirs:
        full_path = item['full_path']
        rel_path = item['rel_path']
        
        # Globus always uses forward slashes (POSIX) for paths, even if the source is Windows.
        rel_path_posix = rel_path.replace(os.sep, '/')
        
        # Map the local relative path to the absolute path on the Globus Source Endpoint
        g_source_path = posixpath.join(GLOBUS_SRC_BASE, rel_path_posix)

        # Create the first "bucket" -- transfer 1 for all regular data 
        g_dest_general = posixpath.join(GEN_DEST_ROOT, rel_path_posix)
        if dry_run:
            logger.info(f"[DRY RUN] General: {rel_path} -> {g_dest_general}")
        else:
            t_general.add_item(g_source_path, g_dest_general, recursive=True)
        counts['general'] += 1

        # Create the second and third buckets -- the project specific ones 
        # Check if the keyword exists anywhere in the absolute path
        if PROJECT_KEYWORD in full_path.lower():
            
            # Add to Project Raw Transfer
            g_dest_proj = posixpath.join(PROJ_DEST_ROOT, rel_path_posix)
            if dry_run:
                logger.info(f"[DRY RUN] Project Raw: {rel_path} -> {g_dest_proj}")
            else:
                t_project_raw.add_item(g_source_path, g_dest_proj, recursive=True)
            counts['project_raw'] += 1

            # Build tarball for the Fortress archive transfer
            # Replace slashes with underscores so we don't create subdirectories in the staging folder
            safe_tar_name = rel_path.replace(os.sep, '_') + '.tar'
            local_tar_path = os.path.join(STAGING_DIR, safe_tar_name)
            
            g_dest_tape = posixpath.join(TAPE_ROOT, f"{rel_path_posix}.tar")
            g_source_tape = posixpath.join(GLOBUS_STAGING, safe_tar_name)

            if dry_run:
                logger.info(f"[DRY RUN] Project Tape: {rel_path} -> {g_dest_tape}")
            else:
                logger.info(f"Tarring {os.path.basename(full_path)} for tape...")
                make_tarfile(local_tar_path, full_path) # Actually create the file locally
                t_project_tape.add_item(g_source_tape, g_dest_tape)
            counts['project_tape'] += 1

    return (t_general, t_project_raw, t_project_tape), counts

# Main execution 

def main():
    # Grab lock to prevent concurrent runs
    lock = LockFile()
    try:
        lock.acquire()
    except RuntimeError as e:
        print(e)
        return

    # Parse command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Scan without submitting transfer.")
    parser.add_argument("--config", default="config_widhalm.ini", help="Path to config file.")
    args = parser.parse_args()

    logger = setup_logging()
    
    if not os.path.exists(args.config):
        logger.error(f"Configuration file '{args.config}' not found.")
        return

    # Read and parse the configuration file
    config = configparser.ConfigParser()
    config.read(args.config)

    try:
        source_roots_str = config['paths']['SOURCE_ROOTS']
        SOURCE_ROOTS = [s for s in source_roots_str.split(',') if s.strip()]
        SERVICE_NAME = config['keyring']['service_name']
        CLIENT_ID    = config['globus']['client_id']
    except KeyError as e:
        logger.error(f"Missing config key: {e}")
        return

    # Define the time window for "Yesterday"
    yesterday    = datetime.now().date() - timedelta(days=1)
    start_window = datetime.combine(yesterday, time.min) # Yesterday 00:00:00
    end_window   = datetime.combine(yesterday, time.max) # Yesterday 23:59:59

    logger.info(f"Starting Nightly Sync for {yesterday}")

    # Scan the filesystem
    valid_dirs = scan_for_directories(SOURCE_ROOTS, start_window, end_window, logger)
    
    if not valid_dirs:
        logger.info("No modified '.d' directories found. Exiting.")
        return

    # Build the Globus Transfer payloads
    transfers, counts = build_transfers(valid_dirs, config, logger, args.dry_run)
    t_general, t_proj_raw, t_proj_tape = transfers

    logger.info(f"Queue Summary: General={counts['general']} | Project Raw={counts['project_raw']} | Tape={counts['project_tape']}")

    # If --dry-run was passed, stop before actually authenticating and sending data
    if args.dry_run:
        logger.info("Dry run complete. No transfers submitted.")
        return

    # 7. Authenticate with Globus
    try:
        authorizer = get_authorizer(SERVICE_NAME, CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        return

    # Submit Transfers to the Globus API
    if counts['general'] > 0:
        try:
            task = tc.submit_transfer(t_general)
            log_task_id(task['task_id'], "General_Raw", logger)
        except Exception as e:
            logger.error(f"General Transfer failed: {e}")

    if counts['project_raw'] > 0:
        try:
            task = tc.submit_transfer(t_proj_raw)
            log_task_id(task['task_id'], "Project_Raw", logger)
        except Exception as e:
            logger.error(f"Project Raw Transfer failed: {e}")

    if counts['project_tape'] > 0:
        try:
            task = tc.submit_transfer(t_proj_tape)
            log_task_id(task['task_id'], "Project_Tape", logger)
        except Exception as e:
            logger.error(f"Project Tape Transfer failed: {e}")

if __name__ == "__main__":
    main()