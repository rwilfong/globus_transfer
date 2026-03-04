""" 
Main script for the Widhalm filtering on two of the instruments in Metabolomics:
- Agilent 6546 
- Agilent 6460


This will be the main script. The first iteration of this run will look for data aged back to June 1, 2025. 
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
    def __init__(self, path="nightly_sync.lock"):
        self.path = path

    def acquire(self):
        if os.path.exists(self.path):
            lock_age = datetime.now() - datetime.fromtimestamp(os.path.getmtime(self.path))
            if lock_age > timedelta(hours=6):
                os.remove(self.path)
            else:
                try:
                    with open(self.path, 'r') as f:
                        old_pid = int(f.read().strip())
                    os.kill(old_pid, 0)
                    raise RuntimeError(f"Another instance is running (PID {old_pid}). Exiting.")
                except (ValueError, ProcessLookupError):
                    os.remove(self.path)

        with open(self.path, 'w') as f:
            f.write(str(os.getpid()))
        atexit.register(self.release)

    def release(self):
        if os.path.exists(self.path):
            os.remove(self.path)

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

def make_tarfile(output_filename, source_dir):
    with tarfile.open(output_filename, "w") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))

def cleanup_old_tarballs(staging_dir, days_old=7):
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

def scan_for_directories(source_roots, start_window, end_window, logger):
    """
    Scans multiple source roots for .d directories modified within the window.
    Returns a list of dicts: {'full_path', 'rel_path', 'root_base'}
    """
    valid_dirs = []
    
    for src_root in source_roots:
        src_root = src_root.strip()
        if not os.path.exists(src_root):
            logger.warning(f"Source root not found, skipping: {src_root}")
            continue

        logger.info(f"Scanning source root: {src_root}")
        
        for root, dirs, files in os.walk(src_root):
            # Identify Agilent data directories
            d_dirs = [d for d in dirs if d.endswith('.d')]
            
            for d in d_dirs:
                d_path = os.path.join(root, d)
                
                try:
                    mtime_ts = get_latest_mtime(d_path)
                    mtime = datetime.fromtimestamp(mtime_ts)

                    if start_window <= mtime <= end_window:
                        # Calculate relative path from the specific source root scanned
                        rel_path = os.path.relpath(d_path, src_root)
                        valid_dirs.append({
                            'full_path': d_path,
                            'rel_path': rel_path,
                            'parent_root': src_root
                        })
                except OSError as e:
                    logger.error(f"Error accessing {d_path}: {e}")
                    continue
            
            # Prevent scanning inside .d folders
            dirs[:] = [d for d in dirs if not d.endswith('.d')]
            
    return valid_dirs


def build_transfers(valid_dirs, config, logger, dry_run):
    """
    Takes the list of valid directories and sorts them into 3 transfers.
    """
    
    # Config loading
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

    # Initialize transfers
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

    # Clean staging
    os.makedirs(STAGING_DIR, exist_ok=True)
    cleanup_old_tarballs(STAGING_DIR)

    counts = {'general': 0, 'project_raw': 0, 'project_tape': 0}

    for item in valid_dirs:
        full_path = item['full_path']
        rel_path = item['rel_path']
        
        # Normalize paths for Globus (POSIX)
        rel_path_posix = rel_path.replace(os.sep, '/')
        
        g_source_path = posixpath.join(GLOBUS_SRC_BASE, rel_path_posix)

        # Add to general transfer
        g_dest_general = posixpath.join(GEN_DEST_ROOT, rel_path_posix)
        if dry_run:
            logger.info(f"[DRY RUN] General: {rel_path} -> {g_dest_general}")
        else:
            t_general.add_item(g_source_path, g_dest_general, recursive=True)
        counts['general'] += 1

        # Check project filter
        if PROJECT_KEYWORD in full_path.lower():
            
            # Add to project raw transfer
            g_dest_proj = posixpath.join(PROJ_DEST_ROOT, rel_path_posix)
            if dry_run:
                logger.info(f"[DRY RUN] Project Raw: {rel_path} -> {g_dest_proj}")
            else:
                t_project_raw.add_item(g_source_path, g_dest_proj, recursive=True)
            counts['project_raw'] += 1

            # Tar and add to project Fortress transfer
            safe_tar_name = rel_path.replace(os.sep, '_') + '.tar'
            local_tar_path = os.path.join(STAGING_DIR, safe_tar_name)
            
            g_dest_tape = posixpath.join(TAPE_ROOT, f"{rel_path_posix}.tar")
            g_source_tape = posixpath.join(GLOBUS_STAGING, safe_tar_name)

            if dry_run:
                logger.info(f"[DRY RUN] Project Tape: {rel_path} -> {g_dest_tape}")
            else:
                logger.info(f"Tarring {os.path.basename(full_path)} for tape...")
                make_tarfile(local_tar_path, full_path)
                t_project_tape.add_item(g_source_tape, g_dest_tape)
            counts['project_tape'] += 1

    return (t_general, t_project_raw, t_project_tape), counts

# Main execution 
def main():
    lock = LockFile()
    try:
        lock.acquire()
    except RuntimeError as e:
        print(e)
        return

    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Scan without submitting transfer.")
    parser.add_argument("--config", default="config_widhalm.ini", help="Path to config file.")
    parser.add_argument("--backfill", action="store_true", help="Transfer historical data from June 1, 2025 up to yesterday.")
    args = parser.parse_args()

    logger = setup_logging()
    
    if not os.path.exists(args.config):
        logger.error(f"Configuration file '{args.config}' not found.")
        return

    config = configparser.ConfigParser()
    config.read(args.config)

    # Helper to safely get config values
    try:
        source_roots_str = config['paths']['SOURCE_ROOTS']
        SOURCE_ROOTS = [s for s in source_roots_str.split(',') if s.strip()]
        
        SERVICE_NAME = config['keyring']['service_name']
        CLIENT_ID    = config['globus']['client_id']
    except KeyError as e:
        logger.error(f"Missing config key: {e}")
        return

    yesterday    = datetime.now().date() - timedelta(days=1)
    
    # Determine the start window: Backfill limit vs Normal Nightly
    if args.backfill:
        start_window = datetime(2025, 6, 1) 
        logger.info(f"Starting historical backfill sync from {start_window.date()} up to {yesterday}")
    else:
        start_window = datetime.combine(yesterday, time.min)
        logger.info(f"Starting Nightly Sync for {yesterday}")
        
    end_window   = datetime.combine(yesterday, time.max)

    # Scan phase
    valid_dirs = scan_for_directories(SOURCE_ROOTS, start_window, end_window, logger)
    
    if not valid_dirs:
        logger.info("No modified '.d' directories found. Exiting.")
        return

    # Build transfer objects
    transfers, counts = build_transfers(valid_dirs, config, logger, args.dry_run)
    t_general, t_proj_raw, t_proj_tape = transfers

    logger.info(f"Queue Summary: General={counts['general']} | Project Raw={counts['project_raw']} | Tape={counts['project_tape']}")

    if args.dry_run:
        logger.info("Dry run complete. No transfers submitted.")
        return

    try:
        authorizer = get_authorizer(SERVICE_NAME, CLIENT_ID)
        tc = globus_sdk.TransferClient(authorizer=authorizer)
    except Exception as e:
        logger.error(f"Auth failed: {e}")
        return

    # Submit transfers
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