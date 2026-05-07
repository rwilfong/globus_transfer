# Scripts Directory

The main motivation behind these scripts was to develop a transfer script for instrument computers. 

## General Prerequisites 
All of the transfer scripts use Globus' SDK, please ensure it is installed: 
`pip install globus_sdk`.

### Configuration
1. **Environment Variables** (`.env`)

    Create a `.env` file in the same directory as your script to store the Globus confidential client secret.

    ```
    GLOBUS_CLIENT_SECRET=your_secret_key
    ```

2. **Configuration File** (`config.ini` or similar)

    The script expects a `config.ini` file to define endpoints, paths, and the client ID. This prevents hardcoding directly into the script. Structure is as follows: 

    ```
    [globus]
    # The Client ID for your Globus App
    client_id = your-globus-client-id-here

    # Endpoint UUIDs
    SOURCE_ENDPOINT_ID = your-source-endpoint-uuid-here
    GENERAL_DEST_ENDPOINT_ID = your-destination-endpoint-uuid-here

    # The base directory on the destination endpoint where files will land
    # The script will append the month_year folder and relative paths to this
    GENERAL_DEST_PATH = /path/on/destination/endpoint/

    [paths]
    # Comma-separated list of local directories to scan for .d files
    SOURCE_ROOTS = /local/path/to/metabolomics/data, /another/local/path/if/needed

    # The base path on the Globus source endpoint that corresponds to your local SOURCE_ROOTS
    # This differs just a bit but overall works 
    GLOBUS_SOURCE_ROOT = /path/on/source/endpoint/
    ```
3. **Globus Credentials**

    Because these scripts utilize Globus confidential clients, users must create guest collections and give the confidential clients permission to read and write to the collections. Please see the `../walkthroughs` directory for information on creating guest collections and assigning permissions.

## Usage
These scripts are designed to be ran as a nightly cron job, but can also be executed manually if needed. 

The `sync_script.bat` file is the Windows batch file used for running these

### To-Do List:
- Dry run features
- Incorporate Globus Search
- Incorporate metadata parsing 

## Recurring Jobs
### Windows Task Scheduler (`sync_script.bat`)
This Windows batch script serves as an execution wrapper for the nightly bacup sequence. I've notied a tendency in Globus Connect Personal to stop running or to fail restarting after a computer update which caused an issue with robust transfers.

This script handles environment management by invoking the Python executable directly from a designated Conda environment and logs all activites to dynamically generated daily log files for auditing and traceback. 

The returned logs will be formated as: `batch_log_YYYY-MM-DD.txt`.

#### Configuration Requirements
Before deploying the script, the variable block at the top must reflect your local system. Update the following variables: 
- `GLOBUS_PATH`: The absolute path to your GCP executable (`globus_connect_personal.exe`)

- `CONDA_ENV`: The name of the Conda environment containing the Globus SDK and Python dependencies (default: `globus_env`)
- `ENV_PYTHON`: The absolute path to your Anaconda/Miniconda installation's environment directory
- `PYTHON_SCRIPT`: The absolute path to the actual backup script
- `CONFIG_FILE`: The absolute path to the `*.ini` configuraiton file required by the script
- `LOG_DIR`: The directory where the daily batch logs will be saved


## `metabolomics/` directory 
### `lookback_transfer.py`
This script automates the nightly transfer of raw metabolomics data using Globus SDK. It's a simplified version of `lookback_transfer_filter.py` designed without specific file filtering logic. It scans predefined source directories for instrument data folders (ending in `.d`) modified within the last 7 days and syncs them to a general destination endpoint, organizing them into `MM_YYYY` folders based on their modification date. To ensure a robust and secure execution, the script authenticates using a `.env` file rather than a keyring. 

Once in these directories, the next step is to create monthly transfers. 

**Main Features**
- *7-Day Lookback Window*: automatically scans and identifies `.d` directories that have been modified in the past 7 days. The main reason for this implementation was in case files were skipped or the destination filesystem was down on the date(s) of the transfer. 

- *Automated Date Grouping*: dynamically builds destination paths, sorting transferred folders into `MM_YYYY` directories based on when the files were actually modified. 

- *Backlog System*: utilizes a local `transfer_backlog.json` file to queue transfers. If a Globus submission fails, the pending items are saved and retried during the next run. 

- *Concurrency Protection*: implements a Process ID (PID) lockfile (`nightly_sync.lock`) to prevent multiple instances of the script running simultaneously. 


### `lookback_transfer_filter.py`
This script is almost the same as the `lookback_transfer.py` script, but it incorporates a keyword filtering and a few extra directory transfers. The objective for this script was to move the raw data and also perform a nightly transfer for a PI for immediate access of raw data. 

This script will transfer all of the raw files to a general destination (`GENERAL_DEST_*`), and then identified filtered data to two separate locations, one on disk (`PROJECT_DEST_*`) and the other long-term tape storage (`TAPE_*`).

Because the final destination is tape archive, the small files will need to be combined in a tarball (`*.tar`) for transfer, making the `*_TAR_STAGING` directories necessary for intermediate local storage. In the case of metabolomics data, each sample is stored in a `*.d` folder and will become a `*.d.tar` on tape. 

The `config.ini` changes a bit too with these new parameters:

```
[globus]
# Your Globus App Client ID
client_id = your_client_id_here

# Source Endpoint (Where the data is coming from)
SOURCE_ENDPOINT_ID = your_source_endpoint_uuid

# General Destination (Where all data goes by default)
GENERAL_DEST_ENDPOINT_ID = your_general_dest_endpoint_uuid
GENERAL_DEST_PATH = /path/to/general/destination/root/

# Project Specific Destination (Where filtered 'keyword' data goes)
PROJECT_DEST_ENDPOINT_ID = your_project_dest_endpoint_uuid
PROJECT_DEST_PATH = /path/to/project/destination/root/

# Tape Archive Destination (Where tarballs of the project data go)
TAPE_ENDPOINT_ID = your_tape_endpoint_uuid
TAPE_BASE_PATH = /path/to/tape/archive/root/


[paths]
# The absolute path on the Globus source endpoint pointing to the data
GLOBUS_SOURCE_ROOT = /path/to/globus/source/root/

# The local paths on the Windows/Linux machine to scan for .d folders (comma-separated)
SOURCE_ROOTS = C:\path\to\instrument\data, D:\another\instrument\data

# Directory on the local machine where temporary .tar files will be generated
LOCAL_TAR_STAGING = C:\nucleus_backup\staging\tar_files

# The exact Globus path corresponding to the LOCAL_TAR_STAGING directory 
# (This allows Globus to find the local tarballs to move them to the Tape endpoint)
GLOBUS_TAR_STAGING = /path/to/globus/staging/tar_files/


[settings]
# The keyword used to filter directories for the project-specific and tape transfers
# If omitted, the script defaults to 'widhalm'
PROJECT_KEYWORD = widhalm
```