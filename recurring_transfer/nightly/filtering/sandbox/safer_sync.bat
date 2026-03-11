@echo off
REM Set the path to Globus executable
set GLOBUS_PATH="C:\Globus Connect Personal\GlobusConnectPersonal.exe"

echo [%date% %time%] Starting Monthly Catchup... >> batch_log.txt

REM 1. Check if Globus Connect Personal is running
tasklist /FI "IMAGENAME eq GlobusConnectPersonal.exe" 2>NUL | find /I /N "GlobusConnectPersonal.exe">NUL
if "%ERRORLEVEL%"=="1" (
    echo [%date% %time%] Globus is down. Starting Globus Connect Personal... >> batch_log.txt
    
    REM The 'start ""' command launches it in the background so the batch script doesn't pause here forever
    start "" %GLOBUS_PATH%
    
    REM Give Globus 15 seconds to fully boot and connect to the cloud before transferring
    timeout /t 15 /nobreak > NUL
) else (
    echo [%date% %time%] Globus is already running. >> batch_log.txt
)

REM 2. Run the Python script in Conda
echo [%date% %time%] Activating Conda environment... >> batch_log.txt
call conda activate globus_env

python initial_nightly_filter.py --config config_filter.ini >> batch_log.txt 2>&1

echo [%date% %time%] Process completed. >> batch_log.txt
call conda deactivate