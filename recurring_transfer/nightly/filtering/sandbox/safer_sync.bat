@echo off
setlocal EnableDelayedExpansion

REM Trying to figure out how to make a robust backup script. 
REM Occasionally GCP stops working on the computers 

REM ============================================================
REM  Configuration 
REM ============================================================
set GLOBUS_PATH=C:\Globus Connect Personal\GlobusConnectPersonal.exe
set CONDA_ACTIVATE=C:\Users\User\miniconda3\Scripts\activate.bat
set CONDA_ENV=globus_env
set PYTHON_SCRIPT=C:\nucleus_backup\filtering\initial_nightly_filter.py
set CONFIG_FILE=C:\nucleus_backup\filtering\config_filter.ini

REM Log file variables
set LOG_DIR=C:\nucleus_backup\filtering
set LOG_BASE=batch_log.txt
set LOG_FILE=%LOG_DIR%\%LOG_BASE%

REM Max seconds to wait for Globus to start (5s per loop iteration)
set GLOBUS_WAIT_MAX=60

REM ============================================================
REM Log Rotation (Keeps the last 5 runs)
REM ============================================================
if exist "%LOG_FILE%.5" del "%LOG_FILE%.5"
if exist "%LOG_FILE%.4" ren "%LOG_FILE%.4" "%LOG_BASE%.5"
if exist "%LOG_FILE%.3" ren "%LOG_FILE%.3" "%LOG_BASE%.4"
if exist "%LOG_FILE%.2" ren "%LOG_FILE%.2" "%LOG_BASE%.3"
if exist "%LOG_FILE%.1" ren "%LOG_FILE%.1" "%LOG_BASE%.2"
if exist "%LOG_FILE%" ren "%LOG_FILE%" "%LOG_BASE%.1"
REM ============================================================

call :LOG "Starting Nightly Backup"

REM Ensure GCP is running 
call :LOG "Checking if Globus Connect Personal is running..."

tasklist /FI "IMAGENAME eq GlobusConnectPersonal.exe" 2>NUL | find /I "GlobusConnectPersonal.exe" >NUL 2>&1
if %ERRORLEVEL% EQU 0 (
    call :LOG "Globus is already running."
) else (
    call :LOG "Globus is not running. Attempting to start..."

    if not exist "%GLOBUS_PATH%" (
        call :LOG "ERROR: Globus executable not found at: %GLOBUS_PATH%"
        call :LOG "Aborting."
        goto :END
    )

    start "" "%GLOBUS_PATH%" -start
    call :LOG "Globus launched. Waiting for it to become ready..."

    REM Loop-wait instead of a blind sleep
    set /a WAITED=0
    :GLOBUS_WAIT_LOOP
        timeout /t 5 /nobreak >NUL
        set /a WAITED+=5
        tasklist /FI "IMAGENAME eq GlobusConnectPersonal.exe" 2>NUL | find /I "GlobusConnectPersonal.exe" >NUL 2>&1
        if %ERRORLEVEL% EQU 0 (
            call :LOG "Globus is now running after !WAITED! seconds."
            goto :GLOBUS_READY
        )
        if !WAITED! GEQ %GLOBUS_WAIT_MAX% (
            call :LOG "ERROR: Globus did not start within %GLOBUS_WAIT_MAX% seconds. Aborting."
            goto :END
        )
    goto :GLOBUS_WAIT_LOOP
    :GLOBUS_READY

    REM Extra settle time for the endpoint to authenticate
    call :LOG "Giving Globus 10 more seconds to authenticate endpoint..."
    timeout /t 10 /nobreak >NUL
)

REM Step 2: Run Python directly from the Conda environment
call :LOG "Running Python script via absolute environment path..."

REM Define the absolute path to the Python executable inside your specific environment
set ENV_PYTHON=C:\Users\User\miniconda3\envs\%CONDA_ENV%\python.exe

if not exist "%ENV_PYTHON%" (
    call :LOG "ERROR: Conda Python executable not found at: %ENV_PYTHON%"
    call :LOG "Aborting."
    goto :END
)

if not exist "%PYTHON_SCRIPT%" (
    call :LOG "ERROR: Python script not found at: %PYTHON_SCRIPT%"
    goto :END
)

REM Call the script directly using the environment's Python
call :LOG "Changing to working directory..."
cd /d "C:\nucleus_backup\filtering"

python "%PYTHON_SCRIPT%" --config "%CONFIG_FILE%" >> "%LOG_FILE%" 2>&1

set PYTHON_EXIT=%ERRORLEVEL%

if %PYTHON_EXIT% EQU 0 (
    call :LOG "Python script completed successfully."
) else (
    call :LOG "ERROR: Python script exited with code %PYTHON_EXIT%."
)

:END

call :LOG "Nightly Catchup finished."
call :LOG "========================================"
endlocal
exit /b %PYTHON_EXIT%

REM Logging subroutine 
:LOG
echo [%date% %time%] %~1
echo [%date% %time%] %~1 >> "%LOG_FILE%"
exit /b 0