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
set LOG_BASE=batch_log
set PYTHON_EXIT=1

REM Call PowerShell to get a safe, consistent YYYY-MM-DD date string
for /f %%a in ('powershell -Command "Get-Date -Format 'yyyy-MM-dd'"') do set SAFE_DATE=%%a

REM Construct the new dynamic log filename
set LOG_FILE=%LOG_DIR%\%LOG_BASE%_%SAFE_DATE%.txt

REM Max seconds to wait for Globus to start (5s per loop iteration)
set GLOBUS_WAIT_MAX=60

REM Begin 
call :LOG "Starting Nightly Backup"

REM Ensure GCP is running 
call :LOG "Checking if Globus Connect Personal is running..."

REM Just dumps all tasks and looks for "globus"
tasklist | find /I "globus" >NUL 2>&1
if !ERRORLEVEL! EQU 0 (
    call :LOG "Globus is already running."
    goto :GLOBUS_READY
)

REM If we reach here, Globus is NOT running
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

REM Just dumps all tasks and looks for "globus"
tasklist | find /I "globus" >NUL 2>&1

REM We can safely use %ERRORLEVEL% here because we are outside a parenthesized block
if %ERRORLEVEL% EQU 0 (
    call :LOG "Globus is now running after !WAITED! seconds."
    goto :GLOBUS_READY_DELAY
)

if !WAITED! GEQ %GLOBUS_WAIT_MAX% (
    call :LOG "ERROR: Globus did not start within %GLOBUS_WAIT_MAX% seconds. Aborting."
    goto :END
)
goto :GLOBUS_WAIT_LOOP

:GLOBUS_READY_DELAY
REM Extra settle time for the endpoint to authenticate 
call :LOG "Giving Globus 10 more seconds to authenticate endpoint..."
timeout /t 10 /nobreak >NUL

:GLOBUS_READY
REM Run Python directly from the Conda environment
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
cd /d "C:\nucleus_backup\filtering" || (
    call :LOG "ERROR: Failed to change directory."
    goto :END
)

"%ENV_PYTHON%" "%PYTHON_SCRIPT%" --config "%CONFIG_FILE%" >> "%LOG_FILE%" 2>&1

set PYTHON_EXIT=%ERRORLEVEL%

if %PYTHON_EXIT% EQU 0 (
    call :LOG "Python script completed successfully."
) else (
    call :LOG "ERROR: Python script exited with code %PYTHON_EXIT%."
)

:END
call :LOG "Nightly Catchup finished."
call :LOG "========================================"
exit /b %PYTHON_EXIT%

REM Logging subroutine 
:LOG
echo [%date% %time%] %~1
echo [%date% %time%] %~1 >> "%LOG_FILE%"
exit /b 0