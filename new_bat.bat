@echo off
setlocal EnableDelayedExpansion

REM ============================================================
REM  Configuration 
REM ============================================================
REM Define both possible Globus paths to handle naming variations
set GLOBUS_PATH_1=C:\Globus Connect Personal\GlobusConnectPersonal.exe
set GLOBUS_PATH_2=C:\Globus Connect Personal\globus_connect_personal.exe

set CONDA_ENV=globus_env
set PYTHON_SCRIPT=C:\nucleus_backup\filtering\initial_nightly_filter.py
set CONFIG_FILE=C:\nucleus_backup\filtering\config_filter.ini

REM Log file variables
set LOG_DIR=C:\nucleus_backup\filtering
set LOG_BASE=batch_log

REM Max seconds to wait for Globus to start (5s per loop iteration)
set GLOBUS_WAIT_MAX=60

REM ============================================================
REM  Log Generation & Cleanup
REM ============================================================
REM Get a reliable YYYYMMDD date stamp using PowerShell
for /f "tokens=*" %%a in ('powershell -Command "Get-Date -format 'yyyyMMdd'"') do set TIMESTAMP=%%a
set LOG_FILE=%LOG_DIR%\%LOG_BASE%_%TIMESTAMP%.txt

REM Delete log files older than 7 days to keep the folder clean
forfiles /p "%LOG_DIR%" /m %LOG_BASE%_*.txt /d -7 /c "cmd /c del @path" 2>NUL

call :LOG "========================================"
call :LOG "Starting Nightly Backup"

REM ============================================================
REM  Globus Check & Start
REM ============================================================
call :LOG "Checking if Globus Connect Personal is running..."

REM Use findstr to check for either executable name
tasklist 2>NUL | findstr /I /C:"GlobusConnectPersonal.exe" /C:"globus_connect_personal.exe" >NUL 2>&1
if %ERRORLEVEL% EQU 0 (
    call :LOG "Globus is already running."
    goto :GLOBUS_READY
)

REM If we reach here, Globus is NOT running. Determine which path to use.
if exist "%GLOBUS_PATH_1%" (
    set "ACTIVE_GLOBUS_PATH=%GLOBUS_PATH_1%"
) else if exist "%GLOBUS_PATH_2%" (
    set "ACTIVE_GLOBUS_PATH=%GLOBUS_PATH_2%"
) else (
    call :LOG "ERROR: Neither Globus executable was found. Aborting."
    goto :END
)

call :LOG "Globus is not running. Attempting to start %ACTIVE_GLOBUS_PATH%..."
start "" "%ACTIVE_GLOBUS_PATH%" -start
call :LOG "Globus launched. Waiting for it to become ready..."

REM Loop-wait instead of a blind sleep
set /a WAITED=0

:GLOBUS_WAIT_LOOP
timeout /t 5 /nobreak >NUL
set /a WAITED+=5

tasklist 2>NUL | findstr /I /C:"GlobusConnectPersonal.exe" /C:"globus_connect_personal.exe" >NUL 2>&1
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
REM ============================================================
REM  Python Execution
REM ============================================================
call :LOG "Running Python script via absolute environment path..."

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

call :LOG "Changing to working directory..."
cd /d "C:\nucleus_backup\filtering"

"%ENV_PYTHON%" "%PYTHON_SCRIPT%" --config "%CONFIG_FILE%" >> "%LOG_FILE%" 2>&1
set PYTHON_EXIT=%ERRORLEVEL%

if %PYTHON_EXIT% EQU 0 (
    call :LOG "Python script completed successfully."
) else (
    call :LOG "ERROR: Python script exited with code %PYTHON_EXIT%."
)

:END
call :LOG "Nightly Catchup finished."
exit /b %PYTHON_EXIT%

REM ============================================================
REM  Logging Subroutine
REM ============================================================
:LOG
echo [%date% %time%] %~1
echo [%date% %time%] %~1 >> "%LOG_FILE%"
exit /b 0