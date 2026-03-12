@echo off
setlocal EnableDelayedExpansion

REM ============================================================
REM  Configuration 
REM ============================================================
set GLOBUS_PATH=C:\Globus Connect Personal\GlobusConnectPersonal.exe
set CONDA_ACTIVATE=C:\Users\User\miniconda3\Scripts\activate.bat
set CONDA_ENV=globus_env
set PYTHON_SCRIPT=C:\nucleus_backup\filtering\initial_nightly_filter.py
set CONFIG_FILE=C:\nucleus_backup\filtering\config_filter.ini
set LOG_FILE=C:\nucleus_backup\filtering\batch_log.txt

REM Max seconds to wait for Globus to start (5s per loop iteration)
set GLOBUS_WAIT_MAX=60
REM ============================================================

call :LOG "Starting Nightly Catchup"

REM Step 1: Ensure GCP is running 
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

    start "" "%GLOBUS_PATH%"
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

REM Step 2: Activate Conda and run Python script!
call :LOG "Activating Conda environment: %CONDA_ENV%"

if not exist "%CONDA_ACTIVATE%" (
    call :LOG "ERROR: Conda activate script not found at: %CONDA_ACTIVATE%"
    call :LOG "Aborting."
    goto :END
)

call "%CONDA_ACTIVATE%" %CONDA_ENV%
if %ERRORLEVEL% NEQ 0 (
    call :LOG "ERROR: Failed to activate Conda environment '%CONDA_ENV%'. Exit code: %ERRORLEVEL%"
    goto :END
)

call :LOG "Running Python script..."

if not exist "%PYTHON_SCRIPT%" (
    call :LOG "ERROR: Python script not found at: %PYTHON_SCRIPT%"
    goto :DEACTIVATE
)

python "%PYTHON_SCRIPT%" --config "%CONFIG_FILE%" >> "%LOG_FILE%" 2>&1
set PYTHON_EXIT=%ERRORLEVEL%

if %PYTHON_EXIT% EQU 0 (
    call :LOG "Python script completed successfully."
) else (
    call :LOG "ERROR: Python script exited with code %PYTHON_EXIT%."
)

:DEACTIVATE
call conda deactivate
call :LOG "Conda environment deactivated."

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