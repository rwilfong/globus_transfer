@echo off 

REM Find conda 
call "C:\ProgramData\anaconda3\Scripts\activate.bat"

:: 2. Activate your specific environment
call conda activate your_env_name

REM 
echo [%date% %time%] Starting Monthly Catchup in Conda environment... >> batch_log.txt
python monthly_catchup.py --config config_nightly.ini >> batch_log.txt 2>&1

echo [%date% %time%] Process completed. >> batch_log.txt
popd