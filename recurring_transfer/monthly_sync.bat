@echo off 

REM Find conda 
call "C:\ProgramData\anaconda3\Scripts\activate.bat"

REM Activate conda environment
call conda activate your_env_name

REM Run the script 
echo [%date% %time%] Starting Monthly Catchup in Conda environment... >> batch_log.txt
python monthly_catchup.py --config config.ini --no-dry-run >> batch_log.txt 2>&1

echo [%date% %time%] Process completed. >> batch_log.txt
conda deactivate 

REM Script for current month:
python current_month_sync.py --config config.ini --no-dry-run >> batch_log.txt 2>&1
