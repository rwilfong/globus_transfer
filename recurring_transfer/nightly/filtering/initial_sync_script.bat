@echo off 

REM Find conda and activate it 
call "C:\ProgramData\anaconda3\Scripts\activate.bat"

REM Activate conda environment
call conda activate globus_env

REM Run the script 
echo [%date% %time%] Starting Monthly Catchup in Conda environment... >> batch_log.txt
python initial_nightly_filter.py --config config_filter.ini --backfill --dry-run >> batch_log.txt 2>&1

echo [%date% %time%] Process completed. >> batch_log.txt
conda deactivate 
