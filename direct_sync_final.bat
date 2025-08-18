@echo off
rem This batch file runs the data synchronization script.

rem Path to your Python executable inside the virtual environment (venv)
rem Assuming your venv is named 'venv' in the same directory as the script.
set PYTHON_PATH="%~dp0venv\Scripts\python.exe"

rem Path to your Python script
set SCRIPT_PATH="%~dp0direct_sync_final.py"

rem Check if the Python executable exists
if not exist %PYTHON_PATH% (
    echo Error: Python executable not found at %PYTHON_PATH%
    echo Make sure you have created and activated your virtual environment.
    echo To fix this, open a terminal, navigate to this folder, and run:
    echo 1. python -m venv venv
    echo 2. .\venv\Scripts\activate
    echo 3. pip install psycopg2-binary pyodbc
    echo After installing, try running this batch file again.
    pause
    exit /b 1
)

echo Starting the data synchronization script...
%PYTHON_PATH% %SCRIPT_PATH%

echo Script finished. Press any key to exit...
pause