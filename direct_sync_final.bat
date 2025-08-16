@echo off
rem This batch file runs the data synchronization script.

rem Path to your Python executable
set PYTHON_PATH="C:\Program Files\Python310\python.exe"

rem Path to your Python script
set SCRIPT_PATH="direct_sync_final.py"

rem Check if the Python executable exists
if not exist %PYTHON_PATH% (
    echo Error: Python executable not found at %PYTHON_PATH%
    echo Please update the PYTHON_PATH variable in this batch file.
    pause
    exit /b 1
)

echo Starting the data synchronization script...
%PYTHON_PATH% %SCRIPT_PATH%

echo Script finished. Press any key to exit...
pause