@echo off
rem This batch file runs the data synchronization script.

rem This activates the virtual environment without using the .ps1 script
set "PATH=%CD%\venv\Scripts;%PATH%"

rem Path to your Python script
set SCRIPT_PATH="%~dp0direct_sync_final.py"

rem Check if the Python executable exists
if not exist "%~dp0venv\Scripts\python.exe" (
    echo Error: Python executable not found in the virtual environment.
    echo Please ensure the venv is created and the python.exe file exists.
    pause
    exit /b 1
)

echo Starting the data synchronization script...
python %SCRIPT_PATH%

echo Script finished. Press any key to exit...
pause