@echo off
:: scada_data_processor.bat
:: This batch file executes the SCADA data processing script in a continuous loop.

:: =========================================================
:: === CONFIGURATION (UPDATE THESE PATHS) ===
:: =========================================================
:: 1. SET PYTHON_EXE: Full path to your Python interpreter.
::    To find this, open Command Prompt and type "where python"
SET "PYTHON_EXE=C:\Users\YourUser\AppData\Local\Programs\Python\Python39\python.exe"

:: 2. SET SCRIPT_PATH: The name of your Python script file.
SET "SCRIPT_NAME=data_cleaning_logic.py"

:: 3. SET SCRIPT_DIR: The full directory path where your script is saved.
SET "SCRIPT_DIR=C:\Path\To\Your\Script"

:: 4. SET LOG_DIR: The directory where you want to save the log file.
SET "LOG_DIR=%SCRIPT_DIR%\logs"

:: =========================================================
:: === SCRIPT EXECUTION ===
:: =========================================================
echo.
echo =========================================================
echo === Starting SCADA Data Processing Loop ===
echo =========================================================
echo Start Time: %date% %time%
echo.

:: Change the current directory to where the script is located.
cd /d "%SCRIPT_DIR%"

:: Ensure the log directory exists
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"

:: Log file name with a unique timestamp
SET "LOG_FILE_NAME=scada_process_log_%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%.log"
SET "LOG_FILE=%LOG_DIR%\%LOG_FILE_NAME%"

echo Log file will be created at: "%LOG_FILE%"
echo Press Ctrl+C to stop the process.

:: The `start` command runs the Python script in a new window,
:: which is useful for a continuous process.
start "%LOG_FILE%" cmd /k ""%PYTHON_EXE%" "%SCRIPT_NAME%" > "%LOG_FILE%" 2>&1"

echo.
echo The SCADA data processing script is now running in a new window.
echo You can check the log file for output.
echo.