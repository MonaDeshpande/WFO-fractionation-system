@echo off
:: data_cleaning_with_report.bat

:: This batch file executes the SCADA data cleaning Python script.
:: It uses explicit paths to avoid "path not found" errors.

:: --- CONFIGURATION ---
:: 1. SET PYTHON_EXE: Replace this with the full path to your Python executable.
::    To find this, open Command Prompt and type "where python"
SET "PYTHON_EXE=C:\Users\YourUser\AppData\Local\Programs\Python\Python39\python.exe"

:: 2. SET SCRIPT_PATH: Replace this with the full path to your Python script.
SET "SCRIPT_PATH=C:\Path\To\Your\Script\data_cleaning_script.py"

:: 3. SET LOG_DIR: Replace with the directory where you want the log file to be saved.
SET "LOG_DIR=C:\Path\To\Your\Reports"

:: 4. SET LOG_FILE_NAME: This creates a unique log file name with a timestamp.
SET "LOG_FILE_NAME=data_cleaning_report_%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%.log"

:: --- SCRIPT EXECUTION ---
echo.
echo =========================================================
echo === Starting SCADA Data Cleaning Process ===
echo =========================================================
echo Start Time: %date% %time%
echo.

:: Execute the Python script and redirect all output to the log file.
echo Running Python script: "%SCRIPT_PATH%"...
"%PYTHON_EXE%" "%SCRIPT_PATH%" > "%LOG_DIR%\%LOG_FILE_NAME%" 2>&1

:: Check the exit code of the Python script.
IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ ERROR: The Python script encountered an error. Check the log file for details.
) ELSE (
    echo.
    echo ✅ SUCCESS: The Python script ran successfully.
)

:: --- REPORTING ---
echo.
echo =========================================================
echo === SCADA Data Cleaning Report ===
echo =========================================================
echo End Time: %date% %time%
echo Log file created at: "%LOG_DIR%\%LOG_FILE_NAME%"
echo.

:: Display a summary of the log file (last 10 lines).
echo --- Log Summary (last 10 lines) ---
type "%LOG_DIR%\%LOG_FILE_NAME%" | tail -n 10
echo -----------------------------------

echo.
echo Process complete.
pause