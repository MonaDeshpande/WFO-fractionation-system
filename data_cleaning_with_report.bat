@echo off

:: data_cleaning_with_report.bat

:: This batch file executes the SCADA data cleaning Python script.
:: It logs the output and provides a simple report.

:: --- CONFIGURATION ---
SET PYTHON_EXE="C:\Users\YourUser\AppData\Local\Programs\Python\Python39\python.exe"
:: IMPORTANT: Replace the path above with the correct path to your Python interpreter.
SET SCRIPT_DIR=%~dp0
SET SCRIPT_NAME=data_cleaning_script.py
SET LOG_DIR=%SCRIPT_DIR%
SET LOG_FILE=%LOG_DIR%data_cleaning_report_%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%.log

:: --- SCRIPT EXECUTION ---
echo.
echo =========================================================
echo === Starting SCADA Data Cleaning Process ===
echo =========================================================
echo Start Time: %date% %time%
echo.

:: Change to the script's directory
cd "%SCRIPT_DIR%"

:: Execute the Python script and redirect output to a log file
:: 2>&1 redirects standard error to standard output
echo Running Python script: "%SCRIPT_NAME%"...
"%PYTHON_EXE%" "%SCRIPT_NAME%" > "%LOG_FILE%" 2>&1

:: Check the exit code of the Python script
IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ ERROR: The Python script encountered an error. Check the log file for details.
    echo.
) ELSE (
    echo.
    echo ✅ SUCCESS: The Python script ran successfully.
    echo.
)

:: --- REPORTING ---
echo.
echo =========================================================
echo === SCADA Data Cleaning Report ===
echo =========================================================
echo End Time: %date% %time%
echo Log file created at: "%LOG_FILE%"
echo.

:: Display a summary of the log file
echo --- Log Summary (last 10 lines) ---
tail -n 10 "%LOG_FILE%"
echo -----------------------------------

echo.
echo Process complete.
pause