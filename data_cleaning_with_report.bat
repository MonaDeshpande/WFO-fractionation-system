@echo off
:: data_cleaning_report.bat
:: This batch file sets up and runs the SCADA data cleaning and reporting script for a specific date range.

:: --- CONFIGURATION (UPDATE THESE PATHS) ---
:: The name of the virtual environment to create/use.
SET "VENV_NAME=scada_venv"

:: The full directory path where your script and the venv will be located.
:: Example: SET "SCRIPT_DIR=C:\Users\YourUser\Documents\SCADA_Scripts"
SET "SCRIPT_DIR=C:\Path\To\Your\Script"

:: The name of your Python script file.
SET "SCRIPT_NAME=data_cleaning_report.py"

:: The directory where you want to save the log and Excel report files.
SET "REPORT_DIR=%SCRIPT_DIR%\reports"

:: --- EXECUTION ---
echo.
echo =========================================================
echo === Preparing and Starting SCADA Data Reporting Process ===
echo =========================================================
echo Start Time: %date% %time%
echo.

:: 1. Navigate to the script's directory.
cd /d "%SCRIPT_DIR%"

:: 2. Create the virtual environment if it doesn't exist.
echo Checking for virtual environment...
if not exist "%VENV_NAME%" (
    echo Virtual environment not found. Creating a new one...
    python -m venv "%VENV_NAME%"
    IF %ERRORLEVEL% NEQ 0 (
        echo ❌ ERROR: Failed to create the virtual environment. Ensure Python is in your system's PATH.
        pause
        goto :EOF
    )
    echo ✅ Virtual environment created.
) else (
    echo ✅ Virtual environment already exists.
)

:: 3. Activate the virtual environment and install required libraries.
echo Activating virtual environment and installing dependencies...
call "%VENV_NAME%\Scripts\activate.bat"
pip install psycopg2 pandas openpyxl
IF %ERRORLEVEL% NEQ 0 (
    echo.
    echo ❌ ERROR: Failed to install one or more required Python libraries.
    echo Check your internet connection.
    deactivate
    pause
    goto :EOF
)
echo ✅ Required libraries installed.

:: 4. Prepare for script execution.
echo.
echo Running Python script: "%SCRIPT_NAME%"...
if not exist "%REPORT_DIR%" mkdir "%REPORT_DIR%"

:: Log file name with a unique timestamp.
SET "LOG_FILE_NAME=scada_report_log_%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%.log"
SET "LOG_FILE=%REPORT_DIR%\%LOG_FILE_NAME%"

:: 5. Execute the Python script.
python "%SCRIPT_NAME%" > "%LOG_FILE%" 2>&1

:: 6. Deactivate the virtual environment.
deactivate

:: 7. Check the exit code and report.
echo.
IF %ERRORLEVEL% NEQ 0 (
    echo ❌ ERROR: The Python script encountered an error. Check the log file for details.
) ELSE (
    echo ✅ SUCCESS: The Python script ran successfully.
)

:: --- REPORTING ---
echo.
echo =========================================================
echo === SCADA Data Reporting Process Complete ===
echo =========================================================
echo End Time: %date% %time%
echo Log file created at: "%LOG_FILE%"
echo.
echo --- Log Summary (last 10 lines) ---
type "%LOG_FILE%" | tail -n 10
echo -----------------------------------

echo.
echo Process complete. Press any key to exit.
pause