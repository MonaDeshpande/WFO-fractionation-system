@echo off
:: scada_data_processor.bat
:: This batch file will set up and run your data cleaning script.
:: It automatically creates a virtual environment if one doesn't exist.

:: --- CONFIGURATION (UPDATE THESE PATHS) ---
:: The name of the virtual environment to create/use.
SET "VENV_NAME=scada_venv"

:: The full directory path where your script and the venv will be located.
:: Example: SET "SCRIPT_DIR=C:\Users\YourUser\Documents\SCADA_Scripts"
SET "SCRIPT_DIR=C:\Path\To\Your\Script"

:: The name of your Python script file.
SET "SCRIPT_NAME=data_cleaning_logic.py"

:: The directory where you want to save the log file.
SET "LOG_DIR=%SCRIPT_DIR%\logs"

:: --- EXECUTION ---
echo.
echo =========================================================
echo === Preparing and Starting SCADA Data Processing Loop ===
echo =========================================================
echo Start Time: %date% %time%
echo.

:: 1. Navigate to the script's directory
cd /d "%SCRIPT_DIR%"

:: 2. Create the virtual environment if it doesn't exist
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

:: 3. Activate the virtual environment and install required libraries
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

:: 4. Prepare for script execution
echo.
echo Running Python script: "%SCRIPT_NAME%"...
if not exist "%LOG_DIR%" mkdir "%LOG_DIR%"
SET "LOG_FILE_NAME=scada_process_log_%date:~10,4%-%date:~4,2%-%date:~7,2%_%time:~0,2%-%time:~3,2%-%time:~6,2%.log"
SET "LOG_FILE=%LOG_DIR%\%LOG_FILE_NAME%"

echo Log file will be created at: "%LOG_FILE%"
echo The script will now run in a new window. Press Ctrl+C in that window to stop it.

:: 5. Launch the script in a new window
start "SCADA Processor" cmd /k "python "%SCRIPT_NAME%" > "%LOG_FILE%" 2>&1"

echo.
echo Script launched in a new window.
echo You can close this window now.
echo.
pause