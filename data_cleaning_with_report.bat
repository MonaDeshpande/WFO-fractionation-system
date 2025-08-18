@echo off
:: scada_data_processor.bat
:: This batch file sets up and runs the SCADA data cleaning and reporting script.
:: It is designed for debugging immediate window closures.

:: --- CONFIGURATION (UPDATE THESE PATHS) ---
SET "VENV_NAME=scada_venv"

:: The FULL directory path where your script and the venv will be located.
:: Example: SET "SCRIPT_DIR=C:\Users\YourUser\Documents\SCADA_Scripts"
SET "SCRIPT_DIR=C:\Path\To\Your\Script"

:: The name of your Python script file.
SET "SCRIPT_NAME=data_cleaning_report.py"

:: --- DEBUGGING STEPS ---
echo.
echo =========================================================
echo === STARTING DEBUGGING MODE ===
echo =========================================================
echo Start Time: %date% %time%
echo.

:: 1. Navigate to the script's directory.
echo Attempting to navigate to script directory...
cd /d "%SCRIPT_DIR%"
IF %ERRORLEVEL% NEQ 0 (
    echo ❌ ERROR: Failed to change directory. The path "%SCRIPT_DIR%" is incorrect.
    echo Please check and correct the SCRIPT_DIR variable.
    pause
    goto :EOF
)
echo ✅ Directory changed successfully.
echo.

:: 2. Check for the Python script file.
echo Checking for script file "%SCRIPT_NAME%"...
if not exist "%SCRIPT_NAME%" (
    echo ❌ ERROR: The Python script "%SCRIPT_NAME%" was not found in the directory.
    echo Ensure the file name and path are correct.
    pause
    goto :EOF
)
echo ✅ Script file found.
echo.

:: 3. Create or use the virtual environment.
echo Checking for virtual environment...
if not exist "%VENV_NAME%" (
    echo Virtual environment not found. Creating one...
    python -m venv "%VENV_NAME%"
    IF %ERRORLEVEL% NEQ 0 (
        echo ❌ ERROR: Failed to create the virtual environment.
        echo Make sure Python is installed and added to your system's PATH.
        pause
        goto :EOF
    )
    echo ✅ Virtual environment created.
) else (
    echo ✅ Virtual environment already exists.
)
echo.

:: 4. Activate the virtual environment.
echo Activating virtual environment...
call "%VENV_NAME%\Scripts\activate.bat"
IF %ERRORLEVEL% NEQ 0 (
    echo ❌ ERROR: Failed to activate the virtual environment.
    pause
    goto :EOF
)
echo ✅ Environment activated.
echo.

:: 5. Install dependencies.
echo Installing required Python libraries...
pip install psycopg2 pandas openpyxl
IF %ERRORLEVEL% NEQ 0 (
    echo ❌ ERROR: Failed to install Python libraries.
    echo Check your internet connection.
    deactivate
    pause
    goto :EOF
)
echo ✅ All required libraries are installed.
echo.

:: 6. Run the Python script.
echo Attempting to run the Python script...
python "%SCRIPT_NAME%"
echo.
echo Script execution complete.
pause