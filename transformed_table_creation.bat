@echo off
set SCRIPT_NAME=transformed_table_creation.py

REM --- 1. Check if a virtual environment exists. If not, create one. ---
if not exist "venv\Scripts\activate.bat" (
    echo Creating virtual environment...
    py -m venv venv
    if %errorlevel% neq 0 (
        echo Error: Failed to create virtual environment. Make sure Python is in your PATH.
        goto :end
    )
    echo Virtual environment created.
)

REM --- 2. Activate the virtual environment ---
echo Activating virtual environment...
call venv\Scripts\activate.bat
if %errorlevel% neq 0 (
    echo Error: Failed to activate virtual environment.
    goto :end
)
echo Virtual environment activated.

REM --- 3. Install required packages (psycopg2-binary is easier for Windows) ---
echo Installing required Python packages...
pip install psycopg2-binary
if %errorlevel% neq 0 (
    echo Error: Failed to install packages.
    goto :end
)
echo Packages installed.

REM --- 4. Run the Python script.
REM    IMPORTANT: Please update the script name if it's different. ---
echo Starting the SCADA data processor...
python "%SCRIPT_NAME%"

:end
echo.
echo Operation complete. The script might still be running in the background.
echo Press any key to exit this window...
pause >nul
