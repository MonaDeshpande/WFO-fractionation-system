@echo off
echo Running SCADA Data Cleaning Script...
echo.

REM --- Check for Python installation ---
python --version >nul 2>nul
if %errorlevel% neq 0 (
    echo.
    echo ❌ ERROR: Python is not found. Please ensure Python is installed and added to your PATH.
    pause
    exit /b 1
)

REM --- Install required libraries if not present ---
echo Verifying required Python libraries...
python -m pip install psycopg2 pandas openpyxl xlsxwriter
echo.

REM --- Execute the Python script ---
python cleaner_01.py

echo.
echo Script execution finished.
pause