@echo off
rem ===========================================================================
rem == All-in-One Data Sync Launcher for Non-Technical Users
rem == This script will set up the environment and run your Python script.
rem ===========================================================================

set SCRIPT_PATH="direct_sync_final.py"
set VENV_PATH="%~dp0venv"
set PYTHON_EXE="%VENV_PATH%\Scripts\python.exe"

echo.
echo === Step 1: Checking for Python Installation... ===
echo.
rem Check if a Python executable is found
where python >nul 2>nul
if %errorlevel% neq 0 (
    echo ❌ ERROR: Python is not installed or not in your system's PATH.
    echo ❌ Please install Python from python.org and try again.
    pause
    exit /b 1
)

echo ✅ Python installation found.

echo.
echo === Step 2: Checking for and creating the Virtual Environment... ===
echo.
if not exist %PYTHON_EXE% (
    echo ℹ️ Virtual environment not found. Creating a new one now...
    python -m venv "%VENV_PATH%"
    if %errorlevel% neq 0 (
        echo ❌ FAILED to create the virtual environment.
        echo ❌ This might be due to a permissions issue. Try running this file as Administrator.
        pause
        exit /b 1
    )
    echo ✅ Virtual environment created successfully.
) else (
    echo ✅ Virtual environment already exists.
)

echo.
echo === Step 3: Installing required libraries (pyodbc, psycopg2)... ===
echo.
rem This will install the libraries into the venv, bypassing any system conflicts.
"%PYTHON_EXE%" -m pip install pyodbc psycopg2-binary
if %errorlevel% neq 0 (
    echo ❌ FAILED to install required libraries.
    echo ❌ Check your internet connection or the spelling of the packages.
    pause
    exit /b 1
)
echo ✅ Libraries installed successfully.

echo.
echo === Step 4: Launching the Data Synchronization Script... ===
echo.
rem Launch the Python script using the venv's python executable
start "Data Sync Process" "%PYTHON_EXE%" "%SCRIPT_PATH%"

echo.
echo The data synchronization process is running in a new window.
echo You can close this window now.
echo.
exit /b 0