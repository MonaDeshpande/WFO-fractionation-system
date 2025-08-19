@echo off
REM =======================================================
REM     Script to Run Advanced Distillation Analysis
REM =======================================================

REM --- SETTINGS ---
REM Set the name of your main Python script
SET SCRIPT_NAME=advanced_complete_system_analysis_01.py

REM Set the path to your virtual environment
REM This is crucial for managing dependencies
SET VENV_PATH=H:\SCADA_DATA_ANALYSIS\GENERATING_DATA\venv
REM ----------------

ECHO.
ECHO Starting the automated analysis process...
ECHO.

REM --- Step 1: Check and Activate Virtual Environment ---
ECHO Checking for virtual environment...
IF EXIST "%VENV_PATH%\Scripts\activate.bat" (
    ECHO Activating virtual environment...
    CALL "%VENV_PATH%\Scripts\activate.bat"
    IF %ERRORLEVEL% NEQ 0 (
        ECHO ERROR: Failed to activate virtual environment. Exiting.
        GOTO :end
    )
) ELSE (
    ECHO WARNING: Virtual environment not found at "%VENV_PATH%".
    ECHO Attempting to use system's default Python. This may cause dependency issues.
)

REM --- Step 2: Ensure All Required Python Libraries are Installed ---
ECHO.
ECHO Checking for required Python libraries...
ECHO.

REM List all necessary packages for your Python script
SET "PACKAGES=psycopg2-binary pandas numpy matplotlib python-docx openpyxl seaborn scikit-learn statsmodels"

REM Loop through each package and install if not found
FOR %%P IN (%PACKAGES%) DO (
    pip show %%P >nul 2>nul
    IF %ERRORLEVEL% NEQ 0 (
        ECHO %%P is not installed. Installing...
        pip install %%P
        IF %ERRORLEVEL% NEQ 0 (
            ECHO ERROR: Failed to install %%P. Check your internet connection or permissions.
            GOTO :end
        )
    ) ELSE (
        ECHO %%P is already installed. Skipping.
    )
)

REM --- Step 3: Run the Python Script ---
ECHO.
ECHO All dependencies are ready.
ECHO Running the main analysis script: "%SCRIPT_NAME%"...
python "%SCRIPT_NAME%"

IF %ERRORLEVEL% NEQ 0 (
    ECHO.
    ECHO ERROR: The Python script encountered an error.
    ECHO Please review the script's output above for details.
) ELSE (
    ECHO.
    ECHO Analysis complete. The report has been generated successfully.
)

:end
ECHO.
ECHO Press any key to exit...
PAUSE > nul