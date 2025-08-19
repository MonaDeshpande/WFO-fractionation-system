@echo off
REM --- SETTINGS ---
SET SCRIPT_NAME=advanced_complete_system_analysis_01.py
SET VENV_PATH=H:\SCADA_DATA_ANALYSIS\GENERATING_DATA\venv
REM ----------------

ECHO.
ECHO =======================================================
ECHO     Starting Advanced Distillation Analysis System
ECHO =======================================================
ECHO.

REM --- Step 1: Activate Virtual Environment ---
ECHO Checking for virtual environment...
IF EXIST "%VENV_PATH%\Scripts\activate.bat" (
    ECHO Activating virtual environment at "%VENV_PATH%"...
    CALL "%VENV_PATH%\Scripts\activate.bat"
    IF %ERRORLEVEL% NEQ 0 (
        ECHO ERROR: Failed to activate virtual environment.
        GOTO :end
    )
) ELSE (
    ECHO WARNING: Virtual environment not found. Using system Python.
    ECHO This may lead to dependency conflicts.
)

REM --- Step 2: Check & Install Python Libraries ---
ECHO.
ECHO Checking for required Python libraries...

REM Define required packages, including the ones that were missing
SET "PACKAGES=psycopg2-binary pandas matplotlib python-docx openpyxl seaborn scikit-learn statsmodels"

REM Check each package individually
FOR %%P IN (%PACKAGES%) DO (
    pip show %%P >nul 2>nul
    IF %ERRORLEVEL% NEQ 0 (
        ECHO %%P is not installed. Installing...
        pip install %%P
        IF %ERRORLEVEL% NEQ 0 (
            ECHO ERROR: Failed to install %%P. Please check your internet connection or permissions.
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
    ECHO Please review the script's output for details.
) ELSE (
    ECHO.
    ECHO Analysis complete. The report has been generated.
)

:end
ECHO.
ECHO Press any key to exit...
PAUSE > nul