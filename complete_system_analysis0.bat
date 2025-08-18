@echo off
REM --- SETTINGS ---
SET SCRIPT_NAME=complete_system_analysis.py
SET VENV_PATH=C:\path\to\your\venv
REM ----------------

REM Activate virtual environment
IF EXIST "%VENV_PATH%\Scripts\activate.bat" (
    ECHO Activating virtual environment...
    CALL "%VENV_PATH%\Scripts\activate.bat"
) ELSE (
    ECHO Virtual environment not found. Skipping activation.
)

REM Install required Python libraries
ECHO Installing required Python libraries...
pip install psycopg2-binary pandas matplotlib python-docx

REM Run the Python script
ECHO Running the data analysis script...
python "%SCRIPT_NAME%"

PAUSE