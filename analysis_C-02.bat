@echo off
rem This batch file checks for required Python libraries and runs the C-02 analysis script.

echo Checking for required Python libraries...

rem Check and install psycopg2-binary
pip show psycopg2-binary > nul 2> nul
if %errorlevel% neq 0 (
    echo psycopg2-binary is not installed. Installing now...
    pip install psycopg2-binary
)

rem Check and install pandas
pip show pandas > nul 2> nul
if %errorlevel% neq 0 (
    echo pandas is not installed. Installing now...
    pip install pandas
)

rem Check and install matplotlib
pip show matplotlib > nul 2> nul
if %errorlevel% neq 0 (
    echo matplotlib is not installed. Installing now...
    pip install matplotlib
)

rem Check and install python-docx
pip show python-docx > nul 2> nul
if %errorlevel% neq 0 (
    echo python-docx is not installed. Installing now...
    pip install python-docx
)

rem Check and install SQLAlchemy
pip show SQLAlchemy > nul 2> nul
if %errorlevel% neq 0 (
    echo SQLAlchemy is not installed. Installing now...
    pip install SQLAlchemy
)

echo.
echo All required libraries are installed.

rem Run the Python script with the correct name.
echo Running the Python analysis script 'analysis_C-02.py'...
python analysis_C-02.py

echo.
echo The script has finished running. Check the generated Word document.
pause