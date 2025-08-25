@echo off
rem This batch file runs the Python script to generate the distillation column report.

rem First, check if the required Python libraries are installed.
echo Checking for required Python libraries...

pip show psycopg2-binary > nul 2> nul
if %errorlevel% neq 0 (
    echo psycopg2-binary is not installed. Installing now...
    pip install psycopg2-binary
)

pip show pandas > nul 2> nul
if %errorlevel% neq 0 (
    echo pandas is not installed. Installing now...
    pip install pandas
)

pip show matplotlib > nul 2> nul
if %errorlevel% neq 0 (
    echo matplotlib is not installed. Installing now...
    pip install matplotlib
)

pip show python-docx > nul 2> nul
if %errorlevel% neq 0 (
    echo python-docx is not installed. Installing now...
    pip install python-docx
)

echo.
echo All required libraries are installed.

rem Run the Python script.
echo Running the Python analysis script...
python your_script_name.py

echo.
echo The script has finished running. Check the generated Word document.
pause