@echo off
:: Change to the directory of this bat file (critical for relative paths)
cd /d "%~dp0"
chcp 65001 > nul
title AnonimousQ - Doctor App

echo =========================================
echo   AnonimousQ v2.0.0 - מערכת תורים
echo =========================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo שגיאה: Python לא מותקן או לא נמצא ב-PATH.
    echo.
    echo אנא הורד והתקן Python מ:
    echo https://www.python.org/downloads/
    echo.
    echo חשוב: בעת ההתקנה סמן "Add Python to PATH"
    echo.
    pause
    exit /b 1
)

:: Create virtual environment if it doesn't exist
if not exist "venv\Scripts\python.exe" (
    echo יוצר סביבה וירטואלית...
    python -m venv venv
    if errorlevel 1 (
        echo שגיאה ביצירת סביבה וירטואלית.
        pause
        exit /b 1
    )
    echo סביבה וירטואלית נוצרה.
    echo.
)

:: Install/update packages (pip skips already-installed packages automatically)
echo מוודא שכל החבילות מותקנות...
venv\Scripts\pip install -r requirements.txt
if errorlevel 1 (
    echo שגיאה בהתקנת חבילות.
    pause
    exit /b 1
)

echo.
echo מפעיל מערכת תורים...
echo חלון התוכנה ייפתח תוך שנייה.
echo.

:: app.py = Flask server + Edge/Chrome app-mode window
start "" venv\Scripts\pythonw app.py
