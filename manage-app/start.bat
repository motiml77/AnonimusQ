@echo off
cd /d "%~dp0"
chcp 65001 > nul
title ClinicTor - Management Dashboard

echo =========================================
echo   ClinicTor - Management Dashboard
echo =========================================
echo.

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Python not found. Please install Python and add to PATH.
    pause
    exit /b 1
)

:: Create virtual environment if needed
if not exist "venv\Scripts\python.exe" (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo Failed to create virtual environment.
        pause
        exit /b 1
    )
)

:: Install dependencies
echo Installing dependencies...
venv\Scripts\pip install -r requirements.txt -q
if errorlevel 1 (
    echo Failed to install dependencies.
    pause
    exit /b 1
)

echo.
echo Starting management dashboard...
echo.

venv\Scripts\python app.py
pause
