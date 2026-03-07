@echo off
cd /d "%~dp0"
chcp 65001 > nul
title ClinicTor - Management Dashboard

echo =========================================
echo   ClinicTor - Management Dashboard
echo =========================================
echo.

:: Check service-account.json
if not exist "service-account.json" (
    echo [!] service-account.json not found!
    echo     Copy it from: doctor-app\data\firebase-service-account.json
    echo.
    pause
    exit /b 1
)

:: Check Python
python --version >nul 2>&1
if errorlevel 1 (
    echo [!] Python not found. Install Python and add to PATH.
    pause
    exit /b 1
)

:: Create venv if needed
if not exist "venv\Scripts\python.exe" (
    echo Creating virtual environment...
    python -m venv venv
    if errorlevel 1 (
        echo [!] Failed to create virtual environment.
        pause
        exit /b 1
    )
    echo Installing dependencies...
    venv\Scripts\pip install -r requirements.txt -q
    if errorlevel 1 (
        echo [!] Failed to install dependencies.
        pause
        exit /b 1
    )
)

echo Starting on http://localhost:8050 ...
echo.
venv\Scripts\python app.py
pause
