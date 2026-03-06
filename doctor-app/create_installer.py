"""
Create a self-contained installer ZIP + setup launcher.
This creates:
  1. AnonimousQ-Setup.zip - all app files compressed
  2. install.bat - a launcher that extracts and installs

No Inno Setup required - uses native Windows capabilities.
"""

import os
import sys
import zipfile
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(BASE_DIR, "dist", "AnonimousQ")
OUTPUT_DIR = os.path.join(BASE_DIR, "Output")


def main():
    if not os.path.isdir(DIST_DIR):
        print("ERROR: dist/AnonimousQ not found. Run build_installer.py first.")
        sys.exit(1)

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # ── Step 1: Create ZIP ──
    zip_path = os.path.join(OUTPUT_DIR, "AnonimousQ-v2.0.0-Setup.zip")
    print(f"Creating {zip_path}...")

    if os.path.exists(zip_path):
        os.remove(zip_path)

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
        for root, dirs, files in os.walk(DIST_DIR):
            for fn in files:
                fpath = os.path.join(root, fn)
                arcname = os.path.join("AnonimousQ", os.path.relpath(fpath, DIST_DIR))
                zf.write(fpath, arcname)
                print(f"  + {arcname}")

    zip_size = os.path.getsize(zip_path) / (1024 * 1024)
    print(f"\nZIP created: {zip_size:.1f} MB")

    # ── Step 2: Create install.bat ──
    bat_path = os.path.join(OUTPUT_DIR, "install.bat")
    bat_content = r'''@echo off
chcp 65001 > nul
title ClinicTor - Installation

echo.
echo =========================================
echo   ClinicTor v2.0.0 - Setup
echo   Installing...
echo =========================================
echo.

:: Set install directory
set "INSTALL_DIR=%LOCALAPPDATA%\AnonimousQ"

:: Create install directory
if not exist "%INSTALL_DIR%" mkdir "%INSTALL_DIR%"

:: Extract files using PowerShell (built-in on Windows 10/11)
echo Extracting files...
powershell -NoProfile -Command "Expand-Archive -Path '%~dp0AnonimousQ-v2.0.0-Setup.zip' -DestinationPath '%INSTALL_DIR%' -Force" 2>nul
if errorlevel 1 (
    echo ERROR: Failed to extract files.
    pause
    exit /b 1
)

:: Move files from nested AnonimousQ subfolder to install dir
if exist "%INSTALL_DIR%\AnonimousQ" (
    xcopy /E /Y /Q "%INSTALL_DIR%\AnonimousQ\*" "%INSTALL_DIR%\" > nul
    rmdir /S /Q "%INSTALL_DIR%\AnonimousQ" 2>nul
)

:: Create Start Menu shortcut
echo Creating shortcuts...
set "START_MENU=%APPDATA%\Microsoft\Windows\Start Menu\Programs"
powershell -NoProfile -Command "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut('%START_MENU%\ClinicTor.lnk'); $s.TargetPath = '%INSTALL_DIR%\AnonimousQ.exe'; $s.WorkingDirectory = '%INSTALL_DIR%'; $s.IconLocation = '%INSTALL_DIR%\AnonimousQ.exe'; $s.Description = 'ClinicTor'; $s.Save()"

:: Create Desktop shortcut
set /p DESKTOP_SHORTCUT="Create desktop shortcut? (Y/N): "
if /i "%DESKTOP_SHORTCUT%"=="Y" (
    powershell -NoProfile -Command "$ws = New-Object -ComObject WScript.Shell; $s = $ws.CreateShortcut([Environment]::GetFolderPath('Desktop') + '\ClinicTor.lnk'); $s.TargetPath = '%INSTALL_DIR%\AnonimousQ.exe'; $s.WorkingDirectory = '%INSTALL_DIR%'; $s.IconLocation = '%INSTALL_DIR%\AnonimousQ.exe'; $s.Description = 'ClinicTor'; $s.Save()"
    echo Desktop shortcut created.
)

:: Create uninstaller
echo @echo off > "%INSTALL_DIR%\uninstall.bat"
echo chcp 65001 ^> nul >> "%INSTALL_DIR%\uninstall.bat"
echo echo Uninstalling ClinicTor... >> "%INSTALL_DIR%\uninstall.bat"
echo set /p CONFIRM="Are you sure? (Y/N): " >> "%INSTALL_DIR%\uninstall.bat"
echo if /i not "%%CONFIRM%%"=="Y" exit /b >> "%INSTALL_DIR%\uninstall.bat"
echo del "%%APPDATA%%\Microsoft\Windows\Start Menu\Programs\ClinicTor.lnk" 2^>nul >> "%INSTALL_DIR%\uninstall.bat"
echo del "%%USERPROFILE%%\Desktop\ClinicTor.lnk" 2^>nul >> "%INSTALL_DIR%\uninstall.bat"
echo echo Files removed. >> "%INSTALL_DIR%\uninstall.bat"
echo echo To delete app data, remove: %%APPDATA%%\AnonimousQ >> "%INSTALL_DIR%\uninstall.bat"
echo rmdir /S /Q "%%LOCALAPPDATA%%\AnonimousQ" >> "%INSTALL_DIR%\uninstall.bat"
echo pause >> "%INSTALL_DIR%\uninstall.bat"

echo.
echo =========================================
echo   Installation complete!
echo   ClinicTor installed to: %INSTALL_DIR%
echo =========================================
echo.

:: Ask to launch
set /p LAUNCH="Launch ClinicTor now? (Y/N): "
if /i "%LAUNCH%"=="Y" (
    start "" "%INSTALL_DIR%\AnonimousQ.exe"
)

pause
'''

    with open(bat_path, "w", encoding="utf-8-sig") as f:
        f.write(bat_content)

    print(f"Installer batch created: {bat_path}")

    print(f"""
{'='*50}
  INSTALLER READY
{'='*50}

  Files in {OUTPUT_DIR}:
  1. AnonimousQ-v2.0.0-Setup.zip  ({zip_size:.1f} MB)
  2. install.bat           (installer script)

  To install on any computer:
  1. Copy both files to the target computer
  2. Run install.bat
  3. The app will be installed to %LOCALAPPDATA%\\AnonimousQ
  4. A Start Menu shortcut will be created
  5. Optionally creates a Desktop shortcut

  User data is stored in %APPDATA%\\AnonimousQ (survives reinstalls)
""")


if __name__ == "__main__":
    main()
