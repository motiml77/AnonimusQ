"""
Build script for AnonimousQ Doctor App v2 installer.
Creates a standalone Windows executable using PyInstaller,
then generates an Inno Setup installer script.

Usage:
    cd doctor-app
    venv\\Scripts\\python build_installer.py
"""

import os
import sys
import subprocess
import shutil

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DIST_DIR = os.path.join(BASE_DIR, "dist")
BUILD_DIR = os.path.join(BASE_DIR, "build")
APP_NAME = "AnonimousQ"
APP_VERSION = "2.1.0"
ICON_PATH = os.path.join(BASE_DIR, "static", "img", "favicon.ico")


def step(msg):
    print(f"\n{'='*50}")
    print(f"  {msg}")
    print(f"{'='*50}")


def run(cmd, **kwargs):
    print(f"  > {cmd}")
    result = subprocess.run(cmd, shell=True, cwd=BASE_DIR, **kwargs)
    if result.returncode != 0:
        print(f"  ERROR: Command failed with code {result.returncode}")
        sys.exit(1)


def copy_tree(src, dst):
    """Copy directory tree, overwriting existing."""
    if os.path.exists(dst):
        shutil.rmtree(dst)
    shutil.copytree(src, dst)


def main():
    # ── Step 1: Ensure PyInstaller is installed ──
    step("1/5 - Installing/verifying PyInstaller")
    run(f'"{sys.executable}" -m pip install pyinstaller')

    # ── Step 2: Clean previous builds ──
    step("2/5 - Cleaning previous build artifacts")
    for d in [BUILD_DIR, DIST_DIR]:
        if os.path.exists(d):
            shutil.rmtree(d)
            print(f"  Removed {d}")

    # ── Step 3: Run PyInstaller ──
    step("3/5 - Building with PyInstaller")

    # Hidden imports that PyInstaller might miss
    hidden_imports = [
        # Flask
        "flask", "flask.json", "jinja2", "markupsafe",
        # Auth
        "bcrypt", "bcrypt._bcrypt",
        # Firebase Admin
        "firebase_admin", "firebase_admin.credentials",
        "firebase_admin.firestore", "firebase_admin._token_gen",
        "firebase_admin._user_mgt", "firebase_admin._auth_utils",
        # Google Cloud Firestore
        "google.cloud.firestore", "google.cloud.firestore_v1",
        "google.cloud.firestore_v1.query",
        "google.cloud.firestore_v1.collection",
        "google.cloud.firestore_v1.document",
        "google.cloud.firestore_v1.transaction",
        "google.cloud.firestore_v1.transforms",
        "google.cloud.firestore_v1._helpers",
        "google.cloud.firestore_v1.base_query",
        "google.cloud.firestore_v1.base_document",
        "google.cloud.firestore_v1.base_collection_reference",
        "google.cloud.firestore_v1.async_query",
        "google.cloud.firestore_v1.async_document",
        "google.cloud.firestore_v1.async_collection",
        "google.cloud.firestore_v1.async_client",
        "google.cloud.firestore_v1.async_transaction",
        "google.cloud.firestore_v1.rate_limiter",
        # Google Auth
        "google.auth", "google.auth.transport",
        "google.auth.transport.requests",
        "google.oauth2", "google.oauth2.service_account",
        # Google API Core
        "google.api_core", "google.api_core.retry",
        "google.api_core.gapic_v1", "google.api_core.gapic_v1.method",
        # gRPC
        "grpc",
        # HTTP
        "requests", "certifi", "charset_normalizer", "urllib3",
        "cachecontrol",
        # Cryptography (Fernet encryption for patient data + treatment notes)
        "cryptography", "cryptography.fernet", "cryptography.hazmat",
        "cryptography.hazmat.primitives", "cryptography.hazmat.backends",
        # Local modules (imported by app.py at runtime)
        "db", "firebase_sync", "firebase_auth", "firebase_config",
        "crypto_utils", "frozen_utils",
    ]

    cmd_parts = [
        f'"{sys.executable}" -m PyInstaller',
        "--noconfirm",
        "--onedir",
        "--noconsole",
        f'--name="{APP_NAME}"',
        f'--icon="{ICON_PATH}"',
        "--noupx",
    ]

    for hi in hidden_imports:
        cmd_parts.append(f"--hidden-import={hi}")

    # Collect all google/grpc packages properly
    cmd_parts.append("--collect-all=google.cloud.firestore")
    cmd_parts.append("--collect-all=firebase_admin")
    cmd_parts.append("--collect-all=google.api_core")
    cmd_parts.append("--collect-all=grpc")
    cmd_parts.append("--collect-all=google.auth")

    # Main script
    cmd_parts.append('"app.py"')

    run(" ".join(cmd_parts), timeout=600)

    # ── Step 4: Copy resource files to dist (next to the exe) ──
    step("4/5 - Copying resources to dist folder")
    dist_app = os.path.join(DIST_DIR, APP_NAME)

    # Templates
    copy_tree(
        os.path.join(BASE_DIR, "templates"),
        os.path.join(dist_app, "templates"),
    )
    print("  Copied templates/")

    # Static files
    copy_tree(
        os.path.join(BASE_DIR, "static"),
        os.path.join(dist_app, "static"),
    )
    print("  Copied static/")

    # Copy data directory with service-account.json (required for Firebase Admin SDK)
    data_dst = os.path.join(dist_app, "data")
    os.makedirs(data_dst, exist_ok=True)
    sa_src = os.path.join(BASE_DIR, "data", "service-account.json")
    if os.path.exists(sa_src):
        shutil.copy2(sa_src, os.path.join(data_dst, "service-account.json"))
        print("  Copied data/service-account.json (Firebase Admin SDK)")
    else:
        print("  WARNING: data/service-account.json not found! Firebase will not work.")

    # Copy local Python modules (PyInstaller may compile them,
    # but we keep .py copies so the frozen __file__ path logic works)
    for py_file in ["db.py", "firebase_sync.py", "firebase_auth.py",
                     "firebase_config.py", "crypto_utils.py", "frozen_utils.py"]:
        src = os.path.join(BASE_DIR, py_file)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(dist_app, py_file))
            print(f"  Copied {py_file}")

    # Verify critical files exist
    print()
    all_ok = True
    for check_path, label in [
        (os.path.join(dist_app, f"{APP_NAME}.exe"), "Executable"),
        (os.path.join(dist_app, "templates", "base.html"), "Templates"),
        (os.path.join(dist_app, "static", "css", "app.css"), "Static CSS"),
        (os.path.join(dist_app, "static", "js", "dashboard.js"), "Dashboard JS"),
        (os.path.join(dist_app, "static", "img", "favicon.ico"), "Favicon"),
        (os.path.join(dist_app, "static", "vendor", "bootstrap.rtl.min.css"), "Bootstrap RTL"),
        (os.path.join(dist_app, "static", "vendor", "fullcalendar.min.js"), "FullCalendar"),
        (os.path.join(dist_app, "crypto_utils.py"), "Crypto Utils"),
        (os.path.join(dist_app, "data", "service-account.json"), "Firebase Service Account"),
    ]:
        exists = os.path.exists(check_path)
        status = "OK" if exists else "MISSING"
        print(f"  [{status}] {label}: {os.path.basename(check_path)}")
        if not exists:
            all_ok = False

    if not all_ok:
        print("\n  WARNING: Some files are missing. The installer may not work correctly.")

    # ── Step 5: Create Inno Setup script ──
    step("5/5 - Generating Inno Setup script")
    iss_path = os.path.join(BASE_DIR, "installer.iss")
    generate_inno_script(iss_path, dist_app)
    print(f"  Created: {iss_path}")

    # ── Done ──
    print(f"\n{'='*50}")
    print(f"  BUILD COMPLETE — v{APP_VERSION}")
    print(f"{'='*50}")
    print(f"  PyInstaller output: {dist_app}")
    print(f"  Inno Setup script:  {iss_path}")
    print(f"  Test: {dist_app}\\{APP_NAME}.exe")
    print(f"  Installer: install Inno Setup, then compile installer.iss")


def generate_inno_script(output_path, dist_app_path):
    """Generate an Inno Setup .iss script for the installer."""

    # Use forward-escaped backslashes for Inno Setup
    dist_escaped = dist_app_path.replace("\\", "\\\\")
    icon_escaped = ICON_PATH.replace("\\", "\\\\")
    output_dir = os.path.join(BASE_DIR, "Output").replace("\\", "\\\\")

    script = f"""; Inno Setup Script for AnonimousQ Doctor App
; Generated by build_installer.py
;
; To compile: open this file in Inno Setup and press Ctrl+F9
; Or from command line: iscc installer.iss

#define MyAppName "AnonimousQ"
#define MyAppVersion "{APP_VERSION}"
#define MyAppPublisher "AnonimousQ"
#define MyAppExeName "AnonimousQ.exe"
#define MyAppDescription "מערכת תורים אנונימיים לרופאים"

[Setup]
AppId={{{{B7E4F2A1-3C5D-4E6F-8A9B-0C1D2E3F4A5B}}}}
AppName={{#MyAppName}}
AppVersion={{#MyAppVersion}}
AppVerName={{#MyAppName}} {{#MyAppVersion}}
AppPublisher={{#MyAppPublisher}}
AppComments={{#MyAppDescription}}
DefaultDirName={{localappdata}}\\{{#MyAppName}}
DefaultGroupName={{#MyAppName}}
DisableProgramGroupPage=yes
OutputDir={output_dir}
OutputBaseFilename=AnonimousQ-Setup-{APP_VERSION}
SetupIconFile={icon_escaped}
UninstallDisplayIcon={{app}}\\{{#MyAppExeName}}
Compression=lzma2/ultra64
SolidCompression=yes
WizardStyle=modern
PrivilegesRequired=lowest
ArchitecturesInstallIn64BitMode=x64compatible
; No admin required – installs to user's AppData
PrivilegesRequiredOverridesAllowed=dialog
; Auto-update support: close running instance and restart after install
CloseApplications=yes
RestartApplications=yes

[Languages]
Name: "hebrew"; MessagesFile: "compiler:Languages\\Hebrew.isl"

[Tasks]
Name: "desktopicon"; Description: "צור קיצור דרך בשולחן העבודה"; GroupDescription: "קיצורי דרך נוספים:"

[Files]
; Copy everything from the PyInstaller dist folder
Source: "{dist_escaped}\\*"; DestDir: "{{app}}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
; Start menu shortcut
Name: "{{group}}\\{{#MyAppName}}"; Filename: "{{app}}\\{{#MyAppExeName}}"; IconFilename: "{{app}}\\{{#MyAppExeName}}"
; Desktop shortcut
Name: "{{userdesktop}}\\{{#MyAppName}}"; Filename: "{{app}}\\{{#MyAppExeName}}"; IconFilename: "{{app}}\\{{#MyAppExeName}}"; Tasks: desktopicon
; Uninstall shortcut
Name: "{{group}}\\הסר את {{#MyAppName}}"; Filename: "{{uninstallexe}}"

[Run]
; Run after installation
Filename: "{{app}}\\{{#MyAppExeName}}"; Description: "הפעל את AnonimousQ"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
; Clean up browser profile data on uninstall (optional)
Type: filesandordirs; Name: "{{app}}\\data\\browser-profile"

[Code]
function InitializeUninstall(): Boolean;
var
  DataDir, BackupDir, Msg: String;
begin
  DataDir := ExpandConstant('{{userappdata}}\\AnonimousQ');
  BackupDir := ExpandConstant('{{userdocs}}\\AnonimousQ Backup');

  Msg := 'האם ברצונך למחוק גם את כל הנתונים השמורים?' + Chr(13) + Chr(10) + Chr(13) + Chr(10) + 'פעולה זו תמחק לצמיתות:' + Chr(13) + Chr(10) + '  - כל רשימת המטופלים' + Chr(13) + Chr(10) + '  - כל התורים השמורים' + Chr(13) + Chr(10) + '  - כל ההגדרות והסיסמאות' + Chr(13) + Chr(10) + '  - גיבויים בתיקיית Documents' + Chr(13) + Chr(10) + Chr(13) + Chr(10) + 'לחץ "כן" למחיקה מלאה, או "לא" להסרת התוכנה בלבד.';

  if MsgBox(Msg, mbConfirmation, MB_YESNO or MB_DEFBUTTON2) = IDYES then
  begin
    if MsgBox('אזהרה אחרונה!' + Chr(13) + Chr(10) + Chr(13) + Chr(10) + 'כל נתוני המטופלים, התורים וההגדרות יימחקו לצמיתות.' + Chr(13) + Chr(10) + 'לא ניתן לשחזר מידע זה!' + Chr(13) + Chr(10) + Chr(13) + Chr(10) + 'האם אתה בטוח?',
              mbCriticalError, MB_YESNO or MB_DEFBUTTON2) = IDYES then
    begin
      if DirExists(DataDir) then
        DelTree(DataDir, True, True, True);
      if DirExists(BackupDir) then
        DelTree(BackupDir, True, True, True);
    end;
  end;

  Result := True;
end;
"""

    with open(output_path, "w", encoding="utf-8-sig") as f:
        f.write(script)


if __name__ == "__main__":
    main()
