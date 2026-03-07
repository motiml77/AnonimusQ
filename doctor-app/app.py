import csv
import io
import json
import logging
import os
import secrets
import subprocess
import sys
import threading
import time
import zipfile
from datetime import datetime
from functools import wraps

# ========================
# Logging system
# ========================
_LOG_DIR = os.path.join(
    os.environ.get("APPDATA", os.path.expanduser("~")), "AnonimousQ", "logs"
)
os.makedirs(_LOG_DIR, exist_ok=True)
_LOG_FILE = os.path.join(_LOG_DIR, "app.log")

# Redirect stdout/stderr to file when frozen (no console)
if getattr(sys, "frozen", False) and sys.stdout is None:
    _log_fh = open(_LOG_FILE, "a", encoding="utf-8")
    sys.stdout = _log_fh
    sys.stderr = _log_fh

# Rotating file handler — keeps last 3 files, 2MB each
from logging.handlers import RotatingFileHandler
_log_handler = RotatingFileHandler(
    _LOG_FILE, maxBytes=2 * 1024 * 1024, backupCount=3, encoding="utf-8"
)
_log_handler.setFormatter(logging.Formatter(
    "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
))
_app_logger = logging.getLogger("anonimousq")
_app_logger.setLevel(logging.INFO)
_app_logger.addHandler(_log_handler)

# Also capture warnings and errors from other libraries
logging.basicConfig(handlers=[_log_handler], level=logging.WARNING)

import requests

from flask import (
    Flask,
    Response,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    url_for,
)

import db
import crypto_utils
import firebase_auth
import firebase_sync

# ========================
# App version
# ========================
APP_VERSION = "2.5.0"

# ========================
# Auto-update state
# ========================
_update_info = {
    "available": False,
    "version": "",
    "download_url": "",
    "release_notes": "",
}

# ========================
# Heartbeat — track browser activity to auto-shutdown when closed
# ========================
_last_heartbeat = time.time()

# ========================
# License / Trial state
# ========================
TRIAL_DAYS = 30

_license_info = {
    "status": "unknown",       # "trial" / "licensed" / "expired" / "unknown"
    "trial_start_date": None,  # datetime object
    "days_remaining": None,
    "days_used": None,
    "licensed": False,
}


def _refresh_license():
    """Fetch license info from Firebase, update global _license_info, cache in SQLite."""
    global _license_info

    if not db.get_current_user():
        return False  # No user context yet

    result = firebase_sync.get_license_info()
    if result["ok"]:
        start_dt = result["trialStartDate"]
        licensed = result["licensed"]

        # Convert Firestore timestamp to datetime
        if hasattr(start_dt, "isoformat"):
            start_iso = start_dt.isoformat()
        else:
            start_iso = str(start_dt)
            start_dt = datetime.fromisoformat(start_iso)

        now = datetime.now(start_dt.tzinfo) if start_dt.tzinfo else datetime.now()
        days_used = (now - start_dt).days
        days_remaining = max(0, TRIAL_DAYS - days_used)

        if licensed:
            status = "licensed"
        elif days_remaining > 0:
            status = "trial"
        else:
            status = "expired"

        _license_info.update({
            "status": status,
            "trial_start_date": start_dt,
            "days_remaining": days_remaining,
            "days_used": days_used,
            "licensed": licensed,
        })

        db.set_cached_license(start_iso, licensed)
        return True

    # Firebase failed — fall back to SQLite cache
    cached = db.get_cached_license()
    if cached:
        start_dt = datetime.fromisoformat(cached["trial_start_date"])
        now = datetime.now(start_dt.tzinfo) if start_dt.tzinfo else datetime.now()
        days_used = (now - start_dt).days
        days_remaining = max(0, TRIAL_DAYS - days_used)
        licensed = cached["licensed"]

        if licensed:
            status = "licensed"
        elif days_remaining > 0:
            status = "trial"
        else:
            status = "expired"

        _license_info.update({
            "status": status,
            "trial_start_date": start_dt,
            "days_remaining": days_remaining,
            "days_used": days_used,
            "licensed": licensed,
        })
        return True

    return False


def _load_license_from_cache():
    """Load license info from SQLite cache ONLY (no Firebase call).
    Used during login for instant access without network wait."""
    global _license_info

    cached = db.get_cached_license()
    if cached:
        start_dt = datetime.fromisoformat(cached["trial_start_date"])
        now = datetime.now(start_dt.tzinfo) if start_dt.tzinfo else datetime.now()
        days_used = (now - start_dt).days
        days_remaining = max(0, TRIAL_DAYS - days_used)
        licensed = cached["licensed"]

        if licensed:
            status = "licensed"
        elif days_remaining > 0:
            status = "trial"
        else:
            status = "expired"

        _license_info.update({
            "status": status,
            "trial_start_date": start_dt,
            "days_remaining": days_remaining,
            "days_used": days_used,
            "licensed": licensed,
        })
    else:
        # No cache at all — allow access, background refresh will correct
        _license_info.update({"status": "trial"})


# ========================
# Password recovery — developer master key
# ========================
_RECOVERY_MASTER_KEY = b"YW5vbmltdXNxLW1hc3Rlci1rZXktMjAyNi0wMw=="  # base64 seed
_recovery_fernet = None


def _get_recovery_fernet():
    global _recovery_fernet
    if _recovery_fernet is None:
        import hashlib, base64
        dk = hashlib.pbkdf2_hmac("sha256", _RECOVERY_MASTER_KEY, b"anonimusq-recovery-salt", 100_000)
        key = base64.urlsafe_b64encode(dk[:32])
        from cryptography.fernet import Fernet
        _recovery_fernet = Fernet(key)
    return _recovery_fernet


def _save_recovery_password(password: str):
    """Encrypt the doctor's password and save to Firebase for developer recovery."""
    try:
        encrypted = _get_recovery_fernet().encrypt(password.encode()).decode()
        firebase_sync.save_recovery_token(encrypted)
    except Exception:
        _app_logger.warning("Failed to save recovery password", exc_info=True)


# ========================
# App setup
# ========================

# PyInstaller compatibility: tell Flask where templates & static files are
if getattr(sys, "frozen", False):
    _frozen_dir = os.path.dirname(sys.executable)
    app = Flask(
        __name__,
        template_folder=os.path.join(_frozen_dir, "templates"),
        static_folder=os.path.join(_frozen_dir, "static"),
    )
else:
    app = Flask(__name__)

_KEY_FILE = os.path.join(
    os.environ.get("APPDATA", os.path.expanduser("~")), "AnonimousQ", "secret.key"
)


def _load_secret_key() -> str:
    os.makedirs(os.path.dirname(_KEY_FILE), exist_ok=True)
    if os.path.exists(_KEY_FILE):
        return open(_KEY_FILE).read().strip()
    key = secrets.token_hex(32)
    with open(_KEY_FILE, "w") as f:
        f.write(key)
    # Hide the key file
    if os.name == 'nt':
        try:
            subprocess.run(['attrib', '+H', _KEY_FILE], capture_output=True, timeout=5)
        except Exception:
            pass
    return key


app.secret_key = _load_secret_key()
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0   # No caching for static files (JS/CSS)

# ── Session cookie security ──
app.config["SESSION_COOKIE_HTTPONLY"] = True      # Block JavaScript access to session cookie
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"     # CSRF protection at cookie level
app.config["PERMANENT_SESSION_LIFETIME"] = 86400  # 24 hours

# ── Update heartbeat on every request ──
@app.before_request
def _update_heartbeat():
    global _last_heartbeat
    _last_heartbeat = time.time()


# ── Restore per-user context after app restart ──
@app.before_request
def _restore_user_context():
    """If session says user is authenticated but db module lost context
    (e.g., app restarted while session cookie persists), restore it."""
    if not session.get("authenticated"):
        return
    uname = session.get("username")
    if uname and db.get_current_user() != uname:
        db.set_current_user(uname)
        db.init_db()
        firebase_sync.set_username(uname)
        _refresh_license()


# ── License guard — block expired trials ──
@app.before_request
def _check_license():
    exempt = {"login", "setup", "logout", "static", "api_heartbeat", "trial_expired"}
    if request.endpoint in exempt or not session.get("authenticated"):
        return
    if _license_info["status"] == "unknown":
        _refresh_license()
    # Only block if license is definitively expired.
    # If unknown (offline / can't verify) — let the user work locally.
    if _license_info["status"] == "expired":
        return redirect(url_for("trial_expired"))


# ── Log unhandled errors ──
@app.errorhandler(500)
def _handle_500(e):
    _app_logger.error("Internal server error: %s | URL: %s", e, request.url, exc_info=True)
    return "שגיאה פנימית בשרת", 500


@app.errorhandler(Exception)
def _handle_exception(e):
    _app_logger.error("Unhandled exception: %s | URL: %s", e, request.url, exc_info=True)
    return "שגיאה פנימית בשרת", 500


# ── CSP header ──
@app.after_request
def add_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "SAMEORIGIN"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    csp = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "style-src 'self' 'unsafe-inline' https://cdn.jsdelivr.net; "
        "img-src 'self' data:; "
        "font-src 'self' https://cdn.jsdelivr.net; "
        "connect-src 'self'; "
    )
    response.headers["Content-Security-Policy"] = csp
    # Prevent stale cached JS/HTML
    if response.content_type and ("javascript" in response.content_type or "text/html" in response.content_type):
        response.headers["Cache-Control"] = "no-cache, must-revalidate"
    return response

# ── Rate limiting for login ──
_login_attempts = {}  # {ip: [timestamps]}
_MAX_LOGIN_ATTEMPTS = 10
_LOGIN_WINDOW = 300   # 5 minutes

def _check_rate_limit(ip):
    """Returns True if request is allowed, False if rate-limited."""
    now = time.time()
    attempts = _login_attempts.get(ip, [])
    # Keep only attempts within the window
    attempts = [t for t in attempts if now - t < _LOGIN_WINDOW]
    _login_attempts[ip] = attempts
    return len(attempts) < _MAX_LOGIN_ATTEMPTS

def _record_login_attempt(ip):
    now = time.time()
    if ip not in _login_attempts:
        _login_attempts[ip] = []
    _login_attempts[ip].append(now)


# ========================
# Auth decorator
# ========================

def login_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login"))
        # Ensure firebase username is set (handles app restart with active session)
        if session.get("username"):
            firebase_sync.set_username(session["username"])
        return f(*args, **kwargs)
    return decorated


# ========================
# Routes – Auth
# ========================

@app.route("/")
def index():
    if not db.has_user():
        return redirect(url_for("setup"))
    if not session.get("authenticated"):
        return redirect(url_for("login"))
    return redirect(url_for("dashboard"))


@app.route("/setup", methods=["GET", "POST"])
def setup():
    if request.method == "POST":
        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        confirm = request.form.get("confirm", "")
        mode = request.form.get("mode", "register")  # "register" or "existing"
        full_name = request.form.get("full_name", "").strip()
        email = request.form.get("email", "").strip()
        phone = request.form.get("phone", "").strip()

        if not username or len(username) < 3:
            flash("שם המשתמש חייב להכיל לפחות 3 תווים (אותיות/ספרות בלבד)", "error")
        elif not username.replace("_", "").replace("-", "").isalnum():
            flash("שם המשתמש יכול להכיל רק אותיות לטיניות, ספרות, מקף וקו-תחתי", "error")
        elif len(password) < 6:
            flash("הסיסמא חייבת להכיל לפחות 6 תווים", "error")
        elif mode == "register" and password != confirm:
            flash("הסיסמאות אינן תואמות", "error")
        else:
            if mode == "existing":
                # Existing account: verify against Firebase Auth
                fb_result = firebase_auth.login(username, password)
                if not fb_result["ok"]:
                    if fb_result["error"] == "offline":
                        flash("נדרש חיבור לאינטרנט לחיבור חשבון קיים", "error")
                    else:
                        flash(fb_result["error"], "error")
                    return render_template("setup.html")
            else:
                # New account: register in Firebase Auth
                fb_result = firebase_auth.register(username, password)
                if not fb_result["ok"]:
                    if "כבר רשום" in fb_result.get("error", ""):
                        flash(fb_result["error"], "warning")
                        return render_template("setup.html", switch_to_existing=True)
                    flash(fb_result["error"], "error")
                    return render_template("setup.html")

            # Firebase Auth succeeded → create local user
            # setup_user writes to auth.db + calls set_current_user()
            result = db.setup_user(username, password)
            if result["ok"]:
                db.init_db()  # initialize per-user data tables
                session["authenticated"] = True
                session["username"] = username
                firebase_sync.set_username(username)

                if mode == "existing":
                    # Existing account: download encryption settings from Firebase
                    _init_encryption_from_password(password)
                else:
                    # New account: initialize encryption v2 from scratch
                    _setup_fresh_encryption(password)
                    # Create license doc in Firestore (written ONCE, never updated by app)
                    firebase_sync.create_license_doc()
                    # Save doctor profile (name, phone, email)
                    if full_name:
                        firebase_sync.save_doctor_profile({
                            "fullName": full_name,
                            "email": email,
                            "phone": phone,
                        })
                    # Save encrypted password for developer recovery
                    _save_recovery_password(password)

                # Load license info into memory + cache
                _refresh_license()

                return redirect(url_for("dashboard"))
            flash(result.get("error", "שגיאה"), "error")
    return render_template("setup.html")


@app.route("/login", methods=["GET", "POST"])
def login():
    if session.get("authenticated"):
        return redirect(url_for("dashboard"))
    if request.method == "POST":
        client_ip = request.remote_addr or "unknown"
        if not _check_rate_limit(client_ip):
            flash("יותר מדי ניסיונות התחברות. נסה שוב בעוד מספר דקות.", "error")
            return render_template("login.html")
        _record_login_attempt(client_ip)

        username = request.form.get("username", "").strip().lower()
        password = request.form.get("password", "")
        ok, uname = db.verify_user(username, password)

        if ok:
            # ── Local auth succeeded — log in INSTANTLY from cache ──
            _app_logger.info("Login successful (local): %s", uname)
            db.set_current_user(uname)
            db.init_db()
            db.auto_backup()

            session["authenticated"] = True
            session["username"] = uname
            firebase_sync.set_username(uname)
            _init_encryption_from_password(password)

            # Use cached license — no network wait
            _load_license_from_cache()

            # Firebase Auth + license refresh + sync queue flush in background
            def _bg_firebase_auth(u, p):
                try:
                    fb_result = firebase_auth.login(u, p)
                    if fb_result.get("ok"):
                        pass
                    elif "לא קיים" in fb_result.get("error", ""):
                        reg_result = firebase_auth.register(u, p)
                        if reg_result.get("ok"):
                            firebase_auth.login(u, p)
                except Exception:
                    pass
                _refresh_license()
                _flush_sync_queue_once()
            threading.Thread(target=_bg_firebase_auth, args=(uname, password), daemon=True).start()

            return redirect(url_for("dashboard"))

        # ── No local match — try Firebase (user may exist on another computer) ──
        fb_result = firebase_auth.login(username, password)
        if fb_result.get("ok"):
            _app_logger.info("Login successful (Firebase, new local): %s", username)
            # Create local user from Firebase credentials
            result = db.setup_user(username, password)
            if result["ok"]:
                db.init_db()
                session["authenticated"] = True
                session["username"] = username
                firebase_sync.set_username(username)
                _init_encryption_from_password(password)
                _refresh_license()
                return redirect(url_for("dashboard"))
            flash(result.get("error", "שגיאה ביצירת משתמש מקומי"), "error")
        elif fb_result.get("error") == "offline":
            # No local user + no internet → can't verify
            _app_logger.warning("Login failed for user: %s (no local + offline)", username)
            flash("שם משתמש או סיסמא שגויים", "error")
        else:
            _app_logger.warning("Login failed for user: %s from %s", username, client_ip)
            flash("שם משתמש או סיסמא שגויים", "error")
    return render_template("login.html", has_user=db.has_user())


@app.route("/logout")
def logout():
    crypto_utils.clear_cached_fernet()
    session.clear()
    return redirect(url_for("login"))


@app.route("/trial-expired")
def trial_expired():
    # Re-check — maybe the developer just unlocked it
    _refresh_license()
    if _license_info["status"] != "expired":
        if session.get("authenticated"):
            return redirect(url_for("dashboard"))
        return redirect(url_for("login"))
    return render_template("trial_expired.html",
                           days_used=_license_info.get("days_used", 0))


# ========================
# Encryption – key derivation, migration, new-device sync
# ========================

def _init_encryption_from_password(password: str):
    """Derive Fernet from password. Handle v1→v2 migration or new-device sync."""
    enc_meta = db.get_encryption_metadata()
    ver = enc_meta["version"]

    if ver == 2 and enc_meta["salt"]:
        # ── Already migrated: derive key from local salt ──
        salt = crypto_utils.b64_to_salt(enc_meta["salt"])
        fernet = crypto_utils.create_fernet(password, salt)
        crypto_utils.set_cached_fernet(fernet)
        return

    if ver == 0:
        # ── Fresh install with no encryption key at all ──
        # Check if there's encryption data on Firebase (existing account, new device)
        if firebase_sync.is_connected():
            fb_enc = firebase_sync.get_encryption_settings()
            if fb_enc.get("encryptionVersion") == 2 and fb_enc.get("pbkdf2Salt"):
                # Existing account → new device sync
                _new_device_sync(password, fb_enc)
                return
        # Truly new: set up fresh v2 encryption
        _setup_fresh_encryption(password)
        return

    if ver == 1:
        # ── Legacy random key → migrate to password-derived ──
        if firebase_sync.is_connected():
            # Check if Firebase already has v2 (another device migrated)
            fb_enc = firebase_sync.get_encryption_settings()
            if fb_enc.get("encryptionVersion") == 2 and fb_enc.get("pbkdf2Salt"):
                _new_device_sync(password, fb_enc)
                return
        _migrate_v1_to_v2(password)
        return

    # Fallback: just derive from local salt if available
    if enc_meta["salt"]:
        salt = crypto_utils.b64_to_salt(enc_meta["salt"])
        fernet = crypto_utils.create_fernet(password, salt)
        crypto_utils.set_cached_fernet(fernet)


def _setup_fresh_encryption(password: str):
    """Initialize encryption v2 from scratch (new account, no existing data)."""
    salt = crypto_utils.generate_salt()
    salt_b64 = crypto_utils.salt_to_b64(salt)
    fernet = crypto_utils.create_fernet(password, salt)
    crypto_utils.set_cached_fernet(fernet)

    verification_token = crypto_utils.make_verification_token(fernet)
    db.save_encryption_metadata(salt_b64, version=2)

    # Push encryption settings to Firebase
    try:
        if firebase_sync.is_connected():
            firebase_sync.save_encryption_settings({
                "pbkdf2Salt": salt_b64,
                "encryptionVersion": 2,
                "keyVerificationToken": verification_token,
                "migratedAt": __import__("datetime").datetime.now().isoformat(),
            })
    except Exception:
        pass  # Settings saved locally; will sync later


def _migrate_v1_to_v2(password: str):
    """Migrate from random Fernet key (v1) to password-derived key (v2).
    1. Decrypt existing data with old key
    2. Re-encrypt with new password-derived key
    3. Push everything to Firebase encrypted
    """
    old_fernet = db.get_legacy_fernet()

    # Generate new password-derived key
    salt = crypto_utils.generate_salt()
    salt_b64 = crypto_utils.salt_to_b64(salt)
    new_fernet = crypto_utils.create_fernet(password, salt)
    crypto_utils.set_cached_fernet(new_fernet)

    verification_token = crypto_utils.make_verification_token(new_fernet)

    # ── Re-encrypt treatment notes ──
    all_notes = db.get_all_treatment_notes_for_sync()
    notes_for_firebase = []
    for note in all_notes:
        content = note["content"]
        # Try decrypting with old key (notes are stored encrypted in SQLite)
        plaintext = crypto_utils.decrypt_with(old_fernet, content)
        if plaintext == "[שגיאת פענוח]":
            plaintext = content  # Wasn't encrypted, use as-is
        # Re-encrypt with new key
        new_encrypted = crypto_utils.encrypt(plaintext)
        db.update_treatment_note_content(note["id"], new_encrypted)
        notes_for_firebase.append({
            "anonymous_id": note["anonymous_id"],
            "note_id": note["id"],
            "encryptedContent": new_encrypted,
            "noteType": note["note_type"],
            "appointmentDate": note["appointment_date"],
            "appointmentTime": note["appointment_time"],
            "createdAt": note["created_at"],
            "updatedAt": note["updated_at"],
        })

    # ── Encrypt patient data (names, phones, notes — stored plaintext locally) ──
    all_patients = db.get_all_patients_for_sync()
    patients_for_firebase = []
    for p in all_patients:
        patients_for_firebase.append({
            "anonymous_id": p["anonymous_id"],
            "encryptedName": crypto_utils.encrypt(p["name"] or ""),
            "encryptedPhone": crypto_utils.encrypt(p["phone"] or ""),
            "encryptedNotes": crypto_utils.encrypt(p["notes"] or ""),
            "price": float(p.get("price", 0)),
            "isAnonymous": bool(p.get("is_anonymous", 0)),
            "active": bool(p.get("active", 1)),
            "registered": True,
        })

    # ── Encrypt emergency contacts ──
    all_ecs = db.get_all_emergency_contacts_for_sync()
    ec_by_patient = {}
    for ec in all_ecs:
        anon_id = ec["anonymous_id"]
        if anon_id not in ec_by_patient:
            ec_by_patient[anon_id] = []
        ec_by_patient[anon_id].append({
            "encryptedName": crypto_utils.encrypt(ec["contact_name"]),
            "encryptedPhone": crypto_utils.encrypt(ec["contact_phone"]),
        })

    # ── Encrypt referral broker names ──
    all_refs = db.get_all_referral_agreements_for_sync()
    refs_by_patient = {}
    for ref in all_refs:
        anon_id = ref["anonymous_id"]
        broker_name = ref["broker_name"]
        # Broker name might already be encrypted with old key
        decrypted = crypto_utils.decrypt_with(old_fernet, broker_name)
        if decrypted == "[שגיאת פענוח]":
            decrypted = broker_name  # Was plaintext
        refs_by_patient[anon_id] = {
            "encryptedBrokerName": crypto_utils.encrypt(decrypted),
            "percentage": ref["percentage"],
            "totalSessions": ref["total_sessions"],
            "enabled": bool(ref["enabled"]),
        }

    # ── Save metadata locally ──
    db.save_encryption_metadata(salt_b64, version=2)

    # ── Push everything to Firebase in background ──
    def _bg_push_migration():
        try:
            if not firebase_sync.is_connected():
                return
            firebase_sync.save_encryption_settings({
                "pbkdf2Salt": salt_b64,
                "encryptionVersion": 2,
                "keyVerificationToken": verification_token,
                "migratedAt": __import__("datetime").datetime.now().isoformat(),
            })
            if patients_for_firebase:
                firebase_sync.push_all_encrypted_patients(patients_for_firebase)
            if notes_for_firebase:
                firebase_sync.push_all_encrypted_notes(notes_for_firebase)
            if ec_by_patient:
                firebase_sync.push_all_encrypted_emergency_contacts(ec_by_patient)
            if refs_by_patient:
                firebase_sync.push_all_encrypted_referrals(refs_by_patient)
            # Migration done → clear legacy key
            db.delete_legacy_fernet_key()
        except Exception as e:
            print(f"[Encryption] migration push error: {e}")

    threading.Thread(target=_bg_push_migration, daemon=True).start()


def _new_device_sync(password: str, fb_enc: dict):
    """Download and decrypt all data from Firebase for a new device."""
    salt_b64 = fb_enc["pbkdf2Salt"]
    salt = crypto_utils.b64_to_salt(salt_b64)
    fernet = crypto_utils.create_fernet(password, salt)

    # Verify password produces the correct key
    token = fb_enc.get("keyVerificationToken", "")
    if token and not crypto_utils.verify_key(fernet, token):
        # Password doesn't match the encryption key — data can't be decrypted
        # This shouldn't happen because bcrypt already verified the password
        # But if it does, we still cache the fernet (it just won't decrypt properly)
        print("[Encryption] WARNING: key verification failed — password may have changed")

    crypto_utils.set_cached_fernet(fernet)
    db.save_encryption_metadata(salt_b64, version=2)

    # Download and decrypt patient data in background
    def _bg_download():
        try:
            if not firebase_sync.is_connected():
                return
            # Download all patients
            fb_patients = firebase_sync.pull_all_patients()
            patients_to_save = []
            for fp in fb_patients:
                anon_id = fp.get("anonymous_id", "")
                if not anon_id:
                    continue
                name = crypto_utils.decrypt(fp.get("encryptedName", "")) if fp.get("encryptedName") else ""
                phone = crypto_utils.decrypt(fp.get("encryptedPhone", "")) if fp.get("encryptedPhone") else ""
                notes = crypto_utils.decrypt(fp.get("encryptedNotes", "")) if fp.get("encryptedNotes") else ""
                patients_to_save.append({
                    "anonymous_id": anon_id,
                    "name": name if name != "[שגיאת פענוח]" else anon_id,
                    "phone": phone if phone != "[שגיאת פענוח]" else "",
                    "notes": notes if notes != "[שגיאת פענוח]" else "",
                    "price": fp.get("price", 0),
                    "is_anonymous": 1 if fp.get("isAnonymous") else 0,
                    "active": 1 if fp.get("active", True) else 0,
                })

                # Download notes for this patient
                fb_notes = firebase_sync.pull_all_notes_for_patient(anon_id)
                if fb_notes:
                    # We need patient_id — look it up after saving patients
                    pass

                # Emergency contacts
                ec_list = fp.get("emergencyContacts", [])
                if ec_list:
                    # Will process after patients are saved
                    pass

            if patients_to_save:
                db.bulk_save_patients_from_firebase(patients_to_save)

            # Now download notes and ECs with proper patient_ids
            for fp in fb_patients:
                anon_id = fp.get("anonymous_id", "")
                if not anon_id:
                    continue
                # Find patient_id from local DB
                patient = db.get_patient_by_anonymous_id(anon_id) if hasattr(db, 'get_patient_by_anonymous_id') else None
                if not patient:
                    # Try to find by anonymous_id
                    all_p = db.get_all_patients_for_sync()
                    patient = next((p for p in all_p if p["anonymous_id"] == anon_id), None)
                if not patient:
                    continue
                patient_id = patient["id"]

                # Download and save treatment notes
                fb_notes = firebase_sync.pull_all_notes_for_patient(anon_id)
                notes_to_save = []
                for fn in fb_notes:
                    encrypted_content = fn.get("encryptedContent", "")
                    notes_to_save.append({
                        "patient_id": patient_id,
                        "anonymous_id": anon_id,
                        "content": encrypted_content,  # Keep encrypted in SQLite
                        "note_type": fn.get("noteType", "freeform"),
                        "appointment_date": fn.get("appointmentDate"),
                        "appointment_time": fn.get("appointmentTime"),
                        "created_at": fn.get("createdAt", ""),
                        "updated_at": fn.get("updatedAt", ""),
                    })
                if notes_to_save:
                    db.bulk_save_notes_from_firebase(notes_to_save)

                # Save emergency contacts
                ec_list = fp.get("emergencyContacts", [])
                ecs_to_save = []
                for ec in ec_list:
                    ec_name = crypto_utils.decrypt(ec.get("encryptedName", "")) if ec.get("encryptedName") else ec.get("name", "")
                    ec_phone = crypto_utils.decrypt(ec.get("encryptedPhone", "")) if ec.get("encryptedPhone") else ec.get("phone", "")
                    ecs_to_save.append({
                        "patient_id": patient_id,
                        "anonymous_id": anon_id,
                        "contact_name": ec_name if ec_name != "[שגיאת פענוח]" else "",
                        "contact_phone": ec_phone if ec_phone != "[שגיאת פענוח]" else "",
                    })
                if ecs_to_save:
                    db.bulk_save_emergency_contacts_from_firebase(ecs_to_save)

                # Save referral agreement
                ref = fp.get("referral")
                if ref and ref.get("enabled", False):
                    broker_name = crypto_utils.decrypt(ref.get("encryptedBrokerName", "")) if ref.get("encryptedBrokerName") else ""
                    if broker_name and broker_name != "[שגיאת פענוח]":
                        db.bulk_save_referrals_from_firebase([{
                            "patient_id": patient_id,
                            "anonymous_id": anon_id,
                            "broker_name": broker_name,
                            "percentage": ref.get("percentage", 0),
                            "total_sessions": ref.get("totalSessions", 0),
                            "enabled": 1,
                        }])

        except Exception as e:
            print(f"[Encryption] new device sync error: {e}")

    threading.Thread(target=_bg_download, daemon=True).start()


# ── Encryption helpers for Firebase sync ──

def _encrypt_ec_list(contacts: list) -> list:
    """Encrypt emergency contacts list for Firebase push."""
    if crypto_utils.is_ready():
        return [
            {"encryptedName": crypto_utils.encrypt(c["contact_name"]),
             "encryptedPhone": crypto_utils.encrypt(c["contact_phone"])}
            for c in contacts
        ]
    # Fallback: send plaintext (shouldn't happen after migration)
    return [{"name": c["contact_name"], "phone": c["contact_phone"]} for c in contacts]


def _push_encrypted_patient_bg(patient: dict):
    """Push encrypted patient data to Firebase in background."""
    def _bg():
        try:
            if not firebase_sync.is_connected() or not crypto_utils.is_ready():
                return
            firebase_sync.push_encrypted_patient(patient["anonymous_id"], {
                "encryptedName": crypto_utils.encrypt(patient.get("name", "")),
                "encryptedPhone": crypto_utils.encrypt(patient.get("phone", "")),
                "encryptedNotes": crypto_utils.encrypt(patient.get("notes", "")),
            })
        except Exception:
            pass
    threading.Thread(target=_bg, daemon=True).start()


# ── Migration status API (used by UI to show progress) ──

@app.route("/api/encryption/status")
@login_required
def api_encryption_status():
    meta = db.get_encryption_metadata()
    return jsonify({
        "ok": True,
        "version": meta["version"],
        "migrated": meta["version"] >= 2,
        "ready": crypto_utils.is_ready(),
    })


# ========================
# Routes – Dashboard
# ========================

@app.route("/dashboard")
@login_required
def dashboard():
    last_sync = db.get_last_sync()
    connected = firebase_sync.is_connected()
    demo_mode = not connected
    username = session.get("username", "")
    patients_list    = db.get_patients()
    availability     = db.get_availability()
    default_duration = int(availability.get("slotDurationMin", 45))
    anonymous_ids    = list(db.get_anonymous_ids())
    return render_template(
        "dashboard.html",
        last_sync=last_sync,
        connected=connected,
        demo_mode=demo_mode,
        username=username,
        patients=patients_list,
        default_duration=default_duration,
        availability_json=json.dumps(availability),
        disable_online_booking=availability.get("disableOnlineBooking", False),
        anonymous_ids_json=json.dumps(anonymous_ids),
    )


@app.route("/api/appointments")
@login_required
def api_appointments():
    """Return FullCalendar-compatible events.
    Default: return cached+local data instantly (fast).
    With ?sync=1: do full Firebase sync first (slower, for background refresh)."""
    uuid_map = db.get_uuid_map()
    connected = firebase_sync.is_connected()
    do_sync = request.args.get("sync", "0") == "1"

    tagged = []  # list of (appt_dict, source_str)

    if connected and do_sync:
        # ── ONLINE: full sync with Firebase ────────────────────────────────
        # Step 1: Fetch ALL appointments from Firebase (any status)
        all_result = firebase_sync.sync_all_appointments()
        if all_result["ok"]:
            all_appts = all_result["appointments"]
            # Step 2: Full cache replacement — local cache = exact mirror of Firebase
            db.cache_appointments(all_appts, replace_all=True)
            # Step 3: Push any un-synced local appointments to Firebase
            local_appts = db.get_local_appointments()
            if local_appts:
                push_result = firebase_sync.push_local_appointments(local_appts)
                if push_result.get("ok"):
                    db.clear_local_appointments()
                    # Merge pushed appointments directly — avoids a second Firebase fetch
                    new_appts = push_result.get("new_appointments", [])
                    if new_appts:
                        all_appts = all_appts + new_appts
                        db.cache_appointments(all_appts, replace_all=True)
            # Step 4: Build display from the now-complete cache
            for a in db.get_cached_appointments():
                tagged.append((a, "firebase"))
            db.log_sync(len(tagged), "ok")
        else:
            # Firebase error — fall back to cache + local
            for a in db.get_cached_appointments():
                tagged.append((a, "cached"))
            for a in db.get_local_appointments():
                tagged.append((a, "local"))
    else:
        # ── OFFLINE: use local cache + un-synced local appointments ────────
        cached_keys = set()
        for a in db.get_cached_appointments():
            tagged.append((a, "cached"))
            cached_keys.add((a.get("anonymousId", ""), a.get("date", ""), a.get("time", "")))
        for a in db.get_local_appointments():
            key = (a.get("anonymousId", ""), a.get("date", ""), a.get("time", ""))
            if key not in cached_keys:
                tagged.append((a, "local"))

    global_source = "firebase" if (connected and do_sync) else "cached"

    events = []
    for appt, appt_source in tagged:
        if appt.get("status") == "cancelled":
            continue
        uid      = appt.get("anonymousId", "")
        name     = uuid_map.get(uid, f"מטופל לא מזוהה ({uid[:8]}...)" if uid else "מטופל לא מזוהה")
        date     = appt.get("date", "")
        time_val = appt.get("time", "")
        status   = appt.get("status", "booked")
        dur      = int(appt.get("durationMin") or appt.get("duration_min") or 45)
        # Compute end time for FullCalendar block sizing
        try:
            h, m   = time_val.split(":")
            total  = int(h) * 60 + int(m) + dur
            end_time = f"{(total // 60) % 24:02d}:{total % 60:02d}"
        except Exception:
            end_time = time_val
        if uid == db.WALKIN_ID:
            bg_color = "#fd7e14"   # orange – walk-in (no registered patient)
        elif status == "cancel_requested":
            bg_color = "#dc3545"   # red – patient requested cancellation
        elif status == "pending":
            bg_color = "#e6a817"   # amber – awaiting approval
        elif appt_source == "local":
            bg_color = "#198754"   # green – local
        else:
            bg_color = "#0d6efd"   # blue – Firebase booked
        events.append({
            "id":              appt.get("id"),
            "title":           name,
            "start":           f"{date}T{time_val}",
            "end":             f"{date}T{end_time}",
            "backgroundColor": bg_color,
            "borderColor":     bg_color,
            "extendedProps": {
                "anonymousId":            uid,
                "status":                 status,
                "source":                 appt_source,
                "treated":                bool(appt.get("treated")),
                "paid":                   bool(appt.get("paid")),
                "paymentMethod":          appt.get("paymentMethod"),
                "durationMin":            dur,
                "patientMarkedPaid":      bool(appt.get("patientMarkedPaid")),
                "patientPaymentMethod":   appt.get("patientPaymentMethod"),
            },
        })

    return jsonify({"ok": True, "events": events, "source": global_source})


# ========================
# API – Patient utilities
# ========================

@app.route("/api/generate-patient-id")
@login_required
def api_generate_patient_id():
    """Generate a preview patient ID (unique locally + remotely) for the add-patient modal."""
    firebase_check = firebase_sync.patient_id_exists if firebase_sync.is_connected() else None
    try:
        candidate = db.generate_patient_id_preview(extra_check=firebase_check)
        return jsonify({"ok": True, "id": candidate})
    except Exception as e:
        print(f"  [ERROR] generate-patient-id: {e}")
        return jsonify({"ok": False, "error": "שגיאה ביצירת מזהה מטופל"})


@app.route("/api/patients/<anonymous_id>/appointments")
@login_required
def api_patient_appointments(anonymous_id):
    appointments = db.get_patient_appointments(anonymous_id)
    return jsonify({"ok": True, "appointments": appointments})


# ========================
# Routes – Patients
# ========================

@app.route("/patients")
@login_required
def patients():
    active_patients   = db.get_patients()
    inactive_patients = db.get_inactive_patients()
    username = session.get("username", "")
    return render_template(
        "patients.html",
        patients=active_patients,
        inactive_patients=inactive_patients,
        username=username,
    )


@app.route("/patients/set-active/<int:patient_id>", methods=["POST"])
@login_required
def patients_set_active(patient_id):
    data = request.get_json(silent=True) or {}
    active = 1 if data.get("active") else 0
    ok = db.set_patient_active(patient_id, active)
    return jsonify({"ok": ok})


@app.route("/patients/add", methods=["POST"])
@login_required
def patients_add():
    name = request.form.get("name", "").strip()
    phone = request.form.get("phone", "").strip()
    notes = request.form.get("notes", "").strip()

    is_ajax = request.headers.get("X-Requested-With") == "XMLHttpRequest"

    if not name:
        if is_ajax:
            return jsonify({"ok": False, "error": "שם המטופל הוא שדה חובה"})
        flash("שם המטופל הוא שדה חובה", "error")
        return redirect(url_for("patients"))

    price        = float(request.form.get("price", 0) or 0)
    suggested_id = request.form.get("suggested_id", "").strip()
    email        = request.form.get("email", "").strip()
    is_anonymous = 1 if request.form.get("is_anonymous") else 0
    if price == 0:
        price = db.get_payment_settings().get("defaultPrice", 0)

    # Save locally first (instant) — no Firebase check needed for speed
    result = db.add_patient(name, phone, notes,
                            price=price, suggested_id=suggested_id,
                            is_anonymous=is_anonymous, email=email)
    if result["ok"]:
        anon_id = result["anonymous_id"]
        # Queue Firebase sync in background
        patient_payload = {
            "anonymous_id": anon_id, "name": name, "phone": phone,
            "notes": notes, "price": price, "is_anonymous": is_anonymous,
        }
        def _sync_patient_bg():
            try:
                if firebase_sync.is_connected():
                    firebase_sync.register_patient(anon_id, price=price, is_anonymous=bool(is_anonymous))
                    _push_encrypted_patient_bg(patient_payload)
                else:
                    # Offline — queue for later
                    db.enqueue_firebase_sync(anon_id, "register_patient", payload=patient_payload)
            except Exception:
                # Failed — queue for retry
                db.enqueue_firebase_sync(anon_id, "register_patient", payload=patient_payload)
        threading.Thread(target=_sync_patient_bg, daemon=True).start()

        if is_ajax:
            return jsonify({"ok": True, "anonymous_id": anon_id})
        flash(f"מטופל נוסף! מזהה אנונימי: {anon_id}", "success")
    else:
        if is_ajax:
            return jsonify({"ok": False, "error": result.get("error", "שגיאה בהוספת מטופל")})
        flash(result.get("error", "שגיאה בהוספת מטופל"), "error")

    return redirect(url_for("patients"))


@app.route("/patients/update/<int:patient_id>", methods=["POST"])
@login_required
def patients_update(patient_id):
    name  = request.form.get("name",  "").strip()
    phone = request.form.get("phone", "").strip()
    email = request.form.get("email", "").strip()
    notes = request.form.get("notes", "").strip()
    price = float(request.form.get("price", 0) or 0)
    is_anonymous = 1 if request.form.get("is_anonymous") else 0

    if not name:
        flash("שם המטופל הוא שדה חובה", "error")
        return redirect(url_for("patients"))

    result = db.update_patient(patient_id, name, phone, notes, price=price,
                               is_anonymous=is_anonymous, email=email)
    if result["ok"]:
        patient = db.get_patient_by_id(patient_id)
        if patient:
            patient_payload = {
                "anonymous_id": patient["anonymous_id"],
                "name": name, "phone": phone, "notes": notes,
                "price": price, "is_anonymous": is_anonymous, "email": email,
            }

            def _bg_update_patient():
                try:
                    if firebase_sync.is_connected():
                        firebase_sync.update_patient_price(patient["anonymous_id"], price)
                        _push_encrypted_patient_bg({"anonymous_id": patient["anonymous_id"],
                                                    "name": name, "phone": phone, "notes": notes})
                    else:
                        db.enqueue_firebase_sync(patient["anonymous_id"], "update_patient", payload=patient_payload)
                except Exception:
                    db.enqueue_firebase_sync(patient["anonymous_id"], "update_patient", payload=patient_payload)

            threading.Thread(target=_bg_update_patient, daemon=True).start()
    flash("פרטי המטופל עודכנו" if result["ok"] else result.get("error", "שגיאה"),
          "success" if result["ok"] else "error")
    return redirect(url_for("patients"))


@app.route("/patients/delete/<int:patient_id>", methods=["POST"])
@login_required
def patients_delete(patient_id):
    db.delete_patient(patient_id)
    flash("המטופל נמחק", "success")
    return redirect(url_for("patients"))


@app.route("/patients/set-anonymous/<int:patient_id>", methods=["POST"])
@login_required
def patients_set_anonymous(patient_id):
    """Toggle anonymous flag for a patient (JSON API)."""
    body = request.get_json(silent=True) or {}
    is_anonymous = 1 if body.get("isAnonymous") else 0
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        return jsonify({"ok": False, "error": "מטופל לא נמצא"})
    result = db.update_patient(
        patient_id, patient["name"], patient.get("phone", ""),
        patient.get("notes", ""), price=float(patient.get("price") or 0),
        is_anonymous=is_anonymous,
    )
    if result.get("ok"):
        anon_id = patient["anonymous_id"]
        p_price = float(patient.get("price") or 0)

        def _bg_set_anon():
            try:
                if firebase_sync.is_connected():
                    firebase_sync.update_patient_price(anon_id, p_price, is_anonymous=bool(is_anonymous))
                else:
                    db.enqueue_firebase_sync(anon_id, "set_anonymous", payload={
                        "anonymous_id": anon_id, "price": p_price, "is_anonymous": is_anonymous,
                    })
            except Exception:
                db.enqueue_firebase_sync(anon_id, "set_anonymous", payload={
                    "anonymous_id": anon_id, "price": p_price, "is_anonymous": is_anonymous,
                })

        threading.Thread(target=_bg_set_anon, daemon=True).start()
    return jsonify(result)


# ========================
# Routes – Patient Detail (v2)
# ========================

@app.route("/patients/<int:patient_id>")
@login_required
def patient_detail(patient_id):
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        flash("מטופל לא נמצא", "error")
        return redirect(url_for("patients"))

    anon_id = patient["anonymous_id"]
    notes = db.get_treatment_notes(patient_id)
    contacts = db.get_emergency_contacts(patient_id)
    referral = db.get_referral_agreement(patient_id)
    appointments = db.get_patient_appointments(anon_id)
    referral_summary = None
    if referral and referral.get("enabled"):
        referral_summary = db.calculate_referral_summary(patient_id, float(patient.get("price") or 0))

    # Summary stats — cancelled appointments do NOT count as treated/paid
    total_appts = len(appointments)
    active_appts = [a for a in appointments if a.get("status") != "cancelled"]
    treated_count = sum(1 for a in active_appts if a.get("treated"))
    paid_count = sum(1 for a in active_appts if a.get("paid"))
    price = float(patient.get("price") or 0)
    total_paid_amount = paid_count * price          # only confirmed payments
    total_remaining = (treated_count - paid_count) * price

    return render_template(
        "patient_detail.html",
        patient=patient,
        notes=notes,
        emergency_contacts=contacts,
        referral=referral,
        referral_summary=referral_summary,
        appointments=appointments,
        summary={
            "total_appointments": total_appts,
            "treated": treated_count,
            "paid": paid_count,
            "unpaid": treated_count - paid_count,
            "total_paid_amount": round(total_paid_amount, 2),
            "total_remaining": round(max(0, total_remaining), 2),
        },
        username=session.get("username", ""),
    )


# ── Treatment Notes API ──

@app.route("/api/patients/<int:patient_id>/notes", methods=["POST"])
@login_required
def api_add_note(patient_id):
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        return jsonify({"ok": False, "error": "מטופל לא נמצא"}), 404
    data = request.get_json(silent=True) or {}
    content = data.get("content", "").strip()
    if not content:
        return jsonify({"ok": False, "error": "תוכן ריק"})
    note_type = data.get("noteType", "freeform")
    appt_date = data.get("appointmentDate")
    appt_time = data.get("appointmentTime")

    result = db.add_treatment_note(
        patient_id, patient["anonymous_id"], content,
        note_type=note_type, appointment_date=appt_date, appointment_time=appt_time,
    )
    if result["ok"]:
        anon_id = patient["anonymous_id"]
        note_id_str = str(result["id"])

        def _bg_push_note():
            try:
                if firebase_sync.is_connected():
                    encrypted = db.encrypt_note(content)
                    firebase_sync.push_treatment_note(
                        anon_id, note_id_str, encrypted, note_type, appt_date, appt_time,
                    )
                    db.mark_notes_synced([result["id"]])
                else:
                    db.enqueue_firebase_sync(note_id_str, "push_note", payload={
                        "anonymous_id": anon_id, "note_id": note_id_str,
                        "content": content, "note_type": note_type,
                        "appt_date": appt_date, "appt_time": appt_time,
                    })
            except Exception:
                db.enqueue_firebase_sync(note_id_str, "push_note", payload={
                    "anonymous_id": anon_id, "note_id": note_id_str,
                    "content": content, "note_type": note_type,
                    "appt_date": appt_date, "appt_time": appt_time,
                })

        threading.Thread(target=_bg_push_note, daemon=True).start()
    return jsonify(result)


@app.route("/api/patients/<int:patient_id>/notes/<int:note_id>", methods=["PUT"])
@login_required
def api_update_note(patient_id, note_id):
    data = request.get_json(silent=True) or {}
    content = data.get("content", "").strip()
    if not content:
        return jsonify({"ok": False, "error": "תוכן ריק"})
    result = db.update_treatment_note(note_id, content)
    if result["ok"]:
        patient = db.get_patient_by_id(patient_id)
        if patient:
            anon_id = patient["anonymous_id"]
            n_type = data.get("noteType", "freeform")
            a_date = data.get("appointmentDate")
            a_time = data.get("appointmentTime")
            note_id_str = str(note_id)

            def _bg_update_note():
                try:
                    if firebase_sync.is_connected():
                        encrypted = db.encrypt_note(content)
                        firebase_sync.push_treatment_note(
                            anon_id, note_id_str, encrypted, n_type, a_date, a_time,
                        )
                        db.mark_notes_synced([note_id])
                    else:
                        db.enqueue_firebase_sync(note_id_str, "push_note", payload={
                            "anonymous_id": anon_id, "note_id": note_id_str,
                            "content": content, "note_type": n_type,
                            "appt_date": a_date, "appt_time": a_time,
                        })
                except Exception:
                    db.enqueue_firebase_sync(note_id_str, "push_note", payload={
                        "anonymous_id": anon_id, "note_id": note_id_str,
                        "content": content, "note_type": n_type,
                        "appt_date": a_date, "appt_time": a_time,
                    })

            threading.Thread(target=_bg_update_note, daemon=True).start()
    return jsonify(result)


@app.route("/api/patients/<int:patient_id>/notes/<int:note_id>", methods=["DELETE"])
@login_required
def api_delete_note(patient_id, note_id):
    patient = db.get_patient_by_id(patient_id)
    result = db.delete_treatment_note(note_id)
    if result["ok"] and patient:
        anon_id = patient["anonymous_id"]
        note_id_str = str(note_id)

        def _bg_delete_note():
            try:
                if firebase_sync.is_connected():
                    firebase_sync.delete_treatment_note_firebase(anon_id, note_id_str)
                else:
                    db.enqueue_firebase_sync(note_id_str, "delete_note", payload={
                        "anonymous_id": anon_id, "note_id": note_id_str,
                    })
            except Exception:
                db.enqueue_firebase_sync(note_id_str, "delete_note", payload={
                    "anonymous_id": anon_id, "note_id": note_id_str,
                })

        threading.Thread(target=_bg_delete_note, daemon=True).start()
    return jsonify(result)


# ── Emergency Contacts API ──

@app.route("/api/patients/<int:patient_id>/emergency-contacts", methods=["POST"])
@login_required
def api_add_emergency_contact(patient_id):
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        return jsonify({"ok": False, "error": "מטופל לא נמצא"}), 404
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    phone = data.get("phone", "").strip()
    if not name or not phone:
        return jsonify({"ok": False, "error": "שם וטלפון הם שדות חובה"})
    result = db.add_emergency_contact(patient_id, patient["anonymous_id"], name, phone)
    if result["ok"]:
        anon_id = patient["anonymous_id"]
        p_id = patient_id

        def _bg_sync_ec():
            try:
                if firebase_sync.is_connected():
                    contacts = db.get_emergency_contacts(p_id)
                    firebase_sync.sync_emergency_contacts(anon_id, _encrypt_ec_list(contacts))
                else:
                    db.enqueue_firebase_sync(anon_id, "sync_emergency_contacts", payload={
                        "anonymous_id": anon_id, "patient_id": p_id,
                    })
            except Exception:
                db.enqueue_firebase_sync(anon_id, "sync_emergency_contacts", payload={
                    "anonymous_id": anon_id, "patient_id": p_id,
                })

        threading.Thread(target=_bg_sync_ec, daemon=True).start()
    return jsonify(result)


@app.route("/api/patients/<int:patient_id>/emergency-contacts/<int:ec_id>", methods=["PUT"])
@login_required
def api_update_emergency_contact(patient_id, ec_id):
    data = request.get_json(silent=True) or {}
    name = data.get("name", "").strip()
    phone = data.get("phone", "").strip()
    if not name or not phone:
        return jsonify({"ok": False, "error": "שם וטלפון הם שדות חובה"})
    result = db.update_emergency_contact(ec_id, name, phone)
    if result["ok"]:
        patient = db.get_patient_by_id(patient_id)
        if patient:
            anon_id = patient["anonymous_id"]
            p_id = patient_id

            def _bg_sync_ec():
                try:
                    if firebase_sync.is_connected():
                        contacts = db.get_emergency_contacts(p_id)
                        firebase_sync.sync_emergency_contacts(anon_id, _encrypt_ec_list(contacts))
                    else:
                        db.enqueue_firebase_sync(anon_id, "sync_emergency_contacts", payload={
                            "anonymous_id": anon_id, "patient_id": p_id,
                        })
                except Exception:
                    db.enqueue_firebase_sync(anon_id, "sync_emergency_contacts", payload={
                        "anonymous_id": anon_id, "patient_id": p_id,
                    })

            threading.Thread(target=_bg_sync_ec, daemon=True).start()
    return jsonify(result)


@app.route("/api/patients/<int:patient_id>/emergency-contacts/<int:ec_id>", methods=["DELETE"])
@login_required
def api_delete_emergency_contact(patient_id, ec_id):
    result = db.delete_emergency_contact(ec_id)
    if result["ok"]:
        patient = db.get_patient_by_id(patient_id)
        if patient:
            anon_id = patient["anonymous_id"]
            p_id = patient_id

            def _bg_sync_ec():
                try:
                    if firebase_sync.is_connected():
                        contacts = db.get_emergency_contacts(p_id)
                        firebase_sync.sync_emergency_contacts(anon_id, _encrypt_ec_list(contacts))
                    else:
                        db.enqueue_firebase_sync(anon_id, "sync_emergency_contacts", payload={
                            "anonymous_id": anon_id, "patient_id": p_id,
                        })
                except Exception:
                    db.enqueue_firebase_sync(anon_id, "sync_emergency_contacts", payload={
                        "anonymous_id": anon_id, "patient_id": p_id,
                    })

            threading.Thread(target=_bg_sync_ec, daemon=True).start()
    return jsonify(result)


# ── Referral Agreement API ──

@app.route("/api/patients/<int:patient_id>/referral", methods=["POST"])
@login_required
def api_upsert_referral(patient_id):
    patient = db.get_patient_by_id(patient_id)
    if not patient:
        return jsonify({"ok": False, "error": "מטופל לא נמצא"}), 404
    data = request.get_json(silent=True) or {}
    broker_name = data.get("brokerName", "").strip()
    percentage = float(data.get("percentage", 0))
    total_sessions = int(data.get("totalSessions", 0))
    if not broker_name or percentage <= 0 or total_sessions <= 0:
        return jsonify({"ok": False, "error": "יש למלא שם מתווך, אחוז ומספר מפגשים"})
    result = db.upsert_referral_agreement(
        patient_id, patient["anonymous_id"], broker_name, percentage, total_sessions,
    )
    if result["ok"]:
        anon_id = patient["anonymous_id"]

        def _bg_sync_ref():
            try:
                if firebase_sync.is_connected():
                    encrypted_broker = db.encrypt_note(broker_name)
                    firebase_sync.sync_referral_agreement(anon_id, {
                        "encryptedBrokerName": encrypted_broker,
                        "percentage": percentage,
                        "totalSessions": total_sessions,
                        "enabled": True,
                    })
                else:
                    db.enqueue_firebase_sync(anon_id, "sync_referral", payload={
                        "anonymous_id": anon_id, "broker_name": broker_name,
                        "percentage": percentage, "total_sessions": total_sessions,
                        "enabled": True,
                    })
            except Exception:
                db.enqueue_firebase_sync(anon_id, "sync_referral", payload={
                    "anonymous_id": anon_id, "broker_name": broker_name,
                    "percentage": percentage, "total_sessions": total_sessions,
                    "enabled": True,
                })

        threading.Thread(target=_bg_sync_ref, daemon=True).start()
    return jsonify(result)


@app.route("/api/patients/<int:patient_id>/referral", methods=["DELETE"])
@login_required
def api_delete_referral(patient_id):
    result = db.delete_referral_agreement(patient_id)
    if result["ok"]:
        patient = db.get_patient_by_id(patient_id)
        if patient:
            anon_id = patient["anonymous_id"]

            def _bg_del_ref():
                try:
                    if firebase_sync.is_connected():
                        firebase_sync.sync_referral_agreement(anon_id, {"enabled": False})
                    else:
                        db.enqueue_firebase_sync(anon_id, "sync_referral", payload={
                            "anonymous_id": anon_id, "enabled": False,
                        })
                except Exception:
                    db.enqueue_firebase_sync(anon_id, "sync_referral", payload={
                        "anonymous_id": anon_id, "enabled": False,
                    })

            threading.Thread(target=_bg_del_ref, daemon=True).start()
    return jsonify(result)


@app.route("/api/patients/<int:patient_id>/referral/pay/<int:payment_id>", methods=["POST"])
@login_required
def api_referral_pay(patient_id, payment_id):
    data = request.get_json(silent=True) or {}
    paid = data.get("paid", True)
    if paid:
        result = db.mark_referral_paid(payment_id)
    else:
        result = db.unmark_referral_paid(payment_id)
    return jsonify(result)


# ========================
# Routes – Settings
# ========================

@app.route("/settings")
@login_required
def settings():
    availability     = db.get_availability()
    payment_settings = db.get_payment_settings()
    connected        = firebase_sync.is_connected()
    username         = session.get("username", "")
    return render_template(
        "settings.html",
        availability=availability,
        payment_settings=payment_settings,
        connected=connected,
        username=username,
    )


@app.route("/settings/availability", methods=["POST"])
@login_required
def settings_save_availability():
    working_days = [int(d) for d in request.form.getlist("working_days")]
    start = request.form.get("start_time", "09:00")
    end = request.form.get("end_time", "17:00")
    slot_duration = int(request.form.get("slot_duration", 60))
    blocked_str = request.form.get("blocked_dates", "")
    blocked_dates = [d.strip() for d in blocked_str.split(",") if d.strip()]

    # Preserve the online booking toggle from current settings
    current = db.get_availability()
    disable_online = request.form.get("disable_online_booking") == "on"

    availability = {
        "workingDays": working_days,
        "workingHours": {"start": start, "end": end},
        "slotDurationMin": slot_duration,
        "blockedDates": blocked_dates,
        "disableOnlineBooking": disable_online,
    }
    db.set_availability(availability)

    import json as _avail_json

    def _bg_push_avail():
        try:
            if firebase_sync.is_connected():
                firebase_sync.push_availability(availability)
            else:
                db.enqueue_firebase_sync("availability", "push_availability",
                                         payload=_avail_json.loads(_avail_json.dumps(availability)))
        except Exception:
            db.enqueue_firebase_sync("availability", "push_availability",
                                     payload=_avail_json.loads(_avail_json.dumps(availability)))

    threading.Thread(target=_bg_push_avail, daemon=True).start()
    flash("הגדרות נשמרו", "success")

    return redirect(url_for("settings"))


@app.route("/api/toggle-online-booking", methods=["POST"])
@login_required
def toggle_online_booking():
    """Toggle disableOnlineBooking and push to Firebase."""
    import json as _toggle_json
    availability = db.get_availability()
    new_val = not availability.get("disableOnlineBooking", False)
    availability["disableOnlineBooking"] = new_val
    db.set_availability(availability)

    avail_copy = _toggle_json.loads(_toggle_json.dumps(availability))

    def _bg_toggle():
        try:
            if firebase_sync.is_connected():
                firebase_sync.push_availability(avail_copy)
            else:
                db.enqueue_firebase_sync("availability", "push_availability", payload=avail_copy)
        except Exception:
            db.enqueue_firebase_sync("availability", "push_availability", payload=avail_copy)

    threading.Thread(target=_bg_toggle, daemon=True).start()
    return jsonify({"ok": True, "disabled": new_val})


@app.route("/settings/firebase/connect", methods=["POST"])
@login_required
def firebase_connect():
    """Legacy route kept for backward compatibility.
    Firebase now initializes automatically from embedded service account."""
    if not firebase_sync.is_connected():
        result = firebase_sync.init_embedded()
        if result["ok"]:
            firebase_sync.set_username(session["username"])
            flash("Firebase מחובר!", "success")
        else:
            flash(f"שגיאה בחיבור ל-Firebase: {result.get('error')}", "error")
    else:
        flash("Firebase כבר מחובר", "success")
    return redirect(url_for("settings"))


@app.route("/settings/firebase/sync", methods=["POST"])
@login_required
def firebase_manual_sync():
    # Push un-synced local appointments first
    local_appts = db.get_local_appointments()
    conflicts = []
    if local_appts:
        push_result = firebase_sync.push_local_appointments(local_appts)
        if push_result.get("ok"):
            db.clear_local_appointments()
            conflicts = push_result.get("conflicts", [])
    # Full sync: pull ALL appointments from Firebase into local cache
    all_result = firebase_sync.sync_all_appointments()
    if all_result["ok"]:
        db.cache_appointments(all_result["appointments"], replace_all=True)
        active_count = len([a for a in all_result["appointments"]
                            if a.get("status") in ("pending", "booked")])
        db.log_sync(active_count, "ok")
        flash(f"סנכרון הצליח – {active_count} תורים פעילים", "success")
        if conflicts:
            flash(
                f"⚠️ {len(conflicts)} תור(ים) לא סונכרנו בגלל חפיפה עם תורים מהאתר – "
                "בדוק את הלוח ואשר/בטל ידנית.",
                "warning",
            )
    else:
        flash(f"שגיאת סנכרון: {all_result.get('error')}", "error")
    return redirect(url_for("dashboard"))


@app.route("/settings/payment", methods=["POST"])
@login_required
def settings_save_payment():
    settings = {
        "defaultPrice": float(request.form.get("default_price", 0) or 0),
        "bitPhone":     request.form.get("bit_phone", "").strip(),
        "payboxPhone":  request.form.get("paybox_phone", "").strip(),
        "bitLink":      request.form.get("bit_link", "").strip(),
        "payboxLink":   request.form.get("paybox_link", "").strip(),
    }
    db.set_payment_settings(settings)
    if firebase_sync.is_connected():
        result = firebase_sync.push_payment_settings(settings)
        msg = "הגדרות תשלום נשמרו ועודכנו ב-Firebase" if result["ok"] else \
              f"הגדרות תשלום נשמרו לוקאלית (שגיאת Firebase: {result.get('error')})"
        flash(msg, "success" if result["ok"] else "warning")
    else:
        flash("הגדרות תשלום נשמרו לוקאלית", "success")
    return redirect(url_for("settings"))


@app.route("/settings/change-password", methods=["POST"])
@login_required
def change_password():
    old_pw = request.form.get("old_password", "")
    new_pw = request.form.get("new_password", "")
    confirm = request.form.get("confirm_password", "")

    if not db.verify_current_password(old_pw):
        flash("הסיסמא הנוכחית שגויה", "error")
    elif len(new_pw) < 6:
        flash("הסיסמא החדשה חייבת להכיל לפחות 6 תווים", "error")
    elif new_pw != confirm:
        flash("הסיסמאות החדשות אינן תואמות", "error")
    else:
        # Require internet: first verify Firebase login works
        uname = session.get("username", "")
        id_token = session.get("firebase_id_token")

        # Try to get a valid Firebase token
        if not id_token:
            login_result = firebase_auth.login(uname, old_pw)
            if login_result["ok"]:
                id_token = login_result.get("idToken", "")
            elif login_result.get("error") == "offline":
                flash("שינוי סיסמא דורש חיבור לאינטרנט. התחבר לאינטרנט ונסה שוב.", "error")
                return redirect(url_for("settings"))

        if not id_token:
            flash("שינוי סיסמא דורש חיבור לאינטרנט. התחבר לאינטרנט ונסה שוב.", "error")
            return redirect(url_for("settings"))

        # ── Update Firebase Auth password FIRST (before re-encrypting) ──
        try:
            firebase_sync.set_password_change_flag(True)
        except Exception:
            pass

        fb_result = firebase_auth.change_password(id_token, new_pw)
        if not fb_result["ok"]:
            try:
                firebase_sync.set_password_change_flag(False)
            except Exception:
                pass
            if "אין חיבור" in fb_result.get("error", ""):
                flash("שינוי סיסמא דורש חיבור לאינטרנט. התחבר לאינטרנט ונסה שוב.", "error")
            else:
                flash(f"שגיאה בשינוי סיסמא: {fb_result['error']}", "error")
            return redirect(url_for("settings"))

        # Firebase Auth password changed successfully → now re-encrypt everything
        db.set_password(new_pw)
        new_login = firebase_auth.login(uname, new_pw)
        if new_login["ok"]:
            session["firebase_id_token"] = new_login.get("idToken", "")

        # ── Save old Fernet for re-encryption ──
        old_fernet = crypto_utils.get_cached_fernet()

        # ── Generate new encryption key from new password ──
        new_salt = crypto_utils.generate_salt()
        new_salt_b64 = crypto_utils.salt_to_b64(new_salt)
        new_fernet = crypto_utils.create_fernet(new_pw, new_salt)
        new_verification_token = crypto_utils.make_verification_token(new_fernet)

        # ── Re-encrypt treatment notes (stored encrypted in SQLite) ──
        all_notes = db.get_all_treatment_notes_for_sync()
        notes_for_firebase = []
        for note in all_notes:
            # Decrypt with OLD key (still cached)
            plaintext = crypto_utils.decrypt_with(old_fernet, note["content"]) if old_fernet else note["content"]
            if plaintext == "[שגיאת פענוח]":
                plaintext = note["content"]
            # Re-encrypt with new key
            new_encrypted = crypto_utils.encrypt_with(new_fernet, plaintext)
            db.update_treatment_note_content(note["id"], new_encrypted)
            notes_for_firebase.append({
                "anonymous_id": note["anonymous_id"],
                "note_id": note["id"],
                "encryptedContent": new_encrypted,
                "noteType": note["note_type"],
                "appointmentDate": note["appointment_date"],
                "appointmentTime": note["appointment_time"],
                "createdAt": note["created_at"],
                "updatedAt": note["updated_at"],
            })

        # ── Encrypt patient data (plaintext locally) ──
        all_patients = db.get_all_patients_for_sync()
        patients_for_firebase = []
        for p in all_patients:
            patients_for_firebase.append({
                "anonymous_id": p["anonymous_id"],
                "encryptedName": crypto_utils.encrypt_with(new_fernet, p["name"] or ""),
                "encryptedPhone": crypto_utils.encrypt_with(new_fernet, p["phone"] or ""),
                "encryptedNotes": crypto_utils.encrypt_with(new_fernet, p["notes"] or ""),
                "price": float(p.get("price", 0)),
                "isAnonymous": bool(p.get("is_anonymous", 0)),
                "active": bool(p.get("active", 1)),
                "registered": True,
            })

        # ── Encrypt emergency contacts (plaintext locally) ──
        all_ecs = db.get_all_emergency_contacts_for_sync()
        ec_by_patient = {}
        for ec in all_ecs:
            anon_id = ec["anonymous_id"]
            if anon_id not in ec_by_patient:
                ec_by_patient[anon_id] = []
            ec_by_patient[anon_id].append({
                "encryptedName": crypto_utils.encrypt_with(new_fernet, ec["contact_name"]),
                "encryptedPhone": crypto_utils.encrypt_with(new_fernet, ec["contact_phone"]),
            })

        # ── Encrypt referral broker names (plaintext locally) ──
        all_refs = db.get_all_referral_agreements_for_sync()
        refs_by_patient = {}
        for ref in all_refs:
            anon_id = ref["anonymous_id"]
            refs_by_patient[anon_id] = {
                "encryptedBrokerName": crypto_utils.encrypt_with(new_fernet, ref["broker_name"]),
                "percentage": ref["percentage"],
                "totalSessions": ref["total_sessions"],
                "enabled": bool(ref["enabled"]),
            }

        # ── Switch to new key locally ──
        crypto_utils.set_cached_fernet(new_fernet)
        db.save_encryption_metadata(new_salt_b64, version=2)

        # ── Push re-encrypted data to Firebase in background ──
        from datetime import datetime as _dt

        def _bg_push_reencrypted():
            try:
                if not firebase_sync.is_connected():
                    return
                firebase_sync.save_encryption_settings({
                    "pbkdf2Salt": new_salt_b64,
                    "encryptionVersion": 2,
                    "keyVerificationToken": new_verification_token,
                    "migratedAt": _dt.now().isoformat(),
                    "passwordChangeInProgress": False,
                })
                if patients_for_firebase:
                    firebase_sync.push_all_encrypted_patients(patients_for_firebase)
                if notes_for_firebase:
                    firebase_sync.push_all_encrypted_notes(notes_for_firebase)
                if ec_by_patient:
                    firebase_sync.push_all_encrypted_emergency_contacts(ec_by_patient)
                if refs_by_patient:
                    firebase_sync.push_all_encrypted_referrals(refs_by_patient)
            except Exception as e:
                print(f"[Encryption] password change push error: {e}")

        threading.Thread(target=_bg_push_reencrypted, daemon=True).start()
        flash("הסיסמא שונתה בהצלחה וכל הנתונים הוצפנו מחדש", "success")

    return redirect(url_for("settings"))


# ========================
# Routes – Appointment approval
# ========================

@app.route("/appointments/approve/<appt_id>", methods=["POST"])
@login_required
def appointments_approve(appt_id):
    source = request.args.get("source", "firebase")
    if source == "local":
        result = db.approve_local_appointment(int(appt_id))
    else:
        # Cache-first: update local cache immediately
        db.update_cached_appointment_status(appt_id, "booked")

        def _bg_approve():
            res = firebase_sync.approve_appointment(appt_id)
            if res.get("ok"):
                for rejected_id in res.get("rejected", []):
                    db.update_cached_appointment_status(rejected_id, "cancelled")
            else:
                db.enqueue_firebase_sync(appt_id, "approve", payload={"appointment_id": appt_id})

        threading.Thread(target=_bg_approve, daemon=True).start()
        result = {"ok": True}
    return jsonify(result)


@app.route("/appointments/reject/<appt_id>", methods=["POST"])
@login_required
def appointments_reject(appt_id):
    source = request.args.get("source", "firebase")
    if source == "local":
        result = db.reject_local_appointment(int(appt_id))
    else:
        # Cache-first: update local cache immediately
        db.update_cached_appointment_status(appt_id, "cancelled")
        db.update_cached_appointment(appt_id, "treated", False, None)
        db.update_cached_appointment(appt_id, "paid", False, None)

        def _bg_reject():
            res = firebase_sync.reject_appointment(appt_id)
            if res.get("ok"):
                try:
                    firebase_sync.mark_appointment(appt_id, "treated", False, None)
                    firebase_sync.mark_appointment(appt_id, "paid", False, None)
                except Exception:
                    pass
            else:
                db.enqueue_firebase_sync(appt_id, "reject", payload={"appointment_id": appt_id})

        threading.Thread(target=_bg_reject, daemon=True).start()
        result = {"ok": True}
    return jsonify(result)


@app.route("/appointments/approve-cancel/<appt_id>", methods=["POST"])
@login_required
def appointments_approve_cancel(appt_id):
    """Doctor approves a patient's cancellation request."""
    # Cache-first
    db.update_cached_appointment_status(appt_id, "cancelled")

    def _bg_approve_cancel():
        res = firebase_sync.approve_cancel_request(appt_id)
        if not res.get("ok"):
            db.enqueue_firebase_sync(appt_id, "approve_cancel", payload={"appointment_id": appt_id})

    threading.Thread(target=_bg_approve_cancel, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/appointments/reject-cancel/<appt_id>", methods=["POST"])
@login_required
def appointments_reject_cancel(appt_id):
    """Doctor rejects a patient's cancellation request → appointment reverts to pending."""
    # Cache-first
    db.update_cached_appointment_status(appt_id, "pending")

    def _bg_reject_cancel():
        res = firebase_sync.reject_cancel_request(appt_id)
        if not res.get("ok"):
            db.enqueue_firebase_sync(appt_id, "reject_cancel", payload={"appointment_id": appt_id})

    threading.Thread(target=_bg_reject_cancel, daemon=True).start()
    return jsonify({"ok": True})


@app.route("/appointments/delete/<appt_id>", methods=["POST"])
@login_required
def appointments_delete(appt_id):
    """Permanently delete an appointment from everywhere."""
    source = request.args.get("source", "firebase")
    if source == "local":
        local_id = int(str(appt_id).replace("local-", ""))
        result = db.delete_local_appointment(local_id)
    elif source == "cached":
        db.delete_cached_appointment(appt_id)
        result = {"ok": True}
    else:
        # Cache-first: delete locally, sync Firebase in background
        db.delete_cached_appointment(appt_id)

        def _bg_delete():
            res = firebase_sync.delete_appointment(appt_id)
            if not res.get("ok"):
                db.enqueue_firebase_sync(appt_id, "delete", payload={"appointment_id": appt_id})

        threading.Thread(target=_bg_delete, daemon=True).start()
        result = {"ok": True}
    return jsonify(result)


@app.route("/appointments/mark/<appt_id>", methods=["POST"])
@login_required
def appointments_mark(appt_id):
    source         = request.args.get("source", "firebase")
    body           = request.json or {}
    field          = body.get("field")           # "treated" | "paid" | "status"
    value          = body.get("value")           # true/false or status string
    payment_method = body.get("paymentMethod")   # "bit" | "paybox" | "cash" | "bank" | None

    # Payment requires treatment first
    if field == "paid" and value:
        treated = db.is_appointment_treated(appt_id, source)
        if not treated:
            return jsonify({"ok": False, "error": "יש לסמן טיפול בוצע לפני סימון תשלום"})

    if source == "local":
        local_id = int(str(appt_id).replace("local-", ""))
        result = db.mark_local_appointment(local_id, field, value, payment_method)
        # Un-treating → also clear payment
        if field == "treated" and not value:
            db.mark_local_appointment(local_id, "paid", False, None)
    elif source == "cached":
        db.update_cached_appointment(appt_id, field, value, payment_method)
        if field == "treated" and not value:
            db.update_cached_appointment(appt_id, "paid", False, None)
        result = {"ok": True}
    else:
        # Cache-first: update local cache immediately, sync Firebase in background
        db.update_cached_appointment(appt_id, field, value, payment_method)
        if field == "treated" and not value:
            db.update_cached_appointment(appt_id, "paid", False, None)

        def _bg_firebase_mark():
            res = firebase_sync.mark_appointment(appt_id, field, value, payment_method)
            if not res.get("ok"):
                db.enqueue_firebase_sync(appt_id, "mark", field, value, payment_method)
            if field == "treated" and not value:
                res2 = firebase_sync.mark_appointment(appt_id, "paid", False, None)
                if not res2.get("ok"):
                    db.enqueue_firebase_sync(appt_id, "mark", "paid", False, None)

        threading.Thread(target=_bg_firebase_mark, daemon=True).start()
        result = {"ok": True}
    return jsonify(result)


# ========================
# Routes – Reports
# ========================

_PAYMENT_LABELS = {"bit": "ביט", "paybox": "פייבוקס", "cash": "מזומן", "bank": "העברה בנקאית"}
_STATUS_LABELS  = {"booked": "מאושר", "pending": "ממתין", "cancelled": "בוטל", "cancel_requested": "בקשת ביטול"}


@app.route("/reports")
@login_required
def reports():
    import json as _json
    appointments = db.get_all_cached_for_reports()
    uuid_map = db.get_uuid_map()
    price_map = db.get_price_map()
    for appt in appointments:
        uid = appt.get("anonymousId", "")
        appt["patientName"] = uuid_map.get(uid, f"({uid})")
        appt["patientPrice"] = price_map.get(uid, 0)
    anonymous_ids = list(db.get_anonymous_ids())
    return render_template(
        "reports.html",
        appointments_json=_json.dumps(appointments, ensure_ascii=False),
        anonymous_ids_json=_json.dumps(anonymous_ids),
        username=session.get("username", ""),
    )


@app.route("/reports/backup.zip")
@login_required
def reports_backup():
    appointments = db.get_all_cached_for_reports()
    uuid_map = db.get_uuid_map()
    anonymous_ids = db.get_anonymous_ids()
    # Group by year
    by_year: dict = {}
    for appt in appointments:
        year = (appt.get("date") or "0000")[:4]
        by_year.setdefault(year, []).append(appt)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for year, appts in sorted(by_year.items()):
            csv_buf = io.StringIO()
            writer = csv.writer(csv_buf)
            writer.writerow(["תאריך", "שעה", "מטופל", "מזהה", "סטטוס", "טיפול", "תשלום", "שיטת תשלום"])
            for appt in sorted(appts, key=lambda a: (a.get("date", ""), a.get("time", ""))):
                uid = appt.get("anonymousId", "")
                # Anonymous patients: show ID instead of name
                patient_name = uid if uid in anonymous_ids else uuid_map.get(uid, f"({uid})")
                writer.writerow([
                    appt.get("date", ""),
                    appt.get("time", ""),
                    patient_name,
                    uid,
                    _STATUS_LABELS.get(appt.get("status", ""), appt.get("status", "")),
                    "כן" if appt.get("treated") else "טרם התקיים",
                    "כן" if appt.get("paid") else "לא",
                    _PAYMENT_LABELS.get(appt.get("paymentMethod") or "", ""),
                ])
            # UTF-8 BOM for Excel Hebrew compatibility
            zf.writestr(f"appointments_{year}.csv", "\ufeff" + csv_buf.getvalue())
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=clinicTor_backup.zip"},
    )


# ========================
# Routes – Appointment create / reschedule
# ========================

@app.route("/appointments/create", methods=["POST"])
@login_required
def appointments_create():
    body         = request.json or {}
    anonymous_id = body.get("anonymousId", "").strip()
    date         = body.get("date", "").strip()
    time_val     = body.get("time", "").strip()
    duration_min = int(body.get("durationMin") or db.get_availability().get("slotDurationMin", 45))

    if not anonymous_id or not date or not time_val:
        return jsonify({"ok": False, "error": "חסרים פרטים"})

    connected = firebase_sync.is_connected()

    # Conflict check (range-based)
    # When offline, doctor-created appointments ignore pending web requests
    # (exclude_pending=True) — those pending entries will be auto-rejected at approval time.
    if connected:
        conflict = firebase_sync.check_slot_conflict(date, time_val, duration_min)
    else:
        conflict = (db.check_slot_conflict_cached(date, time_val, duration_min,
                                                   exclude_pending=True) or
                    db.check_slot_conflict_local(date, time_val, duration_min))

    if conflict:
        return jsonify({"ok": False, "error": f"חפיפה עם תור קיים בתאריך {date} סביב השעה {time_val}"})

    # Create
    if connected:
        result = firebase_sync.create_appointment(anonymous_id, date, time_val, duration_min)
        if result["ok"]:
            db.cache_appointments([{
                "id":          result["id"],
                "anonymousId": anonymous_id,
                "date":        date,
                "time":        time_val,
                "status":      "booked",
                "treated":     False,
                "paid":        False,
                "paymentMethod": None,
                "durationMin": duration_min,
            }], replace_all=False)
    else:
        result = db.create_local_appointment(anonymous_id, date, time_val, "booked", duration_min)

    return jsonify(result)


@app.route("/appointments/reschedule/<appt_id>", methods=["POST"])
@login_required
def appointments_reschedule(appt_id):
    source       = request.args.get("source", "firebase")
    body         = request.json or {}
    new_date     = body.get("date", "").strip()
    new_time     = body.get("time", "").strip()
    duration_min = body.get("durationMin")
    if duration_min is not None:
        duration_min = int(duration_min)

    if not new_date or not new_time:
        return jsonify({"ok": False, "error": "חסרים תאריך/שעה"})

    # Duration for conflict check: use provided or fall back to settings default
    check_dur = duration_min or int(db.get_availability().get("slotDurationMin", 45))
    connected = firebase_sync.is_connected()

    # Conflict check (range-based, excluding the appointment being moved)
    if source == "local":
        conflict = db.check_slot_conflict_local(new_date, new_time, check_dur,
                                                 exclude_id=int(appt_id))
    elif connected:
        conflict = firebase_sync.check_slot_conflict(new_date, new_time, check_dur,
                                                      exclude_id=appt_id)
    else:
        conflict = db.check_slot_conflict_cached(new_date, new_time, check_dur,
                                                  exclude_id=appt_id)

    if conflict:
        return jsonify({"ok": False, "error": f"חפיפה עם תור קיים בתאריך {new_date} סביב השעה {new_time}"})

    # Reschedule
    if source == "local":
        result = db.reschedule_local_appointment(int(appt_id), new_date, new_time, duration_min)
    elif source == "cached":
        db.reschedule_cached_appointment(appt_id, new_date, new_time, duration_min)
        # Also queue for Firebase sync
        db.enqueue_firebase_sync(appt_id, "reschedule", payload={
            "appointment_id": appt_id, "date": new_date,
            "time": new_time, "duration_min": duration_min,
        })
        result = {"ok": True}
    else:
        # Cache-first: update cache immediately, sync Firebase in background
        db.reschedule_cached_appointment(appt_id, new_date, new_time, duration_min)

        def _bg_reschedule():
            res = firebase_sync.reschedule_appointment(appt_id, new_date, new_time, duration_min)
            if not res.get("ok"):
                db.enqueue_firebase_sync(appt_id, "reschedule", payload={
                    "appointment_id": appt_id, "date": new_date,
                    "time": new_time, "duration_min": duration_min,
                })

        threading.Thread(target=_bg_reschedule, daemon=True).start()
        result = {"ok": True}

    return jsonify(result)




# ========================
# Routes – Auto-update
# ========================

@app.route("/api/heartbeat")
def api_heartbeat():
    """Browser pings this every 10s so the server knows it's still open."""
    return jsonify({"ok": True})


@app.route("/api/update-check")
@login_required
def api_update_check():
    """Return current update status (called by frontend JS)."""
    return jsonify(_update_info)


@app.route("/api/app-version")
@login_required
def api_app_version():
    """Return current app version."""
    return jsonify({"version": APP_VERSION})


@app.route("/api/license-info")
@login_required
def api_license_info():
    """Return current license/trial status for the settings page UI."""
    from datetime import timedelta
    trial_end = None
    if _license_info.get("trial_start_date"):
        trial_end = (_license_info["trial_start_date"] + timedelta(days=TRIAL_DAYS)).strftime("%d/%m/%Y")
    return jsonify({
        "status": _license_info["status"],
        "daysUsed": _license_info.get("days_used"),
        "daysRemaining": _license_info.get("days_remaining"),
        "trialDays": TRIAL_DAYS,
        "licensed": _license_info.get("licensed", False),
        "trialEndDate": trial_end,
    })


@app.route("/api/logs")
@login_required
def api_logs():
    """Download all logs as a ZIP for troubleshooting."""
    import zipfile as zf
    buf = io.BytesIO()
    with zf.ZipFile(buf, "w", zf.ZIP_DEFLATED) as z:
        for fname in os.listdir(_LOG_DIR):
            fpath = os.path.join(_LOG_DIR, fname)
            if os.path.isfile(fpath):
                z.write(fpath, fname)
    buf.seek(0)
    return Response(
        buf.getvalue(),
        mimetype="application/zip",
        headers={"Content-Disposition": "attachment; filename=ClinicTor-logs.zip"},
    )


@app.route("/update/install", methods=["POST"])
@login_required
def update_install():
    """Download and install the latest update."""
    if not _update_info["available"] or not _update_info["download_url"]:
        return jsonify({"ok": False, "error": "אין עדכון זמין"})

    try:
        _app_logger.info("Update install started — downloading v%s", _update_info["version"])

        # 1. Force backup before update
        db.auto_backup()
        _app_logger.info("Pre-update backup completed")

        # 2. Download installer to temp
        url = _update_info["download_url"]
        tmp_path = os.path.join(
            os.environ.get("TEMP", os.path.expanduser("~")),
            "AnonimousQ-Update.exe",
        )
        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            for chunk in resp.iter_content(8192):
                f.write(chunk)
        _app_logger.info("Update downloaded to %s", tmp_path)

        # 3. Launch installer in silent mode and exit
        subprocess.Popen([
            tmp_path, "/SILENT", "/CLOSEAPPLICATIONS", "/RESTARTAPPLICATIONS",
        ])
        _app_logger.info("Installer launched — exiting in 2s")

        # 4. Give installer time to start, then exit current app
        threading.Timer(2.0, lambda: os._exit(0)).start()
        return jsonify({"ok": True, "message": "מעדכן... התוכנה תיסגר ותיפתח מחדש"})

    except requests.ConnectionError:
        _app_logger.error("Update failed — no internet connection")
        return jsonify({"ok": False, "error": "אין חיבור לאינטרנט"})
    except Exception as e:
        _app_logger.error("Update failed: %s", e, exc_info=True)
        return jsonify({"ok": False, "error": f"שגיאת עדכון: {e}"})


@app.route("/update/check-now", methods=["POST"])
@login_required
def update_check_now():
    """Force an immediate update check (ignores daily limit)."""
    try:
        resp = requests.get(
            _GITHUB_RELEASES_URL, timeout=15,
            headers={"Accept": "application/vnd.github.v3+json"},
        )
        if resp.status_code != 200:
            return jsonify({"ok": False, "error": "לא ניתן לבדוק עדכונים כעת"})

        data = resp.json()
        latest = data.get("tag_name", "").lstrip("v")
        if latest and _is_newer(latest, APP_VERSION):
            for asset in data.get("assets", []):
                if asset["name"].lower().endswith(".exe"):
                    _update_info["available"] = True
                    _update_info["version"] = latest
                    _update_info["download_url"] = asset["browser_download_url"]
                    _update_info["release_notes"] = data.get("body", "")
                    return jsonify({"ok": True, "available": True, "version": latest})
            return jsonify({"ok": True, "available": False})
        return jsonify({"ok": True, "available": False,
                        "message": f"הגרסה שלך ({APP_VERSION}) מעודכנת"})
    except requests.ConnectionError:
        return jsonify({"ok": False, "error": "אין חיבור לאינטרנט"})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


# ========================
# Process cleanup helpers
# ========================

def _setup_browser_job(proc):
    """Windows Job Object: all browser child processes die when Python exits.
    On Windows 8+, child processes inherit job membership automatically."""
    if os.name != 'nt' or not proc:
        return None
    try:
        import ctypes
        from ctypes import wintypes

        kernel32 = ctypes.windll.kernel32

        job = kernel32.CreateJobObjectW(None, None)
        if not job:
            return None

        class JOBOBJECT_BASIC_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("PerProcessUserTimeLimit", ctypes.c_int64),
                ("PerJobUserTimeLimit", ctypes.c_int64),
                ("LimitFlags", wintypes.DWORD),
                ("MinimumWorkingSetSize", ctypes.c_size_t),
                ("MaximumWorkingSetSize", ctypes.c_size_t),
                ("ActiveProcessLimit", wintypes.DWORD),
                ("Affinity", ctypes.c_size_t),
                ("PriorityClass", wintypes.DWORD),
                ("SchedulingClass", wintypes.DWORD),
            ]

        class IO_COUNTERS(ctypes.Structure):
            _fields_ = [
                ("ReadOperationCount", ctypes.c_uint64),
                ("WriteOperationCount", ctypes.c_uint64),
                ("OtherOperationCount", ctypes.c_uint64),
                ("ReadTransferCount", ctypes.c_uint64),
                ("WriteTransferCount", ctypes.c_uint64),
                ("OtherTransferCount", ctypes.c_uint64),
            ]

        class JOBOBJECT_EXTENDED_LIMIT_INFORMATION(ctypes.Structure):
            _fields_ = [
                ("BasicLimitInformation", JOBOBJECT_BASIC_LIMIT_INFORMATION),
                ("IoInfo", IO_COUNTERS),
                ("ProcessMemoryLimit", ctypes.c_size_t),
                ("JobMemoryLimit", ctypes.c_size_t),
                ("PeakProcessMemoryUsed", ctypes.c_size_t),
                ("PeakJobMemoryUsed", ctypes.c_size_t),
            ]

        info = JOBOBJECT_EXTENDED_LIMIT_INFORMATION()
        info.BasicLimitInformation.LimitFlags = 0x2000  # JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE

        kernel32.SetInformationJobObject(
            job, 9,  # JobObjectExtendedLimitInformation
            ctypes.byref(info), ctypes.sizeof(info),
        )

        # Open process with rights needed for job assignment
        handle = kernel32.OpenProcess(0x0100 | 0x0001, False, proc.pid)
        if handle:
            kernel32.AssignProcessToJobObject(job, handle)
            kernel32.CloseHandle(handle)

        return job  # Must keep reference alive until process exits
    except Exception:
        return None


# ========================
# App window helper
# ========================

def _open_app_window():
    """
    Open Edge or Chrome in --app mode so the UI appears as a standalone
    desktop window (no address bar, no tabs, no browser chrome).
    Uses a dedicated user-data-dir so the process stays alive (doesn't
    delegate to an existing browser) and we can track when it closes.
    Falls back to the default browser if neither is found.
    Returns the subprocess.Popen object (or None for default browser).
    """
    url = "http://localhost:5000"
    # Dedicated browser profile so the process doesn't delegate to an
    # existing browser instance – this lets us detect window close.
    _browser_profile = os.path.join(
        os.environ.get("APPDATA", os.path.expanduser("~")),
        "AnonimousQ", "browser-profile",
    )
    os.makedirs(_browser_profile, exist_ok=True)

    # Detect screen size for centering
    try:
        import ctypes
        user32 = ctypes.windll.user32
        scr_w = user32.GetSystemMetrics(0)
        scr_h = user32.GetSystemMetrics(1)
    except Exception:
        scr_w, scr_h = 1920, 1080

    win_w, win_h = min(1400, scr_w - 100), min(900, scr_h - 100)
    pos_x = max(0, (scr_w - win_w) // 2)
    pos_y = max(0, (scr_h - win_h) // 2)

    args = [
        "--app=" + url,
        f"--user-data-dir={_browser_profile}",
        f"--window-size={win_w},{win_h}",
        f"--window-position={pos_x},{pos_y}",
        "--no-first-run",
        "--disable-extensions",
        "--disable-background-mode",
        "--disable-backgrounding-occluded-windows",
        "--disable-renderer-backgrounding",
    ]

    # Suppress any helper-process console windows
    _si = subprocess.STARTUPINFO()
    _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
    _si.wShowWindow = 1  # SW_SHOWNORMAL for main window
    _creation = subprocess.CREATE_NO_WINDOW  # suppress child console windows

    candidates = [
        # Edge (always present on Windows 10/11)
        r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        r"C:\Program Files\Microsoft\Edge\Application\msedge.exe",
        # Chrome
        r"C:\Program Files\Google\Chrome\Application\chrome.exe",
        r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
    ]

    for browser in candidates:
        if os.path.exists(browser):
            proc = subprocess.Popen(
                [browser] + args,
                startupinfo=_si,
                creationflags=_creation,
            )
            print(f"  פתיחת חלון תוכנה דרך: {os.path.basename(browser)}")
            # Wait briefly to see if the process stays alive.
            # If Edge/Chrome is already running it may "delegate" to
            # the existing instance and exit immediately — in that case
            # we fall through to the default-browser fallback below.
            try:
                proc.wait(timeout=5)
                # Process exited within 5 s → delegation happened
                print("  הדפדפן העביר לחלון קיים – פותח בדפדפן ברירת מחדל")
            except subprocess.TimeoutExpired:
                # Still running after 5 s → it's our own process, track it
                return proc

    # Fallback – open in default browser and keep alive via Flask thread
    import webbrowser
    print("  פותח בדפדפן ברירת מחדל")
    webbrowser.open(url)
    return None


# ========================
# Auto-update checker
# ========================

_APPDATA_DIR = db.BASE_DATA_DIR
_UPDATE_CHECK_FILE = os.path.join(_APPDATA_DIR, "last_update_check.txt")
_GITHUB_RELEASES_URL = "https://api.github.com/repos/motiml77/AnonimusQ/releases/latest"


def _is_newer(remote: str, local: str) -> bool:
    """Compare semantic versions: '2.1.0' > '2.0.0'."""
    try:
        r = tuple(int(x) for x in remote.split("."))
        l = tuple(int(x) for x in local.split("."))
        return r > l
    except Exception:
        return False


def _check_for_updates():
    """Background thread: check GitHub Releases for a new version once per day."""
    time.sleep(30)  # let app fully start
    while True:
        try:
            # Already checked today?
            today = datetime.now().strftime("%Y-%m-%d")
            if os.path.exists(_UPDATE_CHECK_FILE):
                last = open(_UPDATE_CHECK_FILE).read().strip()
                if last == today:
                    time.sleep(3600)
                    continue

            resp = requests.get(
                _GITHUB_RELEASES_URL, timeout=15,
                headers={"Accept": "application/vnd.github.v3+json"},
            )
            if resp.status_code == 200:
                data = resp.json()
                latest = data.get("tag_name", "").lstrip("v")
                if latest and _is_newer(latest, APP_VERSION):
                    for asset in data.get("assets", []):
                        if asset["name"].lower().endswith(".exe"):
                            _update_info["available"] = True
                            _update_info["version"] = latest
                            _update_info["download_url"] = asset["browser_download_url"]
                            _update_info["release_notes"] = data.get("body", "")
                            _app_logger.info("Update available: v%s (current: v%s)", latest, APP_VERSION)
                            print(f"  [Update] גרסה {latest} זמינה להורדה")
                            break
                else:
                    _app_logger.info("Update check: up to date (v%s)", APP_VERSION)

            # Mark today as checked
            os.makedirs(_APPDATA_DIR, exist_ok=True)
            with open(_UPDATE_CHECK_FILE, "w") as f:
                f.write(today)

        except Exception:
            _app_logger.warning("Update check failed", exc_info=True)

        time.sleep(3600)  # re-check every hour (date guard prevents API spam)


# ========================
# Background license refresh
# ========================

def _refresh_license_periodically():
    """Background thread: refresh license status from Firebase every hour."""
    time.sleep(60)  # let app start
    while True:
        try:
            if firebase_sync.is_connected() and firebase_sync._username:
                _refresh_license()
                _app_logger.info("License refresh: status=%s", _license_info["status"])
        except Exception:
            _app_logger.warning("License refresh failed", exc_info=True)
        time.sleep(3600)


# ========================
# Background sync queue flusher
# ========================

def _process_sync_queue():
    """Single pass: process all pending sync operations. Returns True if any were processed."""
    if not db.get_current_user():
        return False
    if not firebase_sync.is_connected():
        return False

    db.clear_stale_sync_operations()
    ops = db.get_pending_sync_operations()
    if not ops:
        return False

    for op in ops:
        success = False
        operation = op["operation"]
        payload = op.get("payload") or {}

        try:
            if operation == "mark":
                res = firebase_sync.mark_appointment(
                    op["appointment_id"], op["field"],
                    op["value"], op["payment_method"],
                )
                success = res.get("ok", False)

            elif operation == "register_patient":
                anon_id = op["appointment_id"]
                firebase_sync.register_patient(
                    anon_id,
                    price=payload.get("price", 0),
                    is_anonymous=bool(payload.get("is_anonymous", 0)),
                )
                _push_encrypted_patient_bg(payload)
                success = True

            elif operation == "approve":
                res = firebase_sync.approve_appointment(payload["appointment_id"])
                success = res.get("ok", False)
                if success:
                    for rejected_id in res.get("rejected", []):
                        db.update_cached_appointment_status(rejected_id, "cancelled")

            elif operation == "reject":
                res = firebase_sync.reject_appointment(payload["appointment_id"])
                success = res.get("ok", False)
                if success:
                    try:
                        firebase_sync.mark_appointment(payload["appointment_id"], "treated", False, None)
                        firebase_sync.mark_appointment(payload["appointment_id"], "paid", False, None)
                    except Exception:
                        pass

            elif operation == "delete":
                res = firebase_sync.delete_appointment(payload["appointment_id"])
                success = res.get("ok", False)

            elif operation == "reschedule":
                res = firebase_sync.reschedule_appointment(
                    payload["appointment_id"], payload["date"],
                    payload["time"], payload.get("duration_min"),
                )
                success = res.get("ok", False)

            elif operation == "approve_cancel":
                res = firebase_sync.approve_cancel_request(payload["appointment_id"])
                success = res.get("ok", False)

            elif operation == "reject_cancel":
                res = firebase_sync.reject_cancel_request(payload["appointment_id"])
                success = res.get("ok", False)

            elif operation == "update_patient":
                anon_id = payload.get("anonymous_id") or op["appointment_id"]
                firebase_sync.update_patient_price(
                    anon_id, payload.get("price", 0),
                    is_anonymous=bool(payload.get("is_anonymous", 0)),
                )
                _push_encrypted_patient_bg(payload)
                success = True

            elif operation == "set_anonymous":
                anon_id = payload.get("anonymous_id") or op["appointment_id"]
                firebase_sync.update_patient_price(
                    anon_id, payload.get("price", 0),
                    is_anonymous=bool(payload.get("is_anonymous", 0)),
                )
                success = True

            elif operation == "push_note":
                encrypted = db.encrypt_note(payload["content"])
                firebase_sync.push_treatment_note(
                    payload["anonymous_id"], payload["note_id"],
                    encrypted, payload.get("note_type", "freeform"),
                    payload.get("appt_date"), payload.get("appt_time"),
                )
                try:
                    db.mark_notes_synced([int(payload["note_id"])])
                except Exception:
                    pass
                success = True

            elif operation == "delete_note":
                firebase_sync.delete_treatment_note_firebase(
                    payload["anonymous_id"], payload["note_id"],
                )
                success = True

            elif operation == "sync_emergency_contacts":
                anon_id = payload["anonymous_id"]
                p_id = payload["patient_id"]
                contacts = db.get_emergency_contacts(p_id)
                firebase_sync.sync_emergency_contacts(anon_id, _encrypt_ec_list(contacts))
                success = True

            elif operation == "sync_referral":
                anon_id = payload["anonymous_id"]
                if payload.get("enabled") is False:
                    firebase_sync.sync_referral_agreement(anon_id, {"enabled": False})
                else:
                    encrypted_broker = db.encrypt_note(payload.get("broker_name", ""))
                    firebase_sync.sync_referral_agreement(anon_id, {
                        "encryptedBrokerName": encrypted_broker,
                        "percentage": payload.get("percentage", 0),
                        "totalSessions": payload.get("total_sessions", 0),
                        "enabled": True,
                    })
                success = True

            elif operation == "push_availability":
                res = firebase_sync.push_availability(payload)
                success = res.get("ok", False)

        except Exception:
            success = False

        if success:
            db.remove_sync_operation(op["id"])
        else:
            _app_logger.warning("Sync retry failed for op %s (%s)", op["id"], operation)
            db.increment_sync_retry(op["id"])

    return True


def _flush_sync_queue():
    """Periodically retry failed Firebase sync operations."""
    while True:
        time.sleep(60)  # check every 60 seconds
        try:
            _process_sync_queue()
        except Exception:
            _app_logger.error("Sync queue error", exc_info=True)


def _flush_sync_queue_once():
    """Single pass through the sync queue — called on login."""
    try:
        _process_sync_queue()
    except Exception:
        _app_logger.error("Sync queue single-pass error", exc_info=True)


# ========================
# Entry point
# ========================

if __name__ == "__main__":
    try:
        _app_logger.info("=" * 50)
        _app_logger.info("App starting — v%s (frozen=%s)", APP_VERSION, getattr(sys, "frozen", False))

        db.init_auth_db()
        _app_logger.info("Auth database initialized")

        # Initialize Firebase Admin SDK from embedded service account
        fb_init = firebase_sync.init_embedded()
        if fb_init["ok"]:
            _app_logger.info("Firebase Admin SDK initialized")
            print("  Firebase Admin SDK initialized")
        else:
            _app_logger.warning("Firebase init failed: %s", fb_init.get("error", "unknown"))
            print(f"  Firebase init: {fb_init.get('error', 'failed')}")

        # License and per-user data are loaded after login (see login/setup routes)
        _app_logger.info("Waiting for user login to load per-user data")

        print("=" * 40)
        print("  ClinicTor - Doctor App")
        print("  סגור את חלון הדפדפן כדי לעצור את התוכנה")
        print("=" * 40)

        # Background sync queue flusher — retries failed Firebase writes
        threading.Thread(target=_flush_sync_queue, daemon=True).start()

        # Background update checker — checks GitHub once per day
        threading.Thread(target=_check_for_updates, daemon=True).start()

        # Background license refresh — checks Firebase every hour
        threading.Thread(target=_refresh_license_periodically, daemon=True).start()

        # Flask in background daemon thread
        flask_thread = threading.Thread(
            target=lambda: app.run(
                debug=False, port=5000, host="127.0.0.1", use_reloader=False
            ),
            daemon=True,
        )
        flask_thread.start()
        time.sleep(1.0)  # let Flask bind the port
        _app_logger.info("Flask server started on port 5000")

        # Warm up Flask so first page load is instant
        try:
            requests.get("http://127.0.0.1:5000/login", timeout=3)
        except Exception:
            pass

        browser_proc = _open_app_window()
        _app_logger.info("Browser opened (tracked=%s)", browser_proc is not None)

        # Assign browser to Job Object so all its child processes
        # (GPU, renderer, network) die when Python exits
        _browser_job = _setup_browser_job(browser_proc)

        # Always monitor heartbeat to detect when the user closes the tab.
        # Even with a tracked browser process, the user may close just the
        # app tab while keeping the browser open — browser_proc.wait() would
        # never return in that case.  Heartbeat is the reliable signal.
        _HEARTBEAT_TIMEOUT = 30
        try:
            while True:
                time.sleep(5)
                elapsed = time.time() - _last_heartbeat
                if elapsed > _HEARTBEAT_TIMEOUT:
                    _app_logger.info("No heartbeat for %ds — shutting down", int(elapsed))
                    print("\n  לא זוהה חלון דפדפן פעיל – מכבה את השרת...")
                    break
                # Also check if the tracked browser process has exited
                if browser_proc and browser_proc.poll() is not None:
                    _app_logger.info("Browser process exited — shutting down")
                    print("\n  חלון הדפדפן נסגר – מכבה את השרת...")
                    break
        except KeyboardInterrupt:
            _app_logger.info("KeyboardInterrupt — shutting down")

    except Exception:
        _app_logger.critical("FATAL: App failed to start", exc_info=True)

    _app_logger.info("App exiting")
    os._exit(0)
