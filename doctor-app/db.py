import sqlite3
import os
import json
import random
import string
import re
import shutil
import bcrypt
from datetime import datetime

import sys as _sys

# PyInstaller compatibility: use exe directory when frozen
if getattr(_sys, "frozen", False):
    _BASE_DIR = os.path.dirname(_sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ── Persistent data directory ─────────────────────────────────────────────
# Stored in %APPDATA%\AnonimousQ so data survives app folder deletion/update.
_APPDATA      = os.environ.get("APPDATA", os.path.expanduser("~"))
DATA_DIR      = os.path.join(_APPDATA, "AnonimousQ")
_OLD_DATA_DIR = os.path.join(_BASE_DIR, "data")   # legacy location

DB_PATH       = os.path.join(DATA_DIR, "anonimousq.db")
SETTINGS_PATH = os.path.join(DATA_DIR, "settings.json")
PAYMENT_PATH  = os.path.join(DATA_DIR, "payment_settings.json")

# ── Backup directory ──────────────────────────────────────────────────────
_DOCS_DIR  = os.path.join(os.path.expanduser("~"), "Documents")
BACKUP_DIR = os.path.join(_DOCS_DIR, "AnonimousQ Backup")


def _migrate_old_data():
    """If data exists only in the old app folder, copy it to DATA_DIR."""
    os.makedirs(DATA_DIR, exist_ok=True)
    for fname in ("anonimousq.db", "settings.json",
                  "payment_settings.json", "firebase-service-account.json",
                  "secret.key"):
        old_path = os.path.join(_OLD_DATA_DIR, fname)
        new_path = os.path.join(DATA_DIR, fname)
        if os.path.exists(old_path) and not os.path.exists(new_path):
            shutil.copy2(old_path, new_path)


def _secure_data_dir():
    """Hide and protect the data directory on Windows.
    - Sets hidden + system attributes so it doesn't show in Explorer
    - Sets ACL to current user only (no other users can access)
    """
    if os.name != 'nt' or not os.path.isdir(DATA_DIR):
        return
    try:
        import subprocess
        # Hide the folder (hidden + system attributes)
        subprocess.run(
            ['attrib', '+H', '+S', DATA_DIR],
            capture_output=True, timeout=5
        )
        # Restrict access to current user only
        username = os.environ.get('USERNAME', '')
        if username:
            subprocess.run(
                ['icacls', DATA_DIR, '/inheritance:r',
                 '/grant:r', f'{username}:(OI)(CI)F'],
                capture_output=True, timeout=10
            )
    except Exception:
        pass  # non-critical, best-effort


def auto_backup():
    """Copy the SQLite DB to Documents/AnonimousQ Backup/ (once per day)."""
    if not os.path.exists(DB_PATH):
        return
    os.makedirs(BACKUP_DIR, exist_ok=True)
    today     = datetime.now().strftime("%Y-%m-%d")
    dest_name = f"anonimousq_{today}.db"
    dest_path = os.path.join(BACKUP_DIR, dest_name)
    if not os.path.exists(dest_path):
        shutil.copy2(DB_PATH, dest_path)


# ========================
# Time helpers
# ========================

def _time_to_minutes(t: str) -> int:
    """Convert 'HH:MM' string to total minutes from midnight."""
    try:
        h, m = t.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0


def compute_end_time(start_time: str, duration_min: int) -> str:
    """Return 'HH:MM' end-time string given a start time and duration in minutes."""
    total = _time_to_minutes(start_time) + duration_min
    return f"{(total // 60) % 24:02d}:{total % 60:02d}"


def ranges_overlap(s1: int, d1: int, s2: int, d2: int) -> bool:
    """Return True if [s1, s1+d1) overlaps [s2, s2+d2) (all in minutes)."""
    return s1 < s2 + d2 and s1 + d1 > s2


def _get_conn():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    _migrate_old_data()          # copy old data/ → %APPDATA%\AnonimousQ\ if needed
    os.makedirs(DATA_DIR, exist_ok=True)
    _secure_data_dir()           # hide folder + restrict ACL to current user only
    conn = _get_conn()
    c = conn.cursor()

    c.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id            INTEGER PRIMARY KEY CHECK(id = 1),
            username      TEXT NOT NULL,
            password_hash TEXT NOT NULL
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS patients (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            name         TEXT NOT NULL,
            anonymous_id TEXT UNIQUE NOT NULL,
            phone        TEXT DEFAULT '',
            notes        TEXT DEFAULT '',
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)

    c.execute("""
        CREATE TABLE IF NOT EXISTS sync_log (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            synced_at           TEXT DEFAULT CURRENT_TIMESTAMP,
            appointments_count  INTEGER DEFAULT 0,
            status              TEXT DEFAULT 'ok'
        )
    """)

    # Migrate: add price column to patients if missing
    try:
        conn.execute("ALTER TABLE patients ADD COLUMN price INTEGER DEFAULT 0")
    except Exception:
        pass

    # Migrate: add active column to patients if missing (1=active, 0=inactive)
    try:
        conn.execute("ALTER TABLE patients ADD COLUMN active INTEGER DEFAULT 1")
    except Exception:
        pass

    # Migrate: add is_anonymous column to patients if missing
    try:
        conn.execute("ALTER TABLE patients ADD COLUMN is_anonymous INTEGER DEFAULT 0")
    except Exception:
        pass

    # New tables for patient detail features (v2)
    _ensure_treatment_notes_table(conn)
    _ensure_emergency_contacts_table(conn)
    _ensure_referral_tables(conn)
    _ensure_encryption_keys_table(conn)

    conn.commit()
    conn.close()


# ========================
# Patient ID generation  (format: A234B)
# uppercase + 3 digits + uppercase — 676,000 combinations
# ========================

def _generate_candidate_id() -> str:
    """Generate a candidate ID where first letter == last letter (e.g. A334A, B939B)."""
    letter = random.choice(string.ascii_uppercase)
    digits = ''.join(random.choices(string.digits, k=3))
    return f"{letter}{digits}{letter}"


def _unique_patient_id(conn, extra_check=None) -> str:
    """Generate a patient ID that is unique locally (and optionally remotely).

    extra_check: optional callable(candidate) -> bool
                 Returns True if the ID is already taken remotely.
    """
    for _ in range(2000):
        candidate = _generate_candidate_id()
        row = conn.execute(
            "SELECT 1 FROM patients WHERE anonymous_id=?", (candidate,)
        ).fetchone()
        if row:
            continue
        if extra_check is not None and extra_check(candidate):
            continue
        return candidate
    raise RuntimeError("Could not generate a unique patient ID after 2000 attempts")


def generate_patient_id_preview(extra_check=None) -> str:
    """Generate a candidate patient ID for UI preview (not saved to DB).

    extra_check: optional callable(id)->bool for remote uniqueness verification.
    """
    conn = _get_conn()
    try:
        return _unique_patient_id(conn, extra_check=extra_check)
    finally:
        conn.close()


# ========================
# Auth / User
# ========================

def has_user() -> bool:
    conn = _get_conn()
    row = conn.execute("SELECT id FROM users LIMIT 1").fetchone()
    conn.close()
    return row is not None


def setup_user(username: str, password: str) -> dict:
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(12)).decode()
    try:
        conn = _get_conn()
        conn.execute(
            "INSERT OR REPLACE INTO users (id, username, password_hash) VALUES (1, ?, ?)",
            (username, hashed),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def verify_user(username: str, password: str):
    conn = _get_conn()
    row = conn.execute("SELECT username, password_hash FROM users LIMIT 1").fetchone()
    conn.close()
    if not row:
        return False, None
    if row["username"] != username:
        return False, None
    if not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        return False, None
    return True, row["username"]


def get_username():
    conn = _get_conn()
    row = conn.execute("SELECT username FROM users LIMIT 1").fetchone()
    conn.close()
    return row["username"] if row else None


def set_password(new_password: str):
    hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt(12)).decode()
    conn = _get_conn()
    conn.execute("UPDATE users SET password_hash=? WHERE id=1", (hashed,))
    conn.commit()
    conn.close()


def verify_current_password(password: str) -> bool:
    conn = _get_conn()
    row = conn.execute("SELECT password_hash FROM users LIMIT 1").fetchone()
    conn.close()
    if not row:
        return False
    return bcrypt.checkpw(password.encode(), row["password_hash"].encode())


# ========================
# Patients
# ========================

def get_patients() -> list:
    """Return active patients (active=1 or column missing — treated as 1)."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM patients WHERE COALESCE(active,1)=1 ORDER BY name COLLATE NOCASE"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_inactive_patients() -> list:
    """Return inactive patients (active=0)."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT * FROM patients WHERE COALESCE(active,1)=0 ORDER BY name COLLATE NOCASE"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def set_patient_active(patient_id: int, active: int) -> bool:
    """Set active=1 (return to active) or active=0 (end of treatment)."""
    try:
        conn = _get_conn()
        conn.execute("UPDATE patients SET active=? WHERE id=?", (int(active), patient_id))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"[DB] set_patient_active error: {e}")
        return False


def add_patient(name: str, phone: str = "", notes: str = "",
                firebase_check=None, price: float = 0,
                suggested_id: str = "", is_anonymous: int = 0) -> dict:
    """Add a new patient.
    suggested_id: if provided and still unique, use it (preserves the previewed ID).
    firebase_check: optional callable(id)->bool to verify remote uniqueness.
    """
    try:
        conn = _get_conn()
        # Use the previewed ID if it's still available
        anonymous_id = None
        if suggested_id and re.match(r'^[A-Za-z]\d{3}[A-Za-z]$', suggested_id):
            sid = suggested_id.upper()
            taken_locally  = conn.execute(
                "SELECT 1 FROM patients WHERE anonymous_id=?", (sid,)
            ).fetchone()
            taken_remotely = firebase_check(sid) if firebase_check else False
            if not taken_locally and not taken_remotely:
                anonymous_id = sid
        if not anonymous_id:
            anonymous_id = _unique_patient_id(conn, extra_check=firebase_check)
        conn.execute(
            "INSERT INTO patients (name, anonymous_id, phone, notes, price, is_anonymous) VALUES (?, ?, ?, ?, ?, ?)",
            (name, anonymous_id, phone, notes, float(price or 0), int(is_anonymous)),
        )
        conn.commit()
        conn.close()
        return {"ok": True, "anonymous_id": anonymous_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def update_patient(patient_id: int, name: str, phone: str, notes: str,
                   price: float = 0, is_anonymous: int = 0) -> dict:
    try:
        conn = _get_conn()
        conn.execute(
            "UPDATE patients SET name=?, phone=?, notes=?, price=?, is_anonymous=? WHERE id=?",
            (name, phone, notes, float(price or 0), int(is_anonymous), patient_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_patient(patient_id: int):
    conn = _get_conn()
    conn.execute("DELETE FROM patients WHERE id=?", (patient_id,))
    conn.commit()
    conn.close()


WALKIN_ID = "WALKIN"   # reserved ID for walk-in (unregistered) appointments


def get_anonymous_ids() -> set:
    """Return set of anonymous_ids for patients marked as anonymous."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT anonymous_id FROM patients WHERE COALESCE(is_anonymous,0)=1"
    ).fetchall()
    conn.close()
    return {r["anonymous_id"] for r in rows}


def get_uuid_map() -> dict:
    conn = _get_conn()
    rows = conn.execute("SELECT anonymous_id, name FROM patients").fetchall()
    conn.close()
    uuid_map = {r["anonymous_id"]: r["name"] for r in rows}
    uuid_map[WALKIN_ID] = "תור מזדמן"   # always present, never overwritten by a real patient
    return uuid_map


def get_price_map() -> dict:
    """Return {anonymous_id: price} for all patients."""
    conn = _get_conn()
    rows = conn.execute("SELECT anonymous_id, price FROM patients").fetchall()
    conn.close()
    return {r["anonymous_id"]: float(r["price"] or 0) for r in rows}


# ========================
# Settings (Availability)
# ========================

DEFAULT_AVAILABILITY = {
    "workingDays": [1, 2, 3, 4, 5],
    "workingHours": {"start": "09:00", "end": "17:00"},
    "slotDurationMin": 45,
    "blockedDates": [],
    "disableOnlineBooking": False,
}


def get_availability() -> dict:
    if not os.path.exists(SETTINGS_PATH):
        return dict(DEFAULT_AVAILABILITY)
    try:
        with open(SETTINGS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return dict(DEFAULT_AVAILABILITY)


def set_availability(availability: dict):
    os.makedirs(os.path.dirname(SETTINGS_PATH), exist_ok=True)
    with open(SETTINGS_PATH, "w", encoding="utf-8") as f:
        json.dump(availability, f, ensure_ascii=False, indent=2)


# ========================
# Sync Log
# ========================

def log_sync(count: int, status: str):
    conn = _get_conn()
    conn.execute(
        "INSERT INTO sync_log (synced_at, appointments_count, status) VALUES (?, ?, ?)",
        (datetime.now().isoformat(), count, status),
    )
    conn.execute("""
        DELETE FROM sync_log
        WHERE id NOT IN (SELECT id FROM sync_log ORDER BY id DESC LIMIT 50)
    """)
    conn.commit()
    conn.close()


def get_last_sync():
    conn = _get_conn()
    row = conn.execute("SELECT * FROM sync_log ORDER BY id DESC LIMIT 1").fetchone()
    conn.close()
    return dict(row) if row else None


# ========================
# Local / Demo Appointments
# ========================

def _ensure_local_appointments_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS local_appointments (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            anonymous_id TEXT NOT NULL,
            date         TEXT NOT NULL,
            time         TEXT NOT NULL,
            status       TEXT DEFAULT 'pending',
            duration_min INTEGER DEFAULT 45,
            created_at   TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    # Migrate existing tables that may not have these columns yet
    for col, col_def in [("duration_min", "INTEGER DEFAULT 45"),
                          ("treated",      "INTEGER DEFAULT 0"),
                          ("paid",         "INTEGER DEFAULT 0"),
                          ("payment_method", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE local_appointments ADD COLUMN {col} {col_def}")
        except Exception:
            pass


def get_local_appointments() -> list:
    conn = _get_conn()
    _ensure_local_appointments_table(conn)
    rows = conn.execute(
        "SELECT * FROM local_appointments WHERE status != 'cancelled' ORDER BY date, time"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["anonymousId"]  = d.pop("anonymous_id")
        d["durationMin"]  = d.pop("duration_min", None) or 45
        d["paymentMethod"] = d.pop("payment_method", None)
        result.append(d)
    return result


def get_all_local_appointments() -> list:
    """All local appointments (any status, including past) — for Firebase push."""
    conn = _get_conn()
    _ensure_local_appointments_table(conn)
    rows = conn.execute(
        "SELECT * FROM local_appointments ORDER BY date, time"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["anonymousId"]   = d.pop("anonymous_id")
        d["durationMin"]   = d.pop("duration_min", None) or 45
        d["paymentMethod"] = d.pop("payment_method", None)
        d["treated"]       = bool(d.get("treated", 0))
        d["paid"]          = bool(d.get("paid", 0))
        result.append(d)
    return result


def approve_local_appointment(appt_id: int) -> dict:
    try:
        conn = _get_conn()
        _ensure_local_appointments_table(conn)
        conn.execute(
            "UPDATE local_appointments SET status='booked' WHERE id=?", (appt_id,)
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def reject_local_appointment(appt_id: int) -> dict:
    try:
        conn = _get_conn()
        _ensure_local_appointments_table(conn)
        conn.execute(
            "UPDATE local_appointments SET status='cancelled', treated=0, paid=0, payment_method=NULL WHERE id=?",
            (appt_id,),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def mark_local_appointment(appt_id: int, field: str, value,
                           payment_method: str = None) -> dict:
    """Mark treated, paid, or status on a local appointment."""
    if field not in ("treated", "paid", "status"):
        return {"ok": False, "error": "שדה לא חוקי"}
    try:
        conn = _get_conn()
        _ensure_local_appointments_table(conn)
        # Add columns if they don't exist yet
        for col, col_def in [("treated", "INTEGER DEFAULT 0"),
                              ("paid",    "INTEGER DEFAULT 0"),
                              ("payment_method", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE local_appointments ADD COLUMN {col} {col_def}")
            except Exception:
                pass
        if field == "status":
            conn.execute(
                "UPDATE local_appointments SET status=? WHERE id=?",
                (value, appt_id),
            )
        else:
            conn.execute(
                f"UPDATE local_appointments SET {field}=? WHERE id=?",
                (1 if value else 0, appt_id),
            )
        if field == "paid":
            pm = payment_method if value else None
            conn.execute(
                "UPDATE local_appointments SET payment_method=? WHERE id=?",
                (pm, appt_id),
            )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_local_appointment(appt_id: int) -> dict:
    """Permanently delete a local appointment."""
    try:
        conn = _get_conn()
        _ensure_local_appointments_table(conn)
        conn.execute("DELETE FROM local_appointments WHERE id=?", (appt_id,))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_cached_appointment(firebase_id: str):
    """Permanently delete an appointment from the local cache."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    conn.execute("DELETE FROM cached_appointments WHERE firebase_id=?", (firebase_id,))
    conn.commit()
    conn.close()


def clear_local_appointments():
    conn = _get_conn()
    _ensure_local_appointments_table(conn)
    conn.execute("DELETE FROM local_appointments")
    conn.commit()
    conn.close()



# ========================
# Payment Settings (price + Bit/Paybox phone numbers)
# ========================

_DEFAULT_PAYMENT = {
    "defaultPrice": 0,
    "bitPhone":     "",
    "payboxPhone":  "",
    "bitLink":      "",
    "payboxLink":   "",
}


def get_payment_settings() -> dict:
    if not os.path.exists(PAYMENT_PATH):
        return dict(_DEFAULT_PAYMENT)
    try:
        with open(PAYMENT_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return dict(_DEFAULT_PAYMENT)


def set_payment_settings(settings: dict):
    os.makedirs(os.path.dirname(PAYMENT_PATH), exist_ok=True)
    with open(PAYMENT_PATH, "w", encoding="utf-8") as f:
        json.dump(settings, f, ensure_ascii=False, indent=2)


# ========================
# Firebase Cache (offline storage)
# ========================

def _ensure_cached_appointments_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS cached_appointments (
            firebase_id          TEXT PRIMARY KEY,
            anonymous_id         TEXT NOT NULL,
            date                 TEXT NOT NULL,
            time                 TEXT NOT NULL,
            status               TEXT DEFAULT 'pending',
            treated              INTEGER DEFAULT 0,
            paid                 INTEGER DEFAULT 0,
            payment_method       TEXT,
            duration_min         INTEGER DEFAULT 45,
            patient_marked_paid  INTEGER DEFAULT 0,
            synced_at            TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    for col, col_def in [("duration_min", "INTEGER DEFAULT 45"),
                          ("patient_marked_paid", "INTEGER DEFAULT 0"),
                          ("patient_payment_method", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE cached_appointments ADD COLUMN {col} {col_def}")
        except Exception:
            pass


def _appt_row(appt: dict) -> tuple:
    return (
        str(appt.get("id", "")),
        appt.get("anonymousId", ""),
        appt.get("date", ""),
        appt.get("time", ""),
        appt.get("status", "pending"),
        1 if appt.get("treated") else 0,
        1 if appt.get("paid") else 0,
        appt.get("paymentMethod"),
        int(appt.get("durationMin") or appt.get("duration_min") or 45),
        1 if appt.get("patientMarkedPaid") else 0,
        appt.get("patientPaymentMethod"),
        datetime.now().isoformat(),
    )


def cache_appointments(appointments: list, replace_all: bool = False):
    """Upsert appointments into local cache.
    replace_all=True replaces the entire cache (used for full sync)."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    if replace_all:
        conn.execute("DELETE FROM cached_appointments")
    for appt in appointments:
        conn.execute("""
            INSERT OR REPLACE INTO cached_appointments
            (firebase_id, anonymous_id, date, time, status, treated, paid,
             payment_method, duration_min, patient_marked_paid,
             patient_payment_method, synced_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, _appt_row(appt))
    conn.commit()
    conn.close()


def get_cached_appointments() -> list:
    """Active (non-cancelled) appointments from local cache — used when offline."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    rows = conn.execute(
        "SELECT * FROM cached_appointments WHERE status != 'cancelled' ORDER BY date, time"
    ).fetchall()
    conn.close()
    return [_cached_row_to_dict(r) for r in rows]


def update_cached_appointment(firebase_id: str, field: str, value,
                              payment_method: str = None):
    """Update a single field in the local cache."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    if field == "status":
        conn.execute(
            "UPDATE cached_appointments SET status=? WHERE firebase_id=?",
            (value, firebase_id),
        )
    elif field == "treated":
        conn.execute(
            "UPDATE cached_appointments SET treated=? WHERE firebase_id=?",
            (1 if value else 0, firebase_id),
        )
    elif field == "paid":
        pm = payment_method if value else None
        conn.execute(
            "UPDATE cached_appointments SET paid=?, payment_method=? WHERE firebase_id=?",
            (1 if value else 0, pm, firebase_id),
        )
    conn.commit()
    conn.close()


def is_appointment_treated(appt_id: str, source: str) -> bool:
    """Check if an appointment has been marked as treated."""
    conn = _get_conn()
    try:
        if source == "local":
            _ensure_local_appointments_table(conn)
            local_id = int(str(appt_id).replace("local-", ""))
            row = conn.execute(
                "SELECT treated FROM local_appointments WHERE id=?", (local_id,)
            ).fetchone()
        else:
            _ensure_cached_appointments_table(conn)
            row = conn.execute(
                "SELECT treated FROM cached_appointments WHERE firebase_id=?", (appt_id,)
            ).fetchone()
        return bool(row and row[0]) if row else False
    except Exception:
        return False
    finally:
        conn.close()


def update_cached_appointment_status(firebase_id: str, new_status: str):
    """Update the status of a cached appointment (e.g. pending → booked)."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    conn.execute(
        "UPDATE cached_appointments SET status=? WHERE firebase_id=?",
        (new_status, firebase_id),
    )
    conn.commit()
    conn.close()


def get_patient_appointments(anonymous_id: str) -> list:
    """All appointments for a patient — queries cache first, then local."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    rows = conn.execute(
        "SELECT * FROM cached_appointments WHERE anonymous_id=? ORDER BY date DESC, time DESC",
        (anonymous_id,),
    ).fetchall()
    if rows:
        conn.close()
        return [_cached_row_to_dict(r) for r in rows]
    # Fallback: local_appointments (demo mode)
    _ensure_local_appointments_table(conn)
    rows = conn.execute(
        "SELECT * FROM local_appointments WHERE anonymous_id=? ORDER BY date DESC, time DESC",
        (anonymous_id,),
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["anonymousId"]   = d.pop("anonymous_id")
        d["paymentMethod"] = d.pop("payment_method", None)
        d["durationMin"]   = d.pop("duration_min", None) or 45
        d["source"]        = "local"
        d["treated"]       = bool(d.get("treated"))
        d["paid"]          = bool(d.get("paid"))
        result.append(d)
    return result


def _cached_row_to_dict(row) -> dict:
    d = dict(row)
    d["anonymousId"]            = d.pop("anonymous_id")
    d["id"]                     = d.pop("firebase_id")
    d["paymentMethod"]          = d.pop("payment_method")
    d["durationMin"]            = d.pop("duration_min", None) or 45
    d["treated"]                = bool(d["treated"])
    d["paid"]                   = bool(d["paid"])
    d["patientMarkedPaid"]      = bool(d.pop("patient_marked_paid", 0))
    d["patientPaymentMethod"]   = d.pop("patient_payment_method", None)
    d["source"]                 = "firebase"
    return d


def get_all_cached_for_reports() -> list:
    """All appointments from cache + local (any status) — for reports and backup."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    _ensure_local_appointments_table(conn)

    cached = [_cached_row_to_dict(r) for r in conn.execute(
        "SELECT * FROM cached_appointments ORDER BY date DESC, time DESC"
    ).fetchall()]

    # Include local/demo appointments (use string id prefixed with 'local-')
    local_rows = conn.execute(
        "SELECT * FROM local_appointments ORDER BY date DESC, time DESC"
    ).fetchall()
    local = []
    for r in local_rows:
        d = dict(r)
        d["id"]            = f"local-{d['id']}"
        d["anonymousId"]   = d.pop("anonymous_id")
        d["paymentMethod"] = d.pop("payment_method", None)
        d["durationMin"]   = d.pop("duration_min", None) or 45
        d["treated"]       = bool(d.get("treated", 0))
        d["paid"]          = bool(d.get("paid", 0))
        d["source"]        = "local"
        local.append(d)

    conn.close()
    # Merge: cached first, then local entries whose date+time don't already exist in cached
    cached_keys = {(a["anonymousId"], a["date"], a["time"]) for a in cached}
    merged = cached + [a for a in local if (a["anonymousId"], a["date"], a["time"]) not in cached_keys]
    merged.sort(key=lambda a: (a["date"], a["time"]), reverse=True)
    return merged


def check_slot_conflict_cached(date: str, time: str, duration_min: int,
                                exclude_id: str = None,
                                exclude_pending: bool = False) -> bool:
    """Returns True if [time, time+duration_min) overlaps any conflicting cached appointment.

    Args:
        exclude_id:      firebase_id to exclude (for reschedule, avoids self-conflict).
        exclude_pending: if True, only booked appointments are checked (doctor creates over
                         a pending web-booking). If False (default), pending is also blocked.
    """
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    status_clause = "status = 'booked'" if exclude_pending else "status != 'cancelled'"
    if exclude_id:
        rows = conn.execute(
            f"SELECT time, duration_min FROM cached_appointments "
            f"WHERE date=? AND {status_clause} AND firebase_id != ?",
            (date, exclude_id),
        ).fetchall()
    else:
        rows = conn.execute(
            f"SELECT time, duration_min FROM cached_appointments "
            f"WHERE date=? AND {status_clause}",
            (date,),
        ).fetchall()
    conn.close()
    new_start = _time_to_minutes(time)
    for row in rows:
        ex_start = _time_to_minutes(row["time"])
        ex_dur   = row["duration_min"] or 45
        if ranges_overlap(new_start, duration_min, ex_start, ex_dur):
            return True
    return False


def check_slot_conflict_local(date: str, time: str, duration_min: int,
                               exclude_id: int = None) -> bool:
    """Returns True if [time, time+duration_min) overlaps any non-cancelled local appointment."""
    conn = _get_conn()
    _ensure_local_appointments_table(conn)
    if exclude_id is not None:
        rows = conn.execute(
            "SELECT time, duration_min FROM local_appointments "
            "WHERE date=? AND status != 'cancelled' AND id != ?",
            (date, exclude_id),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT time, duration_min FROM local_appointments "
            "WHERE date=? AND status != 'cancelled'",
            (date,),
        ).fetchall()
    conn.close()
    new_start = _time_to_minutes(time)
    for row in rows:
        ex_start = _time_to_minutes(row["time"])
        ex_dur   = row["duration_min"] or 45
        if ranges_overlap(new_start, duration_min, ex_start, ex_dur):
            return True
    return False


def create_local_appointment(anonymous_id: str, date: str, time: str,
                              status: str = "booked", duration_min: int = 45) -> dict:
    """Create a new appointment in local_appointments (demo/offline mode)."""
    try:
        conn = _get_conn()
        _ensure_local_appointments_table(conn)
        cur = conn.execute(
            "INSERT INTO local_appointments "
            "(anonymous_id, date, time, status, duration_min) VALUES (?, ?, ?, ?, ?)",
            (anonymous_id, date, time, status, duration_min),
        )
        new_id = cur.lastrowid
        conn.commit()
        conn.close()
        return {"ok": True, "id": new_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def reschedule_local_appointment(appt_id: int, new_date: str, new_time: str,
                                  duration_min: int = None) -> dict:
    """Move a local appointment to a new date/time (optionally update duration)."""
    try:
        conn = _get_conn()
        _ensure_local_appointments_table(conn)
        if duration_min is not None:
            conn.execute(
                "UPDATE local_appointments SET date=?, time=?, duration_min=? WHERE id=?",
                (new_date, new_time, duration_min, appt_id),
            )
        else:
            conn.execute(
                "UPDATE local_appointments SET date=?, time=? WHERE id=?",
                (new_date, new_time, appt_id),
            )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def reschedule_cached_appointment(firebase_id: str, new_date: str, new_time: str,
                                   duration_min: int = None):
    """Update date/time (and optionally duration) of a cached appointment."""
    conn = _get_conn()
    _ensure_cached_appointments_table(conn)
    if duration_min is not None:
        conn.execute(
            "UPDATE cached_appointments SET date=?, time=?, duration_min=? WHERE firebase_id=?",
            (new_date, new_time, duration_min, firebase_id),
        )
    else:
        conn.execute(
            "UPDATE cached_appointments SET date=?, time=? WHERE firebase_id=?",
            (new_date, new_time, firebase_id),
        )
    conn.commit()
    conn.close()


def populate_demo_data() -> dict:
    """Insert sample patients + appointments for demo testing."""
    from datetime import date, timedelta

    # (name, phone, notes, price)
    demo_patients = [
        ("ישראל ישראלי", "050-1111111", "",                   350),
        ("שרה כהן",      "052-2222222", "אלרגיה לפניצילין",  400),
        ("דוד לוי",      "054-3333333", "",                   300),
        ("מרים אברהם",   "050-4444444", "לחץ דם גבוה",       450),
        ("יוסף מזרחי",  "053-5555555", "",                   350),
    ]

    conn = _get_conn()
    _ensure_local_appointments_table(conn)
    conn.execute("DELETE FROM local_appointments")

    patient_ids = []
    added_patients = 0

    for name, phone, notes, price in demo_patients:
        row = conn.execute(
            "SELECT anonymous_id FROM patients WHERE name=?", (name,)
        ).fetchone()
        if row:
            # Update price if patient already exists
            conn.execute("UPDATE patients SET price=? WHERE name=?", (price, name))
            patient_ids.append(row["anonymous_id"])
        else:
            anon_id = _unique_patient_id(conn)
            try:
                conn.execute(
                    "INSERT INTO patients (name, anonymous_id, phone, notes, price) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (name, anon_id, phone, notes, price),
                )
                patient_ids.append(anon_id)
                added_patients += 1
            except Exception:
                existing = conn.execute(
                    "SELECT anonymous_id FROM patients WHERE name=?", (name,)
                ).fetchone()
                if existing:
                    patient_ids.append(existing["anonymous_id"])

    if not patient_ids:
        all_p = conn.execute("SELECT anonymous_id FROM patients LIMIT 5").fetchall()
        patient_ids = [r["anonymous_id"] for r in all_p]

    today = date.today()
    times = ["09:00", "10:00", "11:00", "14:00", "15:00", "16:00"]
    appointment_count = 0

    # ── Past appointments (for history / mark-paid testing) ──────────────────
    past_scenarios = [
        # (days_ago, time, status, treated, paid, payment_method)
        (14, "09:00", "booked", 1, 1, "bit"),
        (14, "11:00", "booked", 1, 1, "cash"),
        (10, "10:00", "booked", 1, 0, None),   # treated but not paid
        (10, "14:00", "booked", 0, 0, None),
        (7,  "09:00", "booked", 1, 1, "paybox"),
        (7,  "15:00", "booked", 1, 0, None),   # treated but not paid → mark-paid demo
        (3,  "10:00", "booked", 1, 1, "bit"),
        (3,  "11:00", "booked", 0, 0, None),
    ]
    for days_ago, t, status, treated, paid, pm in past_scenarios:
        appt_date = today - timedelta(days=days_ago)
        if appt_date.weekday() >= 5:
            appt_date -= timedelta(days=1)
        patient = patient_ids[appointment_count % len(patient_ids)]
        conn.execute(
            "INSERT INTO local_appointments "
            "(anonymous_id, date, time, status, treated, paid, payment_method) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (patient, appt_date.isoformat(), t, status, treated, paid, pm),
        )
        appointment_count += 1

    # ── Future appointments ──────────────────────────────────────────────────
    for i in range(1, 15):
        appt_date = today + timedelta(days=i)
        if appt_date.weekday() >= 5:
            continue
        day_times = random.sample(times, random.randint(1, 3))
        for t in day_times:
            patient = random.choice(patient_ids)
            # Alternate between booked and pending for demo variety
            status = "pending" if appointment_count % 3 == 0 else "booked"
            conn.execute(
                "INSERT INTO local_appointments (anonymous_id, date, time, status) VALUES (?, ?, ?, ?)",
                (patient, appt_date.isoformat(), t, status),
            )
            appointment_count += 1

    conn.commit()
    conn.close()
    return {"ok": True, "patients": added_patients, "appointments": appointment_count}


# ========================
# Table Definitions (Patient Detail v2)
# ========================

def _ensure_treatment_notes_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS treatment_notes (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id       INTEGER NOT NULL,
            anonymous_id     TEXT NOT NULL,
            appointment_date TEXT,
            appointment_time TEXT,
            note_type        TEXT NOT NULL DEFAULT 'freeform',
            content          TEXT NOT NULL DEFAULT '',
            created_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at       TEXT DEFAULT CURRENT_TIMESTAMP,
            synced           INTEGER DEFAULT 0
        )
    """)


def _ensure_emergency_contacts_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS emergency_contacts (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id    INTEGER NOT NULL,
            anonymous_id  TEXT NOT NULL,
            contact_name  TEXT NOT NULL,
            contact_phone TEXT NOT NULL,
            sort_order    INTEGER DEFAULT 0,
            synced        INTEGER DEFAULT 0
        )
    """)


def _ensure_referral_tables(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS referral_agreements (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            patient_id     INTEGER NOT NULL UNIQUE,
            anonymous_id   TEXT NOT NULL,
            broker_name    TEXT NOT NULL DEFAULT '',
            percentage     REAL NOT NULL DEFAULT 0,
            total_sessions INTEGER NOT NULL DEFAULT 0,
            enabled        INTEGER DEFAULT 1,
            created_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at     TEXT DEFAULT CURRENT_TIMESTAMP,
            synced         INTEGER DEFAULT 0
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS referral_payments (
            id               INTEGER PRIMARY KEY AUTOINCREMENT,
            referral_id      INTEGER NOT NULL,
            appointment_date TEXT NOT NULL,
            appointment_time TEXT NOT NULL,
            amount           REAL NOT NULL DEFAULT 0,
            paid_to_broker   INTEGER DEFAULT 0,
            paid_at          TEXT
        )
    """)


def _ensure_encryption_keys_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS encryption_keys (
            id         INTEGER PRIMARY KEY CHECK(id = 1),
            fernet_key TEXT NOT NULL
        )
    """)
    # Migrate: add columns for password-derived encryption (v2)
    for col, default in [
        ("pbkdf2_salt", "''"),
        ("encryption_ver", "1"),
        ("migrated_at", "''"),
    ]:
        try:
            conn.execute(
                f"ALTER TABLE encryption_keys ADD COLUMN {col} TEXT DEFAULT {default}"
            )
        except Exception:
            pass  # column already exists


# ========================
# Encryption Helpers
# ========================

import crypto_utils


def _get_or_create_legacy_fernet():
    """Get or create the LEGACY random Fernet key (encryption_ver=1).
    Used only for migration from v1 → v2.
    """
    from cryptography.fernet import Fernet
    conn = _get_conn()
    _ensure_encryption_keys_table(conn)
    row = conn.execute("SELECT fernet_key FROM encryption_keys WHERE id=1").fetchone()
    if row:
        key = row["fernet_key"]
    else:
        key = Fernet.generate_key().decode()
        conn.execute("INSERT INTO encryption_keys (id, fernet_key) VALUES (1, ?)", (key,))
        conn.commit()
    conn.close()
    return Fernet(key.encode() if isinstance(key, str) else key)


def get_legacy_fernet():
    """Return the legacy (v1) random Fernet for migration purposes."""
    return _get_or_create_legacy_fernet()


def encrypt_note(plaintext: str) -> str:
    """Encrypt using the password-derived key (v2) or legacy key (v1)."""
    if crypto_utils.is_ready():
        return crypto_utils.encrypt(plaintext)
    # Fallback to legacy key (shouldn't happen in normal flow after migration)
    f = _get_or_create_legacy_fernet()
    return f.encrypt(plaintext.encode('utf-8')).decode('utf-8')


def decrypt_note(ciphertext: str) -> str:
    """Decrypt using the password-derived key (v2) or legacy key (v1)."""
    if crypto_utils.is_ready():
        return crypto_utils.decrypt(ciphertext)
    # Fallback to legacy key
    f = _get_or_create_legacy_fernet()
    try:
        return f.decrypt(ciphertext.encode('utf-8')).decode('utf-8')
    except Exception:
        return "[שגיאת פענוח]"


# ── Encryption metadata (for password-derived encryption v2) ─────────────────

def get_encryption_version() -> int:
    """Return current encryption version: 1=legacy random key, 2=password-derived."""
    conn = _get_conn()
    _ensure_encryption_keys_table(conn)
    row = conn.execute("SELECT encryption_ver FROM encryption_keys WHERE id=1").fetchone()
    conn.close()
    if not row:
        return 0  # no encryption key at all (fresh install)
    return int(row["encryption_ver"] or 1)


def get_encryption_metadata() -> dict:
    """Return encryption metadata: salt, version, migrated_at."""
    conn = _get_conn()
    _ensure_encryption_keys_table(conn)
    row = conn.execute(
        "SELECT pbkdf2_salt, encryption_ver, migrated_at FROM encryption_keys WHERE id=1"
    ).fetchone()
    conn.close()
    if not row:
        return {"salt": "", "version": 0, "migrated_at": ""}
    return {
        "salt": row["pbkdf2_salt"] or "",
        "version": int(row["encryption_ver"] or 1),
        "migrated_at": row["migrated_at"] or "",
    }


def save_encryption_metadata(salt_b64: str, version: int):
    """Save PBKDF2 salt and encryption version after migration."""
    conn = _get_conn()
    _ensure_encryption_keys_table(conn)
    now = datetime.now().isoformat()
    row = conn.execute("SELECT id FROM encryption_keys WHERE id=1").fetchone()
    if row:
        conn.execute(
            """UPDATE encryption_keys
               SET pbkdf2_salt=?, encryption_ver=?, migrated_at=?
               WHERE id=1""",
            (salt_b64, str(version), now),
        )
    else:
        conn.execute(
            """INSERT INTO encryption_keys (id, fernet_key, pbkdf2_salt, encryption_ver, migrated_at)
               VALUES (1, '', ?, ?, ?)""",
            (salt_b64, str(version), now),
        )
    conn.commit()
    conn.close()


def delete_legacy_fernet_key():
    """Clear the legacy random Fernet key after successful migration.
    Keeps the row but blanks the fernet_key field.
    """
    conn = _get_conn()
    conn.execute("UPDATE encryption_keys SET fernet_key='' WHERE id=1")
    conn.commit()
    conn.close()


# ── Bulk data access for migration / new-device sync ─────────────────────────

def get_all_patients_for_sync() -> list:
    """Return ALL patients (active + inactive) with fields needed for encryption sync."""
    conn = _get_conn()
    rows = conn.execute(
        "SELECT id, anonymous_id, name, phone, notes, price, is_anonymous, active FROM patients"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_treatment_notes_for_sync() -> list:
    """Return ALL treatment notes for encryption re-encryption."""
    conn = _get_conn()
    _ensure_treatment_notes_table(conn)
    rows = conn.execute(
        """SELECT id, patient_id, anonymous_id, appointment_date, appointment_time,
                  note_type, content, created_at, updated_at
           FROM treatment_notes"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_emergency_contacts_for_sync() -> list:
    """Return ALL emergency contacts for encryption sync."""
    conn = _get_conn()
    _ensure_emergency_contacts_table(conn)
    rows = conn.execute(
        """SELECT id, patient_id, anonymous_id, contact_name, contact_phone, sort_order
           FROM emergency_contacts"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_referral_agreements_for_sync() -> list:
    """Return ALL referral agreements for encryption sync."""
    conn = _get_conn()
    _ensure_referral_tables(conn)
    rows = conn.execute(
        """SELECT id, patient_id, anonymous_id, broker_name, percentage, total_sessions, enabled
           FROM referral_agreements"""
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_treatment_note_content(note_id: int, encrypted_content: str):
    """Directly update a note's content (used during re-encryption migration)."""
    conn = _get_conn()
    conn.execute(
        "UPDATE treatment_notes SET content=?, synced=0 WHERE id=?",
        (encrypted_content, note_id),
    )
    conn.commit()
    conn.close()


def bulk_save_patients_from_firebase(patients_data: list):
    """Save patients downloaded from Firebase (new device sync).
    patients_data: list of dicts with {anonymous_id, name, phone, notes, price, is_anonymous, active}
    """
    conn = _get_conn()
    for p in patients_data:
        existing = conn.execute(
            "SELECT id FROM patients WHERE anonymous_id=?", (p["anonymous_id"],)
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE patients SET name=?, phone=?, notes=?, price=?, is_anonymous=?, active=?
                   WHERE anonymous_id=?""",
                (p["name"], p.get("phone", ""), p.get("notes", ""),
                 float(p.get("price", 0)), int(p.get("is_anonymous", 0)),
                 int(p.get("active", 1)), p["anonymous_id"]),
            )
        else:
            conn.execute(
                """INSERT INTO patients (name, anonymous_id, phone, notes, price, is_anonymous, active)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (p["name"], p["anonymous_id"], p.get("phone", ""),
                 p.get("notes", ""), float(p.get("price", 0)),
                 int(p.get("is_anonymous", 0)), int(p.get("active", 1))),
            )
    conn.commit()
    conn.close()


def bulk_save_notes_from_firebase(notes_data: list):
    """Save treatment notes downloaded from Firebase (new device sync).
    notes_data: list of dicts with encrypted content.
    """
    conn = _get_conn()
    _ensure_treatment_notes_table(conn)
    for n in notes_data:
        patient_id = n.get("patient_id", 0)
        anonymous_id = n.get("anonymous_id", "")
        # Check if we already have this note (by anonymous_id + created_at)
        existing = conn.execute(
            "SELECT id FROM treatment_notes WHERE anonymous_id=? AND created_at=?",
            (anonymous_id, n.get("created_at", "")),
        ).fetchone()
        if not existing:
            conn.execute(
                """INSERT INTO treatment_notes
                   (patient_id, anonymous_id, appointment_date, appointment_time,
                    note_type, content, created_at, updated_at, synced)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, 1)""",
                (patient_id, anonymous_id,
                 n.get("appointment_date"), n.get("appointment_time"),
                 n.get("note_type", "freeform"), n.get("content", ""),
                 n.get("created_at", ""), n.get("updated_at", "")),
            )
    conn.commit()
    conn.close()


def bulk_save_emergency_contacts_from_firebase(contacts_data: list):
    """Save emergency contacts downloaded from Firebase (new device sync)."""
    conn = _get_conn()
    _ensure_emergency_contacts_table(conn)
    for c in contacts_data:
        patient_id = c.get("patient_id", 0)
        anonymous_id = c.get("anonymous_id", "")
        # Avoid duplicates: check by patient_id + contact_name
        existing = conn.execute(
            "SELECT id FROM emergency_contacts WHERE patient_id=? AND contact_name=?",
            (patient_id, c.get("contact_name", "")),
        ).fetchone()
        if not existing:
            count = conn.execute(
                "SELECT COUNT(*) FROM emergency_contacts WHERE patient_id=?",
                (patient_id,),
            ).fetchone()[0]
            conn.execute(
                """INSERT INTO emergency_contacts
                   (patient_id, anonymous_id, contact_name, contact_phone, sort_order, synced)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (patient_id, anonymous_id, c.get("contact_name", ""),
                 c.get("contact_phone", ""), count),
            )
    conn.commit()
    conn.close()


def bulk_save_referrals_from_firebase(referrals_data: list):
    """Save referral agreements downloaded from Firebase (new device sync)."""
    conn = _get_conn()
    _ensure_referral_tables(conn)
    for r in referrals_data:
        patient_id = r.get("patient_id", 0)
        anonymous_id = r.get("anonymous_id", "")
        existing = conn.execute(
            "SELECT id FROM referral_agreements WHERE patient_id=?",
            (patient_id,),
        ).fetchone()
        if not existing:
            conn.execute(
                """INSERT INTO referral_agreements
                   (patient_id, anonymous_id, broker_name, percentage,
                    total_sessions, enabled, synced)
                   VALUES (?, ?, ?, ?, ?, ?, 1)""",
                (patient_id, anonymous_id, r.get("broker_name", ""),
                 float(r.get("percentage", 0)),
                 int(r.get("total_sessions", 0)),
                 int(r.get("enabled", 1))),
            )
    conn.commit()
    conn.close()


# ========================
# Patient Detail Lookup
# ========================

def get_patient_by_id(patient_id: int):
    conn = _get_conn()
    row = conn.execute("SELECT * FROM patients WHERE id=?", (patient_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_patient_by_anonymous_id(anonymous_id: str):
    conn = _get_conn()
    row = conn.execute("SELECT * FROM patients WHERE anonymous_id=?", (anonymous_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


# ========================
# Treatment Notes CRUD
# ========================

def add_treatment_note(patient_id: int, anonymous_id: str,
                       content: str, note_type: str = "freeform",
                       appointment_date: str = None,
                       appointment_time: str = None) -> dict:
    try:
        conn = _get_conn()
        _ensure_treatment_notes_table(conn)
        now = datetime.now().isoformat()
        cur = conn.execute(
            """INSERT INTO treatment_notes
               (patient_id, anonymous_id, appointment_date, appointment_time,
                note_type, content, created_at, updated_at, synced)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)""",
            (patient_id, anonymous_id, appointment_date, appointment_time,
             note_type, content, now, now),
        )
        conn.commit()
        note_id = cur.lastrowid
        conn.close()
        return {"ok": True, "id": note_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_treatment_notes(patient_id: int) -> list:
    conn = _get_conn()
    _ensure_treatment_notes_table(conn)
    rows = conn.execute(
        """SELECT * FROM treatment_notes
           WHERE patient_id=?
           ORDER BY COALESCE(appointment_date, created_at) DESC,
                    COALESCE(appointment_time, '') DESC,
                    created_at DESC""",
        (patient_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_treatment_note(note_id: int, content: str) -> dict:
    try:
        conn = _get_conn()
        _ensure_treatment_notes_table(conn)
        conn.execute(
            "UPDATE treatment_notes SET content=?, updated_at=?, synced=0 WHERE id=?",
            (content, datetime.now().isoformat(), note_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_treatment_note(note_id: int) -> dict:
    try:
        conn = _get_conn()
        _ensure_treatment_notes_table(conn)
        conn.execute("DELETE FROM treatment_notes WHERE id=?", (note_id,))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_unsynced_notes(patient_id: int) -> list:
    conn = _get_conn()
    _ensure_treatment_notes_table(conn)
    rows = conn.execute(
        "SELECT * FROM treatment_notes WHERE patient_id=? AND synced=0",
        (patient_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_notes_synced(note_ids: list):
    if not note_ids:
        return
    conn = _get_conn()
    placeholders = ','.join('?' for _ in note_ids)
    conn.execute(
        f"UPDATE treatment_notes SET synced=1 WHERE id IN ({placeholders})",
        note_ids,
    )
    conn.commit()
    conn.close()


# ========================
# Emergency Contacts CRUD
# ========================

def get_emergency_contacts(patient_id: int) -> list:
    conn = _get_conn()
    _ensure_emergency_contacts_table(conn)
    rows = conn.execute(
        "SELECT * FROM emergency_contacts WHERE patient_id=? ORDER BY sort_order",
        (patient_id,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def add_emergency_contact(patient_id: int, anonymous_id: str,
                          name: str, phone: str) -> dict:
    try:
        conn = _get_conn()
        _ensure_emergency_contacts_table(conn)
        count = conn.execute(
            "SELECT COUNT(*) FROM emergency_contacts WHERE patient_id=?",
            (patient_id,),
        ).fetchone()[0]
        if count >= 3:
            conn.close()
            return {"ok": False, "error": "מקסימום 3 אנשי קשר לחירום"}
        cur = conn.execute(
            """INSERT INTO emergency_contacts
               (patient_id, anonymous_id, contact_name, contact_phone, sort_order, synced)
               VALUES (?, ?, ?, ?, ?, 0)""",
            (patient_id, anonymous_id, name, phone, count),
        )
        conn.commit()
        ec_id = cur.lastrowid
        conn.close()
        return {"ok": True, "id": ec_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def update_emergency_contact(ec_id: int, name: str, phone: str) -> dict:
    try:
        conn = _get_conn()
        _ensure_emergency_contacts_table(conn)
        conn.execute(
            "UPDATE emergency_contacts SET contact_name=?, contact_phone=?, synced=0 WHERE id=?",
            (name, phone, ec_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_emergency_contact(ec_id: int) -> dict:
    try:
        conn = _get_conn()
        _ensure_emergency_contacts_table(conn)
        conn.execute("DELETE FROM emergency_contacts WHERE id=?", (ec_id,))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ========================
# Referral Agreements CRUD
# ========================

def get_referral_agreement(patient_id: int):
    conn = _get_conn()
    _ensure_referral_tables(conn)
    row = conn.execute(
        "SELECT * FROM referral_agreements WHERE patient_id=?",
        (patient_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def upsert_referral_agreement(patient_id: int, anonymous_id: str,
                               broker_name: str, percentage: float,
                               total_sessions: int) -> dict:
    try:
        conn = _get_conn()
        _ensure_referral_tables(conn)
        now = datetime.now().isoformat()
        existing = conn.execute(
            "SELECT id FROM referral_agreements WHERE patient_id=?",
            (patient_id,),
        ).fetchone()
        if existing:
            conn.execute(
                """UPDATE referral_agreements
                   SET broker_name=?, percentage=?, total_sessions=?,
                       enabled=1, updated_at=?, synced=0
                   WHERE patient_id=?""",
                (broker_name, percentage, total_sessions, now, patient_id),
            )
            ref_id = existing["id"]
        else:
            cur = conn.execute(
                """INSERT INTO referral_agreements
                   (patient_id, anonymous_id, broker_name, percentage,
                    total_sessions, enabled, created_at, updated_at, synced)
                   VALUES (?, ?, ?, ?, ?, 1, ?, ?, 0)""",
                (patient_id, anonymous_id, broker_name, percentage,
                 total_sessions, now, now),
            )
            ref_id = cur.lastrowid
        conn.commit()
        conn.close()
        return {"ok": True, "id": ref_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_referral_agreement(patient_id: int) -> dict:
    try:
        conn = _get_conn()
        _ensure_referral_tables(conn)
        ref = conn.execute(
            "SELECT id FROM referral_agreements WHERE patient_id=?",
            (patient_id,),
        ).fetchone()
        if ref:
            conn.execute("DELETE FROM referral_payments WHERE referral_id=?", (ref["id"],))
            conn.execute("DELETE FROM referral_agreements WHERE id=?", (ref["id"],))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def mark_referral_paid(payment_id: int) -> dict:
    try:
        conn = _get_conn()
        conn.execute(
            "UPDATE referral_payments SET paid_to_broker=1, paid_at=? WHERE id=?",
            (datetime.now().isoformat(), payment_id),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def unmark_referral_paid(payment_id: int) -> dict:
    try:
        conn = _get_conn()
        conn.execute(
            "UPDATE referral_payments SET paid_to_broker=0, paid_at=NULL WHERE id=?",
            (payment_id,),
        )
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def calculate_referral_summary(patient_id: int, patient_price: float) -> dict:
    """Calculate the referral fee summary for a patient.
    Auto-creates referral_payment rows for treated sessions within the agreement window.
    """
    conn = _get_conn()
    _ensure_referral_tables(conn)
    _ensure_cached_appointments_table(conn)
    _ensure_local_appointments_table(conn)

    ref = conn.execute(
        "SELECT * FROM referral_agreements WHERE patient_id=? AND enabled=1",
        (patient_id,),
    ).fetchone()
    if not ref:
        conn.close()
        return None

    ref = dict(ref)
    anon_id = ref["anonymous_id"]
    percentage = ref["percentage"]
    total_sessions = ref["total_sessions"]
    per_session_fee = round(patient_price * (percentage / 100.0), 2)
    total_referral_fee = round(per_session_fee * total_sessions, 2)

    # Get all treated appointments for this patient, sorted by date ASC
    treated = []
    for row in conn.execute(
        """SELECT date, time FROM cached_appointments
           WHERE anonymous_id=? AND treated=1
           ORDER BY date ASC, time ASC""",
        (anon_id,),
    ).fetchall():
        treated.append((row["date"], row["time"]))
    for row in conn.execute(
        """SELECT date, time FROM local_appointments
           WHERE anonymous_id=? AND treated=1
           ORDER BY date ASC, time ASC""",
        (anon_id,),
    ).fetchall():
        key = (row["date"], row["time"])
        if key not in treated:
            treated.append(key)
    treated.sort()

    effective_sessions = min(len(treated), total_sessions)
    owed_so_far = round(per_session_fee * effective_sessions, 2)

    # Auto-create referral_payment rows for treated sessions in the window
    for date_val, time_val in treated[:total_sessions]:
        exists = conn.execute(
            """SELECT 1 FROM referral_payments
               WHERE referral_id=? AND appointment_date=? AND appointment_time=?""",
            (ref["id"], date_val, time_val),
        ).fetchone()
        if not exists:
            conn.execute(
                """INSERT INTO referral_payments
                   (referral_id, appointment_date, appointment_time, amount)
                   VALUES (?, ?, ?, ?)""",
                (ref["id"], date_val, time_val, per_session_fee),
            )
    conn.commit()

    # Fetch all payment records
    payments = [dict(r) for r in conn.execute(
        """SELECT * FROM referral_payments
           WHERE referral_id=?
           ORDER BY appointment_date ASC, appointment_time ASC""",
        (ref["id"],),
    ).fetchall()]
    conn.close()

    paid_to_broker = sum(p["amount"] for p in payments if p["paid_to_broker"])
    unpaid_to_broker = round(owed_so_far - paid_to_broker, 2)

    return {
        "broker_name": ref["broker_name"],
        "percentage": percentage,
        "total_sessions": total_sessions,
        "per_session_fee": per_session_fee,
        "total_referral_fee": total_referral_fee,
        "treated_sessions": len(treated),
        "effective_sessions": effective_sessions,
        "remaining_sessions": max(0, total_sessions - len(treated)),
        "owed_so_far": owed_so_far,
        "paid_to_broker": round(paid_to_broker, 2),
        "unpaid_to_broker": max(0, unpaid_to_broker),
        "payments": payments,
    }


# ═══════════════════════════════════════════════════════════════
# Firebase sync queue — stores operations that failed to sync
# ═══════════════════════════════════════════════════════════════

def _ensure_sync_queue_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS firebase_sync_queue (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            appointment_id  TEXT NOT NULL,
            operation       TEXT NOT NULL,
            field           TEXT,
            value           TEXT,
            payment_method  TEXT,
            created_at      TEXT DEFAULT CURRENT_TIMESTAMP,
            retries         INTEGER DEFAULT 0
        )
    """)


def enqueue_firebase_sync(appointment_id: str, operation: str,
                          field: str = None, value=None,
                          payment_method: str = None):
    """Add a failed Firebase operation to the retry queue."""
    conn = _get_conn()
    _ensure_sync_queue_table(conn)
    conn.execute(
        """INSERT INTO firebase_sync_queue
           (appointment_id, operation, field, value, payment_method)
           VALUES (?, ?, ?, ?, ?)""",
        (appointment_id, operation, field, json.dumps(value), payment_method),
    )
    conn.commit()
    conn.close()


def get_pending_sync_operations() -> list:
    """Return all pending sync operations ordered by creation time."""
    conn = _get_conn()
    _ensure_sync_queue_table(conn)
    rows = conn.execute(
        "SELECT * FROM firebase_sync_queue ORDER BY created_at ASC"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        d = dict(r)
        d["value"] = json.loads(d["value"]) if d["value"] else None
        result.append(d)
    return result


def remove_sync_operation(op_id: int):
    """Remove a successfully synced operation from the queue."""
    conn = _get_conn()
    conn.execute("DELETE FROM firebase_sync_queue WHERE id=?", (op_id,))
    conn.commit()
    conn.close()


def increment_sync_retry(op_id: int):
    """Increment retry count for a failed sync operation."""
    conn = _get_conn()
    conn.execute(
        "UPDATE firebase_sync_queue SET retries=retries+1 WHERE id=?", (op_id,)
    )
    conn.commit()
    conn.close()


def clear_stale_sync_operations(max_retries: int = 50):
    """Remove operations that have exceeded max retries."""
    conn = _get_conn()
    _ensure_sync_queue_table(conn)
    conn.execute(
        "DELETE FROM firebase_sync_queue WHERE retries >= ?", (max_retries,)
    )
    conn.commit()
    conn.close()
