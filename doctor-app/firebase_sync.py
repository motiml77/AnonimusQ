import os
import json
import sys
from datetime import datetime

import firebase_admin
from firebase_admin import credentials, firestore

# PyInstaller compatibility: use exe directory when frozen
if getattr(sys, "frozen", False):
    _BASE_DIR = os.path.dirname(sys.executable)
else:
    _BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Embedded service account (shipped with the app)
_EMBEDDED_SA_PATH = os.path.join(_BASE_DIR, "data", "service-account.json")

# Legacy: user-uploaded service account in %APPDATA% (backward compatibility)
_APPDATA = os.environ.get("APPDATA", os.path.expanduser("~"))
_LEGACY_SA_PATH = os.path.join(_APPDATA, "AnonimousQ", "firebase-service-account.json")

_db = None        # Firestore client
_username = None  # Doctor's username — Firestore namespace


def is_connected() -> bool:
    return _db is not None and _username is not None


def set_username(username: str):
    global _username
    _username = username


def _doctor():
    return _db.collection("doctors").document(_username)


def _init_from_file(path: str) -> dict:
    """Initialize Firebase Admin SDK from a service account JSON file."""
    global _db
    try:
        with open(path, "r", encoding="utf-8") as f:
            cred_data = json.load(f)
        cred = credentials.Certificate(cred_data)
        try:
            existing = firebase_admin.get_app()
            firebase_admin.delete_app(existing)
        except ValueError:
            pass
        firebase_admin.initialize_app(cred)
        _db = firestore.client()
        return {"ok": True}
    except Exception as e:
        _db = None
        return {"ok": False, "error": str(e)}


def init_firebase(json_str: str) -> dict:
    """Initialize from a JSON string (legacy - used by old firebase_connect route)."""
    global _db
    try:
        cred_data = json.loads(json_str)
        cred = credentials.Certificate(cred_data)
        try:
            existing = firebase_admin.get_app()
            firebase_admin.delete_app(existing)
        except ValueError:
            pass
        firebase_admin.initialize_app(cred)
        _db = firestore.client()
        return {"ok": True}
    except Exception as e:
        _db = None
        return {"ok": False, "error": str(e)}


def init_embedded() -> dict:
    """Initialize Firebase from the embedded service account (shipped with app)."""
    if os.path.exists(_EMBEDDED_SA_PATH):
        result = _init_from_file(_EMBEDDED_SA_PATH)
        if result["ok"]:
            return result
        print(f"[Firebase] embedded init failed: {result.get('error')}")
    else:
        print(f"[Firebase] embedded service account not found: {_EMBEDDED_SA_PATH}")
    # Fallback: try legacy user-uploaded service account
    if os.path.exists(_LEGACY_SA_PATH):
        print("[Firebase] trying legacy service account...")
        result = _init_from_file(_LEGACY_SA_PATH)
        if result["ok"]:
            return result
        print(f"[Firebase] legacy init failed: {result.get('error')}")
    return {"ok": False, "error": "Service account not found"}


def save_service_account(json_str: str):
    """Legacy: save user-uploaded service account to AppData."""
    os.makedirs(os.path.dirname(_LEGACY_SA_PATH), exist_ok=True)
    data = json.loads(json_str)
    with open(_LEGACY_SA_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def init_from_saved():
    """Legacy: load service account from AppData. Prefer init_embedded() instead."""
    init_embedded()


# ========================
# Appointments
# ========================

def sync_appointments() -> dict:
    """Fetch pending, booked, and cancel_requested appointments (not cancelled)."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        appointments = []
        for status in ("pending", "booked", "cancel_requested"):
            docs = (
                _doctor()
                .collection("appointments")
                .where("status", "==", status)
                .stream()
            )
            for d in docs:
                appt = d.to_dict()
                appt["id"] = d.id
                appointments.append(appt)
        return {"ok": True, "appointments": appointments}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@firestore.transactional
def _approve_in_transaction(transaction, col, appointment_id):
    """Atomic approve: read the appointment + all same-date appointments inside
    the transaction, then approve + auto-reject in a single commit."""
    # Read the appointment being approved
    doc_ref = col.document(appointment_id)
    doc = doc_ref.get(transaction=transaction)
    if not doc.exists:
        raise ValueError("תור לא נמצא")
    appt = doc.to_dict()
    appt_date = appt.get("date", "")
    appt_time = appt.get("time", "00:00")
    appt_dur  = appt.get("durationMin") or 45

    new_start = _fb_time_to_minutes(appt_time)
    new_end   = new_start + appt_dur

    # Read ALL appointments on the same date (tracked by transaction)
    all_on_date = col.where("date", "==", appt_date).get(transaction=transaction)

    # Check for conflict with already-booked appointments
    for d in all_on_date:
        other = d.to_dict()
        if other.get("status") != "booked":
            continue
        ex_start = _fb_time_to_minutes(other.get("time", "00:00"))
        ex_end   = ex_start + (other.get("durationMin") or 45)
        if new_start < ex_end and new_end > ex_start:
            raise ValueError(f"חפיפה עם תור מאושר קיים ב-{other.get('time', '')}")

    # Approve this appointment
    transaction.update(doc_ref, {
        "status": "booked",
        "approvedAt": datetime.now().isoformat(),
    })

    # Auto-reject overlapping pending appointments
    rejected = []
    for d in all_on_date:
        if d.id == appointment_id:
            continue
        other = d.to_dict()
        if other.get("status") != "pending":
            continue
        ex_start = _fb_time_to_minutes(other.get("time", "00:00"))
        ex_dur   = other.get("durationMin") or 45
        ex_end   = ex_start + ex_dur
        if new_start < ex_end and new_end > ex_start:
            transaction.update(col.document(d.id), {
                "status": "cancelled",
                "autoRejectedAt": datetime.now().isoformat(),
                "autoRejectedReason": f"חפיפה עם תור מאושר ({appt_time})",
            })
            rejected.append(d.id)

    return rejected


def approve_appointment(appointment_id: str) -> dict:
    """Doctor approves a pending appointment → status becomes 'booked'.
    Automatically rejects any other pending appointments that overlap the same time slot.
    Uses a Firestore transaction for atomic read-check-write."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        col = _doctor().collection("appointments")
        transaction = _db.transaction()
        rejected = _approve_in_transaction(transaction, col, appointment_id)
        return {"ok": True, "rejected": rejected}
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def reject_appointment(appointment_id: str) -> dict:
    """Doctor rejects a pending appointment → status becomes 'cancelled'."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        _doctor().collection("appointments").document(appointment_id).update({
            "status": "cancelled",
            "rejectedAt": datetime.now().isoformat(),
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def approve_cancel_request(appointment_id: str) -> dict:
    """Doctor approves a patient's cancellation request → status becomes 'cancelled'."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        _doctor().collection("appointments").document(appointment_id).update({
            "status": "cancelled",
            "cancelApprovedAt": datetime.now().isoformat(),
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def reject_cancel_request(appointment_id: str) -> dict:
    """Doctor rejects a patient's cancellation request → status reverts to 'pending', flag set."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        _doctor().collection("appointments").document(appointment_id).update({
            "status": "pending",
            "cancelRejected": True,
            "cancelRejectedAt": datetime.now().isoformat(),
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def sync_all_appointments() -> dict:
    """Fetch ALL appointments (any status) — used for local caching/history."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        docs = _doctor().collection("appointments").stream()
        appointments = []
        for d in docs:
            appt = d.to_dict()
            appt["id"] = d.id
            appointments.append(appt)
        return {"ok": True, "appointments": appointments}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def mark_appointment(appointment_id: str, field: str, value,
                     payment_method: str = None) -> dict:
    """Mark treated/paid/status (with optional payment method) on Firestore."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    if field not in ("treated", "paid", "status"):
        return {"ok": False, "error": "שדה לא חוקי"}
    try:
        update_data: dict = {field: value, "markedAt": datetime.now().isoformat()}
        if field == "paid":
            update_data["paymentMethod"] = payment_method if value else None
        _doctor().collection("appointments").document(appointment_id).update(update_data)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_appointment(appointment_id: str) -> dict:
    """Permanently delete an appointment from Firestore."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        _doctor().collection("appointments").document(appointment_id).delete()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_availability(settings: dict) -> dict:
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        _doctor().collection("settings").document("availability").set(settings)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def patient_id_exists(anonymous_id: str) -> bool:
    """Returns True if the anonymous_id is already registered in Firestore."""
    if not _db or not _username:
        return False
    try:
        doc = _doctor().collection("patients").document(anonymous_id).get()
        return doc.exists
    except Exception as e:
        print(f"[Firebase] patient_id_exists error: {e}")
        return False


def register_patient(anonymous_id: str, price: int = 0, is_anonymous: bool = False) -> bool:
    if not _db or not _username:
        return False
    try:
        _doctor().collection("patients").document(anonymous_id).set(
            {"registered": True, "createdAt": datetime.now().isoformat(),
             "price": float(price or 0), "isAnonymous": is_anonymous}
        )
        return True
    except Exception as e:
        print(f"[Firebase] register_patient failed: {e}")
        return False


def update_patient_price(anonymous_id: str, price: int, is_anonymous: bool = None) -> dict:
    """Update the price field (and optionally isAnonymous) on a patient's Firestore document."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        data = {"price": float(price or 0)}
        if is_anonymous is not None:
            data["isAnonymous"] = is_anonymous
        _doctor().collection("patients").document(anonymous_id).update(data)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_payment_settings(settings: dict) -> dict:
    """Push payment settings (defaultPrice, bitPhone, payboxPhone) to Firestore /settings/payment."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        _doctor().collection("settings").document("payment").set(settings)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def _fb_time_to_minutes(t: str) -> int:
    try:
        h, m = t.split(":")
        return int(h) * 60 + int(m)
    except Exception:
        return 0


def check_slot_conflict(date: str, time: str, duration_min: int,
                         exclude_id: str = None) -> bool:
    """Returns True if [time, time+duration_min) overlaps any non-cancelled appointment on that date."""
    if not _db or not _username:
        return False
    try:
        docs = _doctor().collection("appointments").where("date", "==", date).stream()
        new_start = _fb_time_to_minutes(time)
        new_end   = new_start + duration_min
        for d in docs:
            if d.id == exclude_id:
                continue
            appt = d.to_dict()
            if appt.get("status") == "cancelled":
                continue
            ex_start = _fb_time_to_minutes(appt.get("time", "00:00"))
            ex_dur   = appt.get("durationMin") or 45
            ex_end   = ex_start + ex_dur
            if new_start < ex_end and new_end > ex_start:
                return True
        return False
    except Exception as e:
        print(f"[Firebase] check_slot_conflict error: {e}")
        return False


@firestore.transactional
def _create_in_transaction(transaction, col, date, time_str, duration_min, doc_data):
    """Atomic check-then-write: read all appointments on the date inside
    the transaction so Firestore will retry if any of them change before commit."""
    new_start = _fb_time_to_minutes(time_str)
    new_end   = new_start + duration_min

    # Read all appointments on this date (tracked by the transaction)
    existing = col.where("date", "==", date).get(transaction=transaction)
    for d in existing:
        appt = d.to_dict()
        if appt.get("status") == "cancelled":
            continue
        ex_start = _fb_time_to_minutes(appt.get("time", "00:00"))
        ex_dur   = appt.get("durationMin") or 45
        ex_end   = ex_start + ex_dur
        if new_start < ex_end and new_end > ex_start:
            raise ValueError(f"חפיפה עם תור קיים בתאריך {date} בשעה {time_str}")

    ref = col.document()
    transaction.set(ref, doc_data)
    return ref.id


def create_appointment(anonymous_id: str, date: str, time: str,
                        duration_min: int = 45) -> dict:
    """Create a new doctor-approved appointment in Firestore (status='booked').
    Uses a Firestore transaction for atomic conflict check + write."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        col = _doctor().collection("appointments")
        doc_data = {
            "anonymousId":     anonymous_id,
            "date":            date,
            "time":            time,
            "status":          "booked",
            "treated":         False,
            "paid":            False,
            "paymentMethod":   None,
            "durationMin":     duration_min,
            "createdAt":       datetime.now().isoformat(),
            "createdByDoctor": True,
        }
        transaction = _db.transaction()
        ref_id = _create_in_transaction(transaction, col, date, time, duration_min, doc_data)
        return {"ok": True, "id": ref_id}
    except ValueError as e:
        return {"ok": False, "error": str(e)}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_local_appointments(appointments: list) -> dict:
    """Push a list of local appointments to Firestore.

    For each appointment:
      - Skips if the same anonymousId+date+time already exists in Firebase.
      - Skips (and records as conflict) if another appointment already occupies
        that time slot on that date (overlap detected via check_slot_conflict).
      - Uses a WriteBatch (flushed every 499 ops) to minimise round-trips.

    Returns:
        {
          "ok": True,
          "pushed": <int>,
          "skipped": <int>,
          "conflicts": [<appt dict>, ...],   # local appts skipped due to overlap
          "new_appointments": [<appt dict>, ...],  # successfully pushed, with Firebase id
        }
    """
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        col = _doctor().collection("appointments")
        pushed = 0
        skipped = 0
        conflicts = []
        new_appointments = []

        batch = _db.batch()
        batch_ops = 0

        for appt in appointments:
            anon_id     = appt["anonymousId"]
            date        = appt["date"]
            time_str    = appt["time"]
            duration    = int(appt.get("durationMin") or appt.get("duration_min") or 45)

            # ── 1. Skip if same patient already has this exact slot in Firebase ──
            existing = (
                col.where("anonymousId", "==", anon_id)
                   .where("date",        "==", date)
                   .where("time",        "==", time_str)
                   .limit(1)
                   .get()
            )
            if list(existing):
                skipped += 1
                continue

            # ── 2. Conflict check: does ANY other appointment overlap this slot? ──
            if check_slot_conflict(date, time_str, duration):
                conflicts.append(appt)
                continue

            doc_data = {
                "anonymousId":     anon_id,
                "date":            date,
                "time":            time_str,
                "status":          appt.get("status", "booked"),
                "treated":         bool(appt.get("treated", False)),
                "paid":            bool(appt.get("paid", False)),
                "paymentMethod":   appt.get("paymentMethod") or appt.get("payment_method"),
                "durationMin":     duration,
                "createdAt":       appt.get("createdAt", datetime.now().isoformat()),
                "createdByDoctor": True,
            }

            # ── 3. Accumulate in WriteBatch (max 499 ops per commit) ──
            ref = col.document()
            batch.set(ref, doc_data)
            batch_ops += 1
            pushed += 1

            new_appt = dict(doc_data)
            new_appt["id"] = ref.id
            new_appointments.append(new_appt)

            if batch_ops >= 499:
                batch.commit()
                batch = _db.batch()
                batch_ops = 0

        if batch_ops > 0:
            batch.commit()

        return {
            "ok":               True,
            "pushed":           pushed,
            "skipped":          skipped,
            "conflicts":        conflicts,
            "new_appointments": new_appointments,
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


def reschedule_appointment(appointment_id: str, new_date: str, new_time: str,
                            duration_min: int = None) -> dict:
    """Move an appointment to a new date/time in Firestore (optionally update duration)."""
    if not _db or not _username:
        return {"ok": False, "error": "Firebase לא מחובר"}
    try:
        update_data: dict = {
            "date":          new_date,
            "time":          new_time,
            "rescheduledAt": datetime.now().isoformat(),
        }
        if duration_min is not None:
            update_data["durationMin"] = duration_min
        _doctor().collection("appointments").document(appointment_id).update(update_data)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ========================
# Patient Detail Sync (v2)
# ========================

def push_treatment_note(anonymous_id: str, note_id: str,
                        encrypted_content: str, note_type: str,
                        appt_date: str = None, appt_time: str = None,
                        created_at: str = None, updated_at: str = None) -> dict:
    """Push a single encrypted note to Firestore."""
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        doc_data = {
            "noteType": note_type,
            "encryptedContent": encrypted_content,
            "appointmentDate": appt_date,
            "appointmentTime": appt_time,
            "createdAt": created_at or datetime.now().isoformat(),
            "updatedAt": updated_at or datetime.now().isoformat(),
        }
        _doctor().collection("patients").document(anonymous_id)\
            .collection("notes").document(str(note_id)).set(doc_data)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def delete_treatment_note_firebase(anonymous_id: str, note_id: str) -> dict:
    """Delete a note from Firestore."""
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        _doctor().collection("patients").document(anonymous_id)\
            .collection("notes").document(str(note_id)).delete()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def pull_patient_notes(anonymous_id: str) -> list:
    """Fetch all encrypted notes from Firestore for a patient."""
    if not is_connected():
        return []
    try:
        docs = _doctor().collection("patients").document(anonymous_id)\
            .collection("notes").stream()
        results = []
        for doc in docs:
            d = doc.to_dict()
            d["firebaseId"] = doc.id
            results.append(d)
        return results
    except Exception:
        return []


def sync_emergency_contacts(anonymous_id: str, contacts: list) -> dict:
    """Push emergency contacts array to the patient document."""
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        _doctor().collection("patients").document(anonymous_id).update({
            "emergencyContacts": contacts,
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def pull_emergency_contacts(anonymous_id: str) -> list:
    """Fetch emergency contacts from Firestore."""
    if not is_connected():
        return []
    try:
        doc = _doctor().collection("patients").document(anonymous_id).get()
        if doc.exists:
            return doc.to_dict().get("emergencyContacts", [])
        return []
    except Exception:
        return []


def sync_referral_agreement(anonymous_id: str, referral: dict) -> dict:
    """Push referral agreement fields to the patient document."""
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        _doctor().collection("patients").document(anonymous_id).update({
            "referral": referral,
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def pull_referral_agreement(anonymous_id: str):
    """Fetch referral agreement from Firestore."""
    if not is_connected():
        return None
    try:
        doc = _doctor().collection("patients").document(anonymous_id).get()
        if doc.exists:
            return doc.to_dict().get("referral")
        return None
    except Exception:
        return None


# ========================
# Encryption Settings (v2 — password-derived)
# ========================

def get_encryption_settings() -> dict:
    """Download encryption settings (salt, verification token, version) from Firebase."""
    if not is_connected():
        return {}
    try:
        doc = _doctor().collection("settings").document("encryption").get()
        if doc.exists:
            return doc.to_dict()
        return {}
    except Exception:
        return {}


def save_encryption_settings(settings: dict) -> dict:
    """Save encryption settings to Firebase."""
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        _doctor().collection("settings").document("encryption").set(settings)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def set_password_change_flag(in_progress: bool) -> dict:
    """Set/clear the passwordChangeInProgress flag in Firebase."""
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        _doctor().collection("settings").document("encryption").update({
            "passwordChangeInProgress": in_progress,
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ========================
# Doctor Profile & Password Recovery
# ========================

def save_doctor_profile(profile: dict) -> dict:
    """Save doctor profile (name, phone, email) to Firestore.
    Path: /doctors/{username}/settings/profile
    Also ensures the parent /doctors/{username} document exists.
    """
    if not is_connected():
        return {"ok": False, "error": "Firebase not connected"}
    try:
        # Ensure parent document exists (needed for list queries)
        _doctor().set({"registered": True}, merge=True)
        _doctor().collection("settings").document("profile").set(profile)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def save_recovery_token(encrypted_password: str) -> dict:
    """Save encrypted password for developer recovery.
    Path: /doctors/{username}/settings/recovery
    """
    if not is_connected():
        return {"ok": False, "error": "Firebase not connected"}
    try:
        _doctor().collection("settings").document("recovery").set({
            "encryptedPassword": encrypted_password,
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ========================
# License / Trial
# ========================

def create_license_doc() -> dict:
    """Create the license document for a new doctor. Written ONCE during registration.
    Fields: trialStartDate (server timestamp), licensed (false).
    Only the developer can change 'licensed' to true via Firebase Console.
    """
    if not is_connected():
        return {"ok": False, "error": "Firebase not connected"}
    try:
        _doctor().collection("settings").document("license").set({
            "trialStartDate": firestore.SERVER_TIMESTAMP,
            "licensed": False,
        })
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_license_info() -> dict:
    """Read the license document from Firestore.
    Returns {ok, trialStartDate (datetime), licensed (bool)}.
    """
    if not is_connected():
        return {"ok": False, "error": "Firebase not connected"}
    try:
        doc = _doctor().collection("settings").document("license").get()
        if not doc.exists:
            return {"ok": False, "error": "no license doc"}
        data = doc.to_dict()
        return {
            "ok": True,
            "trialStartDate": data.get("trialStartDate"),
            "licensed": data.get("licensed", False),
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ========================
# Encrypted Patient Data (v2)
# ========================

def push_encrypted_patient(anonymous_id: str, encrypted_data: dict) -> dict:
    """Push encrypted patient fields (name, phone, notes) to Firebase.
    encrypted_data keys: encryptedName, encryptedPhone, encryptedNotes
    """
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        _doctor().collection("patients").document(anonymous_id).update(encrypted_data)
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_all_encrypted_patients(patients_encrypted: list) -> dict:
    """Batch-push encrypted data for all patients to Firebase.
    Each item: {anonymous_id, encryptedName, encryptedPhone, encryptedNotes, price, ...}
    Uses WriteBatch for efficiency.
    """
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        batch = _db.batch()
        batch_ops = 0
        for p in patients_encrypted:
            anon_id = p.pop("anonymous_id")
            ref = _doctor().collection("patients").document(anon_id)
            batch.set(ref, p, merge=True)
            batch_ops += 1
            if batch_ops >= 499:
                batch.commit()
                batch = _db.batch()
                batch_ops = 0
        if batch_ops > 0:
            batch.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_all_encrypted_notes(notes_encrypted: list) -> dict:
    """Batch-push encrypted treatment notes to Firebase.
    Each item: {anonymous_id, note_id, encryptedContent, noteType, appointmentDate, ...}
    """
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        batch = _db.batch()
        batch_ops = 0
        for n in notes_encrypted:
            ref = _doctor().collection("patients").document(n["anonymous_id"])\
                .collection("notes").document(str(n["note_id"]))
            doc_data = {
                "noteType": n.get("noteType", "freeform"),
                "encryptedContent": n["encryptedContent"],
                "appointmentDate": n.get("appointmentDate"),
                "appointmentTime": n.get("appointmentTime"),
                "createdAt": n.get("createdAt", ""),
                "updatedAt": n.get("updatedAt", ""),
            }
            batch.set(ref, doc_data)
            batch_ops += 1
            if batch_ops >= 499:
                batch.commit()
                batch = _db.batch()
                batch_ops = 0
        if batch_ops > 0:
            batch.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_all_encrypted_emergency_contacts(contacts_by_patient: dict) -> dict:
    """Batch-push encrypted emergency contacts to Firebase.
    contacts_by_patient: {anonymous_id: [{encryptedName, encryptedPhone}, ...]}
    """
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        batch = _db.batch()
        batch_ops = 0
        for anon_id, contacts in contacts_by_patient.items():
            ref = _doctor().collection("patients").document(anon_id)
            batch.update(ref, {"emergencyContacts": contacts})
            batch_ops += 1
            if batch_ops >= 499:
                batch.commit()
                batch = _db.batch()
                batch_ops = 0
        if batch_ops > 0:
            batch.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def push_all_encrypted_referrals(referrals_by_patient: dict) -> dict:
    """Batch-push encrypted referral data to Firebase.
    referrals_by_patient: {anonymous_id: {encryptedBrokerName, percentage, ...}}
    """
    if not is_connected():
        return {"ok": False, "error": "not connected"}
    try:
        batch = _db.batch()
        batch_ops = 0
        for anon_id, referral in referrals_by_patient.items():
            ref = _doctor().collection("patients").document(anon_id)
            batch.update(ref, {"referral": referral})
            batch_ops += 1
            if batch_ops >= 499:
                batch.commit()
                batch = _db.batch()
                batch_ops = 0
        if batch_ops > 0:
            batch.commit()
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def pull_all_patients() -> list:
    """Download all patient documents from Firebase (for new device sync)."""
    if not is_connected():
        return []
    try:
        docs = _doctor().collection("patients").stream()
        results = []
        for doc in docs:
            d = doc.to_dict()
            d["anonymous_id"] = doc.id
            results.append(d)
        return results
    except Exception:
        return []


def pull_all_notes_for_patient(anonymous_id: str) -> list:
    """Download all encrypted notes for a patient from Firebase."""
    if not is_connected():
        return []
    try:
        docs = _doctor().collection("patients").document(anonymous_id)\
            .collection("notes").stream()
        results = []
        for doc in docs:
            d = doc.to_dict()
            d["firebase_id"] = doc.id
            results.append(d)
        return results
    except Exception:
        return []
