"""
ClinicTor Management Dashboard
Admin tool for managing doctors, licenses, and password recovery.
Run: python app.py
"""

import os
import sys
import hashlib
import base64
import webbrowser
import threading

from flask import Flask, render_template, jsonify, request
import firebase_admin
from firebase_admin import credentials, firestore
from cryptography.fernet import Fernet

# ── Firebase init ──
_BASE_DIR = os.path.dirname(os.path.abspath(__file__))
_SA_PATH = os.path.join(_BASE_DIR, "service-account.json")

if not os.path.exists(_SA_PATH):
    print("ERROR: service-account.json not found in manage-app/")
    print("Copy it from doctor-app/data/service-account.json")
    sys.exit(1)

cred = credentials.Certificate(_SA_PATH)
firebase_admin.initialize_app(cred)
db = firestore.client()

# ── Recovery master key (must match doctor-app/app.py) ──
_RECOVERY_MASTER_KEY = b"YW5vbmltdXNxLW1hc3Rlci1rZXktMjAyNi0wMw=="
_dk = hashlib.pbkdf2_hmac("sha256", _RECOVERY_MASTER_KEY, b"anonimusq-recovery-salt", 100_000)
_recovery_fernet = Fernet(base64.urlsafe_b64encode(_dk[:32]))

# ── Flask app ──
app = Flask(__name__)
app.secret_key = "manage-app-local-only"


@app.route("/")
def dashboard():
    return render_template("dashboard.html")


@app.route("/api/doctors")
def api_doctors():
    """Fetch all doctors with their profile, license, patient count."""
    doctors = []

    # Discover doctors: list_documents() finds documents even if they only
    # have sub-collections (no explicit parent document created).
    docs_ref = db.collection("doctors").list_documents()

    for doc_ref in docs_ref:
        username = doc_ref.id
        doctor_data = {"username": username}

        # Profile
        try:
            profile_doc = db.collection("doctors").document(username) \
                .collection("settings").document("profile").get()
            if profile_doc.exists:
                p = profile_doc.to_dict()
                doctor_data["fullName"] = p.get("fullName", "")
                doctor_data["email"] = p.get("email", "")
                doctor_data["phone"] = p.get("phone", "")
            else:
                doctor_data["fullName"] = ""
                doctor_data["email"] = ""
                doctor_data["phone"] = ""
        except Exception:
            doctor_data["fullName"] = ""
            doctor_data["email"] = ""
            doctor_data["phone"] = ""

        # License
        try:
            license_doc = db.collection("doctors").document(username) \
                .collection("settings").document("license").get()
            if license_doc.exists:
                lic = license_doc.to_dict()
                doctor_data["licensed"] = lic.get("licensed", False)
                start = lic.get("trialStartDate")
                if start:
                    doctor_data["trialStartDate"] = start.isoformat() if hasattr(start, "isoformat") else str(start)
                else:
                    doctor_data["trialStartDate"] = None
            else:
                doctor_data["licensed"] = False
                doctor_data["trialStartDate"] = None
        except Exception:
            doctor_data["licensed"] = False
            doctor_data["trialStartDate"] = None

        # Patient count
        try:
            patients = db.collection("doctors").document(username) \
                .collection("patients").stream()
            doctor_data["patientCount"] = sum(1 for _ in patients)
        except Exception:
            doctor_data["patientCount"] = 0

        # Payment history
        try:
            payments_doc = db.collection("doctors").document(username) \
                .collection("settings").document("payments").get()
            if payments_doc.exists:
                doctor_data["payments"] = payments_doc.to_dict().get("months", {})
            else:
                doctor_data["payments"] = {}
        except Exception:
            doctor_data["payments"] = {}

        doctors.append(doctor_data)

    return jsonify(doctors)


@app.route("/api/toggle-license", methods=["POST"])
def api_toggle_license():
    """Toggle a doctor's license status."""
    data = request.json
    username = data.get("username")
    licensed = data.get("licensed", False)

    try:
        db.collection("doctors").document(username) \
            .collection("settings").document("license").update({
                "licensed": licensed,
            })
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/set-payment", methods=["POST"])
def api_set_payment():
    """Mark a month as paid/unpaid for a doctor."""
    data = request.json
    username = data.get("username")
    month = data.get("month")  # e.g. "2026-03"
    paid = data.get("paid", False)

    try:
        ref = db.collection("doctors").document(username) \
            .collection("settings").document("payments")
        doc = ref.get()
        if doc.exists:
            months = doc.to_dict().get("months", {})
        else:
            months = {}

        if paid:
            months[month] = True
        else:
            months.pop(month, None)

        ref.set({"months": months})
        return jsonify({"ok": True})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)})


@app.route("/api/recover-password", methods=["POST"])
def api_recover_password():
    """Decrypt and return a doctor's password."""
    data = request.json
    username = data.get("username")

    try:
        recovery_doc = db.collection("doctors").document(username) \
            .collection("settings").document("recovery").get()
        if not recovery_doc.exists:
            return jsonify({"ok": False, "error": "אין נתוני שחזור עבור מטפל זה"})

        encrypted = recovery_doc.to_dict().get("encryptedPassword", "")
        if not encrypted:
            return jsonify({"ok": False, "error": "אין סיסמה מוצפנת"})

        password = _recovery_fernet.decrypt(encrypted.encode()).decode()
        return jsonify({"ok": True, "password": password})
    except Exception as e:
        return jsonify({"ok": False, "error": f"שגיאה בפענוח: {e}"})


if __name__ == "__main__":
    print("=" * 40)
    print("  ClinicTor Management Dashboard")
    print("  http://localhost:8050")
    print("=" * 40)

    def _open():
        import time
        time.sleep(1.5)
        webbrowser.open("http://localhost:8050")

    threading.Thread(target=_open, daemon=True).start()
    app.run(debug=False, port=8050, host="127.0.0.1")
