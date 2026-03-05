"""
Add 10 demo patients + 2 appointments each to SQLite + Firebase.
Run from the doctor-app folder:
    venv\\Scripts\\python add_demo_patients.py
"""
import os
import sys
import sqlite3
from datetime import date, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db
import firebase_sync

# ── Init ──────────────────────────────────────────────────────────────────
db.init_db()
firebase_sync.init_from_saved()

# Get doctor username from local DB for Firebase namespacing
conn0 = sqlite3.connect(db.DB_PATH)
conn0.row_factory = sqlite3.Row
row = conn0.execute("SELECT username FROM users LIMIT 1").fetchone()
conn0.close()

if not row:
    print("ERROR: no auth user found in DB — run the app first and complete setup.")
    sys.exit(1)

username = row["username"]
firebase_sync.set_username(username)

firebase_ok = firebase_sync.is_connected()
print(f"Firebase: {'מחובר ✓' if firebase_ok else 'לא מחובר — שמירה לוקאלית בלבד'}")
print(f"משתמש: {username}\n")

# ── Patient data ──────────────────────────────────────────────────────────
PATIENTS = [
    ("אביגיל כהן",     "052-1001001", "",                     350),
    ("בנציון לוי",     "050-2002002", "",                     400),
    ("גילה מזרחי",     "054-3003003", "אלרגיה לאספירין",      320),
    ("דניאל אברהם",    "053-4004004", "",                     380),
    ("הילה פרץ",       "052-5005005", "לחץ דם גבוה",          450),
    ("ויקטור שפירא",   "050-6006006", "",                     300),
    ("זהבה ביטון",     "054-7007007", "",                     350),
    ("חיים עמר",       "053-8008008", "סוכרת סוג 2",          420),
    ("טלי ג'ורג'",     "052-9009009", "",                     370),
    ("יוסף נחמיאס",    "050-0010010", "פסיכותרפיה",           500),
]

# ── Generate weekday slots for next 14 days ───────────────────────────────
today = date.today()
slot_days = []
d = today + timedelta(days=1)
while len(slot_days) < 20:
    if d.weekday() < 5:   # Monday–Friday (0=Mon … 4=Fri)
        slot_days.append(d)
    d += timedelta(days=1)

# 20 different time slots
TIMES = [
    "09:00", "10:00", "11:00", "12:00", "13:00",
    "14:00", "15:00", "16:00", "09:30", "10:30",
    "11:30", "13:30", "14:30", "15:30", "16:30",
    "10:15", "11:45", "14:15", "15:45", "09:45",
]

# ── Create patients + appointments ────────────────────────────────────────
results = []
time_idx = 0

for i, (name, phone, notes, price) in enumerate(PATIENTS):
    res = db.add_patient(name, phone, notes, price=price)
    if not res.get("ok"):
        print(f"  ERROR adding {name}: {res.get('error')}")
        continue
    anon_id = res["anonymous_id"]

    # Register patient in Firebase
    if firebase_ok:
        firebase_sync.register_patient(anon_id, price=price)

    # 2 appointments on different days
    appt_info = []
    for j in range(2):
        appt_date = slot_days[(i * 2 + j) % len(slot_days)]
        appt_time = TIMES[time_idx % len(TIMES)]
        time_idx += 1

        # Save locally
        db.create_local_appointment(anon_id, appt_date.isoformat(), appt_time, "booked", 45)

        # Push to Firebase
        fb_id = "—"
        if firebase_ok:
            fb_res = firebase_sync.create_appointment(anon_id, appt_date.isoformat(), appt_time, 45)
            fb_id = fb_res.get("id", "ERROR: " + fb_res.get("error", "?"))

        appt_info.append((appt_date.isoformat(), appt_time, fb_id))

    results.append((name, anon_id, appt_info))
    print(f"✓ {name:20s}  מזהה: {anon_id}")
    for appt_date, appt_time, fb_id in appt_info:
        fb_tag = f"(Firebase: {fb_id})" if firebase_ok else "(לוקאלי בלבד)"
        print(f"    תור: {appt_date}  {appt_time}  {fb_tag}")

# ── Summary ───────────────────────────────────────────────────────────────
print("\n" + "=" * 60)
print("סיכום — מטופלים שנוצרו:")
print("=" * 60)
for name, anon_id, appt_info in results:
    print(f"\n  שם:    {name}")
    print(f"  מזהה:  {anon_id}")
    for appt_date, appt_time, _ in appt_info:
        print(f"  תור:   {appt_date}  {appt_time}")
print("\n" + "=" * 60)
print("כניסה לאתר המטופלים: הזן את ה'מזהה' בשדה הכניסה.")
print("=" * 60)
