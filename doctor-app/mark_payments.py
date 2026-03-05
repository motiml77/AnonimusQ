"""Mark some appointments as treated/paid for demo purposes."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import db, firebase_sync

db.init_db()
firebase_sync.init_from_saved()
firebase_sync.set_username("motiml77")

if not firebase_sync.is_connected():
    print("Firebase not connected"); sys.exit(1)

# Firebase appointment IDs from add_demo_patients.py run
MARK = [
    # (firebase_id, treated, paid, payment_method)
    ("9wtY5uo4ihGITUnPDRJd", True,  True,  "bit"),    # אביגיל תור 1
    ("XhhhZpyHNaelqAiI28i7", True,  True,  "bank"),   # אביגיל תור 2
    ("kw7aMM9eVwoo9ue9p4qF", True,  True,  "bit"),    # בנציון תור 1
    ("9TctGqN4HwCAb4NzaKjd", True,  True,  "bank"),   # גילה תור 1
    ("rw9i85jBJqHiJPF1nayZ", True,  True,  "bit"),    # דניאל תור 1
    ("bf73Y7Zrf35kngOKJH23", True,  False, None),      # הילה — טיפל, לא שילם עדיין
    ("hFsg08aLheYXSPF8N9No", False, True,  "bank"),   # דניאל תור 2 — שילם מראש
]

for fb_id, treated, paid, pm in MARK:
    if treated:
        r = firebase_sync.mark_appointment(fb_id, "treated", True)
        print(f"  treated {fb_id[:10]}... : {'OK' if r['ok'] else r.get('error')}")
    if paid:
        r = firebase_sync.mark_appointment(fb_id, "paid", True, pm)
        print(f"  paid({pm}) {fb_id[:10]}... : {'OK' if r['ok'] else r.get('error')}")

print("Done.")
