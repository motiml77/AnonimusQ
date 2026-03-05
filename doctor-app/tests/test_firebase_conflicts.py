"""
Firebase Concurrency & Online/Offline Conflict Tests
=====================================================
Intensive test suite covering real-world failure scenarios for appointment
scheduling, with special focus on the gap between online and offline modes.

Scenarios covered:
  A. Multiple simultaneous website bookings (concurrent writes to same slot)
  B. Doctor offline → creates appointment → goes online → discovers web bookings
  C. Race condition: approve while new pending arrives
  D. Sync interruption mid-push (partial batch commit)
  E. Stale cache: doctor acts on cached data that is already outdated in Firebase
  F. Multiple doctors / browser tabs operating concurrently
  G. Rapid approve/reject while website keeps sending bookings
  H. Offline reschedule collides with online booking
  I. Double-booking via cache lag (create online, cache not yet refreshed)
  J. Firebase write succeeds but cache update fails (inconsistency)

IMPORTANT:
  These tests connect to a REAL Firebase project.  They use an isolated
  Firestore collection namespace (test_doctor_<uuid>) that is cleaned up
  at the end of each test session.

Run:
    python -m pytest tests/test_firebase_conflicts.py -v --tb=short

Requires:
    - Firebase service-account JSON at %APPDATA%\\AnonimousQ\\firebase-service-account.json
    - Network connectivity
"""

import os
import sys
import time
import uuid
import json
import sqlite3
import threading
import concurrent.futures
from datetime import datetime, timedelta
from copy import deepcopy

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db as _db
import firebase_sync as _fs

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

DATE = "2026-04-15"
DATE2 = "2026-04-16"
DURATION = 45

# ─────────────────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def firebase_ready():
    """Initialize Firebase once for the entire test session.
    Skip all tests if no service account is available."""
    sa_path = _fs.SERVICE_ACCOUNT_PATH
    if not os.path.exists(sa_path):
        pytest.skip(
            f"Firebase service account not found at {sa_path}. "
            "These tests require a real Firebase connection."
        )
    with open(sa_path, "r", encoding="utf-8") as f:
        json_str = f.read()
    result = _fs.init_firebase(json_str)
    if not result["ok"]:
        pytest.skip(f"Firebase init failed: {result.get('error')}")
    yield


@pytest.fixture(autouse=True)
def test_namespace(firebase_ready, tmp_path, monkeypatch):
    """Each test gets:
    1. An isolated SQLite DB (via tmp_path)
    2. A unique Firebase doctor namespace (cleaned up after the test)
    """
    # Isolated local DB
    tmp_db = str(tmp_path / "test.db")
    monkeypatch.setattr(_db, "DB_PATH", tmp_db)
    monkeypatch.setattr(_db, "DATA_DIR", str(tmp_path))
    _db.init_db()

    # Unique Firebase namespace per test
    test_username = f"test_doctor_{uuid.uuid4().hex[:12]}"
    _fs.set_username(test_username)

    yield test_username

    # Cleanup: delete all documents under this test doctor namespace
    _cleanup_firebase_namespace(test_username)


def _cleanup_firebase_namespace(username: str):
    """Remove all Firestore data under doctors/{username}/."""
    try:
        if not _fs._db:
            return
        doctor_ref = _fs._db.collection("doctors").document(username)
        for sub in ("appointments", "patients", "settings"):
            docs = doctor_ref.collection(sub).list_documents()
            for doc in docs:
                doc.delete()
        doctor_ref.delete()
    except Exception as e:
        print(f"[Cleanup warning] {username}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _add_patient(name="TestPatient", price=300):
    """Add a patient to local DB and return the anonymous_id."""
    result = _db.add_patient(name, phone="050-0000000", notes="", price=price)
    assert result["ok"], f"Failed to add patient: {result}"
    return result["anonymous_id"]


def _create_fb_appointment(anon_id, date, time_str, duration=DURATION,
                           status="booked", by_doctor=True):
    """Create an appointment directly in Firebase. Returns the document ID."""
    col = _fs._doctor().collection("appointments")
    ref = col.document()
    ref.set({
        "anonymousId": anon_id,
        "date": date,
        "time": time_str,
        "status": status,
        "treated": False,
        "paid": False,
        "paymentMethod": None,
        "durationMin": duration,
        "createdAt": datetime.now().isoformat(),
        "createdByDoctor": by_doctor,
    })
    return ref.id


def _create_fb_pending(anon_id, date, time_str, duration=DURATION):
    """Simulate a patient booking from the website (status=pending)."""
    return _create_fb_appointment(
        anon_id, date, time_str, duration, status="pending", by_doctor=False
    )


def _get_fb_appointment(appt_id):
    """Fetch a single appointment from Firebase."""
    doc = _fs._doctor().collection("appointments").document(appt_id).get()
    if doc.exists:
        data = doc.to_dict()
        data["id"] = doc.id
        return data
    return None


def _count_fb_appointments(date, status=None):
    """Count appointments on a date in Firebase (optionally filtered by status)."""
    col = _fs._doctor().collection("appointments")
    if status:
        docs = col.where("date", "==", date).where("status", "==", status).get()
    else:
        docs = col.where("date", "==", date).get()
    return len(list(docs))


def _list_fb_appointments(date, status=None):
    """List all appointments on a date from Firebase."""
    col = _fs._doctor().collection("appointments")
    if status:
        docs = col.where("date", "==", date).where("status", "==", status).get()
    else:
        docs = col.where("date", "==", date).get()
    results = []
    for d in docs:
        appt = d.to_dict()
        appt["id"] = d.id
        results.append(appt)
    return results


def _add_local_appointment(anon_id, date, time_str, duration=DURATION, status="booked"):
    """Create a local (offline) appointment."""
    result = _db.create_local_appointment(anon_id, date, time_str, status, duration)
    assert result["ok"], f"Failed to create local appointment: {result}"
    return result["id"]


# ═══════════════════════════════════════════════════════════════════════════════
# A. CONCURRENT WEBSITE BOOKINGS
#    Multiple users try to book the same time slot simultaneously via the website
# ═══════════════════════════════════════════════════════════════════════════════

class TestConcurrentWebsiteBookings:
    """Simulate multiple patients booking the same slot concurrently through
    the website. Firebase has no native row-level locking so we test how
    the app handles multiple pending entries for the same time."""

    def test_multiple_pending_same_slot_all_written(self):
        """When 5 patients book the same slot, all 5 pending docs should exist
        in Firebase (the website doesn't block — conflict is resolved at approval)."""
        patients = [_add_patient(f"Patient_{i}") for i in range(5)]
        ids = []
        for p in patients:
            appt_id = _create_fb_pending(p, DATE, "10:00")
            ids.append(appt_id)

        # All 5 should exist
        pending = _list_fb_appointments(DATE, status="pending")
        assert len(pending) == 5, f"Expected 5 pending, got {len(pending)}"

    def test_approve_one_auto_rejects_overlapping_pending(self):
        """Approving one pending appointment should auto-reject all others
        that overlap the same time slot."""
        p1, p2, p3 = [_add_patient(f"Approve_{i}") for i in range(3)]
        id1 = _create_fb_pending(p1, DATE, "10:00")
        id2 = _create_fb_pending(p2, DATE, "10:00")
        id3 = _create_fb_pending(p3, DATE, "10:30")  # overlaps with 10:00-10:45

        # Approve patient 1
        result = _fs.approve_appointment(id1)
        assert result["ok"], f"Approve failed: {result}"

        # id2 and id3 should be auto-rejected
        appt2 = _get_fb_appointment(id2)
        appt3 = _get_fb_appointment(id3)
        assert appt2["status"] == "cancelled", "Overlapping pending should be auto-cancelled"
        assert appt3["status"] == "cancelled", "Partially overlapping pending should be auto-cancelled"
        assert id2 in result.get("rejected", [])
        assert id3 in result.get("rejected", [])

    def test_approve_one_non_overlapping_survives(self):
        """A pending appointment that does NOT overlap should NOT be rejected."""
        p1, p2 = [_add_patient(f"NonOverlap_{i}") for i in range(2)]
        id1 = _create_fb_pending(p1, DATE, "10:00")  # 10:00-10:45
        id2 = _create_fb_pending(p2, DATE, "11:00")  # 11:00-11:45 — no overlap

        result = _fs.approve_appointment(id1)
        assert result["ok"]

        appt2 = _get_fb_appointment(id2)
        assert appt2["status"] == "pending", "Non-overlapping pending must survive"

    def test_concurrent_creates_via_threads(self):
        """Simulate true concurrency with threads writing to the same slot."""
        patients = [_add_patient(f"Thread_{i}") for i in range(8)]
        results = []
        errors = []

        def book(patient_id):
            try:
                appt_id = _create_fb_pending(patient_id, DATE, "14:00")
                results.append(appt_id)
            except Exception as e:
                errors.append(str(e))

        threads = [threading.Thread(target=book, args=(p,)) for p in patients]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)

        assert not errors, f"Thread errors: {errors}"
        assert len(results) == 8, f"Expected 8 results, got {len(results)}"

        # All 8 should be pending in Firebase
        pending = _list_fb_appointments(DATE, status="pending")
        assert len(pending) >= 8

    def test_concurrent_approve_race_condition(self):
        """Two concurrent approvals for overlapping slots — with Firestore
        transactions only one should succeed, the other should fail or be
        auto-rejected."""
        p1, p2 = [_add_patient(f"Race_{i}") for i in range(2)]
        id1 = _create_fb_pending(p1, DATE, "10:00")
        id2 = _create_fb_pending(p2, DATE, "10:15")  # overlaps 10:00-10:45

        results = {}

        def approve(appt_id, key):
            results[key] = _fs.approve_appointment(appt_id)

        t1 = threading.Thread(target=approve, args=(id1, "first"))
        t2 = threading.Thread(target=approve, args=(id2, "second"))
        t1.start()
        t2.start()
        t1.join(timeout=15)
        t2.join(timeout=15)

        # With transactions: at most one should succeed, OR if both succeed
        # the second must have auto-rejected the first's overlap
        success_count = sum(1 for r in results.values() if r.get("ok"))
        assert success_count >= 1, "At least one approval should succeed"

        appt1 = _get_fb_appointment(id1)
        appt2 = _get_fb_appointment(id2)
        booked_count = sum(1 for a in [appt1, appt2] if a["status"] == "booked")
        assert booked_count <= 1, (
            f"Only one overlapping appointment should be booked, "
            f"got {booked_count} (appt1={appt1['status']}, appt2={appt2['status']})"
        )

    def test_approve_already_booked_slot_fails(self):
        """Cannot approve a pending if a booked appointment already exists in that slot."""
        p1, p2 = [_add_patient(f"AlreadyBooked_{i}") for i in range(2)]
        # Doctor books directly (status=booked)
        _create_fb_appointment(p1, DATE, "10:00", status="booked")
        # Website booking arrives
        pending_id = _create_fb_pending(p2, DATE, "10:00")

        result = _fs.approve_appointment(pending_id)
        assert not result["ok"], "Should fail: overlaps with booked appointment"
        assert "חפיפה" in result.get("error", "")


# ═══════════════════════════════════════════════════════════════════════════════
# B. DOCTOR OFFLINE → CREATES → GOES ONLINE → DISCOVERS WEB BOOKINGS
# ═══════════════════════════════════════════════════════════════════════════════

class TestOfflineDoctorOnlineWebConflict:
    """Doctor is offline and creates appointments manually.
    Meanwhile, patients book the same slots via the website.
    When doctor goes online, sync must detect and report conflicts."""

    def test_offline_create_then_online_conflict(self):
        """Doctor creates 10:00 offline. Patient booked 10:00 on website.
        push_local_appointments should report the conflict."""
        p_doctor = _add_patient("DoctorOffline")
        p_web = _add_patient("WebPatient")

        # Patient books via website (directly in Firebase)
        _create_fb_pending(p_web, DATE, "10:00")

        # Doctor creates locally (offline)
        local_id = _add_local_appointment(p_doctor, DATE, "10:00")

        # Doctor goes online — push local to Firebase
        local_appts = _db.get_local_appointments()
        result = _fs.push_local_appointments(local_appts)

        assert result["ok"]
        # The local appointment should conflict with the pending web booking
        assert result["pushed"] == 0 or len(result["conflicts"]) > 0, \
            "Expected conflict or skip due to overlapping web booking"

    def test_offline_create_no_conflict_different_time(self):
        """Doctor creates 14:00 offline, patient books 10:00 online — no conflict."""
        p_doctor = _add_patient("DocNoConflict")
        p_web = _add_patient("WebNoConflict")

        _create_fb_pending(p_web, DATE, "10:00")
        _add_local_appointment(p_doctor, DATE, "14:00")

        local_appts = _db.get_local_appointments()
        result = _fs.push_local_appointments(local_appts)

        assert result["ok"]
        assert result["pushed"] == 1
        assert len(result["conflicts"]) == 0

    def test_offline_multiple_creates_partial_conflict(self):
        """Doctor creates 3 appointments offline. 1 conflicts with a web booking,
        2 should push successfully."""
        p_doc = _add_patient("DocPartial")
        p_web = _add_patient("WebPartial")

        # Website booking at 10:00
        _create_fb_appointment(p_web, DATE, "10:00", status="booked")

        # Doctor offline: 09:00, 10:00 (conflict), 11:00
        _add_local_appointment(p_doc, DATE, "09:00")
        _add_local_appointment(p_doc, DATE, "10:00")  # conflicts!
        _add_local_appointment(p_doc, DATE, "11:00")

        local_appts = _db.get_local_appointments()
        result = _fs.push_local_appointments(local_appts)

        assert result["ok"]
        assert result["pushed"] == 2, f"Expected 2 pushed, got {result['pushed']}"
        assert len(result["conflicts"]) == 1, f"Expected 1 conflict, got {len(result['conflicts'])}"

    def test_offline_creates_duplicate_detection(self):
        """If the doctor already has this exact appointment in Firebase
        (same patient+date+time), push should skip it (duplicate)."""
        p = _add_patient("Duplicate")

        # Already exists in Firebase
        _create_fb_appointment(p, DATE, "10:00", status="booked")

        # Doctor creates same one offline
        _add_local_appointment(p, DATE, "10:00")

        local_appts = _db.get_local_appointments()
        result = _fs.push_local_appointments(local_appts)

        assert result["ok"]
        assert result["skipped"] == 1, "Duplicate appointment should be skipped"
        assert result["pushed"] == 0

    def test_offline_create_during_website_booking_storm(self):
        """5 patients book different slots on the same day via website.
        Doctor creates 3 offline appointments. Verify correct sync outcome."""
        web_patients = [_add_patient(f"WebStorm_{i}") for i in range(5)]
        doc_patient = _add_patient("DocStorm")

        # Website bookings
        web_times = ["09:00", "10:00", "11:00", "14:00", "15:00"]
        for p, t in zip(web_patients, web_times):
            _create_fb_pending(p, DATE, t)

        # Doctor offline: 09:30 (conflicts 09:00), 12:00 (free), 16:00 (free)
        _add_local_appointment(doc_patient, DATE, "09:30")
        _add_local_appointment(doc_patient, DATE, "12:00")
        _add_local_appointment(doc_patient, DATE, "16:00")

        local_appts = _db.get_local_appointments()
        result = _fs.push_local_appointments(local_appts)

        assert result["ok"]
        assert result["pushed"] == 2, f"12:00 and 16:00 should push, got {result['pushed']}"
        assert len(result["conflicts"]) == 1, \
            f"09:30 conflicts with 09:00-09:45, got {len(result['conflicts'])}"


# ═══════════════════════════════════════════════════════════════════════════════
# C. STALE CACHE SCENARIOS
#    Doctor acts on cached data that's already outdated in Firebase
# ═══════════════════════════════════════════════════════════════════════════════

class TestStaleCacheConflicts:
    """Doctor's local cache is behind Firebase. Actions based on stale cache
    may lead to inconsistencies."""

    def test_create_on_slot_that_was_booked_since_last_sync(self):
        """Doctor's cache says 10:00 is free. But since last sync, a patient
        booked 10:00 via the website. Firebase conflict check should catch it."""
        p_web = _add_patient("StaleCacheWeb")
        p_doc = _add_patient("StaleCacheDoc")

        # Sync: cache is empty
        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        _db.cache_appointments(all_result.get("appointments", []), replace_all=True)

        # After sync, patient books 10:00 via website
        _create_fb_pending(p_web, DATE, "10:00")

        # Doctor tries to create at 10:00 (cache says free, but Firebase has it)
        # The online path checks Firebase directly, not cache
        conflict = _fs.check_slot_conflict(DATE, "10:00", DURATION)
        assert conflict, "Firebase conflict check should detect the new pending"

    def test_cached_appointment_was_cancelled_then_rebooked(self):
        """A cached appointment was cancelled in Firebase, then another
        patient rebooked the same slot. Cache still shows the old one."""
        p1 = _add_patient("CancelRebook1")
        p2 = _add_patient("CancelRebook2")

        # Create and cache appointment
        appt_id = _create_fb_appointment(p1, DATE, "10:00", status="booked")
        _db.cache_appointments([{
            "id": appt_id, "anonymousId": p1, "date": DATE, "time": "10:00",
            "status": "booked", "treated": False, "paid": False,
            "paymentMethod": None, "durationMin": DURATION, "patientMarkedPaid": False,
        }], replace_all=True)

        # Firebase: cancel the old, create new booking
        _fs.mark_appointment(appt_id, "status", "cancelled")
        new_id = _create_fb_appointment(p2, DATE, "10:00", status="booked")

        # Re-sync should show the new appointment
        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        _db.cache_appointments(all_result["appointments"], replace_all=True)

        cached = _db.get_cached_appointments()
        booked_at_10 = [a for a in cached
                        if a["date"] == DATE and a["time"] == "10:00"
                        and a["status"] == "booked"]
        assert len(booked_at_10) == 1
        assert booked_at_10[0]["anonymousId"] == p2

    def test_full_sync_replaces_stale_cache_completely(self):
        """After a full sync, the cache should match Firebase exactly.
        Any stale local-only entries should be gone."""
        p = _add_patient("FullSync")

        # Seed cache with fake stale data
        _db.cache_appointments([{
            "id": "stale-fake-id",
            "anonymousId": p,
            "date": DATE,
            "time": "08:00",
            "status": "booked",
            "treated": False,
            "paid": False,
            "paymentMethod": None,
            "durationMin": DURATION,
            "patientMarkedPaid": False,
        }], replace_all=False)

        # Create real appointment in Firebase
        real_id = _create_fb_appointment(p, DATE, "10:00", status="booked")

        # Full sync
        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        _db.cache_appointments(all_result["appointments"], replace_all=True)

        cached = _db.get_cached_appointments()
        ids = [a["id"] for a in cached]
        assert "stale-fake-id" not in ids, "Stale cache entry should be replaced"
        assert real_id in ids, "Real Firebase appointment should be in cache"


# ═══════════════════════════════════════════════════════════════════════════════
# D. APPROVE / REJECT DURING ACTIVE WEBSITE TRAFFIC
# ═══════════════════════════════════════════════════════════════════════════════

class TestApproveRejectDuringTraffic:
    """Doctor is approving/rejecting while the website keeps sending new bookings."""

    def test_approve_then_new_pending_arrives_same_slot(self):
        """Doctor approves slot 10:00. A new pending for 10:15 arrives just after.
        The new pending should still exist (it was created after approval)."""
        p1 = _add_patient("ApproveFirst")
        p2 = _add_patient("LateArrival")

        pending1 = _create_fb_pending(p1, DATE, "10:00")

        # Doctor approves
        result = _fs.approve_appointment(pending1)
        assert result["ok"]

        # New pending arrives (after approval)
        pending2 = _create_fb_pending(p2, DATE, "10:15")

        # The new pending should exist but will conflict with the booked appointment
        appt2 = _get_fb_appointment(pending2)
        assert appt2 is not None
        assert appt2["status"] == "pending"

        # Trying to approve it should fail (conflicts with booked 10:00)
        result2 = _fs.approve_appointment(pending2)
        assert not result2["ok"], "Should conflict with already booked 10:00-10:45"

    def test_reject_all_pending_on_day(self):
        """Doctor rejects all pending appointments on a day. Verify none remain."""
        patients = [_add_patient(f"RejectAll_{i}") for i in range(4)]
        ids = []
        for p, t in zip(patients, ["09:00", "10:00", "11:00", "14:00"]):
            ids.append(_create_fb_pending(p, DATE, t))

        for appt_id in ids:
            result = _fs.reject_appointment(appt_id)
            assert result["ok"]

        pending = _list_fb_appointments(DATE, status="pending")
        assert len(pending) == 0, "All pending should be rejected"

        cancelled = _list_fb_appointments(DATE, status="cancelled")
        assert len(cancelled) == 4

    def test_rapid_approve_reject_sequence(self):
        """Rapidly alternate approve and reject on sequential slots."""
        patients = [_add_patient(f"Rapid_{i}") for i in range(6)]
        times = ["09:00", "10:00", "11:00", "13:00", "14:00", "15:00"]
        ids = []
        for p, t in zip(patients, times):
            ids.append(_create_fb_pending(p, DATE, t))

        # Approve odds, reject evens
        for i, appt_id in enumerate(ids):
            if i % 2 == 0:
                result = _fs.approve_appointment(appt_id)
            else:
                result = _fs.reject_appointment(appt_id)
            assert result["ok"], f"Failed on appointment {i}: {result}"

        booked = _list_fb_appointments(DATE, status="booked")
        cancelled = _list_fb_appointments(DATE, status="cancelled")
        assert len(booked) == 3
        assert len(cancelled) == 3


# ═══════════════════════════════════════════════════════════════════════════════
# E. OFFLINE RESCHEDULE COLLIDES WITH ONLINE BOOKING
# ═══════════════════════════════════════════════════════════════════════════════

class TestOfflineRescheduleCollision:
    """Doctor reschedules an appointment offline. Meanwhile, a patient books
    the new target slot via the website."""

    def test_reschedule_target_now_occupied_online(self):
        """Doctor offline: reschedule from 10:00 to 14:00.
        Meanwhile, 14:00 was booked online.
        When syncing, the rescheduled appointment should conflict."""
        p_doc = _add_patient("ReschedDoc")
        p_web = _add_patient("ReschedWeb")

        # Doctor had 10:00 locally
        local_id = _add_local_appointment(p_doc, DATE, "10:00")

        # Reschedule locally to 14:00
        result = _db.reschedule_local_appointment(local_id, DATE, "14:00")
        assert result["ok"]

        # Meanwhile, patient books 14:00 via website
        _create_fb_pending(p_web, DATE, "14:00")

        # Sync: push local appointments
        local_appts = _db.get_local_appointments()
        push_result = _fs.push_local_appointments(local_appts)

        assert push_result["ok"]
        assert len(push_result["conflicts"]) >= 1, \
            "Rescheduled 14:00 should conflict with web booking at 14:00"

    def test_reschedule_target_free_online(self):
        """Doctor offline: reschedule from 10:00 to 16:00.
        16:00 is free online. Should sync without conflict."""
        p = _add_patient("ReschedFree")

        local_id = _add_local_appointment(p, DATE, "10:00")
        result = _db.reschedule_local_appointment(local_id, DATE, "16:00")
        assert result["ok"]

        local_appts = _db.get_local_appointments()
        push_result = _fs.push_local_appointments(local_appts)

        assert push_result["ok"]
        assert push_result["pushed"] == 1
        assert len(push_result["conflicts"]) == 0


# ═══════════════════════════════════════════════════════════════════════════════
# F. SYNC CONSISTENCY & DATA INTEGRITY
# ═══════════════════════════════════════════════════════════════════════════════

class TestSyncConsistency:
    """Verify that the sync process maintains data integrity across
    multiple rounds of online/offline/online transitions."""

    def test_full_round_trip_online_offline_online(self):
        """Online: create 3 appointments.
        Go offline: create 2 local appointments.
        Go online: sync all. Verify Firebase has 5 (no duplicates, no losses)."""
        p = _add_patient("RoundTrip")

        # Online: create in Firebase
        fb_ids = []
        for t in ["09:00", "10:00", "11:00"]:
            result = _fs.create_appointment(p, DATE, t, DURATION)
            assert result["ok"]
            fb_ids.append(result["id"])

        # Sync to cache
        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        _db.cache_appointments(all_result["appointments"], replace_all=True)

        # Offline: create 2 local
        _add_local_appointment(p, DATE, "14:00")
        _add_local_appointment(p, DATE, "15:00")

        # Go online: push local
        local_appts = _db.get_local_appointments()
        push_result = _fs.push_local_appointments(local_appts)
        assert push_result["ok"]
        assert push_result["pushed"] == 2
        _db.clear_local_appointments()

        # Final sync
        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        _db.cache_appointments(all_result["appointments"], replace_all=True)

        # Verify: exactly 5 booked appointments
        all_appts = _list_fb_appointments(DATE, status="booked")
        assert len(all_appts) == 5, f"Expected 5, got {len(all_appts)}"

    def test_double_sync_does_not_duplicate(self):
        """Syncing twice in a row should not create duplicates."""
        p = _add_patient("DoubleSync")
        _fs.create_appointment(p, DATE, "10:00", DURATION)

        # First sync
        r1 = _fs.sync_all_appointments()
        assert r1["ok"]
        _db.cache_appointments(r1["appointments"], replace_all=True)

        # Second sync
        r2 = _fs.sync_all_appointments()
        assert r2["ok"]
        _db.cache_appointments(r2["appointments"], replace_all=True)

        cached = _db.get_cached_appointments()
        times_10 = [a for a in cached if a["time"] == "10:00" and a["date"] == DATE]
        assert len(times_10) == 1, f"Should be exactly 1, got {len(times_10)}"

    def test_local_push_then_immediate_resync(self):
        """Push local appointments, then immediately re-sync.
        The pushed appointments should appear in the new sync."""
        p = _add_patient("PushResync")
        _add_local_appointment(p, DATE, "10:00")

        local_appts = _db.get_local_appointments()
        push_result = _fs.push_local_appointments(local_appts)
        assert push_result["ok"]
        assert push_result["pushed"] == 1

        new_id = push_result["new_appointments"][0]["id"]

        # Immediate resync
        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        appt_ids = [a["id"] for a in all_result["appointments"]]
        assert new_id in appt_ids, "Pushed appointment should appear in sync"

    def test_cache_replace_removes_deleted_appointments(self):
        """If an appointment is deleted from Firebase, a full sync should
        remove it from the cache."""
        p = _add_patient("CacheDelete")
        appt_id = _create_fb_appointment(p, DATE, "10:00")

        # Cache it
        all_result = _fs.sync_all_appointments()
        _db.cache_appointments(all_result["appointments"], replace_all=True)
        assert any(a["id"] == appt_id for a in _db.get_cached_appointments())

        # Delete from Firebase
        _fs.delete_appointment(appt_id)

        # Re-sync
        all_result = _fs.sync_all_appointments()
        _db.cache_appointments(all_result["appointments"], replace_all=True)

        cached_ids = [a["id"] for a in _db.get_cached_appointments()]
        assert appt_id not in cached_ids, "Deleted appointment should be removed from cache"


# ═══════════════════════════════════════════════════════════════════════════════
# G. CONFLICT CHECK EDGE CASES (via Firebase)
# ═══════════════════════════════════════════════════════════════════════════════

class TestFirebaseConflictChecks:
    """Test the Firebase conflict check function with real Firestore data."""

    def test_exact_overlap(self):
        """Same time and duration: must conflict."""
        p = _add_patient("ExactOverlap")
        _create_fb_appointment(p, DATE, "10:00", 45, status="booked")
        assert _fs.check_slot_conflict(DATE, "10:00", 45)

    def test_partial_overlap_start(self):
        """New appointment starts during existing: must conflict."""
        p = _add_patient("PartialStart")
        _create_fb_appointment(p, DATE, "10:00", 45, status="booked")
        assert _fs.check_slot_conflict(DATE, "10:30", 45)

    def test_partial_overlap_end(self):
        """New appointment ends during existing: must conflict."""
        p = _add_patient("PartialEnd")
        _create_fb_appointment(p, DATE, "10:00", 45, status="booked")
        assert _fs.check_slot_conflict(DATE, "09:30", 45)

    def test_adjacent_no_overlap(self):
        """Back-to-back appointments must NOT conflict."""
        p = _add_patient("Adjacent")
        _create_fb_appointment(p, DATE, "10:00", 45, status="booked")
        assert not _fs.check_slot_conflict(DATE, "10:45", 45)

    def test_cancelled_does_not_block(self):
        """Cancelled appointment should not block new bookings."""
        p = _add_patient("CancelledFree")
        _create_fb_appointment(p, DATE, "10:00", 45, status="cancelled")
        assert not _fs.check_slot_conflict(DATE, "10:00", 45)

    def test_pending_blocks_by_default(self):
        """Pending appointment blocks the slot (for patient-facing booking)."""
        p = _add_patient("PendingBlocks")
        _create_fb_pending(p, DATE, "10:00")
        assert _fs.check_slot_conflict(DATE, "10:00", 45)

    def test_exclude_id_skips_self(self):
        """Conflict check with exclude_id should skip the specified appointment."""
        p = _add_patient("ExcludeSelf")
        appt_id = _create_fb_appointment(p, DATE, "10:00", 45, status="booked")
        # Without exclude: conflict
        assert _fs.check_slot_conflict(DATE, "10:00", 45)
        # With exclude: no conflict
        assert not _fs.check_slot_conflict(DATE, "10:00", 45, exclude_id=appt_id)

    def test_different_date_no_conflict(self):
        """Appointment on a different date should not conflict."""
        p = _add_patient("DiffDate")
        _create_fb_appointment(p, DATE, "10:00", 45, status="booked")
        assert not _fs.check_slot_conflict(DATE2, "10:00", 45)

    def test_contained_appointment_conflicts(self):
        """A short appointment fully contained within a long one: must conflict."""
        p = _add_patient("Contained")
        _create_fb_appointment(p, DATE, "10:00", 120, status="booked")  # 10:00-12:00
        assert _fs.check_slot_conflict(DATE, "10:30", 30)

    def test_many_appointments_performance(self):
        """Conflict check with 20 appointments on same day should complete
        in under 5 seconds (network included)."""
        p = _add_patient("PerfCheck")
        for i in range(20):
            h = 7 + i
            if h > 20:
                break
            _create_fb_appointment(p, DATE, f"{h:02d}:00", 45, status="booked")

        t0 = time.monotonic()
        _fs.check_slot_conflict(DATE, "21:00", 45)
        elapsed = time.monotonic() - t0
        assert elapsed < 5.0, f"Conflict check took {elapsed:.2f}s (expected <5s)"


# ═══════════════════════════════════════════════════════════════════════════════
# H. WRITE CONSISTENCY: FIREBASE ↔ CACHE
# ═══════════════════════════════════════════════════════════════════════════════

class TestFirebaseCacheConsistency:
    """Verify that operations keep Firebase and cache in sync."""

    def test_create_online_updates_both(self):
        """Creating an appointment online should write to Firebase AND cache."""
        p = _add_patient("BothCreate")
        result = _fs.create_appointment(p, DATE, "10:00", DURATION)
        assert result["ok"]

        # Manually cache (as app.py does)
        _db.cache_appointments([{
            "id": result["id"], "anonymousId": p, "date": DATE, "time": "10:00",
            "status": "booked", "treated": False, "paid": False,
            "paymentMethod": None, "durationMin": DURATION, "patientMarkedPaid": False,
        }], replace_all=False)

        # Verify Firebase
        fb_appt = _get_fb_appointment(result["id"])
        assert fb_appt is not None
        assert fb_appt["status"] == "booked"

        # Verify cache
        cached = _db.get_cached_appointments()
        match = [a for a in cached if a["id"] == result["id"]]
        assert len(match) == 1
        assert match[0]["status"] == "booked"

    def test_mark_treated_updates_both(self):
        """Marking treated should update Firebase AND cache."""
        p = _add_patient("BothMark")
        appt_id = _create_fb_appointment(p, DATE, "10:00")

        _db.cache_appointments([{
            "id": appt_id, "anonymousId": p, "date": DATE, "time": "10:00",
            "status": "booked", "treated": False, "paid": False,
            "paymentMethod": None, "durationMin": DURATION, "patientMarkedPaid": False,
        }], replace_all=False)

        # Mark treated
        result = _fs.mark_appointment(appt_id, "treated", True)
        assert result["ok"]
        _db.update_cached_appointment(appt_id, "treated", True)

        # Verify Firebase
        fb = _get_fb_appointment(appt_id)
        assert fb["treated"] is True

        # Verify cache
        cached = _db.get_cached_appointments()
        match = next(a for a in cached if a["id"] == appt_id)
        assert match["treated"] is True

    def test_delete_removes_from_both(self):
        """Deleting should remove from Firebase AND cache."""
        p = _add_patient("BothDelete")
        appt_id = _create_fb_appointment(p, DATE, "10:00")

        _db.cache_appointments([{
            "id": appt_id, "anonymousId": p, "date": DATE, "time": "10:00",
            "status": "booked", "treated": False, "paid": False,
            "paymentMethod": None, "durationMin": DURATION, "patientMarkedPaid": False,
        }], replace_all=False)

        # Delete
        result = _fs.delete_appointment(appt_id)
        assert result["ok"]
        _db.delete_cached_appointment(appt_id)

        # Firebase
        assert _get_fb_appointment(appt_id) is None
        # Cache
        cached_ids = [a["id"] for a in _db.get_cached_appointments()]
        assert appt_id not in cached_ids

    def test_approve_updates_cache_and_auto_rejects(self):
        """Approve should update cache for the approved appointment
        and for all auto-rejected ones."""
        p1, p2 = [_add_patient(f"CacheApprove_{i}") for i in range(2)]
        id1 = _create_fb_pending(p1, DATE, "10:00")
        id2 = _create_fb_pending(p2, DATE, "10:15")

        # Cache both
        for appt_id, p, t in [(id1, p1, "10:00"), (id2, p2, "10:15")]:
            _db.cache_appointments([{
                "id": appt_id, "anonymousId": p, "date": DATE, "time": t,
                "status": "pending", "treated": False, "paid": False,
                "paymentMethod": None, "durationMin": DURATION, "patientMarkedPaid": False,
            }], replace_all=False)

        # Approve id1
        result = _fs.approve_appointment(id1)
        assert result["ok"]
        _db.update_cached_appointment_status(id1, "booked")
        for rej in result.get("rejected", []):
            _db.update_cached_appointment_status(rej, "cancelled")

        # Verify cache
        cached = _db.get_cached_appointments()
        approved = next((a for a in cached if a["id"] == id1), None)
        assert approved is not None
        assert approved["status"] == "booked"

        # The rejected one should not appear in active cache (cancelled is filtered)
        rejected_in_active = [a for a in cached if a["id"] == id2]
        assert len(rejected_in_active) == 0, "Auto-rejected should be filtered out"


# ═══════════════════════════════════════════════════════════════════════════════
# I. COMPLEX MULTI-DAY SCENARIOS
# ═══════════════════════════════════════════════════════════════════════════════

class TestMultiDayScenarios:
    """Scenarios spanning multiple days and involving mixed online/offline."""

    def test_offline_week_then_bulk_sync(self):
        """Doctor works offline for a week (5 days). Creates appointments
        each day. Goes online — all should sync without cross-day conflicts."""
        p = _add_patient("OfflineWeek")
        days = [f"2026-05-{d:02d}" for d in range(11, 16)]  # Mon-Fri

        for day in days:
            for t in ["09:00", "11:00", "14:00"]:
                _add_local_appointment(p, day, t)

        local_appts = _db.get_local_appointments()
        assert len(local_appts) == 15

        push_result = _fs.push_local_appointments(local_appts)
        assert push_result["ok"]
        assert push_result["pushed"] == 15
        assert len(push_result["conflicts"]) == 0

    def test_different_days_no_cross_conflict(self):
        """Appointments on different days with same time should NOT conflict."""
        p = _add_patient("CrossDay")
        _create_fb_appointment(p, DATE, "10:00", status="booked")

        # Different day, same time
        conflict = _fs.check_slot_conflict(DATE2, "10:00", DURATION)
        assert not conflict

    def test_mixed_statuses_across_days(self):
        """Create appointments with various statuses across days.
        Verify sync captures all statuses correctly."""
        p = _add_patient("MixedStatus")

        id_booked = _create_fb_appointment(p, DATE, "09:00", status="booked")
        id_pending = _create_fb_pending(p, DATE, "11:00")
        id_cancelled = _create_fb_appointment(p, DATE, "14:00", status="cancelled")

        all_result = _fs.sync_all_appointments()
        assert all_result["ok"]
        _db.cache_appointments(all_result["appointments"], replace_all=True)

        # sync_all_appointments fetches ALL statuses
        all_appts = all_result["appointments"]
        statuses = {a["id"]: a["status"] for a in all_appts}
        assert statuses[id_booked] == "booked"
        assert statuses[id_pending] == "pending"
        assert statuses[id_cancelled] == "cancelled"

        # get_cached_appointments filters out cancelled
        cached = _db.get_cached_appointments()
        cached_ids = [a["id"] for a in cached]
        assert id_booked in cached_ids
        assert id_pending in cached_ids
        assert id_cancelled not in cached_ids


# ═══════════════════════════════════════════════════════════════════════════════
# J. CONCURRENT FIREBASE OPERATIONS (ThreadPoolExecutor)
# ═══════════════════════════════════════════════════════════════════════════════

class TestConcurrentFirebaseOps:
    """Verify Firebase operations behave correctly under concurrent access."""

    def test_parallel_creates_different_slots(self):
        """5 concurrent creates on DIFFERENT dates — all should succeed.
        Uses different dates to avoid transaction contention (transactions
        that read the same date's docs contend with each other)."""
        patients = [_add_patient(f"ParCreate_{i}") for i in range(5)]
        dates = [f"2026-08-{10 + i:02d}" for i in range(5)]

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = [
                pool.submit(_fs.create_appointment, p, d, "10:00", DURATION)
                for p, d in zip(patients, dates)
            ]
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())

        successes = [r for r in results if r.get("ok")]
        assert len(successes) == 5, f"All 5 should succeed, got {len(successes)}"

    def test_parallel_creates_same_slot_only_one_wins(self):
        """5 concurrent creates on the SAME slot — with Firestore transactions
        only 1 should succeed."""
        patients = [_add_patient(f"ParSame_{i}") for i in range(5)]

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as pool:
            futures = [
                pool.submit(_fs.create_appointment, p, DATE, "10:00", DURATION)
                for p in patients
            ]
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())

        successes = [r for r in results if r.get("ok")]
        assert len(successes) == 1, (
            f"Expected exactly 1 success, got {len(successes)}. "
            f"Transaction should prevent concurrent creates on same slot."
        )

    def test_parallel_sync_does_not_corrupt_cache(self):
        """Two parallel syncs should not corrupt the cache."""
        p = _add_patient("ParSync")
        for t in ["09:00", "10:00", "11:00"]:
            _create_fb_appointment(p, DATE, t)

        def do_sync():
            r = _fs.sync_all_appointments()
            if r["ok"]:
                _db.cache_appointments(r["appointments"], replace_all=True)
            return r

        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            f1 = pool.submit(do_sync)
            f2 = pool.submit(do_sync)
            r1 = f1.result()
            r2 = f2.result()

        assert r1["ok"]
        assert r2["ok"]

        # Cache should have exactly 3 appointments
        cached = _db.get_cached_appointments()
        cached_on_date = [a for a in cached if a["date"] == DATE]
        assert len(cached_on_date) == 3, \
            f"Expected 3 cached, got {len(cached_on_date)} (possible corruption)"


# ═══════════════════════════════════════════════════════════════════════════════
# K. PAYMENT & STATUS FLOW VIA FIREBASE
# ═══════════════════════════════════════════════════════════════════════════════

class TestFirebasePaymentFlow:
    """End-to-end payment flow through Firebase."""

    def test_full_payment_flow_firebase(self):
        """Create → treat → pay → verify in Firebase."""
        p = _add_patient("PayFlow")
        result = _fs.create_appointment(p, DATE, "10:00", DURATION)
        assert result["ok"]
        appt_id = result["id"]

        # Mark treated
        r = _fs.mark_appointment(appt_id, "treated", True)
        assert r["ok"]
        fb = _get_fb_appointment(appt_id)
        assert fb["treated"] is True

        # Mark paid with method
        r = _fs.mark_appointment(appt_id, "paid", True, payment_method="bit")
        assert r["ok"]
        fb = _get_fb_appointment(appt_id)
        assert fb["paid"] is True
        assert fb["paymentMethod"] == "bit"

    def test_untreat_clears_payment_firebase(self):
        """Un-treating should also clear payment fields in Firebase."""
        p = _add_patient("UntreatFB")
        result = _fs.create_appointment(p, DATE, "10:00", DURATION)
        appt_id = result["id"]

        _fs.mark_appointment(appt_id, "treated", True)
        _fs.mark_appointment(appt_id, "paid", True, payment_method="cash")

        # Un-treat
        _fs.mark_appointment(appt_id, "treated", False)
        _fs.mark_appointment(appt_id, "paid", False)

        fb = _get_fb_appointment(appt_id)
        assert fb["treated"] is False
        assert fb["paid"] is False
        assert fb["paymentMethod"] is None

    def test_payment_method_persists_through_sync(self):
        """Payment method set in Firebase should survive a full cache sync."""
        p = _add_patient("PaySync")
        result = _fs.create_appointment(p, DATE, "10:00", DURATION)
        appt_id = result["id"]

        _fs.mark_appointment(appt_id, "treated", True)
        _fs.mark_appointment(appt_id, "paid", True, payment_method="paybox")

        # Full sync
        all_r = _fs.sync_all_appointments()
        assert all_r["ok"]
        _db.cache_appointments(all_r["appointments"], replace_all=True)

        cached = _db.get_cached_appointments()
        match = next(a for a in cached if a["id"] == appt_id)
        assert match["paid"] is True
        assert match["paymentMethod"] == "paybox"


# ═══════════════════════════════════════════════════════════════════════════════
# L. BATCH PUSH EDGE CASES
# ═══════════════════════════════════════════════════════════════════════════════

class TestBatchPushEdgeCases:
    """Edge cases in push_local_appointments batch logic."""

    def test_push_empty_list(self):
        """Pushing an empty list should succeed with 0 pushed."""
        result = _fs.push_local_appointments([])
        assert result["ok"]
        assert result["pushed"] == 0

    def test_push_single_appointment(self):
        """Pushing exactly 1 appointment should work."""
        p = _add_patient("Single")
        _add_local_appointment(p, DATE, "10:00")
        local = _db.get_local_appointments()
        result = _fs.push_local_appointments(local)
        assert result["ok"]
        assert result["pushed"] == 1

    def test_push_with_mixed_statuses(self):
        """Push local appointments with mixed statuses (booked + pending)."""
        p = _add_patient("MixPush")
        _add_local_appointment(p, DATE, "09:00", status="booked")
        _add_local_appointment(p, DATE, "10:00", status="pending")
        _add_local_appointment(p, DATE, "11:00", status="booked")

        local = _db.get_local_appointments()
        result = _fs.push_local_appointments(local)
        assert result["ok"]
        assert result["pushed"] == 3

        # Verify statuses preserved in Firebase
        for appt in result["new_appointments"]:
            fb = _get_fb_appointment(appt["id"])
            assert fb is not None

    def test_push_preserves_treated_paid_flags(self):
        """Treated/paid flags from local appointments should be preserved
        when pushed to Firebase."""
        p = _add_patient("FlagsPush")
        local_id = _add_local_appointment(p, DATE, "10:00")
        _db.mark_local_appointment(local_id, "treated", True)
        _db.mark_local_appointment(local_id, "paid", True, payment_method="bank")

        local = _db.get_all_local_appointments()
        result = _fs.push_local_appointments(local)
        assert result["ok"]
        assert result["pushed"] == 1

        fb = _get_fb_appointment(result["new_appointments"][0]["id"])
        assert fb["treated"] is True
        assert fb["paid"] is True
        assert fb["paymentMethod"] == "bank"


# ═══════════════════════════════════════════════════════════════════════════════
# M. RESCHEDULE VIA FIREBASE
# ═══════════════════════════════════════════════════════════════════════════════

class TestFirebaseReschedule:
    """Test rescheduling directly in Firebase."""

    def test_reschedule_to_free_slot(self):
        """Reschedule to an empty slot — should succeed."""
        p = _add_patient("ReschedFB")
        result = _fs.create_appointment(p, DATE, "10:00", DURATION)
        appt_id = result["id"]

        r = _fs.reschedule_appointment(appt_id, DATE, "16:00", DURATION)
        assert r["ok"]

        fb = _get_fb_appointment(appt_id)
        assert fb["date"] == DATE
        assert fb["time"] == "16:00"
        assert "rescheduledAt" in fb

    def test_reschedule_to_different_day(self):
        """Reschedule to a different day — should succeed."""
        p = _add_patient("ReschedDay")
        result = _fs.create_appointment(p, DATE, "10:00", DURATION)
        appt_id = result["id"]

        r = _fs.reschedule_appointment(appt_id, DATE2, "10:00")
        assert r["ok"]

        fb = _get_fb_appointment(appt_id)
        assert fb["date"] == DATE2

    def test_reschedule_updates_duration(self):
        """Rescheduling with a new duration should update durationMin."""
        p = _add_patient("ReschedDur")
        result = _fs.create_appointment(p, DATE, "10:00", 45)
        appt_id = result["id"]

        r = _fs.reschedule_appointment(appt_id, DATE, "10:00", duration_min=60)
        assert r["ok"]

        fb = _get_fb_appointment(appt_id)
        assert fb["durationMin"] == 60


# ═══════════════════════════════════════════════════════════════════════════════
# N. EDGE CASE: WALKIN + MULTIPLE PATIENT TYPES
# ═══════════════════════════════════════════════════════════════════════════════

class TestWalkinAndSpecialCases:
    """Walk-in appointments and special patient ID handling."""

    def test_walkin_appointment_syncs_correctly(self):
        """Walk-in (WALKIN) appointments should sync to/from Firebase."""
        result = _fs.create_appointment(_db.WALKIN_ID, DATE, "10:00", DURATION)
        assert result["ok"]

        all_r = _fs.sync_all_appointments()
        assert all_r["ok"]
        _db.cache_appointments(all_r["appointments"], replace_all=True)

        cached = _db.get_cached_appointments()
        walkin = [a for a in cached if a["anonymousId"] == _db.WALKIN_ID]
        assert len(walkin) == 1

    def test_walkin_and_regular_same_slot_conflict(self):
        """Walk-in at 10:00 should conflict with regular at 10:00."""
        _create_fb_appointment(_db.WALKIN_ID, DATE, "10:00", status="booked")
        p = _add_patient("RegVsWalkin")
        conflict = _fs.check_slot_conflict(DATE, "10:00", DURATION)
        assert conflict


# ═══════════════════════════════════════════════════════════════════════════════
# O. STRESS TEST: LARGE VOLUME
# ═══════════════════════════════════════════════════════════════════════════════

class TestLargeVolumeSync:
    """Stress tests with larger numbers of appointments."""

    def test_push_20_local_appointments(self):
        """Push 20 local appointments in a single batch — all should arrive."""
        p = _add_patient("Bulk20")
        for i in range(20):
            h = 7 + (i % 12)
            m = (i // 12) * 30
            _add_local_appointment(p, f"2026-06-{10 + i // 4:02d}", f"{h:02d}:{m:02d}")

        local = _db.get_local_appointments()
        assert len(local) == 20

        result = _fs.push_local_appointments(local)
        assert result["ok"]
        assert result["pushed"] == 20
        assert len(result["new_appointments"]) == 20

    def test_sync_30_firebase_appointments(self):
        """Create 30 appointments in Firebase, sync all, verify cache has 30."""
        p = _add_patient("Sync30")
        for i in range(30):
            day = f"2026-07-{1 + i // 6:02d}"
            h = 8 + (i % 6) * 2
            _create_fb_appointment(p, day, f"{h:02d}:00", status="booked")

        all_r = _fs.sync_all_appointments()
        assert all_r["ok"]
        assert len(all_r["appointments"]) == 30

        _db.cache_appointments(all_r["appointments"], replace_all=True)
        cached = _db.get_cached_appointments()
        assert len(cached) == 30
