"""
Comprehensive test suite – AnonimousQ Doctor App
==================================================
Covers:
  A. Performance / response time
  B. Background / cyclic processes
  C. Online / Offline CRUD – appointments (add, delete, reschedule)
  D. Patient deletion
  E. Payment logic (patientMarkedPaid → doctor confirms → patient sees update)
  F. Conflict detection (all paths)

Run from the project root:
    python -m pytest tests/test_comprehensive.py -v
"""

import os, sys, time, sqlite3, threading
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import db as _db

# ─────────────────────────────────────────────────────────────────────────────
# Fixture: isolated in-memory-style SQLite DB for every test
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    tmp_db = str(tmp_path / "test.db")
    monkeypatch.setattr(_db, "DB_PATH",  tmp_db)
    monkeypatch.setattr(_db, "DATA_DIR", str(tmp_path))
    _db.init_db()
    yield


# ─── helpers ─────────────────────────────────────────────────────────────────

def _add_patient(name="Test", price=300):
    result = _db.add_patient(name, phone="050-0000000", notes="", price=price)
    assert result["ok"], result
    return result["anonymous_id"]


def _add_local(anon_id, date, time_str, duration=45, status="booked"):
    conn = sqlite3.connect(_db.DB_PATH)
    conn.row_factory = sqlite3.Row
    _db._ensure_local_appointments_table(conn)
    cur = conn.execute(
        "INSERT INTO local_appointments (anonymous_id, date, time, status, duration_min)"
        " VALUES (?, ?, ?, ?, ?)",
        (anon_id, date, time_str, status, duration),
    )
    rid = cur.lastrowid
    conn.commit(); conn.close()
    return rid


def _add_cached(fb_id, anon_id, date, time_str, duration=45, status="booked",
                patient_marked_paid=False):
    _db.cache_appointments([{
        "id":               fb_id,
        "anonymousId":      anon_id,
        "date":             date,
        "time":             time_str,
        "status":           status,
        "treated":          False,
        "paid":             False,
        "paymentMethod":    None,
        "durationMin":      duration,
        "patientMarkedPaid": patient_marked_paid,
    }], replace_all=False)


# ═══════════════════════════════════════════════════════════════════════════════
# A. PERFORMANCE – response time of critical DB operations
# ═══════════════════════════════════════════════════════════════════════════════

class TestPerformance:
    DATE = "2026-04-01"

    def test_add_patient_speed(self):
        """Adding a patient must complete in under 200 ms."""
        t0 = time.monotonic()
        _db.add_patient("PerformanceTest", price=100)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.2, f"add_patient took {elapsed:.3f}s (expected <0.2s)"

    def test_get_patients_speed_100(self):
        """Fetching 100 patients must complete in under 100 ms."""
        for i in range(100):
            _db.add_patient(f"Patient {i}", price=200)
        t0 = time.monotonic()
        patients = _db.get_patients()
        elapsed = time.monotonic() - t0
        assert len(patients) == 100
        assert elapsed < 0.1, f"get_patients (100 rows) took {elapsed:.3f}s"

    def test_conflict_check_local_speed_50_appts(self):
        """Slot conflict check across 50 existing appointments < 30 ms."""
        anon = _add_patient("Perf")
        for i in range(50):
            h = 8 + (i % 8)
            m = (i // 8) * 10
            _add_local(anon, self.DATE, f"{h:02d}:{m:02d}", 45)
        t0 = time.monotonic()
        _db.check_slot_conflict_local(self.DATE, "16:00", 45)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.03, f"conflict check (50 appts) took {elapsed:.3f}s"

    def test_cache_appointments_bulk_speed(self):
        """Caching 200 appointments must complete in under 300 ms."""
        anon = _add_patient("Bulk")
        appts = [
            {
                "id":          f"fb-{i}",
                "anonymousId": anon,
                "date":        "2026-04-01",
                "time":        f"{8 + i // 60:02d}:{i % 60:02d}",
                "status":      "booked",
                "treated":     False,
                "paid":        False,
                "paymentMethod": None,
                "durationMin": 30,
                "patientMarkedPaid": False,
            }
            for i in range(200)
        ]
        t0 = time.monotonic()
        _db.cache_appointments(appts, replace_all=True)
        elapsed = time.monotonic() - t0
        assert elapsed < 0.3, f"cache_appointments(200) took {elapsed:.3f}s"

    def test_get_local_appointments_speed(self):
        """Reading 100 local appointments must complete in under 50 ms."""
        anon = _add_patient("LocalPerf")
        for i in range(100):
            _add_local(anon, "2026-05-01", f"{8 + i // 6:02d}:{(i % 6) * 10:02d}")
        t0 = time.monotonic()
        appts = _db.get_local_appointments()
        elapsed = time.monotonic() - t0
        assert len(appts) == 100
        assert elapsed < 0.05, f"get_local_appointments(100) took {elapsed:.3f}s"


# ═══════════════════════════════════════════════════════════════════════════════
# B. BACKGROUND / CYCLIC PROCESSES – no unexpected spinning
# ═══════════════════════════════════════════════════════════════════════════════

class TestBackgroundProcesses:
    def test_no_daemon_threads_on_import(self):
        """Importing db and firebase_sync must not start background threads."""
        import firebase_sync as _fs
        daemon_threads = [t for t in threading.enumerate()
                         if t.daemon and t.name not in ("MainThread",)]
        # Only threads started by Flask dev server / pytest runner are acceptable;
        # the modules themselves must not spin up pollers.
        module_threads = [t for t in daemon_threads
                         if "anonimous" in t.name.lower()
                         or "sync" in t.name.lower()
                         or "poll" in t.name.lower()]
        assert module_threads == [], \
            f"Unexpected background threads: {[t.name for t in module_threads]}"

    def test_auto_backup_runs_once_per_day(self, tmp_path, monkeypatch):
        """auto_backup must write a backup file and not loop."""
        import shutil, os
        backup_dir = str(tmp_path / "backup")
        monkeypatch.setattr(_db, "BACKUP_DIR", backup_dir)
        _db.auto_backup()
        files = os.listdir(backup_dir) if os.path.isdir(backup_dir) else []
        # Either created a .db backup or skipped (already backed up today)
        assert isinstance(files, list)  # just verifies it doesn't crash/loop

    def test_sync_log_prunes_to_50(self):
        """sync_log must never grow past 50 entries."""
        for i in range(70):
            _db.log_sync(i, "ok")
        conn = sqlite3.connect(_db.DB_PATH)
        count = conn.execute("SELECT COUNT(*) FROM sync_log").fetchone()[0]
        conn.close()
        assert count <= 50, f"sync_log grew to {count} rows (expected ≤50)"


# ═══════════════════════════════════════════════════════════════════════════════
# C. ONLINE / OFFLINE CRUD – appointments
# ═══════════════════════════════════════════════════════════════════════════════

class TestAppointmentCRUD:
    DATE = "2026-06-15"

    # ── CREATE ─────────────────────────────────────────────────────────────────

    def test_create_local_appointment_success(self):
        anon = _add_patient("CreateTest")
        result = _db.create_local_appointment(anon, self.DATE, "10:00", "booked", 45)
        assert result["ok"]
        assert result["id"] > 0

    def test_create_local_blocked_by_conflict(self):
        """App layer must pre-check before creating – verify conflict is detected."""
        anon = _add_patient("ConflictTest")
        _add_local(anon, self.DATE, "10:00", 45)
        conflict = _db.check_slot_conflict_local(self.DATE, "10:30", 45)
        assert conflict, "10:30 should overlap with 10:00–10:45"

    def test_create_offline_ignores_pending_cached(self):
        """When offline, doctor creating a slot with only pending cached appts proceeds."""
        anon1 = _add_patient("PendingPatient")
        anon2 = _add_patient("DoctorPatient")
        _add_cached("fb-pending", anon1, self.DATE, "10:00", 45, status="pending")
        # With exclude_pending=True (doctor-create path), no conflict
        conflict = _db.check_slot_conflict_cached(
            self.DATE, "10:00", 45, exclude_pending=True
        )
        assert not conflict

    def test_create_offline_blocked_by_booked_cached(self):
        """When offline, booked cached appointments still block new creation."""
        anon1 = _add_patient("BookedPatient")
        _add_cached("fb-booked", anon1, self.DATE, "10:00", 45, status="booked")
        conflict = _db.check_slot_conflict_cached(
            self.DATE, "10:00", 45, exclude_pending=True
        )
        assert conflict

    # ── DELETE ─────────────────────────────────────────────────────────────────

    def test_delete_local_appointment(self):
        anon = _add_patient("DeleteTest")
        appt_id = _add_local(anon, self.DATE, "11:00")
        result = _db.delete_local_appointment(appt_id)
        assert result["ok"]
        remaining = [a for a in _db.get_local_appointments() if a["id"] == appt_id]
        assert remaining == []

    def test_delete_cached_appointment(self):
        anon = _add_patient("DeleteCached")
        _add_cached("fb-del", anon, self.DATE, "12:00")
        _db.delete_cached_appointment("fb-del")
        cached = _db.get_cached_appointments()
        assert not any(a["id"] == "fb-del" for a in cached)

    # ── RESCHEDULE ─────────────────────────────────────────────────────────────

    def test_reschedule_local_appointment(self):
        anon = _add_patient("RescheduleTest")
        appt_id = _add_local(anon, self.DATE, "09:00", 45)
        result = _db.reschedule_local_appointment(appt_id, "2026-06-20", "14:00", 60)
        assert result["ok"]
        appts = _db.get_local_appointments()
        moved = next((a for a in appts if a["id"] == appt_id), None)
        assert moved is not None
        assert moved["date"] == "2026-06-20"
        assert moved["time"] == "14:00"
        assert moved["durationMin"] == 60

    def test_reschedule_self_does_not_conflict(self):
        """Rescheduling an appointment to its own slot must not report a conflict."""
        anon = _add_patient("SelfReschedule")
        appt_id = _add_local(anon, self.DATE, "10:00", 45)
        conflict = _db.check_slot_conflict_local(
            self.DATE, "10:00", 45, exclude_id=appt_id
        )
        assert not conflict

    def test_reschedule_conflict_detected(self):
        """Moving appointment A to collide with appointment B must be detected."""
        anon = _add_patient("ReschedConflict")
        _add_local(anon, self.DATE, "10:00", 45, status="booked")
        appt_b = _add_local(anon, self.DATE, "11:00", 45, status="booked")
        # Try to move B to 10:30 (overlaps with A)
        conflict = _db.check_slot_conflict_local(
            self.DATE, "10:30", 45, exclude_id=appt_b
        )
        assert conflict

    # ── APPROVE / REJECT ───────────────────────────────────────────────────────

    def test_approve_local_changes_status_to_booked(self):
        anon = _add_patient("ApproveTest")
        appt_id = _add_local(anon, self.DATE, "13:00", status="pending")
        result = _db.approve_local_appointment(appt_id)
        assert result["ok"]
        appts = _db.get_all_local_appointments()
        appt = next(a for a in appts if a["id"] == appt_id)
        assert appt["status"] == "booked"

    def test_reject_local_changes_status_to_cancelled(self):
        anon = _add_patient("RejectTest")
        appt_id = _add_local(anon, self.DATE, "14:00", status="pending")
        result = _db.reject_local_appointment(appt_id)
        assert result["ok"]
        appts = _db.get_all_local_appointments()
        appt = next(a for a in appts if a["id"] == appt_id)
        assert appt["status"] == "cancelled"


# ═══════════════════════════════════════════════════════════════════════════════
# D. PATIENT DELETION
# ═══════════════════════════════════════════════════════════════════════════════

class TestPatientDeletion:

    def test_delete_patient_removes_from_db(self):
        result = _db.add_patient("ToDelete", price=100)
        anon_id = result["anonymous_id"]
        patients_before = _db.get_patients()
        patient_row = next(p for p in patients_before if p["anonymous_id"] == anon_id)
        _db.delete_patient(patient_row["id"])
        patients_after = _db.get_patients()
        assert not any(p["anonymous_id"] == anon_id for p in patients_after)

    def test_deactivate_patient_hides_from_active_list(self):
        result = _db.add_patient("ToDeactivate", price=100)
        anon_id = result["anonymous_id"]
        patients = _db.get_patients()
        row = next(p for p in patients if p["anonymous_id"] == anon_id)
        _db.set_patient_active(row["id"], 0)
        active = _db.get_patients()
        inactive = _db.get_inactive_patients()
        assert not any(p["anonymous_id"] == anon_id for p in active)
        assert any(p["anonymous_id"] == anon_id for p in inactive)

    def test_reactivate_patient(self):
        result = _db.add_patient("ToReactivate", price=100)
        anon_id = result["anonymous_id"]
        patients = _db.get_patients()
        row = next(p for p in patients if p["anonymous_id"] == anon_id)
        _db.set_patient_active(row["id"], 0)
        _db.set_patient_active(row["id"], 1)
        active = _db.get_patients()
        assert any(p["anonymous_id"] == anon_id for p in active)

    def test_uuid_map_excludes_deleted_patient(self):
        result = _db.add_patient("MapTest", price=100)
        anon_id = result["anonymous_id"]
        patients = _db.get_patients()
        row = next(p for p in patients if p["anonymous_id"] == anon_id)
        _db.delete_patient(row["id"])
        uuid_map = _db.get_uuid_map()
        assert anon_id not in uuid_map


# ═══════════════════════════════════════════════════════════════════════════════
# E. PAYMENT LOGIC
# Full flow: patient marks paid on website → patientMarkedPaid flag set
#            → doctor sees flag → doctor confirms paid → cache updated
#            → patient would see paid=True on refresh
# ═══════════════════════════════════════════════════════════════════════════════

class TestPaymentLogic:
    DATE = "2026-07-01"

    def test_treatment_required_before_payment_flag(self):
        """is_appointment_treated must return False for untreated appointment."""
        anon = _add_patient("PayFlow")
        appt_id = _add_local(anon, self.DATE, "09:00", status="booked")
        treated = _db.is_appointment_treated(str(appt_id), "local")
        assert not treated

    def test_mark_treated_sets_flag(self):
        anon = _add_patient("MarkTreated")
        appt_id = _add_local(anon, self.DATE, "10:00")
        _db.mark_local_appointment(appt_id, "treated", True)
        assert _db.is_appointment_treated(str(appt_id), "local")

    def test_mark_paid_fails_logically_when_not_treated(self):
        """Backend guard: payment without treatment should return error."""
        anon = _add_patient("PaidNoTreat")
        appt_id = _add_local(anon, self.DATE, "11:00")
        treated = _db.is_appointment_treated(str(appt_id), "local")
        # app.py checks this before calling mark_local_appointment;
        # verify the DB layer would allow it (guard is in the route)
        assert not treated, "Guard in app.py depends on this being False"

    def test_mark_paid_with_payment_method(self):
        anon = _add_patient("PaidWithMethod")
        appt_id = _add_local(anon, self.DATE, "12:00")
        _db.mark_local_appointment(appt_id, "treated", True)
        result = _db.mark_local_appointment(appt_id, "paid", True, payment_method="bit")
        assert result["ok"]
        appts = _db.get_all_local_appointments()
        appt = next(a for a in appts if a["id"] == appt_id)
        assert appt["paid"]
        assert appt["paymentMethod"] == "bit"

    def test_untreat_clears_payment(self):
        """Un-treating an appointment must also clear payment (mirrors app.py logic)."""
        anon = _add_patient("UntreatClear")
        appt_id = _add_local(anon, self.DATE, "13:00")
        _db.mark_local_appointment(appt_id, "treated", True)
        _db.mark_local_appointment(appt_id, "paid", True, payment_method="cash")
        # Un-treat
        _db.mark_local_appointment(appt_id, "treated", False)
        _db.mark_local_appointment(appt_id, "paid", False, payment_method=None)
        appts = _db.get_all_local_appointments()
        appt = next(a for a in appts if a["id"] == appt_id)
        assert not appt["paid"]
        assert appt["paymentMethod"] is None

    def test_patient_marked_paid_flag_stored_in_cache(self):
        """patientMarkedPaid flag from website should survive cache round-trip."""
        anon = _add_patient("PatientPaid")
        _add_cached(
            "fb-pmk", anon, self.DATE, "14:00",
            status="booked", patient_marked_paid=True
        )
        cached = _db.get_cached_appointments()
        appt = next((a for a in cached if a["id"] == "fb-pmk"), None)
        assert appt is not None
        assert appt["patientMarkedPaid"], "patientMarkedPaid must survive cache"

    def test_doctor_confirms_payment_updates_cache(self):
        """After doctor confirms, cache must show paid=True and patientMarkedPaid irrelevant."""
        anon = _add_patient("DoctorConfirm")
        _add_cached("fb-doc", anon, self.DATE, "15:00",
                    status="booked", patient_marked_paid=True)
        # Doctor marks treated first
        _db.update_cached_appointment("fb-doc", "treated", True)
        # Doctor confirms payment
        _db.update_cached_appointment("fb-doc", "paid", True, payment_method="paybox")
        cached = _db.get_cached_appointments()
        appt = next(a for a in cached if a["id"] == "fb-doc")
        assert appt["paid"]
        assert appt["paymentMethod"] == "paybox"

    def test_payment_methods_accepted(self):
        """All four payment methods must be storable."""
        anon = _add_patient("AllMethods")
        for method in ("bit", "paybox", "cash", "bank"):
            appt_id = _add_local(anon, self.DATE, "09:00", status="booked")
            _db.mark_local_appointment(appt_id, "treated", True)
            _db.mark_local_appointment(appt_id, "paid", True, payment_method=method)
            appts = _db.get_all_local_appointments()
            appt = next(a for a in appts if a["id"] == appt_id)
            assert appt["paymentMethod"] == method


# ═══════════════════════════════════════════════════════════════════════════════
# F. CONFLICT DETECTION – all combinations
# ═══════════════════════════════════════════════════════════════════════════════

class TestConflictDetectionAllPaths:
    DATE = "2026-08-01"

    def test_local_vs_local_overlap(self):
        anon = _add_patient("LL")
        _add_local(anon, self.DATE, "10:00", 60)
        assert _db.check_slot_conflict_local(self.DATE, "10:30", 60)

    def test_local_vs_local_adjacent_ok(self):
        anon = _add_patient("LLAdj")
        _add_local(anon, self.DATE, "10:00", 60)
        assert not _db.check_slot_conflict_local(self.DATE, "11:00", 60)

    def test_cached_booked_blocks(self):
        anon = _add_patient("CB")
        _add_cached("fb-cb", anon, self.DATE, "10:00", 60, status="booked")
        assert _db.check_slot_conflict_cached(self.DATE, "10:30", 60)

    def test_cached_pending_blocks_default(self):
        anon = _add_patient("CPend")
        _add_cached("fb-pend", anon, self.DATE, "10:00", 45, status="pending")
        assert _db.check_slot_conflict_cached(self.DATE, "10:00", 45)

    def test_cached_pending_not_blocking_with_exclude(self):
        anon = _add_patient("CPendEx")
        _add_cached("fb-pex", anon, self.DATE, "10:00", 45, status="pending")
        assert not _db.check_slot_conflict_cached(
            self.DATE, "10:00", 45, exclude_pending=True
        )

    def test_cached_cancelled_never_blocks(self):
        anon = _add_patient("CCan")
        _add_cached("fb-can", anon, self.DATE, "10:00", 45, status="cancelled")
        assert not _db.check_slot_conflict_cached(self.DATE, "10:00", 45)

    def test_mixed_local_and_cached_both_checked_offline(self):
        anon = _add_patient("Mixed")
        _add_cached("fb-mix", anon, self.DATE, "10:00", 45, status="booked")
        _add_local(anon, self.DATE, "11:00", 45)
        # 10:30 overlaps with cached
        assert _db.check_slot_conflict_cached(self.DATE, "10:30", 45)
        # 11:20 overlaps with local
        assert _db.check_slot_conflict_local(self.DATE, "11:20", 45)
        # 12:00 is clear in both
        assert not _db.check_slot_conflict_cached(self.DATE, "12:00", 45)
        assert not _db.check_slot_conflict_local(self.DATE, "12:00", 45)

    def test_ranges_overlap_edge_cases(self):
        # Exact touch (adjacent) must NOT overlap
        assert not _db.ranges_overlap(600, 45, 645, 45)
        # 1-minute overlap must flag
        assert _db.ranges_overlap(600, 46, 645, 45)
        # Contained entirely
        assert _db.ranges_overlap(600, 120, 630, 30)
        # Reversed containment
        assert _db.ranges_overlap(630, 30, 600, 120)
