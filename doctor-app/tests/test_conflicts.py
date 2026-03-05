"""
Unit tests for appointment conflict detection logic.
Run from the project root:
    python -m pytest tests/test_conflicts.py -v

No Firebase connection required – all tests operate on an in-memory SQLite DB.
"""

import os
import sys
import sqlite3
import tempfile
import pytest

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import db as _db

# ─────────────────────────────────────────────────────────────────
# Fixtures
# ─────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def isolated_db(tmp_path, monkeypatch):
    """Redirect DB_PATH and DATA_DIR to a temp directory for each test."""
    tmp_db = str(tmp_path / "test.db")
    monkeypatch.setattr(_db, "DB_PATH",   tmp_db)
    monkeypatch.setattr(_db, "DATA_DIR",  str(tmp_path))
    _db.init_db()
    yield


# ─────────────────────────────────────────────────────────────────
# 1. ranges_overlap – pure maths, no DB needed
# ─────────────────────────────────────────────────────────────────

class TestRangesOverlap:
    def test_exact_match(self):
        # [10:00–10:45) vs [10:00–10:45)
        assert _db.ranges_overlap(600, 45, 600, 45)

    def test_partial_overlap_start(self):
        # [10:00–10:45) vs [10:30–11:15)
        assert _db.ranges_overlap(600, 45, 630, 45)

    def test_partial_overlap_end(self):
        # [10:30–11:15) vs [10:00–10:45)
        assert _db.ranges_overlap(630, 45, 600, 45)

    def test_adjacent_no_overlap(self):
        # [10:00–10:45) vs [10:45–11:30) – adjacent, not overlapping
        assert not _db.ranges_overlap(600, 45, 645, 45)

    def test_no_overlap(self):
        # [10:00–10:45) vs [11:00–11:45)
        assert not _db.ranges_overlap(600, 45, 660, 45)

    def test_contained(self):
        # [10:00–11:00) contains [10:15–10:45)
        assert _db.ranges_overlap(600, 60, 615, 30)

    def test_wrapping(self):
        # [10:15–10:45) is contained in [10:00–11:00)
        assert _db.ranges_overlap(615, 30, 600, 60)


# ─────────────────────────────────────────────────────────────────
# 2. check_slot_conflict_local
# ─────────────────────────────────────────────────────────────────

def _add_local(anon_id, date, time, duration=45, status="booked"):
    conn = sqlite3.connect(_db.DB_PATH)
    conn.row_factory = sqlite3.Row
    _db._ensure_local_appointments_table(conn)
    cur = conn.execute(
        "INSERT INTO local_appointments (anonymous_id, date, time, status, duration_min) "
        "VALUES (?, ?, ?, ?, ?)",
        (anon_id, date, time, status, duration),
    )
    row_id = cur.lastrowid
    conn.commit()
    conn.close()
    return row_id


class TestCheckSlotConflictLocal:
    DATE = "2026-03-01"

    def test_no_conflict_on_empty_db(self):
        assert not _db.check_slot_conflict_local(self.DATE, "10:00", 45)

    def test_exact_overlap(self):
        _add_local("A001A", self.DATE, "10:00", 45)
        assert _db.check_slot_conflict_local(self.DATE, "10:00", 45)

    def test_partial_overlap(self):
        _add_local("A001A", self.DATE, "10:00", 45)
        # 10:30 starts before 10:45 ends
        assert _db.check_slot_conflict_local(self.DATE, "10:30", 45)

    def test_adjacent_no_overlap(self):
        _add_local("A001A", self.DATE, "10:00", 45)
        # 10:45 starts exactly when the first ends
        assert not _db.check_slot_conflict_local(self.DATE, "10:45", 45)

    def test_cancelled_ignored(self):
        appt_id = _add_local("A001A", self.DATE, "10:00", 45, status="cancelled")
        assert not _db.check_slot_conflict_local(self.DATE, "10:00", 45)

    def test_exclude_id_skips_self(self):
        appt_id = _add_local("A001A", self.DATE, "10:00", 45)
        # Reschedule self to same slot – should not conflict with itself
        assert not _db.check_slot_conflict_local(self.DATE, "10:00", 45, exclude_id=appt_id)

    def test_different_date_no_conflict(self):
        _add_local("A001A", self.DATE, "10:00", 45)
        assert not _db.check_slot_conflict_local("2026-03-02", "10:00", 45)


# ─────────────────────────────────────────────────────────────────
# 3. check_slot_conflict_cached
# ─────────────────────────────────────────────────────────────────

def _add_cached(fb_id, anon_id, date, time, duration=45, status="booked"):
    _db.cache_appointments([{
        "id":          fb_id,
        "anonymousId": anon_id,
        "date":        date,
        "time":        time,
        "status":      status,
        "treated":     False,
        "paid":        False,
        "paymentMethod": None,
        "durationMin": duration,
        "patientMarkedPaid": False,
    }], replace_all=False)


class TestCheckSlotConflictCached:
    DATE = "2026-03-05"

    def test_no_conflict_on_empty_cache(self):
        assert not _db.check_slot_conflict_cached(self.DATE, "10:00", 45)

    def test_booked_blocks(self):
        _add_cached("fb-001", "A001A", self.DATE, "10:00", 45, status="booked")
        assert _db.check_slot_conflict_cached(self.DATE, "10:00", 45)

    def test_pending_blocks_by_default(self):
        _add_cached("fb-002", "A002A", self.DATE, "10:00", 45, status="pending")
        # Default: pending IS a conflict
        assert _db.check_slot_conflict_cached(self.DATE, "10:00", 45)

    def test_pending_ignored_when_exclude_pending(self):
        _add_cached("fb-003", "A003A", self.DATE, "10:00", 45, status="pending")
        # Doctor creates over a pending web-booking
        assert not _db.check_slot_conflict_cached(
            self.DATE, "10:00", 45, exclude_pending=True
        )

    def test_cancelled_ignored(self):
        _add_cached("fb-004", "A004A", self.DATE, "10:00", 45, status="cancelled")
        assert not _db.check_slot_conflict_cached(self.DATE, "10:00", 45)

    def test_exclude_id_skips_self(self):
        _add_cached("fb-005", "A005A", self.DATE, "10:00", 45)
        assert not _db.check_slot_conflict_cached(
            self.DATE, "10:00", 45, exclude_id="fb-005"
        )

    def test_adjacent_no_conflict(self):
        _add_cached("fb-006", "A006A", self.DATE, "10:00", 45)
        assert not _db.check_slot_conflict_cached(self.DATE, "10:45", 45)


# ─────────────────────────────────────────────────────────────────
# 4. create_local_appointment – double-booking prevented
# ─────────────────────────────────────────────────────────────────

class TestCreateLocalAppointment:
    DATE = "2026-03-10"

    def test_create_succeeds(self):
        result = _db.create_local_appointment("A001A", self.DATE, "10:00", "booked", 45)
        assert result["ok"]

    def test_conflict_not_auto_checked_at_db_level(self):
        """create_local_appointment itself doesn't check for conflicts
        (that's the caller's responsibility in app.py).
        Verify it returns ok=True each time — conflict check lives in app.py."""
        _db.create_local_appointment("A001A", self.DATE, "10:00", "booked", 45)
        result = _db.create_local_appointment("A002A", self.DATE, "10:00", "booked", 45)
        # DB layer allows it — caller must pre-check
        assert result["ok"]

    def test_local_conflict_detected_before_create(self):
        """Simulate what app.py does: check conflict before calling create."""
        _db.create_local_appointment("A001A", self.DATE, "10:00", "booked", 45)
        conflict = _db.check_slot_conflict_local(self.DATE, "10:30", 45)
        # 10:30 overlaps with 10:00–10:45
        assert conflict


# ─────────────────────────────────────────────────────────────────
# 5. _time_to_minutes and compute_end_time helpers
# ─────────────────────────────────────────────────────────────────

class TestTimeHelpers:
    def test_time_to_minutes_basic(self):
        assert _db._time_to_minutes("09:00") == 540
        assert _db._time_to_minutes("10:30") == 630
        assert _db._time_to_minutes("00:00") == 0
        assert _db._time_to_minutes("23:59") == 1439

    def test_time_to_minutes_bad_input(self):
        # Should return 0 rather than crash
        assert _db._time_to_minutes("bad") == 0
        assert _db._time_to_minutes("") == 0

    def test_compute_end_time(self):
        assert _db.compute_end_time("09:00", 45)  == "09:45"
        assert _db.compute_end_time("10:30", 60)  == "11:30"
        assert _db.compute_end_time("23:30", 60)  == "00:30"   # midnight wrap
