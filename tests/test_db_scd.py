"""Tests for db_scd.py — SCD Type 2 change detection and history."""

import tempfile
from pathlib import Path

import pytest

from db import ListingDB
from db_scd import ListingHistoryDB


@pytest.fixture
def db_path(tmp_path):
    return tmp_path / "test_scd.db"


@pytest.fixture
def db(db_path):
    with ListingDB(db_path) as listing_db:
        yield listing_db


@pytest.fixture
def scd_db(db):
    return ListingHistoryDB(db)


def _make_car(**overrides):
    base = {
        "listing_id": 100,
        "car_name": "Toyota Corolla",
        "detail_url": "/used-cars/info/100",
        "listing_type": "Dealer",
        "price": 85000,
        "installment": 1200,
        "depreciation": 8000,
        "reg_date": "15-Jan-2020",
        "coe_remaining": "5y 3m",
        "mileage_km": 45000,
        "engine_cap_cc": 1600,
        "owners": 2,
        "fuel_type": "Petrol",
        "posted_date": "10-May-2026",
        "car_model": "Toyota Corolla",
        "coe": 35000,
        "road_tax": 800,
        "omv": 20000,
        "arf": 18000,
        "power": 132.0,
        "transmission": "Auto",
        "manufactured": 2020,
        "dereg_value": 5000,
        "curb_weight": 1300,
        "status": "Available",
        "features": "LED",
        "accessories": "19\" rims",
        "vehicle_type": "Sedan",
    }
    base.update(overrides)
    return base


# --- Change detection ---

class TestDetectChanges:
    def test_no_changes(self, scd_db):
        car = _make_car()
        changes = scd_db.detect_changes(car, car)
        assert changes == {}

    def test_price_change(self, scd_db):
        current = _make_car(price=85000)
        incoming = _make_car(price=75000)
        changes = scd_db.detect_changes(current, incoming)
        assert "price" in changes
        assert changes["price"] == (85000, 75000)

    def test_null_incoming_means_no_change(self, scd_db):
        current = _make_car(price=85000)
        incoming = _make_car(price=None)
        changes = scd_db.detect_changes(current, incoming)
        assert "price" not in changes

    def test_null_current_with_value_is_change(self, scd_db):
        current = _make_car(price=None)
        incoming = _make_car(price=85000)
        changes = scd_db.detect_changes(current, incoming)
        assert "price" in changes
        assert changes["price"] == (None, 85000)

    def test_both_null_no_change(self, scd_db):
        current = _make_car(price=None)
        incoming = _make_car(price=None)
        changes = scd_db.detect_changes(current, incoming)
        assert "price" not in changes

    def test_float_epsilon_no_change(self, scd_db):
        current = _make_car(power=132.0)
        incoming = _make_car(power=132.005)
        changes = scd_db.detect_changes(current, incoming)
        assert "power" not in changes

    def test_float_epsilon_real_change(self, scd_db):
        # power is not an SCD-tracked field, so no change detected
        current = _make_car(power=132.0)
        incoming = _make_car(power=140.0)
        changes = scd_db.detect_changes(current, incoming)
        assert "power" not in changes

    def test_multiple_changes(self, scd_db):
        # Both price and status are SCD-tracked
        current = _make_car(price=85000, status="Available")
        incoming = _make_car(price=75000, status="Sold")
        changes = scd_db.detect_changes(current, incoming)
        assert len(changes) == 2
        assert "price" in changes
        assert "status" in changes

    def test_status_change(self, scd_db):
        current = _make_car(status="Available")
        incoming = _make_car(status="Sold")
        changes = scd_db.detect_changes(current, incoming)
        assert "status" in changes

    def test_depreciation_change(self, scd_db):
        current = _make_car(depreciation=10000)
        incoming = _make_car(depreciation=12000)
        changes = scd_db.detect_changes(current, incoming)
        assert "depreciation" in changes


# --- Upsert with history ---

class TestUpsertWithHistory:
    def test_new_listing(self, scd_db, db):
        car = _make_car()
        counts = scd_db.upsert_with_history([car], run_id=1)
        assert counts["new"] == 1
        assert counts["changed"] == 0
        assert counts["unchanged"] == 0

        # Verify listing in base table
        assert db.get_count() == 1

        # Verify history row exists and is current
        history = scd_db.get_history(100)
        assert len(history) == 1
        assert history[0]["is_current"] == 1
        assert history[0]["valid_to"] is None

    def test_unchanged_listing(self, scd_db, db):
        car = _make_car()
        scd_db.upsert_with_history([car], run_id=1)

        # Same data again
        counts = scd_db.upsert_with_history([car], run_id=2)
        assert counts["new"] == 0
        assert counts["changed"] == 0
        assert counts["unchanged"] == 1

        # Should still be only one history row
        history = scd_db.get_history(100)
        assert len(history) == 1

    def test_changed_listing(self, scd_db, db):
        car = _make_car()
        scd_db.upsert_with_history([car], run_id=1)

        # Change the price
        car2 = _make_car(price=75000)
        counts = scd_db.upsert_with_history([car2], run_id=2)
        assert counts["changed"] == 1

        # Should have two history rows
        history = scd_db.get_history(100)
        assert len(history) == 2

        # Old row should be closed
        old = history[0]
        assert old["is_current"] == 0
        assert old["valid_to"] is not None
        assert old["price"] == 85000

        # New row should be current
        new = history[1]
        assert new["is_current"] == 1
        assert new["valid_to"] is None
        assert new["price"] == 75000

    def test_multiple_listings(self, scd_db, db):
        cars = [
            _make_car(listing_id=100),
            _make_car(listing_id=101, price=50000),
            _make_car(listing_id=102, price=120000),
        ]
        counts = scd_db.upsert_with_history(cars, run_id=1)
        assert counts["new"] == 3
        assert db.get_count() == 3

    def test_empty_batch(self, scd_db, db):
        counts = scd_db.upsert_with_history([])
        assert counts["total"] == 0

    def test_price_drop_sequence(self, scd_db, db):
        """Simulate a price drop over multiple scrapes."""
        scd_db.upsert_with_history([_make_car(price=90000)], run_id=1)
        scd_db.upsert_with_history([_make_car(price=85000)], run_id=2)
        scd_db.upsert_with_history([_make_car(price=80000)], run_id=3)

        history = scd_db.get_history(100)
        assert len(history) == 3
        prices = [h["price"] for h in history]
        assert prices == [90000, 85000, 80000]

        # Only last row should be current
        assert history[0]["is_current"] == 0
        assert history[1]["is_current"] == 0
        assert history[2]["is_current"] == 1


# --- Query methods ---

class TestQueryMethods:
    def test_get_history_empty(self, scd_db):
        history = scd_db.get_history(999)
        assert history == []

    def test_get_recent_changes(self, scd_db):
        car = _make_car()
        scd_db.upsert_with_history([car], run_id=1)

        changes = scd_db.get_recent_changes(hours=1)
        assert len(changes) >= 1

    def test_get_current_with_history_count(self, scd_db):
        car = _make_car()
        scd_db.upsert_with_history([car], run_id=1)
        scd_db.upsert_with_history([_make_car(price=75000)], run_id=2)

        result = scd_db.get_current_with_history_count()
        assert len(result) == 1
        assert result[0]["listing_id"] == 100
        assert result[0]["versions"] == 2

    def test_get_current_with_history_no_multi(self, scd_db):
        car = _make_car()
        scd_db.upsert_with_history([car], run_id=1)

        result = scd_db.get_current_with_history_count()
        assert len(result) == 0  # only 1 version, filtered by HAVING > 1
