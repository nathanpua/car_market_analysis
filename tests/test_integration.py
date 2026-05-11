"""Integration tests for the full validation + SCD pipeline."""

import pytest

from db import ListingDB, QuarantineDB
from db_scd import ListingHistoryDB
from validators import ListingValidator


def _make_car(**overrides):
    base = {
        "listing_id": 200,
        "car_name": "Honda Civic",
        "detail_url": "/used-cars/info/200",
        "listing_type": "Dealer",
        "price": 120000,
        "installment": 1800,
        "depreciation": 12000,
        "reg_date": "01-Mar-2021",
        "coe_remaining": "4y 9m",
        "mileage_km": 30000,
        "engine_cap_cc": 1500,
        "owners": 1,
        "fuel_type": "Petrol",
        "posted_date": "05-May-2026",
        "car_model": "Honda Civic",
        "coe": 40000,
        "road_tax": 900,
        "omv": 25000,
        "arf": 22000,
        "power": 150.0,
        "transmission": "Auto",
        "manufactured": 2021,
        "dereg_value": 8000,
        "curb_weight": 1400,
        "status": "Available",
        "features": "LED headlights, GPS",
        "accessories": "Leather seats",
        "vehicle_type": "Sedan",
    }
    base.update(overrides)
    return base


@pytest.fixture
def db(tmp_path):
    with ListingDB(tmp_path / "test.db") as listing_db:
        yield listing_db


@pytest.fixture
def validator():
    return ListingValidator()


class TestValidationAndQuarantineIntegration:
    def test_valid_record_passes_through(self, db, validator):
        quarantine_db = QuarantineDB(db)
        car = _make_car()
        cleaned = validator.validate_and_quarantine(
            car,
            quarantine_fn=lambda f, rid=None: quarantine_db.insert_failures(
                ListingValidator.failures_to_dicts(f, rid), rid,
            ),
        )
        assert cleaned["price"] == 120000
        stats = quarantine_db.get_stats()
        assert stats["total"] == 0

    def test_invalid_fields_quarantined_and_cleaned(self, db, validator):
        quarantine_db = QuarantineDB(db)
        car = _make_car(price=-100, transmission="CVT")
        cleaned = validator.validate_and_quarantine(
            car,
            quarantine_fn=lambda f, rid=None: quarantine_db.insert_failures(
                ListingValidator.failures_to_dicts(f, rid), rid,
            ),
            run_id=1,
        )
        assert cleaned["price"] is None
        assert cleaned["transmission"] is None
        assert cleaned["listing_id"] == 200

        stats = quarantine_db.get_stats()
        assert stats["total"] == 2
        assert stats["unresolved"] == 2

    def test_batch_validation_with_quarantine(self, db, validator):
        quarantine_db = QuarantineDB(db)
        cars = [
            _make_car(listing_id=200, price=50000),
            _make_car(listing_id=201, price=-50),
            _make_car(listing_id=202, price=30000, mileage_km=5_000_000),
        ]
        all_failures = []
        for car in cars:
            result = validator.validate(car)
            all_failures.extend(result.failures)

        dicts = ListingValidator.failures_to_dicts(all_failures, run_id=1)
        quarantine_db.insert_failures(dicts, run_id=1)

        stats = quarantine_db.get_stats()
        assert stats["total"] == 2  # 201 price + 202 mileage
        assert "price_range" in stats["by_rule"]
        assert "mileage_range" in stats["by_rule"]


class TestSCDIntegration:
    def test_new_listing_creates_history(self, db):
        scd = ListingHistoryDB(db)
        car = _make_car()
        counts = scd.upsert_with_history([car], run_id=1)

        assert counts["new"] == 1
        history = scd.get_history(200)
        assert len(history) == 1
        assert history[0]["price"] == 120000
        assert history[0]["is_current"] == 1

    def test_price_drop_creates_new_history_row(self, db):
        scd = ListingHistoryDB(db)
        scd.upsert_with_history([_make_car()], run_id=1)
        scd.upsert_with_history([_make_car(price=110000)], run_id=2)

        history = scd.get_history(200)
        assert len(history) == 2
        assert history[0]["price"] == 120000
        assert history[0]["is_current"] == 0
        assert history[1]["price"] == 110000
        assert history[1]["is_current"] == 1

    def test_validation_before_history(self, db, validator):
        """Verify validation cleans data before history is written."""
        scd = ListingHistoryDB(db)
        quarantine_db = QuarantineDB(db)

        car = _make_car(price=-100)
        cleaned = validator.validate_and_quarantine(
            car,
            quarantine_fn=lambda f, rid=None: quarantine_db.insert_failures(
                ListingValidator.failures_to_dicts(f, rid), rid,
            ),
        )
        # History should see the cleaned value (price=None)
        scd.upsert_with_history([cleaned], run_id=1)
        history = scd.get_history(200)
        assert history[0]["price"] is None

    def test_full_pipeline_new_then_change(self, db, validator):
        """Full pipeline: validate -> quarantine -> SCD history."""
        quarantine_db = QuarantineDB(db)
        scd = ListingHistoryDB(db)

        # First scrape: valid car
        car1 = _make_car()
        cleaned1 = validator.validate_and_quarantine(
            car1,
            quarantine_fn=lambda f, rid=None: quarantine_db.insert_failures(
                ListingValidator.failures_to_dicts(f, rid), rid,
            ),
        )
        scd.upsert_with_history([cleaned1], run_id=1)

        # Second scrape: price drop + invalid transmission
        car2 = _make_car(price=100000, transmission="INVALID")
        cleaned2 = validator.validate_and_quarantine(
            car2,
            quarantine_fn=lambda f, rid=None: quarantine_db.insert_failures(
                ListingValidator.failures_to_dicts(f, rid), rid,
            ),
        )
        counts = scd.upsert_with_history([cleaned2], run_id=2)

        # Verify: 1 change (price), transmission was cleaned to None
        assert counts["changed"] == 1
        history = scd.get_history(200)
        assert len(history) == 2
        assert history[1]["price"] == 100000
        assert history[1]["transmission"] is None

        # Verify quarantine has 1 failure
        stats = quarantine_db.get_stats()
        assert stats["total"] == 1
        assert stats["by_rule"]["transmission_enum"] == 1


class TestQuarantineQueries:
    def test_get_recent(self, db, validator):
        quarantine_db = QuarantineDB(db)
        car = _make_car(price=-100, fuel_type="Nuclear")
        result = validator.validate(car)
        dicts = ListingValidator.failures_to_dicts(result.failures, run_id=1)
        quarantine_db.insert_failures(dicts, run_id=1)

        recent = quarantine_db.get_recent(limit=10)
        assert len(recent) == 2
        fields = {r["field_name"] for r in recent}
        assert "price" in fields
        assert "fuel_type" in fields

    def test_stats_empty(self, db):
        quarantine_db = QuarantineDB(db)
        stats = quarantine_db.get_stats()
        assert stats["total"] == 0
        assert stats["unresolved"] == 0
