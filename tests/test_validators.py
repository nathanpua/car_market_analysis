"""Tests for validators.py — field validation rules."""

import pytest
from validators import ListingValidator, ValidationFailure, ValidationResult


@pytest.fixture
def validator():
    return ListingValidator()


def _make_record(**overrides):
    """Create a valid base record with optional overrides."""
    base = {
        "listing_id": 12345,
        "car_name": "Toyota Corolla",
        "detail_url": "/used-cars/info/12345",
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
        "features": "LED headlights",
        "accessories": "19\" rims",
        "vehicle_type": "Sedan",
    }
    base.update(overrides)
    return base


# --- NULL values always pass ---

class TestNullPassThrough:
    """NULL values should never trigger validation failures."""

    def test_all_null_passes(self, validator):
        record = {"listing_id": 1}
        result = validator.validate(record)
        assert result.is_valid
        assert result.cleaned["listing_id"] == 1

    def test_null_price_passes(self, validator):
        result = validator.validate(_make_record(price=None))
        assert result.is_valid

    def test_null_mileage_passes(self, validator):
        result = validator.validate(_make_record(mileage_km=None))
        assert result.is_valid

    def test_null_transmission_passes(self, validator):
        result = validator.validate(_make_record(transmission=None))
        assert result.is_valid


# --- Range checks ---

class TestRangeChecks:
    def test_price_valid(self, validator):
        result = validator.validate(_make_record(price=85000))
        assert result.is_valid
        assert result.cleaned["price"] == 85000

    def test_price_too_low(self, validator):
        result = validator.validate(_make_record(price=0))
        assert not result.is_valid
        assert result.cleaned["price"] is None
        assert result.failures[0].rule_name == "price_range"

    def test_price_too_high(self, validator):
        result = validator.validate(_make_record(price=25_000_000))
        assert not result.is_valid
        assert result.failures[0].rule_name == "price_range"

    def test_price_max_valid(self, validator):
        result = validator.validate(_make_record(price=20_000_000))
        assert result.is_valid

    def test_mileage_valid(self, validator):
        result = validator.validate(_make_record(mileage_km=50000))
        assert result.is_valid

    def test_mileage_zero(self, validator):
        result = validator.validate(_make_record(mileage_km=0))
        assert result.is_valid

    def test_mileage_too_high(self, validator):
        result = validator.validate(_make_record(mileage_km=2_000_000))
        assert not result.is_valid
        assert result.failures[0].rule_name == "mileage_range"

    def test_engine_cap_valid(self, validator):
        result = validator.validate(_make_record(engine_cap_cc=1600))
        assert result.is_valid

    def test_engine_cap_too_high(self, validator):
        result = validator.validate(_make_record(engine_cap_cc=25000))
        assert not result.is_valid
        assert result.failures[0].rule_name == "engine_cap_range"

    def test_engine_cap_zero(self, validator):
        result = validator.validate(_make_record(engine_cap_cc=0))
        assert not result.is_valid

    def test_owners_valid(self, validator):
        result = validator.validate(_make_record(owners=5))
        assert result.is_valid

    def test_owners_negative(self, validator):
        result = validator.validate(_make_record(owners=-1))
        assert not result.is_valid

    def test_manufactured_valid(self, validator):
        result = validator.validate(_make_record(manufactured=2023))
        assert result.is_valid

    def test_manufactured_too_old(self, validator):
        result = validator.validate(_make_record(manufactured=1920))
        assert not result.is_valid

    def test_manufactured_future(self, validator):
        result = validator.validate(_make_record(manufactured=2026))
        assert result.is_valid

    def test_curb_weight_valid(self, validator):
        result = validator.validate(_make_record(curb_weight=2500))
        assert result.is_valid

    def test_curb_weight_negative(self, validator):
        result = validator.validate(_make_record(curb_weight=-100))
        assert not result.is_valid

    def test_power_valid(self, validator):
        result = validator.validate(_make_record(power=132.0))
        assert result.is_valid

    def test_power_negative(self, validator):
        result = validator.validate(_make_record(power=-5.0))
        assert not result.is_valid

    def test_installment_valid(self, validator):
        result = validator.validate(_make_record(installment=1200))
        assert result.is_valid

    def test_depreciation_valid(self, validator):
        result = validator.validate(_make_record(depreciation=8000))
        assert result.is_valid

    def test_coe_valid(self, validator):
        result = validator.validate(_make_record(coe=35000))
        assert result.is_valid

    def test_road_tax_valid(self, validator):
        result = validator.validate(_make_record(road_tax=800))
        assert result.is_valid

    def test_omv_valid(self, validator):
        result = validator.validate(_make_record(omv=20000))
        assert result.is_valid

    def test_arf_valid(self, validator):
        result = validator.validate(_make_record(arf=18000))
        assert result.is_valid

    def test_dereg_value_valid(self, validator):
        result = validator.validate(_make_record(dereg_value=5000))
        assert result.is_valid

    def test_listing_id_valid(self, validator):
        result = validator.validate(_make_record(listing_id=12345))
        assert result.is_valid

    def test_listing_id_zero(self, validator):
        result = validator.validate(_make_record(listing_id=0))
        assert not result.is_valid

    def test_listing_id_negative(self, validator):
        result = validator.validate(_make_record(listing_id=-1))
        assert not result.is_valid


# --- Enum checks ---

class TestEnumChecks:
    def test_transmission_auto(self, validator):
        result = validator.validate(_make_record(transmission="Auto"))
        assert result.is_valid

    def test_transmission_manual(self, validator):
        result = validator.validate(_make_record(transmission="Manual"))
        assert result.is_valid

    def test_transmission_invalid(self, validator):
        result = validator.validate(_make_record(transmission="CVT"))
        assert not result.is_valid
        assert result.failures[0].rule_name == "transmission_enum"

    def test_fuel_type_petrol(self, validator):
        result = validator.validate(_make_record(fuel_type="Petrol"))
        assert result.is_valid

    def test_fuel_type_hybrid(self, validator):
        result = validator.validate(_make_record(fuel_type="Hybrid"))
        assert result.is_valid

    def test_fuel_type_electric(self, validator):
        result = validator.validate(_make_record(fuel_type="Electric"))
        assert result.is_valid

    def test_fuel_type_invalid(self, validator):
        result = validator.validate(_make_record(fuel_type="Nuclear"))
        assert not result.is_valid
        assert result.failures[0].rule_name == "fuel_type_enum"

    def test_listing_type_dealer(self, validator):
        result = validator.validate(_make_record(listing_type="Dealer"))
        assert result.is_valid

    def test_listing_type_direct_owner(self, validator):
        result = validator.validate(_make_record(listing_type="Direct Owner"))
        assert result.is_valid

    def test_listing_type_invalid(self, validator):
        result = validator.validate(_make_record(listing_type="Agent"))
        assert not result.is_valid

    def test_status_available(self, validator):
        result = validator.validate(_make_record(status="Available"))
        assert result.is_valid

    def test_status_available_for_sale(self, validator):
        result = validator.validate(_make_record(status="Available for sale"))
        assert result.is_valid

    def test_status_sold(self, validator):
        result = validator.validate(_make_record(status="Sold"))
        assert result.is_valid

    def test_status_sold_uppercase(self, validator):
        result = validator.validate(_make_record(status="SOLD"))
        assert result.is_valid

    def test_status_closed(self, validator):
        result = validator.validate(_make_record(status="CLOSED"))
        assert result.is_valid

    def test_status_invalid(self, validator):
        result = validator.validate(_make_record(status="Deleted"))
        assert not result.is_valid


# --- Regex checks ---

class TestRegexChecks:
    def test_reg_date_full(self, validator):
        result = validator.validate(_make_record(reg_date="15-Jan-2020"))
        assert result.is_valid

    def test_reg_date_short(self, validator):
        result = validator.validate(_make_record(reg_date="01-2020"))
        assert result.is_valid

    def test_reg_date_invalid(self, validator):
        result = validator.validate(_make_record(reg_date="January 2020"))
        assert not result.is_valid
        assert result.failures[0].rule_name == "reg_date_format"

    def test_coe_remaining_years_months(self, validator):
        result = validator.validate(_make_record(coe_remaining="5y 3m"))
        assert result.is_valid

    def test_coe_remaining_years_only(self, validator):
        result = validator.validate(_make_record(coe_remaining="5y"))
        assert result.is_valid

    def test_coe_remaining_renewed(self, validator):
        result = validator.validate(_make_record(coe_remaining="5y (renewed)"))
        assert result.is_valid

    def test_coe_remaining_months_only(self, validator):
        result = validator.validate(_make_record(coe_remaining="3m"))
        assert result.is_valid

    def test_coe_remaining_invalid(self, validator):
        result = validator.validate(_make_record(coe_remaining="about 5 years"))
        assert not result.is_valid
        assert result.failures[0].rule_name == "coe_remaining_format"

    def test_posted_date_full(self, validator):
        result = validator.validate(_make_record(posted_date="10-May-2026"))
        assert result.is_valid

    def test_posted_date_iso(self, validator):
        result = validator.validate(_make_record(posted_date="2026-05-10"))
        assert result.is_valid

    def test_posted_date_invalid(self, validator):
        result = validator.validate(_make_record(posted_date="May 10"))
        assert not result.is_valid

    def test_detail_url_valid(self, validator):
        result = validator.validate(_make_record(detail_url="/used-cars/info/12345"))
        assert result.is_valid

    def test_detail_url_invalid(self, validator):
        result = validator.validate(_make_record(detail_url="https://example.com/car"))
        assert not result.is_valid
        assert result.failures[0].rule_name == "url_prefix"


# --- Pass-through fields ---

class TestPassThrough:
    def test_features_pass_through(self, validator):
        result = validator.validate(_make_record(features="LED, ABS, GPS"))
        assert result.is_valid
        assert result.cleaned["features"] == "LED, ABS, GPS"

    def test_accessories_pass_through(self, validator):
        result = validator.validate(_make_record(accessories="19\" rims"))
        assert result.is_valid

    def test_car_name_pass_through(self, validator):
        result = validator.validate(_make_record(car_name="Toyota Corolla"))
        assert result.is_valid

    def test_vehicle_type_pass_through(self, validator):
        result = validator.validate(_make_record(vehicle_type="SUV"))
        assert result.is_valid


# --- Batch validation ---

class TestBatchValidation:
    def test_validate_batch(self, validator):
        records = [
            _make_record(listing_id=1, price=50000),
            _make_record(listing_id=2, price=-100),  # invalid
            _make_record(listing_id=3, price=75000),
        ]
        valid, failures = validator.validate_batch(records)
        assert len(valid) == 3
        assert len(failures) == 1
        assert failures[0].listing_id == 2

    def test_validate_batch_all_valid(self, validator):
        records = [
            _make_record(listing_id=1),
            _make_record(listing_id=2),
        ]
        valid, failures = validator.validate_batch(records)
        assert len(valid) == 2
        assert len(failures) == 0

    def test_validate_batch_empty(self, validator):
        valid, failures = validator.validate_batch([])
        assert valid == []
        assert failures == []


# --- Failure conversion ---

class TestFailureConversion:
    def test_failures_to_dicts(self, validator):
        record = _make_record(price=-100)
        result = validator.validate(record)
        dicts = ListingValidator.failures_to_dicts(result.failures, run_id=42)
        assert len(dicts) == 1
        assert dicts[0]["listing_id"] == 12345
        assert dicts[0]["field_name"] == "price"
        assert dicts[0]["rule_name"] == "price_range"
        assert dicts[0]["scrape_run_id"] == 42

    def test_failures_to_dicts_empty(self):
        dicts = ListingValidator.failures_to_dicts([])
        assert dicts == []


# --- ValidationResult ---

class TestValidationResult:
    def test_is_valid_true(self):
        result = ValidationResult(cleaned={"a": 1})
        assert result.is_valid

    def test_is_valid_false(self):
        result = ValidationResult(
            cleaned={"a": 1},
            failures=[ValidationFailure(1, "a", 1, "rule", "reason")],
        )
        assert not result.is_valid


# --- validate_and_quarantine ---

class TestValidateAndQuarantine:
    def test_calls_quarantine_fn(self, validator):
        captured = []
        def mock_quarantine(failures, run_id):
            captured.extend(failures)

        record = _make_record(price=-100)
        cleaned = validator.validate_and_quarantine(
            record, quarantine_fn=mock_quarantine, run_id=5,
        )
        assert cleaned["price"] is None
        assert len(captured) == 1

    def test_no_failures_no_callback(self, validator):
        record = _make_record(price=50000)
        cleaned = validator.validate_and_quarantine(record)
        assert cleaned["price"] == 50000

    def test_without_quarantine_fn(self, validator):
        record = _make_record(price=-100)
        cleaned = validator.validate_and_quarantine(record)
        assert cleaned["price"] is None
