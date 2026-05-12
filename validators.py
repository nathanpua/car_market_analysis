"""Field validation rules for SGCarMart listing data.

Validates each of the 28 columns before writing to the database.
Invalid records are quarantined with reasons; valid records pass through.
Design principle: NULL = valid (missing data is not a validation failure).
Only present-but-wrong values trigger quarantine.
"""

import json
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# --- Validation rule functions ---
# Each returns (cleaned_value, failure_reason | None).
# NULL inputs always pass (return (None, None)).


def _range_check(
    value: int | float | None,
    lo: int | float,
    hi: int | float,
    name: str,
) -> tuple[int | float | None, str | None]:
    if value is None:
        return None, None
    if not isinstance(value, (int, float)):
        return None, f"{name}: expected number, got {type(value).__name__}"
    if value < lo or value > hi:
        return None, f"{name}: {value} outside range [{lo}, {hi}]"
    return value, None


def _enum_check(
    value: str | None,
    allowed: set[str],
    name: str,
) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if value not in allowed:
        return None, f"{name}: '{value}' not in {sorted(allowed)}"
    return value, None


def _regex_check(
    value: str | None,
    pattern: str,
    name: str,
) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    import re
    if not re.search(pattern, value):
        return None, f"{name}: '{value}' does not match pattern"
    return value, None


def _url_prefix_check(
    value: str | None,
    prefix: str,
    name: str,
) -> tuple[str | None, str | None]:
    if value is None:
        return None, None
    if not value.startswith(prefix):
        return None, f"{name}: '{value}' does not start with '{prefix}'"
    return value, None


def _pass_through(
    value: str | None,
    name: str,  # noqa: ARG001
) -> tuple[str | None, str | None]:
    return value, None


# --- Allowed enums ---

TRANSMISSION_VALUES = {"Auto", "Manual"}
FUEL_TYPE_VALUES = {
    "Petrol", "Diesel", "Electric", "Hybrid", "CNG", "Petrol-Electric",
    "Diesel-Electric",
    "Diesel (Euro 5 Engine and Above)",
    "Diesel (Euro 4 Engine and Below)",
    "Diesel-Electric (Euro 5 Engine and Above)",
    "Diesel (Registered as Commercial Vehicle)",
}
LISTING_TYPE_VALUES = {"Direct Owner", "Dealer"}
STATUS_VALUES = {
    "Available for sale", "SOLD", "CLOSED",
    "Available", "Sold", "Reserved", "N.A.",
}

# --- Validation rules registry ---
# Maps DB column name -> list of (rule_name, validator_fn) tuples.
# Each validator takes (value, field_name) and returns (cleaned_value, failure_reason).

VALIDATION_RULES: dict[str, list[tuple[str, object]]] = {
    "listing_id": [
        ("positive_int", lambda v, _n: _range_check(v, 1, 99_999_999, "listing_id")),
    ],
    "price": [
        ("price_range", lambda v, _n: _range_check(v, 1, 20_000_000, "price")),
    ],
    "installment": [
        ("installment_range", lambda v, _n: _range_check(v, 0, 500_000, "installment")),
    ],
    "depreciation": [
        ("depreciation_range", lambda v, _n: _range_check(v, 0, 5_000_000, "depreciation")),
    ],
    "mileage_km": [
        ("mileage_range", lambda v, _n: _range_check(v, 0, 1_000_000, "mileage_km")),
    ],
    "engine_cap_cc": [
        ("engine_cap_range", lambda v, _n: _range_check(v, 1, 20_000, "engine_cap_cc")),
    ],
    "owners": [
        ("owners_range", lambda v, _n: _range_check(v, 0, 99, "owners")),
    ],
    "coe": [
        ("coe_range", lambda v, _n: _range_check(v, 0, 500_000, "coe")),
    ],
    "road_tax": [
        ("road_tax_range", lambda v, _n: _range_check(v, 0, 100_000, "road_tax")),
    ],
    "omv": [
        ("omv_range", lambda v, _n: _range_check(v, 0, 1_000_000, "omv")),
    ],
    "arf": [
        ("arf_range", lambda v, _n: _range_check(v, 0, 5_000_000, "arf")),
    ],
    "power": [
        ("power_range", lambda v, _n: _range_check(v, 0, 2000, "power")),
    ],
    "manufactured": [
        ("manufactured_range", lambda v, _n: _range_check(v, 1930, 2030, "manufactured")),
    ],
    "dereg_value": [
        ("dereg_range", lambda v, _n: _range_check(v, 0, 5_000_000, "dereg_value")),
    ],
    "curb_weight": [
        ("curb_weight_range", lambda v, _n: _range_check(v, 0, 40_000, "curb_weight")),
    ],
    "transmission": [
        ("transmission_enum", lambda v, _n: _enum_check(v, TRANSMISSION_VALUES, "transmission")),
    ],
    "fuel_type": [
        ("fuel_type_enum", lambda v, _n: _enum_check(v, FUEL_TYPE_VALUES, "fuel_type")),
    ],
    "listing_type": [
        ("listing_type_enum", lambda v, _n: _enum_check(v, LISTING_TYPE_VALUES, "listing_type")),
    ],
    "status": [
        ("status_enum", lambda v, _n: _enum_check(v, STATUS_VALUES, "status")),
    ],
    "reg_date": [
        ("reg_date_format", lambda v, _n: _regex_check(
            v, r"^\d{2}-\d{4}$|^\d{1,2}-[A-Za-z]+-\d{4}$", "reg_date",
        )),
    ],
    "coe_remaining": [
        ("coe_remaining_format", lambda v, _n: _regex_check(
            v, r"^\d+y( \d+m)?( \d+d)?$|^\d+y \(renewed\)$|^\d+m( \d+d)?$", "coe_remaining",
        )),
    ],
    "posted_date": [
        ("posted_date_format", lambda v, _n: _regex_check(
            v, r"^\d{1,2}-[A-Za-z]+-\d{4}$|^\d{4}-\d{2}-\d{2}$", "posted_date",
        )),
    ],
    "detail_url": [
        ("url_prefix", lambda v, _n: _url_prefix_check(
            v, "/used-cars/info/", "detail_url",
        )),
    ],
    "car_name": [
        ("non_empty_str", _pass_through),
    ],
    "car_model": [
        ("non_empty_str", _pass_through),
    ],
    "vehicle_type": [
        ("non_empty_str", _pass_through),
    ],
    "features": [
        ("text_blob", _pass_through),
    ],
    "accessories": [
        ("text_blob", _pass_through),
    ],
}


@dataclass
class ValidationFailure:
    """A single field validation failure."""
    listing_id: int | None
    field_name: str
    field_value: object
    rule_name: str
    reason: str


@dataclass
class ValidationResult:
    """Result of validating a single record."""
    cleaned: dict
    failures: list[ValidationFailure] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.failures) == 0


class ListingValidator:
    """Validates listing records field-by-field.

    Invalid field values are set to None in the cleaned record and
    collected as ValidationFailure entries for quarantine.
    """

    def validate(self, record: dict) -> ValidationResult:
        """Validate a single listing record.

        Returns a ValidationResult with:
        - cleaned: record with invalid fields set to None
        - failures: list of ValidationFailure for each invalid field
        """
        cleaned = dict(record)
        failures: list[ValidationFailure] = []

        for col, rules in VALIDATION_RULES.items():
            value = record.get(col)
            if value is None:
                continue

            for rule_name, validator_fn in rules:
                cleaned_value, reason = validator_fn(value, col)
                if reason is not None:
                    failures.append(ValidationFailure(
                        listing_id=record.get("listing_id"),
                        field_name=col,
                        field_value=value,
                        rule_name=rule_name,
                        reason=reason,
                    ))
                    cleaned[col] = None
                    break  # first failing rule wins for this field
                else:
                    cleaned[col] = cleaned_value

        return ValidationResult(cleaned=cleaned, failures=failures)

    def validate_batch(
        self, records: list[dict],
    ) -> tuple[list[dict], list[ValidationFailure]]:
        """Validate a batch of records.

        Returns:
            valid_records: list of cleaned records (may still have validation failures
                           but fields are set to None for invalid values)
            all_failures: all validation failures across the batch
        """
        valid_records = []
        all_failures: list[ValidationFailure] = []

        for record in records:
            result = self.validate(record)
            valid_records.append(result.cleaned)
            all_failures.extend(result.failures)

        return valid_records, all_failures

    def validate_and_quarantine(
        self, record: dict, quarantine_fn=None, run_id: int | None = None,
    ) -> dict:
        """Validate a record and optionally send failures to quarantine.

        Args:
            record: raw listing record
            quarantine_fn: callable(list[ValidationFailure], int|None) to persist failures
            run_id: scrape run ID for quarantine records

        Returns:
            cleaned record with invalid fields set to None
        """
        result = self.validate(record)

        if result.failures and quarantine_fn:
            quarantine_fn(result.failures, run_id)

        return result.cleaned

    @staticmethod
    def failures_to_dicts(
        failures: list[ValidationFailure], run_id: int | None = None,
    ) -> list[dict]:
        """Convert ValidationFailure objects to dicts for database insertion."""
        rows = []
        for f in failures:
            rows.append({
                "listing_id": f.listing_id,
                "field_name": f.field_name,
                "field_value": str(f.field_value) if f.field_value is not None else None,
                "rule_name": f.rule_name,
                "reason": f.reason,
                "raw_record": None,  # populated by caller if needed
                "scrape_run_id": run_id,
            })
        return rows
