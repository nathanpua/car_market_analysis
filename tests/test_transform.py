"""Tests for Silver-to-Gold transformation logic."""

import sqlite3
import tempfile
from datetime import date
from pathlib import Path

import pytest

from transform import (
    GOLD_COLUMNS,
    GOLD_TABLE_DDL,
    compute_days_on_market,
    compute_value_score,
    extract_brand_model,
    is_commercial,
    normalize_fuel_type,
    normalize_status,
    normalize_vehicle_type,
    parse_coe_months,
    parse_reg_date,
    run_transform,
)


# ---------------------------------------------------------------------------
# extract_brand_model
# ---------------------------------------------------------------------------

class TestExtractBrandModel:
    def test_simple_brand(self):
        brand, model, trim = extract_brand_model("Toyota Corolla 1.6A")
        assert brand == "Toyota"
        assert model == "Corolla"
        assert "1.6A" in (trim or "")

    def test_multi_word_brand(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz C-Class C180 Avantgarde")
        assert brand == "Mercedes-Benz"
        assert model == "C-Class C180"
        assert "Avantgarde" in (trim or "")

    def test_mercedes_eclass(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz E-Class E200 Avantgarde")
        assert brand == "Mercedes-Benz"
        assert model == "E-Class E200"
        assert trim == "Avantgarde"

    def test_mercedes_glc_class(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz GLC-Class GLC300 Coupe Mild Hybrid AMG Line")
        assert brand == "Mercedes-Benz"
        assert model == "GLC-Class GLC300"
        assert "AMG Line" in (trim or "")

    def test_mercedes_sclass_diesel(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz S-Class S350d")
        assert brand == "Mercedes-Benz"
        assert model == "S-Class S350d"
        assert trim is None

    def test_mercedes_eq_variant(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz EQA Electric EQA250 AMG Line")
        assert brand == "Mercedes-Benz"
        assert model == "EQA250"
        assert "AMG Line" in (trim or "")

    def test_mercedes_amg_gt(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz AMG GT 63 S 4MATIC Premium Plus")
        assert brand == "Mercedes-Benz"
        assert model == "AMG GT"
        assert "Premium Plus" in (trim or "")

    def test_mercedes_maybach(self):
        brand, model, trim = extract_brand_model("Mercedes-Benz Maybach S580")
        assert brand == "Mercedes-Benz"
        assert model == "Maybach"
        assert "S580" in (trim or "")

    def test_bmw_series_with_engine(self):
        brand, model, trim = extract_brand_model("BMW 3 Series 330i M-Sport")
        assert brand == "BMW"
        assert model == "3 Series 330i"
        assert trim == "M-Sport"

    def test_bmw_series_1series(self):
        brand, model, trim = extract_brand_model("BMW 1 Series 118i 5DR")
        assert brand == "BMW"
        assert model == "1 Series 118i"
        assert "5DR" in (trim or "")

    def test_bmw_series_no_engine(self):
        brand, model, trim = extract_brand_model("BMW 3 Series M-Sport")
        assert brand == "BMW"
        assert model == "3 Series"
        assert trim == "M-Sport"

    def test_bmw_m_car(self):
        brand, model, trim = extract_brand_model("BMW M3 Competition")
        assert brand == "BMW"
        assert model == "M3"
        assert "Competition" in (trim or "")

    def test_bmw_m5(self):
        brand, model, trim = extract_brand_model("BMW M5 Competition")
        assert brand == "BMW"
        assert model == "M5"
        assert trim == "Competition"

    def test_bmw_m_performance(self):
        brand, model, trim = extract_brand_model("BMW M135i 5DR")
        assert brand == "BMW"
        assert model == "M135i"
        assert "5DR" in (trim or "")

    def test_bmw_x_series(self):
        brand, model, trim = extract_brand_model("BMW X5 xDrive40i M-Sport")
        assert brand == "BMW"
        assert model == "X5"
        assert "xDrive40i" in (trim or "")
        assert "M-Sport" in (trim or "")

    def test_bmw_ix_numbered(self):
        brand, model, trim = extract_brand_model("BMW iX1 Electric eDrive20")
        assert brand == "BMW"
        assert model == "iX1"
        assert "eDrive20" in (trim or "")

    def test_bmw_ix_standalone(self):
        brand, model, trim = extract_brand_model("BMW iX Electric xDrive40")
        assert brand == "BMW"
        assert model == "iX"
        assert "xDrive40" in (trim or "")

    def test_bmw_xm(self):
        brand, model, trim = extract_brand_model("BMW XM")
        assert brand == "BMW"
        assert model == "XM"
        assert trim is None

    def test_bmw_i_series(self):
        brand, model, trim = extract_brand_model("BMW i4 eDrive40 M-Sport")
        assert brand == "BMW"
        assert model == "i4"
        assert "eDrive40" in (trim or "")
        assert "M-Sport" in (trim or "")

    def test_tesla_model_3(self):
        brand, model, trim = extract_brand_model("Tesla Model 3 Electric RWD")
        assert brand == "Tesla"
        assert model == "Model 3"
        assert "Electric" in (trim or "")

    def test_tesla_model_s(self):
        brand, model, trim = extract_brand_model("Tesla Model S Performance")
        assert brand == "Tesla"
        assert model == "Model S"
        assert "Performance" in (trim or "")

    def test_tesla_cybertruck(self):
        brand, model, trim = extract_brand_model("Tesla Cybertruck AWD")
        assert brand == "Tesla"
        assert model == "Cybertruck"
        assert "AWD" in (trim or "")

    def test_lexus_is(self):
        brand, model, trim = extract_brand_model("Lexus IS250 Luxury")
        assert brand == "Lexus"
        assert model == "IS250"
        assert trim == "Luxury"

    def test_lexus_rx(self):
        brand, model, trim = extract_brand_model("Lexus RX350 Premium")
        assert brand == "Lexus"
        assert model == "RX350"
        assert trim == "Premium"

    def test_lexus_es_hybrid(self):
        brand, model, trim = extract_brand_model("Lexus ES Hybrid ES300h Executive Sunroof")
        assert brand == "Lexus"
        assert model == "ES300h"
        assert "Executive" in (trim or "")

    def test_lexus_rz_electric(self):
        brand, model, trim = extract_brand_model("Lexus RZ Electric RZ450e Luxury")
        assert brand == "Lexus"
        assert model == "RZ450e"
        assert "Luxury" in (trim or "")

    def test_lexus_no_trim(self):
        brand, model, trim = extract_brand_model("Lexus GS300")
        assert brand == "Lexus"
        assert model == "GS300"
        assert trim is None

    def test_volvo_xc(self):
        brand, model, trim = extract_brand_model("Volvo XC60 T6 R-Design")
        assert brand == "Volvo"
        assert model == "XC60"
        assert "T6" in (trim or "")
        assert "R-Design" in (trim or "")

    def test_volvo_sedan(self):
        brand, model, trim = extract_brand_model("Volvo S60 T5")
        assert brand == "Volvo"
        assert model == "S60"
        assert "T5" in (trim or "")

    def test_volvo_ex(self):
        brand, model, trim = extract_brand_model("Volvo EX40 Twin Motor")
        assert brand == "Volvo"
        assert model == "EX40"
        assert "Twin" in (trim or "")

    def test_byd_atto3(self):
        brand, model, trim = extract_brand_model("BYD Atto 3 Electric Extended Range")
        assert brand == "BYD"
        assert model == "Atto 3"
        assert "Extended Range" in (trim or "")

    def test_byd_atto2(self):
        brand, model, trim = extract_brand_model("BYD Atto 2 Electric Premium")
        assert brand == "BYD"
        assert model == "Atto 2"
        assert "Premium" in (trim or "")

    def test_byd_sealion7(self):
        brand, model, trim = extract_brand_model("BYD Sealion 7 Electric Dynamic")
        assert brand == "BYD"
        assert model == "Sealion 7"
        assert "Dynamic" in (trim or "")

    def test_byd_seal6(self):
        brand, model, trim = extract_brand_model("BYD Seal 6 Electric Premium")
        assert brand == "BYD"
        assert model == "Seal 6"
        assert "Premium" in (trim or "")

    def test_byd_seal(self):
        brand, model, trim = extract_brand_model("BYD Seal Electric Performance")
        assert brand == "BYD"
        assert model == "Seal"
        assert "Performance" in (trim or "")

    def test_porsche_cayenne(self):
        brand, model, trim = extract_brand_model("Porsche Cayenne Platinum Edition")
        assert brand == "Porsche"
        assert model == "Cayenne"
        assert "Platinum" in (trim or "")

    def test_porsche_911(self):
        brand, model, trim = extract_brand_model("Porsche 911 Carrera S")
        assert brand == "Porsche"
        assert model == "911"
        assert "Carrera" in (trim or "")

    def test_lamborghini_huracan(self):
        brand, model, trim = extract_brand_model("Lamborghini Huracan STO")
        assert brand == "Lamborghini"
        assert model == "Huracan"
        assert "STO" in (trim or "")

    def test_land_rover(self):
        brand, model, trim = extract_brand_model("Land Rover Range Rover 3.0A")
        assert brand == "Land Rover"
        assert "Range" in (model or "")

    def test_empty_string(self):
        brand, model, trim = extract_brand_model("")
        assert brand == "Unknown"
        assert model is None
        assert trim is None

    def test_none_input(self):
        brand, model, trim = extract_brand_model(None)
        assert brand == "Unknown"

    def test_single_word(self):
        brand, model, trim = extract_brand_model("BMW")
        assert brand == "BMW"
        assert model is None

    def test_coe_suffix_stripped(self):
        brand, model, trim = extract_brand_model("Honda Civic 1.6A (COE till 03/2030)")
        assert brand == "Honda"
        assert "COE" not in (model or "")
        assert "COE" not in (trim or "")

    def test_new_coe_suffix(self):
        brand, model, trim = extract_brand_model("Mazda 3 2.0A (New 10-yr COE)")
        assert brand == "Mazda"

    def test_unknown_brand(self):
        brand, model, trim = extract_brand_model("Zippy Zoomer GT")
        assert brand == "Zippy"

    def test_engine_code_splits_trim(self):
        brand, model, trim = extract_brand_model("Audi A4 2.0A Sport")
        assert brand == "Audi"
        assert model == "A4"
        assert "2.0A" in (trim or "")

    def test_trim_keyword_fallback(self):
        """Generic brand with trim keyword but no engine code."""
        brand, model, trim = extract_brand_model("Honda Civic Sport")
        assert brand == "Honda"
        assert model == "Civic"
        assert "Sport" in (trim or "")


# ---------------------------------------------------------------------------
# parse_coe_months
# ---------------------------------------------------------------------------

class TestParseCoeMonths:
    def test_years_and_months(self):
        assert parse_coe_months("5y 3m") == 63

    def test_years_only(self):
        assert parse_coe_months("10y") == 120

    def test_months_only(self):
        assert parse_coe_months("3m 15d") == 3

    def test_renewed_suffix(self):
        assert parse_coe_months("5y (renewed)") == 60

    def test_none(self):
        assert parse_coe_months(None) is None

    def test_empty(self):
        assert parse_coe_months("") is None

    def test_complex(self):
        assert parse_coe_months("2y 11m") == 35


# ---------------------------------------------------------------------------
# normalize_fuel_type
# ---------------------------------------------------------------------------

class TestNormalizeFuelType:
    def test_petrol(self):
        assert normalize_fuel_type("Petrol") == "Petrol"

    def test_hybrid(self):
        assert normalize_fuel_type("Petrol-Electric") == "Hybrid"

    def test_diesel(self):
        assert normalize_fuel_type("Diesel") == "Diesel"

    def test_diesel_euro5(self):
        assert normalize_fuel_type("Diesel (Euro 5 Engine and Above)") == "Diesel"

    def test_electric(self):
        assert normalize_fuel_type("Electric") == "Electric"

    def test_none(self):
        assert normalize_fuel_type(None) is None

    def test_unknown(self):
        assert normalize_fuel_type("CNG") is None


# ---------------------------------------------------------------------------
# normalize_vehicle_type
# ---------------------------------------------------------------------------

class TestNormalizeVehicleType:
    def test_suv(self):
        assert normalize_vehicle_type("SUV") == "SUV"

    def test_sedan(self):
        assert normalize_vehicle_type("Mid-Sized Sedan") == "Sedan"

    def test_hatchback(self):
        assert normalize_vehicle_type("Hatchback") == "Hatchback"

    def test_coe_category_code(self):
        assert normalize_vehicle_type("$15c") is None

    def test_none(self):
        assert normalize_vehicle_type(None) is None

    def test_commercial_van(self):
        # Van is NOT in VEHICLE_TYPE_MAP — returns None (filtered by is_commercial)
        assert normalize_vehicle_type("Van") is None


# ---------------------------------------------------------------------------
# normalize_status
# ---------------------------------------------------------------------------

class TestNormalizeStatus:
    def test_available(self):
        assert normalize_status("Available for sale") == "Available"

    def test_sold(self):
        assert normalize_status("SOLD") == "Sold"

    def test_closed(self):
        assert normalize_status("CLOSED") == "Closed"

    def test_none(self):
        assert normalize_status(None) == "Available"

    def test_na(self):
        assert normalize_status("N.A.") == "Available"


# ---------------------------------------------------------------------------
# parse_reg_date
# ---------------------------------------------------------------------------

class TestParseRegDate:
    def test_normal(self):
        assert parse_reg_date("27-Mar-2008") == "2008-03-27"

    def test_another(self):
        assert parse_reg_date("06-Jan-2016") == "2016-01-06"

    def test_none(self):
        assert parse_reg_date(None) is None

    def test_empty(self):
        assert parse_reg_date("") is None

    def test_invalid(self):
        assert parse_reg_date("not-a-date") is None


# ---------------------------------------------------------------------------
# compute_days_on_market
# ---------------------------------------------------------------------------

class TestComputeDaysOnMarket:
    def test_normal(self):
        result = compute_days_on_market("01-Jan-2024", date(2024, 1, 15))
        assert result == 14

    def test_none(self):
        assert compute_days_on_market(None) is None

    def test_future_date_clamped(self):
        result = compute_days_on_market("01-Jan-2030", date(2024, 1, 1))
        assert result == 0

    def test_iso_format(self):
        result = compute_days_on_market("2024-01-01", date(2024, 1, 15))
        assert result == 14


# ---------------------------------------------------------------------------
# is_commercial
# ---------------------------------------------------------------------------

class TestIsCommercial:
    def test_van(self):
        assert is_commercial("Van", "Toyota") is True

    def test_truck(self):
        assert is_commercial("Truck", "Isuzu") is True

    def test_bus(self):
        assert is_commercial("Bus/Mini Bus", "Toyota") is True

    def test_commercial_brand(self):
        assert is_commercial(None, "Hino") is True
        assert is_commercial(None, "Scania") is True
        assert is_commercial(None, "Maxus") is True

    def test_consumer_vehicle(self):
        assert is_commercial("SUV", "Toyota") is False

    def test_none_safe(self):
        assert is_commercial(None, "Toyota") is False

    def test_commercial_model_pattern(self):
        assert is_commercial("MPV", "Toyota", "Toyota ProAce 2.0A") is True

    def test_commercial_model_nissan_nv200(self):
        assert is_commercial("MPV", "Nissan", "Nissan NV200 1.6A") is True

    def test_consumer_model_not_flagged(self):
        assert is_commercial("SUV", "Toyota", "Toyota Corolla Cross 1.8A") is False


# ---------------------------------------------------------------------------
# compute_value_score
# ---------------------------------------------------------------------------

class TestComputeValueScore:
    def test_empty_list(self):
        result = compute_value_score([])
        assert result == []

    def test_scores_assigned(self):
        # Need enough rows for meaningful percentile ranking.
        rows = []
        for i in range(10):
            rows.append({
                "depreciation": 5000 + i * 2000,
                "age_years": 1 + i,
                "mileage_km": 10000 + i * 15000,
                "price_to_omv_ratio": 0.5 + i * 0.15,
                "coe_remaining_months": 120 - i * 10,
            })
        result = compute_value_score(rows)
        # The first row (lowest depreciation, age, mileage) should have highest score
        assert result[0]["value_score"] > result[-1]["value_score"]
        # All scores should be in 0-100 range
        for r in result:
            assert 0 <= r["value_score"] <= 100

    def test_none_fields_handled(self):
        rows = [
            {"depreciation": None, "age_years": None, "mileage_km": None,
             "price_to_omv_ratio": None, "coe_remaining_months": None},
        ]
        result = compute_value_score(rows)
        assert result[0]["value_score"] is None


# ---------------------------------------------------------------------------
# run_transform (integration)
# ---------------------------------------------------------------------------

def _make_test_db(db_path: Path, rows: list[dict]) -> None:
    """Create a minimal test DB with listings table."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS listings (
            listing_id INTEGER PRIMARY KEY,
            car_name TEXT,
            price INTEGER,
            installment INTEGER,
            depreciation INTEGER,
            dereg_value INTEGER,
            manufactured INTEGER,
            mileage_km INTEGER,
            engine_cap_cc INTEGER,
            transmission TEXT,
            fuel_type TEXT,
            power REAL,
            curb_weight INTEGER,
            reg_date TEXT,
            coe INTEGER,
            coe_remaining TEXT,
            road_tax INTEGER,
            omv INTEGER,
            arf INTEGER,
            vehicle_type TEXT,
            listing_type TEXT,
            owners INTEGER,
            posted_date TEXT,
            features TEXT,
            accessories TEXT,
            detail_url TEXT,
            status TEXT,
            scraped_at TEXT
        )
    """)
    for row in rows:
        cols = ", ".join(row.keys())
        placeholders = ", ".join(["?"] * len(row))
        conn.execute(f"INSERT INTO listings ({cols}) VALUES ({placeholders})", list(row.values()))
    conn.commit()
    conn.close()


class TestRunTransform:
    def test_basic_transform(self, tmp_path):
        db_path = tmp_path / "test.db"
        _make_test_db(db_path, [
            {
                "listing_id": 1,
                "car_name": "Toyota Corolla 1.6A",
                "price": 80000,
                "depreciation": 8000,
                "manufactured": 2020,
                "vehicle_type": "Mid-Sized Sedan",
                "fuel_type": "Petrol",
                "status": "Available for sale",
                "scraped_at": "2024-01-01T00:00:00",
            },
            {
                "listing_id": 2,
                "car_name": "Hino 300 Series",
                "price": 50000,
                "vehicle_type": "Truck",
                "status": "Available for sale",
                "scraped_at": "2024-01-01T00:00:00",
            },
        ])
        result = run_transform(db_path)
        assert result["total_silver"] == 2
        assert result["excluded_commercial"] == 1
        assert result["gold_rows"] == 1

    def test_gold_table_schema(self, tmp_path):
        db_path = tmp_path / "test.db"
        _make_test_db(db_path, [
            {
                "listing_id": 1,
                "car_name": "Honda Civic 1.6A",
                "price": 90000,
                "omv": 60000,
                "manufactured": 2021,
                "vehicle_type": "Mid-Sized Sedan",
                "fuel_type": "Petrol",
                "status": "Available for sale",
                "scraped_at": "2024-01-01T00:00:00",
            },
        ])
        run_transform(db_path)

        conn = sqlite3.connect(str(db_path))
        cols = {row[1] for row in conn.execute("PRAGMA table_info(sgcarmart_business_table)").fetchall()}
        conn.close()

        for col in GOLD_COLUMNS:
            assert col in cols, f"Missing gold column: {col}"

    def test_lifecycle_delisted(self, tmp_path):
        """Delisted listings should not appear in gold table."""
        db_path = tmp_path / "test.db"

        # First run: listing is present
        _make_test_db(db_path, [
            {
                "listing_id": 1,
                "car_name": "Honda Civic 1.6A",
                "price": 90000,
                "vehicle_type": "Mid-Sized Sedan",
                "status": "Available for sale",
                "scraped_at": "2024-01-01T00:00:00",
            },
        ])
        result1 = run_transform(db_path)
        assert result1["available"] == 1

        # Second run: listing removed from silver (not in current scrape)
        conn = sqlite3.connect(str(db_path))
        conn.execute("DELETE FROM listings WHERE listing_id = 1")
        conn.commit()
        conn.close()

        # Delisted should NOT appear in gold
        result2 = run_transform(db_path)
        assert result2["gold_rows"] == 0
        assert result2["available"] == 0

    def test_sold_status(self, tmp_path):
        db_path = tmp_path / "test.db"
        _make_test_db(db_path, [
            {
                "listing_id": 1,
                "car_name": "BMW 320i 2.0A",
                "price": 150000,
                "vehicle_type": "Luxury Sedan",
                "status": "SOLD",
                "scraped_at": "2024-01-01T00:00:00",
            },
        ])
        result = run_transform(db_path)
        assert result["sold"] == 1

    def test_missing_db_raises(self):
        with pytest.raises(FileNotFoundError):
            run_transform("/nonexistent/path.db")

    def test_value_score_populated(self, tmp_path):
        db_path = tmp_path / "test.db"
        rows = []
        for i in range(20):
            rows.append({
                "listing_id": i + 1,
                "car_name": f"Car Model {i}",
                "price": 50000 + i * 5000,
                "depreciation": 5000 + i * 1000,
                "manufactured": 2020 + (i % 4),
                "mileage_km": 20000 + i * 10000,
                "omv": 30000 + i * 2000,
                "vehicle_type": "Mid-Sized Sedan",
                "fuel_type": "Petrol",
                "status": "Available for sale",
                "scraped_at": "2024-01-01T00:00:00",
            })
        _make_test_db(db_path, rows)
        result = run_transform(db_path)

        conn = sqlite3.connect(str(db_path))
        conn.row_factory = sqlite3.Row
        scored = conn.execute(
            "SELECT value_score FROM sgcarmart_business_table WHERE value_score IS NOT NULL"
        ).fetchall()
        conn.close()

        assert len(scored) > 0, "Expected some rows to have value scores"
        for row in scored:
            assert 0 <= row["value_score"] <= 100
