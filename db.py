"""SQLite database module for SGCarMart listing scraper.

Provides ACID transactions, efficient upsert by listing_id,
and query capability without loading the full dataset into memory.
"""

import logging
import re
import sqlite3
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)

SCHEMA_LISTINGS = """
CREATE TABLE IF NOT EXISTS listings (
    listing_id    INTEGER PRIMARY KEY,
    car_name      TEXT,
    detail_url    TEXT,
    listing_type  TEXT,
    price         INTEGER,
    installment   INTEGER,
    depreciation  INTEGER,
    reg_date      TEXT,
    coe_remaining TEXT,
    mileage_km    INTEGER,
    engine_cap_cc INTEGER,
    owners        INTEGER,
    fuel_type     TEXT,
    posted_date   TEXT,
    car_model     TEXT,
    coe           INTEGER,
    road_tax      INTEGER,
    omv           INTEGER,
    arf           INTEGER,
    power         REAL,
    transmission  TEXT,
    manufactured  INTEGER,
    dereg_value   INTEGER,
    curb_weight   INTEGER,
    status        TEXT,
    features      TEXT,
    accessories   TEXT,
    vehicle_type  TEXT,
    scraped_at    TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now'))
);
"""

# Idempotent ALTER TABLE statements for migrating existing DBs
_DETAIL_MIGRATIONS = [
    "ALTER TABLE listings ADD COLUMN car_model TEXT",
    "ALTER TABLE listings ADD COLUMN coe INTEGER",
    "ALTER TABLE listings ADD COLUMN road_tax INTEGER",
    "ALTER TABLE listings ADD COLUMN omv INTEGER",
    "ALTER TABLE listings ADD COLUMN arf INTEGER",
    "ALTER TABLE listings ADD COLUMN power REAL",
    "ALTER TABLE listings ADD COLUMN transmission TEXT",
    "ALTER TABLE listings ADD COLUMN manufactured INTEGER",
    "ALTER TABLE listings ADD COLUMN dereg_value INTEGER",
    "ALTER TABLE listings ADD COLUMN curb_weight INTEGER",
    "ALTER TABLE listings ADD COLUMN status TEXT",
    "ALTER TABLE listings ADD COLUMN features TEXT",
    "ALTER TABLE listings ADD COLUMN accessories TEXT",
    "ALTER TABLE listings ADD COLUMN vehicle_type TEXT",
]

SCHEMA_SCRAPE_RUNS = """
CREATE TABLE IF NOT EXISTS scrape_runs (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    started_at      TEXT,
    finished_at     TEXT,
    pages_fetched   INTEGER,
    listings_count  INTEGER,
    status          TEXT
);
"""

LISTING_COLUMNS = [
    "listing_id", "car_name", "detail_url", "listing_type",
    "price", "installment", "depreciation", "reg_date",
    "coe_remaining", "mileage_km", "engine_cap_cc",
    "owners", "fuel_type", "posted_date",
    "car_model", "coe", "road_tax", "omv", "arf", "power",
    "transmission", "manufactured", "dereg_value", "curb_weight",
    "status", "features", "accessories", "vehicle_type",
]

# Pre-computed for SQL queries
_COLUMNS_STR = ", ".join(LISTING_COLUMNS)
_COALESCE_CLAUSE = ", ".join(
    f"{c}=COALESCE(excluded.{c}, listings.{c})"
    for c in LISTING_COLUMNS if c != "listing_id"
)
_UPSERT_SQL = (
    f"INSERT INTO listings ({_COLUMNS_STR}) VALUES ({', '.join(['?'] * len(LISTING_COLUMNS))}) "
    f"ON CONFLICT(listing_id) DO UPDATE SET {_COALESCE_CLAUSE}"
)


class ListingDB:
    """Manages the SQLite database for scraped car listings."""

    def __init__(self, path: str | Path = "output/scrapling_listings.db"):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.path))
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    def _init_schema(self):
        self.conn.executescript(SCHEMA_LISTINGS + SCHEMA_SCRAPE_RUNS)
        self._migrate_detail_columns()
        self.conn.commit()

    def _migrate_detail_columns(self):
        """Add detail-page columns if they don't exist (idempotent)."""
        existing = {
            row[1] for row in
            self.conn.execute("PRAGMA table_info(listings)").fetchall()
        }
        for sql in _DETAIL_MIGRATIONS:
            col_name = re.search(r'ADD COLUMN (\w+)', sql).group(1)
            if col_name not in existing:
                self.conn.execute(sql)

    def upsert_listings(self, cars: list[dict]):
        """Batch upsert listings by listing_id.

        Uses ON CONFLICT with COALESCE so that:
        - New data overwrites existing data when non-null
        - Existing data is preserved when the incoming value is null
        - scraped_at is always preserved from the original row
        """
        if not cars:
            return
        rows = [tuple(car.get(col) for col in LISTING_COLUMNS) for car in cars]
        self.conn.executemany(_UPSERT_SQL, rows)
        self.conn.commit()

    def get_seen_ids(self) -> set[int]:
        """Load all existing listing_ids for dedup."""
        cursor = self.conn.execute("SELECT listing_id FROM listings")
        return {row[0] for row in cursor.fetchall()}

    def get_count(self) -> int:
        cursor = self.conn.execute("SELECT COUNT(*) FROM listings")
        return cursor.fetchone()[0]

    def get_all_listings(self) -> list[dict]:
        """Return all listings as list of dicts."""
        cursor = self.conn.execute(f"SELECT {_COLUMNS_STR} FROM listings")
        return [dict(zip(LISTING_COLUMNS, row)) for row in cursor.fetchall()]

    def count_missing_details(self) -> int:
        """Count listings missing detail data (cheap COUNT query)."""
        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM listings "
            "WHERE price IS NULL AND detail_url IS NOT NULL"
        )
        return cursor.fetchone()[0]

    def get_field_coverage(self) -> list[tuple[str, int, int]]:
        """Return (field_name, non_null_count, total) per column via SQL."""
        total = self.get_count()
        results = []
        for col in LISTING_COLUMNS:
            cursor = self.conn.execute(
                f"SELECT COUNT(*) FROM listings WHERE {col} IS NOT NULL"
            )
            non_null = cursor.fetchone()[0]
            results.append((col, non_null, total))
        return results

    def get_price_stats(self) -> dict | None:
        """Return price min/max/mean via SQL aggregation."""
        cursor = self.conn.execute(
            "SELECT COUNT(*), MIN(price), MAX(price), AVG(price) "
            "FROM listings WHERE price IS NOT NULL"
        )
        row = cursor.fetchone()
        if row and row[0] > 0:
            return {"count": row[0], "min": row[1], "max": row[2], "mean": int(row[3])}
        return None

    def start_run(self) -> int:
        """Insert a scrape_runs row and return the run id."""
        cursor = self.conn.execute(
            "INSERT INTO scrape_runs (started_at, status) VALUES (?, 'running')",
            (datetime.now().isoformat(),),
        )
        self.conn.commit()
        return cursor.lastrowid

    def finish_run(self, run_id: int, pages_fetched: int,
                   listings_count: int, status: str = "completed"):
        """Update a scrape_runs row with final stats."""
        self.conn.execute(
            "UPDATE scrape_runs SET finished_at = ?, pages_fetched = ?, "
            "listings_count = ?, status = ? WHERE id = ?",
            (datetime.now().isoformat(), pages_fetched,
             listings_count, status, run_id),
        )
        self.conn.commit()

    def get_last_run(self) -> dict | None:
        """Return the most recent scrape run."""
        cursor = self.conn.execute(
            "SELECT id, started_at, finished_at, pages_fetched, "
            "listings_count, status FROM scrape_runs "
            "ORDER BY id DESC LIMIT 1"
        )
        row = cursor.fetchone()
        if not row:
            return None
        keys = ["id", "started_at", "finished_at",
                "pages_fetched", "listings_count", "status"]
        return dict(zip(keys, row))

    def export_parquet(self, path: str | Path):
        """Export listings to Parquet for analysis."""
        import polars as pl
        listings = self.get_all_listings()
        if listings:
            pl.DataFrame(listings).write_parquet(str(path))

    def get_listings_missing_details(self, limit: int | None = None) -> list[tuple[int, str]]:
        """Return (listing_id, detail_url) for rows missing detail data.

        Includes listings that either never had detail data fetched
        (price IS NULL) or had fields reset for re-scrape
        (accessories IS NULL but price exists).
        """
        sql = (
            "SELECT listing_id, detail_url FROM listings "
            "WHERE detail_url IS NOT NULL "
            "AND (price IS NULL OR (price IS NOT NULL AND accessories IS NULL)) "
            "ORDER BY listing_id"
        )
        if limit:
            return self.conn.execute(sql + " LIMIT ?", (int(limit),)).fetchall()
        return self.conn.execute(sql).fetchall()

    def get_active_listings_for_rescrape(self) -> list[tuple[int, str]]:
        """Return (listing_id, detail_url) for fully-scraped active listings.

        Used by SCD to re-fetch listings for price/depreciation change detection.
        Excludes SOLD/CLOSED listings.
        """
        return self.conn.execute(
            "SELECT listing_id, detail_url FROM listings "
            "WHERE detail_url IS NOT NULL "
            "AND price IS NOT NULL AND accessories IS NOT NULL "
            "AND (status IS NULL OR status NOT IN ('SOLD', 'Sold', 'CLOSED', 'Closed')) "
            "ORDER BY listing_id"
        ).fetchall()

    def close(self):
        self.conn.close()


# ============================================================
# Quarantine table for validation failures
# ============================================================

SCHEMA_QUARANTINE = """
CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id     INTEGER,
    field_name     TEXT NOT NULL,
    field_value    TEXT,
    rule_name      TEXT NOT NULL,
    reason         TEXT NOT NULL,
    raw_record     TEXT,
    quarantined_at TEXT DEFAULT (strftime('%Y-%m-%dT%H:%M:%S','now')),
    scrape_run_id  INTEGER,
    resolved       INTEGER DEFAULT 0
);
"""

_INSERT_QUARANTINE_SQL = (
    "INSERT INTO quarantine "
    "(listing_id, field_name, field_value, rule_name, reason, raw_record, scrape_run_id) "
    "VALUES (?, ?, ?, ?, ?, ?, ?)"
)


class QuarantineDB:
    """Manages the quarantine table for invalid field values."""

    def __init__(self, db: ListingDB):
        self.db = db
        self.conn = db.conn
        self._init_quarantine_schema()

    def _init_quarantine_schema(self):
        self.conn.executescript(SCHEMA_QUARANTINE)
        self.conn.commit()

    def insert_failures(
        self, failures: list[dict], run_id: int | None = None,
    ):
        """Insert validation failure records into quarantine.

        Args:
            failures: list of dicts with keys: listing_id, field_name,
                      field_value, rule_name, reason, raw_record
            run_id: optional scrape run ID
        """
        if not failures:
            return
        rows = [
            (
                f.get("listing_id"),
                f["field_name"],
                f.get("field_value"),
                f["rule_name"],
                f["reason"],
                f.get("raw_record"),
                run_id,
            )
            for f in failures
        ]
        self.conn.executemany(_INSERT_QUARANTINE_SQL, rows)
        self.conn.commit()

    def get_stats(self) -> dict:
        """Return quarantine statistics: total, by rule, unresolved count."""
        cursor = self.conn.execute("SELECT COUNT(*) FROM quarantine")
        total = cursor.fetchone()[0]

        cursor = self.conn.execute(
            "SELECT COUNT(*) FROM quarantine WHERE resolved = 0"
        )
        unresolved = cursor.fetchone()[0]

        cursor = self.conn.execute(
            "SELECT rule_name, COUNT(*) as cnt FROM quarantine "
            "GROUP BY rule_name ORDER BY cnt DESC"
        )
        by_rule = {row[0]: row[1] for row in cursor.fetchall()}

        return {"total": total, "unresolved": unresolved, "by_rule": by_rule}

    def get_recent(self, limit: int = 50) -> list[dict]:
        """Get most recent quarantine records."""
        cursor = self.conn.execute(
            "SELECT quarantine_id, listing_id, field_name, field_value, "
            "rule_name, reason, quarantined_at, resolved "
            "FROM quarantine ORDER BY quarantined_at DESC LIMIT ?",
            (limit,),
        )
        keys = [
            "quarantine_id", "listing_id", "field_name", "field_value",
            "rule_name", "reason", "quarantined_at", "resolved",
        ]
        return [dict(zip(keys, row)) for row in cursor.fetchall()]
