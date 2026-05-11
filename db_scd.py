"""SCD Type 2 (Slowly Changing Dimension) for listing history tracking.

Tracks all changes to listings over time using valid_from/valid_to/is_current.
Each scrape run creates new history rows for changed listings and closes
previous rows by setting valid_to and is_current=0.
"""

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

from db import LISTING_COLUMNS, ListingDB

logger = logging.getLogger(__name__)

SCHEMA_LISTINGS_HISTORY = """
CREATE TABLE IF NOT EXISTS listings_history (
    history_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    listing_id    INTEGER NOT NULL,
    {columns},
    valid_from    TEXT NOT NULL,
    valid_to      TEXT,
    is_current    INTEGER NOT NULL DEFAULT 1,
    scrape_run_id INTEGER,
    FOREIGN KEY (listing_id) REFERENCES listings(listing_id)
);
"""

INDEX_LISTINGS_HISTORY = [
    "CREATE INDEX IF NOT EXISTS idx_history_listing_current "
    "ON listings_history (listing_id, is_current)",

    "CREATE INDEX IF NOT EXISTS idx_history_listing_valid_from "
    "ON listings_history (listing_id, valid_from)",

    "CREATE INDEX IF NOT EXISTS idx_history_run_id "
    "ON listings_history (scrape_run_id)",
]

# Build the column definitions for the history table
_HISTORY_COL_DEFS = ",\n    ".join(f"{col} {'REAL' if col == 'power' else 'TEXT' if col in ('reg_date', 'coe_remaining', 'posted_date', 'detail_url', 'listing_type', 'fuel_type', 'transmission', 'status', 'car_name', 'car_model', 'vehicle_type', 'features', 'accessories') else 'INTEGER'}" for col in LISTING_COLUMNS if col != "listing_id")

SCHEMA_LISTINGS_HISTORY_FINAL = SCHEMA_LISTINGS_HISTORY.format(
    columns=_HISTORY_COL_DEFS,
)

# SQL for inserting a history row
_HISTORY_COLUMNS = ["listing_id"] + [c for c in LISTING_COLUMNS if c != "listing_id"] + ["valid_from", "valid_to", "is_current", "scrape_run_id"]
_HISTORY_PLACEHOLDERS = ", ".join(["?"] * len(_HISTORY_COLUMNS))
_INSERT_HISTORY_SQL = (
    f"INSERT INTO listings_history ({', '.join(_HISTORY_COLUMNS)}) "
    f"VALUES ({_HISTORY_PLACEHOLDERS})"
)

# SQL for closing a current row
_CLOSE_CURRENT_SQL = (
    "UPDATE listings_history SET valid_to = ?, is_current = 0 "
    "WHERE listing_id = ? AND is_current = 1"
)

# Epsilon for float comparison
_FLOAT_EPSILON = 0.01

# Fields that are REAL type and need epsilon comparison
_FLOAT_FIELDS = {"power"}

# Only price and depreciation changes trigger SCD history rows.
# Other field changes still update the base listings table via upsert.
SCD_TRACKED_FIELDS = frozenset({"price", "depreciation", "status"})


class ListingHistoryDB:
    """SCD Type 2 wrapper around ListingDB.

    Extends the base database with change detection and history tracking.
    Each listing maintains a chain of history rows:
    - New listing: insert one history row with is_current=1
    - Changed listing: close old row (valid_to=now, is_current=0),
      insert new row with is_current=1
    - Unchanged listing: skip (no new history row)
    """

    def __init__(self, db: ListingDB):
        self.db = db
        self.conn: sqlite3.Connection = db.conn
        self._init_history_schema()

    def _init_history_schema(self):
        """Create listings_history table and indexes if they don't exist."""
        self.conn.executescript(SCHEMA_LISTINGS_HISTORY_FINAL)
        for idx_sql in INDEX_LISTINGS_HISTORY:
            self.conn.execute(idx_sql)
        self.conn.commit()

    @staticmethod
    def _values_equal(current: object, incoming: object, field: str) -> bool:
        """Compare two field values for change detection.

        Rules:
        - Both None -> equal (no change)
        - One None, other not -> depends: incoming None means "keep existing"
          so we treat that as equal (COALESCE logic)
        - Both non-None -> direct comparison with epsilon for floats
        """
        if current is None and incoming is None:
            return True
        if incoming is None:
            # NULL incoming = "keep existing" -> no change
            return True
        if current is None:
            # existing is None but incoming has value -> change
            return False

        if field in _FLOAT_FIELDS:
            try:
                return abs(float(current) - float(incoming)) < _FLOAT_EPSILON
            except (TypeError, ValueError):
                return str(current) == str(incoming)

        return current == incoming

    @staticmethod
    def detect_changes(current: dict, incoming: dict) -> dict[str, tuple]:
        """Detect changes in SCD-tracked fields (price, depreciation).

        Only price and depreciation changes trigger history row creation.
        Other field changes still update the base listings table via upsert.

        Args:
            current: existing listing dict from DB
            incoming: new listing dict from scraper

        Returns:
            dict of {field_name: (old_value, new_value)} for changed fields
        """
        changes: dict[str, tuple] = {}
        for col in SCD_TRACKED_FIELDS:
            cur_val = current.get(col)
            inc_val = incoming.get(col)
            if not ListingHistoryDB._values_equal(cur_val, inc_val, col):
                changes[col] = (cur_val, inc_val)
        return changes

    def _get_current_history_row(self, listing_id: int) -> dict | None:
        """Get the current history row for a listing."""
        cols = ", ".join(_HISTORY_COLUMNS)
        cursor = self.conn.execute(
            f"SELECT {cols} FROM listings_history "
            f"WHERE listing_id = ? AND is_current = 1",
            (listing_id,),
        )
        row = cursor.fetchone()
        if not row:
            return None
        return dict(zip(_HISTORY_COLUMNS, row))

    def _insert_history_row(
        self, listing_id: int, record: dict,
        valid_from: str, run_id: int | None,
    ):
        """Insert a new history row for a listing."""
        values = [listing_id]
        for col in LISTING_COLUMNS:
            if col == "listing_id":
                continue
            values.append(record.get(col))
        values.extend([valid_from, None, 1, run_id])
        self.conn.execute(_INSERT_HISTORY_SQL, values)

    def upsert_with_history(
        self, cars: list[dict], run_id: int | None = None,
    ) -> dict:
        """Upsert listings with SCD Type 2 history tracking.

        For each listing:
        - New listing: insert into listings + create initial history row
        - Changed listing: update listings, close old history row,
          create new history row
        - Unchanged listing: skip history (listing data already current)

        Args:
            cars: list of listing dicts from scraper
            run_id: scrape run ID for tracking

        Returns:
            dict with counts: new, changed, unchanged, total
        """
        if not cars:
            return {"new": 0, "changed": 0, "unchanged": 0, "total": 0}

        now = datetime.now().isoformat()
        counts = {"new": 0, "changed": 0, "unchanged": 0, "total": len(cars)}

        # Get existing listings from DB for change detection
        incoming_ids = {c["listing_id"] for c in cars if "listing_id" in c}
        existing_map: dict[int, dict] = {}
        if incoming_ids:
            placeholders = ",".join(["?"] * len(incoming_ids))
            cols = ", ".join(LISTING_COLUMNS)
            cursor = self.conn.execute(
                f"SELECT {cols} FROM listings WHERE listing_id IN ({placeholders})",
                list(incoming_ids),
            )
            for row in cursor.fetchall():
                d = dict(zip(LISTING_COLUMNS, row))
                existing_map[d["listing_id"]] = d

        for car in cars:
            listing_id = car.get("listing_id")
            if listing_id is None:
                continue

            existing = existing_map.get(listing_id)

            if existing is None:
                # New listing
                self._insert_history_row(listing_id, car, now, run_id)
                counts["new"] += 1
            else:
                changes = self.detect_changes(existing, car)
                if changes:
                    # Changed listing: close old row, insert new
                    self.conn.execute(_CLOSE_CURRENT_SQL, (now, listing_id))
                    self._insert_history_row(listing_id, car, now, run_id)
                    counts["changed"] += 1
                    logger.debug(
                        "Listing %d changed: %s",
                        listing_id,
                        ", ".join(changes.keys()),
                    )
                else:
                    # Unchanged listing
                    counts["unchanged"] += 1

        # Upsert the base listings table
        self.db.upsert_listings(cars)
        self.conn.commit()

        return counts

    def get_history(self, listing_id: int) -> list[dict]:
        """Get full history for a listing, ordered by valid_from."""
        all_cols = _HISTORY_COLUMNS
        cols_str = ", ".join(all_cols)
        cursor = self.conn.execute(
            f"SELECT {cols_str} FROM listings_history "
            f"WHERE listing_id = ? ORDER BY valid_from",
            (listing_id,),
        )
        return [dict(zip(all_cols, row)) for row in cursor.fetchall()]

    def get_recent_changes(self, hours: int = 24) -> list[dict]:
        """Get listings that changed within the last N hours."""
        all_cols = _HISTORY_COLUMNS
        cols_str = ", ".join(all_cols)
        cursor = self.conn.execute(
            f"SELECT {cols_str} FROM listings_history "
            f"WHERE valid_from >= datetime('now', ?) "
            f"ORDER BY valid_from DESC",
            (f"-{hours} hours",),
        )
        return [dict(zip(all_cols, row)) for row in cursor.fetchall()]

    def get_current_with_history_count(self) -> list[dict]:
        """Get current listings with their history count."""
        cursor = self.conn.execute(
            "SELECT l.listing_id, l.car_name, l.price, COUNT(h.history_id) as versions "
            "FROM listings l "
            "LEFT JOIN listings_history h ON l.listing_id = h.listing_id "
            "GROUP BY l.listing_id "
            "HAVING versions > 1 "
            "ORDER BY versions DESC"
        )
        rows = cursor.fetchall()
        keys = ["listing_id", "car_name", "price", "versions"]
        return [dict(zip(keys, row)) for row in rows]
