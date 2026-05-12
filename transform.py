"""Silver-to-Gold transformation for SGCarMart listings.

Transforms raw validated listings (Silver) into a business-ready
consumer vehicle table (Gold) with:
- Commercial vehicle exclusion (vans, trucks, buses)
- Brand/model/trim extraction from car_name
- Normalized enums (fuel_type, vehicle_type, status)
- Derived metrics (age, COE months, value score)
"""

import logging
import re
from datetime import date, datetime
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Multi-word brands matched first (longest match wins).
# Sorted by word count descending, then alphabetically.
MULTI_WORD_BRANDS = [
    "Mitsubishi Fuso",  # commercial — kept here for detection
    "Aston Martin",
    "Alfa Romeo",
    "Land Rover",
    "Mercedes-Benz",
    "Rolls-Royce",
]

# Single-word brands (from actual data, 90+ brands).
# Includes both consumer and commercial — filtering happens separately.
SINGLE_WORD_BRANDS = sorted({
    "Audi", "AVATR", "Aion", "Austin", "BYD", "BMW", "Bentley",
    "CUPRA", "Cadillac", "CAMC", "Caterham", "Chevrolet", "Chrysler",
    "Citroen", "Dacia", "Daihatsu", "Datsun", "DENZA", "DFSK",
    "DS", "Daimler", "Dodge", "Dongfeng", "Farizon", "Ferrari",
    "Fiat", "Ford", "Forland", "Foton", "Golden", "Higer", "Hino",
    "Honda", "Hummer", "Hyundai", "Infiniti", "Isuzu", "Iveco",
    "JAC", "Jaecoo", "Jaguar", "Jeep", "JMEV", "Kia", "King",
    "KYC", "Lamborghini", "Lexus", "Leyland", "Lotus", "MAN",
    "MG", "MINI", "Maserati", "Maybach", "Mazda",
    "McLaren", "Mitsuoka", "Mitsubishi", "Morgan", "Morris",
    "Nissan", "OHM", "ORA", "Omoda", "Opel", "Peugeot", "Perodua",
    "Polestar", "Pontiac", "Porsche", "Proton", "QingLing", "RUF",
    "Renault", "Riley", "Rover", "SANY", "SEAT", "SRM", "Saab",
    "Scania", "Scion", "Seres", "SINOTRUK", "Skoda", "Smart",
    "Ssangyong", "Subaru", "Suzuki", "TD", "Tesla", "Triumph",
    "UD", "Valiant", "Volkswagen", "Volvo", "XPENG", "Yutong",
    "ZEEKR", "smart",
}, key=str.lower)

ALL_BRANDS = MULTI_WORD_BRANDS + SINGLE_WORD_BRANDS

# Brand-specific model prefix patterns for model/trim splitting.
# Each pattern matches the model portion of `remaining` (after brand is stripped).
# Everything after the match becomes trim. First match wins per brand.
BRAND_MODEL_PATTERNS: dict[str, list[re.Pattern]] = {
    "Mercedes-Benz": [
        # -Class with variant code: C-Class C180, E-Class E200, GLC-Class GLC300
        re.compile(r"^((?:[A-Z]{1,3}-Class)\s+[A-Z]{1,3}\d+\w*)", re.I),
        # EQ variant codes (search, not anchored): EQA250, EQB350, EQE300, EQS450+
        re.compile(r"(EQ[A-ESV]\d+\+?)", re.I),
        # AMG GT (separate model line)
        re.compile(r"^(AMG GT)\b", re.I),
        # Maybach
        re.compile(r"^(Maybach)\b", re.I),
        # -Class fallback (no variant code): E-Class, S-Class
        re.compile(r"^([A-Z]{1,3}-Class)\b", re.I),
        # EQ base (fallback): EQA, EQB, EQE, EQS, EQV
        re.compile(r"^(EQ[A-ESV])\b", re.I),
    ],
    "BMW": [
        # Series + engine code: 3 Series 330i, 1 Series 118i, 5 Series 530e
        re.compile(r"^(\d+ Series \d{3}[a-z]*)\b", re.I),
        # M Performance: M135i, M235i, M340i, M850i
        re.compile(r"^(M\d{3}[a-z]*)\b", re.I),
        # M cars: M2, M3, M4, M5, M6
        re.compile(r"^(M\d{1,2})\b", re.I),
        # iX numbered: iX1, iX3
        re.compile(r"^(iX\d+)\b", re.I),
        # iX standalone
        re.compile(r"^(iX)\b", re.I),
        # XM (standalone model)
        re.compile(r"^(XM)\b", re.I),
        # i models: i4, i5, i7, i8
        re.compile(r"^(i\d+)\b", re.I),
        # X models: X1, X3, X5
        re.compile(r"^(X\d+)\b", re.I),
        # Z models: Z3, Z4
        re.compile(r"^(Z\d+)\b", re.I),
        # Series without engine code (fallback): 3 Series
        re.compile(r"^(\d+ Series)\b", re.I),
    ],
    "Tesla": [
        re.compile(r"^(Model [S3XY])\b", re.I),
        re.compile(r"^(Cybertruck)\b", re.I),
    ],
    "Lexus": [
        # Full model code (search, not anchored): IS250, ES300h, RX350L, RZ450e
        re.compile(r"((?:IS|ES|GS|LS|RX|NX|UX|LX|RC|LC|RZ)\d+\w?)", re.I),
    ],
    "Volvo": [
        re.compile(r"^([SVC]X\d+)\b", re.I),
        re.compile(r"^(XC\d+)\b", re.I),
        re.compile(r"^([SVC]\d+)\b", re.I),
        re.compile(r"^(EX\d+|EC\d+|EM\d+)\b", re.I),
    ],
    "BYD": [
        re.compile(r"^(Atto \d+|Denza D9|Sealion \d+|Seal \d+|Seal|Dolphin|eT\d+|T\d+|M6|e6|C\d+|Tang|Han|Song|Yuan|Seagull)\b", re.I),
    ],
    "Porsche": [
        re.compile(r"^(911|Cayenne|Panamera|Macan|Taycan|Boxster|Cayman|718)\b", re.I),
    ],
    "Lamborghini": [
        re.compile(r"^(Huracan|Aventador|Urus|Gallardo|Revuelto|Temerario)\b", re.I),
    ],
}

# Trim-level keywords used as a last-resort fallback to split model from trim.
TRIM_KEYWORDS = re.compile(
    r"\b(AMG|M-Sport|MSport|R-Design|Luxury|Premium|Sport|Exclusive|"
    r"Progressive|Avantgarde|Classic|Elegance|Style|Urban|Highline|"
    r"Edition|Limited|Signature|Competition)\b",
    re.I,
)

# Vehicle types that indicate commercial vehicles — excluded from gold.
COMMERCIAL_VEHICLE_TYPES = frozenset({
    "Van", "Truck", "Bus/Mini Bus",
})

# Brands that are exclusively commercial — excluded from gold.
COMMERCIAL_BRANDS = frozenset({
    "Hino", "Scania", "Golden", "Yutong", "SOKON", "Farizon",
    "Forland", "KYC", "Higer", "CAMC", "Sinotruk", "SANY", "JMEV",
    "QingLing", "MAN", "Iveco", "Leyland", "Mitsubishi Fuso",
    "Maxus", "Isuzu", "UD", "SRM",
})

# Model name patterns that indicate commercial vehicles, even if the brand
# makes some consumer models (e.g. Toyota ProAce, Nissan NV200).
COMMERCIAL_MODEL_PATTERNS = [
    re.compile(p, re.IGNORECASE)
    for p in [
        r"\bProAce\b",
        r"\bNV200\b",
        r"\beDeliver\b",
        r"\be-Dispatch\b",
        r"\be-Berlingo\b",
        r"\bVanette\b",
        r"\bCombinot\b",
        r"\bCanter\b",
        r"\bActros\b",
        r"\bHiace\b",
        r"\bCommuter\b",
    ]
]

# COE category codes scraped as vehicle_type — legitimate consumer cars.
COE_CATEGORY_CODES = frozenset({
    "$15a", "$15c", "$15d", "$15e", "$15f", "$14e",
})

# Fuel type normalization map (silver value -> gold value).
FUEL_TYPE_MAP = {
    "Petrol": "Petrol",
    "Petrol-Electric": "Hybrid",
    "Diesel": "Diesel",
    "Diesel (Euro 5 Engine and Above)": "Diesel",
    "Diesel (Euro 4 Engine and Below)": "Diesel",
    "Diesel-Electric (Euro 5 Engine and Above)": "Diesel-Hybrid",
    "Diesel (Registered as Commercial Vehicle)": "Diesel",
    "Electric": "Electric",
}

# Vehicle type normalization map (silver value -> gold value).
VEHICLE_TYPE_MAP = {
    "SUV": "SUV",
    "Luxury Sedan": "Luxury Sedan",
    "Sports Car": "Sports Car",
    "MPV": "MPV",
    "Mid-Sized Sedan": "Sedan",
    "Hatchback": "Hatchback",
    "Stationwagon": "Stationwagon",
    "Others": "Classic/Others",
}

# Status normalization map (silver value -> gold value).
STATUS_MAP = {
    "Available for sale": "Available",
    "Available": "Available",
    "SOLD": "Sold",
    "Sold": "Sold",
    "CLOSED": "Closed",
    "Closed": "Closed",
    "Reserved": "Reserved",
    "N.A.": "Available",  # treat N.A. as available
}

# Gold table DDL.
GOLD_TABLE_DDL = """\
CREATE TABLE IF NOT EXISTS sgcarmart_business_table (
    listing_id          INTEGER PRIMARY KEY,
    brand               TEXT NOT NULL,
    model               TEXT,
    trim                TEXT,
    car_name            TEXT,
    price               INTEGER,
    installment         INTEGER,
    depreciation        INTEGER,
    dereg_value         INTEGER,
    manufactured        INTEGER,
    age_years           INTEGER,
    mileage_km          INTEGER,
    engine_cap_cc       INTEGER,
    transmission        TEXT,
    fuel_type           TEXT,
    power               REAL,
    curb_weight         INTEGER,
    reg_date            TEXT,
    coe                 INTEGER,
    coe_remaining_months INTEGER,
    road_tax            INTEGER,
    omv                 INTEGER,
    arf                 INTEGER,
    vehicle_type        TEXT,
    listing_type        TEXT,
    owners              INTEGER,
    days_on_market      INTEGER,
    features            TEXT,
    accessories         TEXT,
    detail_url          TEXT,
    price_to_omv_ratio  REAL,
    value_score         REAL,
    status              TEXT NOT NULL DEFAULT 'Available',
    first_seen_at       TEXT,
    last_seen_at        TEXT,
    CHECK (status IN ('Available', 'Sold', 'Closed', 'Reserved', 'Delisted'))
)\
"""

GOLD_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_biz_brand ON sgcarmart_business_table(brand)",
    "CREATE INDEX IF NOT EXISTS idx_biz_model ON sgcarmart_business_table(model)",
    "CREATE INDEX IF NOT EXISTS idx_biz_price ON sgcarmart_business_table(price)",
    "CREATE INDEX IF NOT EXISTS idx_biz_fuel ON sgcarmart_business_table(fuel_type)",
    "CREATE INDEX IF NOT EXISTS idx_biz_vtype ON sgcarmart_business_table(vehicle_type)",
    "CREATE INDEX IF NOT EXISTS idx_biz_status ON sgcarmart_business_table(status)",
    "CREATE INDEX IF NOT EXISTS idx_biz_dep ON sgcarmart_business_table(depreciation)",
    "CREATE INDEX IF NOT EXISTS idx_biz_score ON sgcarmart_business_table(value_score)",
]

GOLD_COLUMNS = [
    "listing_id", "brand", "model", "trim", "car_name",
    "price", "installment", "depreciation", "dereg_value",
    "manufactured", "age_years", "mileage_km", "engine_cap_cc",
    "transmission", "fuel_type", "power", "curb_weight",
    "reg_date", "coe", "coe_remaining_months", "road_tax",
    "omv", "arf", "vehicle_type", "listing_type", "owners",
    "days_on_market", "features", "accessories", "detail_url",
    "price_to_omv_ratio", "value_score", "status",
    "first_seen_at", "last_seen_at",
]

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def extract_brand_model(car_name: str) -> tuple[str, str | None, str | None]:
    """Extract (brand, model, trim) from a car_name string.

    Strategy (in order):
    1. Strip COE suffixes, match brand (multi-word then single-word)
    2. Brand-specific model prefix patterns → model/trim split
    3. Engine-code heuristic (``\\d+\\.\\d+[AM]``) for generic brands
    4. Trim-keyword fallback
    5. Last resort: first 1-2 words as model, rest as trim
    """
    if not car_name:
        return "Unknown", None, None

    name = car_name.strip()

    # Strip COE suffix: "(COE till 03/2030)", "(New 10-yr COE)", "(New 5-yr COE)"
    clean = re.sub(r"\s*\((COE till \d{2}/\d{4}|New \d+-yr COE|COE till \d{2}/\d{4} renewed)\)", "", name)
    # Also strip trailing "(renewed)" etc.
    clean = re.sub(r"\s*\(renewed\)", "", clean)

    brand = None
    remaining = clean

    # Try multi-word brands first (longer match wins).
    for bw in MULTI_WORD_BRANDS:
        if clean.lower().startswith(bw.lower()):
            brand = bw
            remaining = clean[len(bw):].strip()
            break

    # Try single-word brands.
    if brand is None:
        first_word = clean.split()[0] if clean.split() else ""
        for sw in SINGLE_WORD_BRANDS:
            if first_word.lower() == sw.lower():
                brand = sw
                remaining = clean[len(first_word):].strip()
                break

    # Fallback.
    if brand is None:
        parts = clean.split(None, 1)
        brand = parts[0] if parts else "Unknown"
        remaining = parts[1] if len(parts) > 1 else ""

    if not remaining:
        return brand, None, None

    # --- Strategy 1: Brand-specific model patterns ---
    model, trim = _try_brand_pattern(brand, remaining)
    if model is not None:
        return brand, model, trim

    # --- Strategy 2: Engine-code heuristic ---
    model, trim = _try_engine_code_split(remaining)
    if model is not None:
        return brand, model, trim

    # --- Strategy 3: Trim-keyword fallback ---
    model, trim = _try_trim_keyword_split(remaining)
    if model is not None:
        return brand, model, trim

    # --- Last resort: entire remaining text is model, no trim ---
    return brand, remaining or None, None


def _try_brand_pattern(brand: str, remaining: str) -> tuple[str | None, str | None]:
    """Try brand-specific regex to split model/trim. Returns (model, trim) or (None, None)."""
    patterns = BRAND_MODEL_PATTERNS.get(brand)
    if not patterns:
        return None, None

    for pattern in patterns:
        m = pattern.search(remaining)
        if m:
            matched_model = m.group(1)
            after = remaining[m.end():].strip()
            trim = after if after else None
            return matched_model, trim

    return None, None


def _try_engine_code_split(remaining: str) -> tuple[str | None, str | None]:
    """Split on engine codes like ``1.6A``, ``2.0M``. Returns (model, trim) or (None, None)."""
    tokens = remaining.split()
    model_tokens = []
    trim_start = len(tokens)

    for i, tok in enumerate(tokens):
        if re.match(r"^\d+\.\d+[AM]$", tok):
            trim_start = i
            break
        if tok in ("A", "M") and i > 0:
            trim_start = i
            break
        model_tokens.append(tok)

    model = " ".join(model_tokens) if model_tokens else None
    trim_tokens = tokens[trim_start:]
    trim = " ".join(trim_tokens) if trim_tokens else None

    if model and trim:
        return model, trim
    if model and not trim:
        # No engine code found — signal that this strategy didn't fire.
        return None, None

    return None, None


def _try_trim_keyword_split(remaining: str) -> tuple[str | None, str | None]:
    """Split on the first trim-level keyword. Returns (model, trim) or (None, None)."""
    m = TRIM_KEYWORDS.search(remaining)
    if not m:
        return None, None

    split_pos = m.start()
    if split_pos == 0:
        return None, None

    model = remaining[:split_pos].strip()
    trim = remaining[split_pos:].strip()
    return (model or None, trim or None)


def parse_coe_months(coe_remaining: str | None) -> int | None:
    """Parse COE remaining text to total months.

    Examples:
        "5y 3m" -> 63
        "10y" -> 120
        "3m 15d" -> 3
        "5y (renewed)" -> 60
        "2y 11m" -> 35
    """
    if not coe_remaining:
        return None

    text = coe_remaining.strip()

    # Extract years.
    y_match = re.search(r"(\d+)y", text)
    years = int(y_match.group(1)) if y_match else 0

    # Extract months.
    m_match = re.search(r"(\d+)m", text)
    months = int(m_match.group(1)) if m_match else 0

    total = years * 12 + months
    return total if total > 0 else None


def normalize_fuel_type(fuel_type: str | None) -> str | None:
    """Normalize fuel type to clean enum."""
    if not fuel_type:
        return None
    return FUEL_TYPE_MAP.get(fuel_type)


def normalize_vehicle_type(vehicle_type: str | None) -> str | None:
    """Normalize vehicle type to clean enum.

    Returns None for COE category codes (no clean mapping).
    """
    if not vehicle_type:
        return None
    if vehicle_type in COE_CATEGORY_CODES:
        return None  # unknown category
    return VEHICLE_TYPE_MAP.get(vehicle_type)


def normalize_status(status: str | None) -> str:
    """Normalize listing status to gold enum."""
    if not status:
        return "Available"
    return STATUS_MAP.get(status, "Available")


def parse_reg_date(reg_date: str | None) -> str | None:
    """Parse reg_date to ISO format (YYYY-MM-DD).

    Input formats: "27-Mar-2008", "06-Jan-2016"
    Output: "2008-03-27"
    """
    if not reg_date:
        return None
    try:
        dt = datetime.strptime(reg_date.strip(), "%d-%b-%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def compute_days_on_market(posted_date: str | None, today: date | None = None) -> int | None:
    """Compute days between posted_date and today."""
    if not posted_date:
        return None
    today = today or date.today()
    try:
        dt = datetime.strptime(posted_date.strip(), "%d-%b-%Y").date()
        delta = (today - dt).days
        return max(delta, 0)
    except ValueError:
        try:
            dt = datetime.strptime(posted_date.strip(), "%Y-%m-%d").date()
            delta = (today - dt).days
            return max(delta, 0)
        except ValueError:
            return None


def is_commercial(vehicle_type: str | None, brand: str | None, car_name: str = "") -> bool:
    """Check if a vehicle should be excluded as commercial."""
    if vehicle_type and vehicle_type in COMMERCIAL_VEHICLE_TYPES:
        return True
    if brand and brand in COMMERCIAL_BRANDS:
        return True
    # Check model name patterns for commercial models from mixed brands.
    if car_name:
        for pattern in COMMERCIAL_MODEL_PATTERNS:
            if pattern.search(car_name):
                return True
    return False


# ---------------------------------------------------------------------------
# Value score computation
# ---------------------------------------------------------------------------


def compute_value_score(rows: list[dict]) -> list[dict]:
    """Compute a 0-100 value score for each row based on percentile ranks.

    Higher score = better value buy.
    Weights:
        - depreciation (lower is better): 30%
        - age_years (lower is better): 20%
        - mileage_km (lower is better): 20%
        - price_to_omv_ratio (lower is better): 15%
        - coe_remaining_months (higher is better): 15%
    """
    n = len(rows)
    if n == 0:
        return rows

    def percentile_rank(values: list[float | None], reverse: bool = False) -> list[float | None]:
        """Compute percentile rank (0-1) for each value.

        By default, lower values get lower ranks (0).
        With reverse=True, lower values get higher ranks (1) — used for
        "lower is better" metrics so the best value gets the highest score.
        """
        # Filter to non-None values for ranking.
        valid = sorted([v for v in values if v is not None])
        if not valid:
            return [None] * len(values)

        ranks = []
        for v in values:
            if v is None:
                ranks.append(None)
                continue
            # Count how many values are below this one.
            below = sum(1 for x in valid if x < v)
            rank = below / max(len(valid) - 1, 1)
            if reverse:
                rank = 1.0 - rank
            ranks.append(rank)
        return ranks

    dep_ranks = percentile_rank([r.get("depreciation") for r in rows], reverse=True)
    age_ranks = percentile_rank([r.get("age_years") for r in rows], reverse=True)
    mileage_ranks = percentile_rank([r.get("mileage_km") for r in rows], reverse=True)
    ratio_ranks = percentile_rank([r.get("price_to_omv_ratio") for r in rows], reverse=True)
    coe_ranks = percentile_rank([r.get("coe_remaining_months") for r in rows], reverse=True)

    for i, row in enumerate(rows):
        scores = []
        weights = []

        for rank, weight in [
            (dep_ranks[i], 0.30),
            (age_ranks[i], 0.20),
            (mileage_ranks[i], 0.20),
            (ratio_ranks[i], 0.15),
            (coe_ranks[i], 0.15),
        ]:
            if rank is not None:
                scores.append(rank * weight)
                weights.append(weight)

        if weights:
            # Normalize so total weight = 1.0, then scale to 0-100.
            row["value_score"] = round(sum(scores) / sum(weights) * 100, 1)
        else:
            row["value_score"] = None

    return rows


# ---------------------------------------------------------------------------
# Main transformation
# ---------------------------------------------------------------------------


def _init_gold_table(conn) -> None:
    """Create gold table and indexes if they don't exist."""
    conn.execute(GOLD_TABLE_DDL)
    for idx_sql in GOLD_INDEXES:
        conn.execute(idx_sql)
    conn.commit()


def _transform_row(row: dict, today: date) -> dict:
    """Transform a single silver row into a gold row."""
    brand, model, trim = extract_brand_model(row.get("car_name", ""))

    price = row.get("price")
    omv = row.get("omv")
    manufactured = row.get("manufactured")

    price_to_omv_ratio = None
    if price and omv and omv > 0:
        price_to_omv_ratio = round(price / omv, 3)

    age_years = None
    if manufactured:
        age_years = today.year - manufactured

    return {
        "listing_id": row["listing_id"],
        "brand": brand,
        "model": model,
        "trim": trim,
        "car_name": row.get("car_name"),
        "price": price,
        "installment": row.get("installment"),
        "depreciation": row.get("depreciation"),
        "dereg_value": row.get("dereg_value"),
        "manufactured": manufactured,
        "age_years": age_years,
        "mileage_km": row.get("mileage_km"),
        "engine_cap_cc": row.get("engine_cap_cc"),
        "transmission": row.get("transmission"),
        "fuel_type": normalize_fuel_type(row.get("fuel_type")),
        "power": row.get("power"),
        "curb_weight": row.get("curb_weight"),
        "reg_date": parse_reg_date(row.get("reg_date")),
        "coe": row.get("coe"),
        "coe_remaining_months": parse_coe_months(row.get("coe_remaining")),
        "road_tax": row.get("road_tax"),
        "omv": omv,
        "arf": row.get("arf"),
        "vehicle_type": normalize_vehicle_type(row.get("vehicle_type")),
        "listing_type": row.get("listing_type"),
        "owners": row.get("owners"),
        "days_on_market": compute_days_on_market(row.get("posted_date"), today),
        "features": row.get("features"),
        "accessories": row.get("accessories"),
        "detail_url": row.get("detail_url"),
        "price_to_omv_ratio": price_to_omv_ratio,
        "value_score": None,  # computed in batch after
        "status": normalize_status(row.get("status")),
        "first_seen_at": row.get("scraped_at"),
        "last_seen_at": row.get("scraped_at"),
    }


def run_transform(db_path: str | Path = "output/scrapling_listings.db") -> dict:
    """Run the full Silver-to-Gold transformation.

    Reads silver listings, filters commercial vehicles, transforms and
    enriches rows, and writes to gold table.

    Returns stats dict.
    """
    import sqlite3

    db_path = Path(db_path)
    if not db_path.exists():
        raise FileNotFoundError(f"DB not found: {db_path}")

    today = date.today()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Ensure gold table exists.
    _init_gold_table(conn)

    # --- Step 1: Load current silver data ---
    silver_rows = conn.execute(
        "SELECT * FROM listings ORDER BY listing_id"
    ).fetchall()
    silver_dicts = [dict(r) for r in silver_rows]
    logger.info("Loaded %d silver rows", len(silver_dicts))

    # --- Step 2: Filter & transform ---
    gold_rows = []
    excluded_commercial = 0

    for row in silver_dicts:
        brand, _, _ = extract_brand_model(row.get("car_name", ""))
        if is_commercial(row.get("vehicle_type"), brand, row.get("car_name", "")):
            excluded_commercial += 1
            continue

        gold_rows.append(_transform_row(row, today))

    logger.info(
        "Filtered %d commercial vehicles, %d consumer vehicles remain",
        excluded_commercial, len(gold_rows),
    )

    # --- Step 3: Compute value scores ---
    # Only score rows that are Available (for relevance).
    available_rows = [r for r in gold_rows if r["status"] == "Available"]
    compute_value_score(available_rows)

    # Merge scores back.
    score_map = {r["listing_id"]: r.get("value_score") for r in available_rows}
    for r in gold_rows:
        if r["listing_id"] in score_map:
            r["value_score"] = score_map[r["listing_id"]]

    # --- Step 4: Write gold table ---
    # Delete all rows and re-insert (clean rebuild).
    conn.execute("DELETE FROM sgcarmart_business_table")

    placeholders = ", ".join(["?"] * len(GOLD_COLUMNS))
    col_list = ", ".join(GOLD_COLUMNS)
    insert_sql = f"INSERT INTO sgcarmart_business_table ({col_list}) VALUES ({placeholders})"

    for r in gold_rows:
        values = [r.get(c) for c in GOLD_COLUMNS]
        conn.execute(insert_sql, values)

    conn.commit()

    # --- Stats ---
    stats = {
        "total_silver": len(silver_dicts),
        "excluded_commercial": excluded_commercial,
        "gold_rows": len(gold_rows),
        "available": sum(1 for r in gold_rows if r["status"] == "Available"),
        "sold": sum(1 for r in gold_rows if r["status"] == "Sold"),
    }

    logger.info(
        "Gold transform complete: %d silver -> %d gold (%d commercial excluded)",
        stats["total_silver"], stats["gold_rows"], stats["excluded_commercial"],
    )

    conn.close()
    return stats
