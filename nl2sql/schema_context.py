"""Static schema context for the NL2SQL system.

Provides database metadata (DDL, column descriptions, top brands, sample rows,
domain hints) as hardcoded strings for injection into Generator and Reviewer
LLM prompts. No database reads -- everything is static.
"""

# ---------------------------------------------------------------------------
# Section 1: Table DDL
# ---------------------------------------------------------------------------

SECTION_1_DDL = """\
### Table DDL

```sql
CREATE TABLE sgcarmart_business_table (
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
    last_seen_at        TEXT
);
```"""

# ---------------------------------------------------------------------------
# Section 2: Column Descriptions
# ---------------------------------------------------------------------------

SECTION_2_COLUMNS = """\
### Column Descriptions

| Column | Type | Description | Unit/Range |
|--------|------|-------------|------------|
| `listing_id` | INTEGER PK | Unique SGCarMart listing identifier | 1–99999999 |
| `brand` | TEXT NOT NULL | Car manufacturer name | 81 distinct values (e.g., Toyota, Mercedes-Benz, BMW, Honda, Porsche) |
| `model` | TEXT | Car model name extracted from car_name | 1,286 distinct values (e.g., Corolla, C-Class, 3 Series) |
| `trim` | TEXT | Trim level / variant | e.g., "M-Sport", "Avantgarde", "1.6A" |
| `car_name` | TEXT | Full listing title from SGCarMart | e.g., "Toyota Corolla 1.6A" |
| `price` | INTEGER | Asking price | SGD, range: $1,500 – $2,788,000, avg: $137,397 |
| `installment` | INTEGER | Monthly installment amount | SGD |
| `depreciation` | INTEGER | Annual depreciation | SGD/year, range: $2,460 – $587,360, avg: $24,769 |
| `dereg_value` | INTEGER | Deregistration value (scrap value) | SGD |
| `manufactured` | INTEGER | Year of manufacture | 1939 – 2026 |
| `age_years` | INTEGER | Vehicle age computed from manufactured year | 0 – 87, avg: 9 |
| `mileage_km` | INTEGER | Odometer reading | km, range: 1 – 999,999, avg: 88,842. ~15% NULL |
| `engine_cap_cc` | INTEGER | Engine displacement | cc, range: 647 – 6,761, avg: 2,111 |
| `transmission` | TEXT | Gearbox type | 'Auto' (12,824) or 'Manual' (479) |
| `fuel_type` | TEXT | Normalized fuel type | 'Petrol' (10,415), 'Hybrid' (1,925), 'Electric' (578), 'Diesel' (384), 'Diesel-Hybrid' (1) |
| `power` | REAL | Engine power | kW |
| `curb_weight` | INTEGER | Vehicle weight | kg |
| `reg_date` | TEXT | Registration date | ISO format: 'YYYY-MM-DD' (e.g., '2008-03-27') |
| `coe` | INTEGER | COE (Certificate of Entitlement) cost | SGD |
| `coe_remaining_months` | INTEGER | Months until COE expires | range: 12 – 120, avg: 56 |
| `road_tax` | INTEGER | Annual road tax | SGD |
| `omv` | INTEGER | Open Market Value (import value before taxes) | SGD, range: $1,766 – $765,955, avg: $51,591 |
| `arf` | INTEGER | Additional Registration Fee (tiered tax on OMV) | SGD, range: $0 – $980,594, avg: $60,033 |
| `vehicle_type` | TEXT | Normalized body type | 'SUV' (3,503), 'Luxury Sedan' (2,580), 'Sports Car' (2,305), 'MPV' (1,515), 'Sedan' (1,338), 'Hatchback' (1,275), 'Stationwagon' (268), 'Classic/Others' (61). ~3.5% NULL |
| `listing_type` | TEXT | Seller type | 'Dealer' (12,229) or 'Direct Owner' (616) |
| `owners` | INTEGER | Number of previous owners | range: 0 – 6, avg: 2 |
| `days_on_market` | INTEGER | Days since listing was posted | range: 0 – 65, avg: 18 |
| `features` | TEXT | Comma-separated feature list | e.g., "Leather seats, Sunroof, GPS" |
| `accessories` | TEXT | Comma-separated accessory list | e.g., "Alloy rims, Reverse camera" |
| `detail_url` | TEXT | URL path to listing on SGCarMart | e.g., "/used-cars/info/1234567" |
| `price_to_omv_ratio` | REAL | Price divided by OMV | range: 0 – 61, avg: 2 |
| `value_score` | REAL | Percentile-based value score (0-100) | Only populated for status='Available'. Higher = better value |
| `status` | TEXT NOT NULL | Listing lifecycle status | 'Available' (13,294), 'Sold' (18) |
| `first_seen_at` | TEXT | Timestamp when first scraped | ISO format |
| `last_seen_at` | TEXT | Timestamp when last seen | ISO format |"""

# ---------------------------------------------------------------------------
# Section 3: Top Brands
# ---------------------------------------------------------------------------

SECTION_3_BRANDS = """\
### Top Brands (for query matching)

| Brand | Count | Brand | Count |
|-------|-------|-------|-------|
| Mercedes-Benz | 2,135 | Porsche | 626 |
| BMW | 1,734 | Volkswagen | 535 |
| Toyota | 1,634 | Mazda | 398 |
| Honda | 1,403 | Hyundai | 368 |
| Audi | 697 | Nissan | 343 |
| Mitsubishi | 331 | Kia | 305 |
| Lexus | 260 | Subaru | 212 |
| MINI | 190 | Volvo | 174 |
| Ferrari | 147 | Land Rover | 124 |
| Maserati | 121 | Rolls-Royce | 111 |

Plus 61 other brands with <100 listings each (e.g., BYD, Tesla, Lamborghini, Bentley, McLaren)."""

# ---------------------------------------------------------------------------
# Section 4: Sample Rows
# ---------------------------------------------------------------------------

SECTION_4_SAMPLES = """\
### Sample Rows

Eight representative rows (one per vehicle type) showing actual data shape:

```
listing_id | brand          | model               | trim                  | price    | depreciation | fuel_type | vehicle_type    | mileage_km | manufactured | age_years | coe_remaining_months | transmission | status    | value_score | owners | days_on_market | price_to_omv_ratio
708763     | Austin         | Princess            |                       | 139000   | 31630        | Petrol    | Classic/Others  | NULL       | 1948         | 78        | 50                   | Manual       | Available | 18.2        | 5      | 54             | NULL
1060155    | Honda          | E Electric Advance  |                       | 179888   | NULL         | Electric  | Hatchback       | 50         | NULL         | NULL      | NULL                 | Auto         | Available | 67.0        | 0      | 11             | 3.928
1048339    | Rolls-Royce    | Silver Spur         |                       | 109000   | 52700        | Petrol    | Luxury Sedan    | 2300       | 1983         | 43        | 24                   | Auto         | Available | 40.0        | 6      | 17             | NULL
1208978    | Mercedes-Benz  | R-Class R350L       |                       | 72800    | 14650        | Petrol    | MPV             | NULL       | 2011         | 15        | 59                   | Auto         | Available | 58.8        | 3      | 44             | 1.067
881872     | Dodge          | Journey             | 2.4A SXT              | 55800    | 12960        | Petrol    | SUV             | 99812      | 2008         | 18        | 51                   | Auto         | Available | 55.0        | 4      | 31             | 1.777
1175224    | Renault        | Fluence Diesel      | 1.5A dCi Sunroof      | 13800    | 15010        | Diesel    | Sedan           | NULL       | 2016         | 10        | NULL                 | Auto         | Available | 68.0        | 1      | 17             | 0.699
365241     | Rover          | 216 Cabriolet       |                       | 45800    | 15420        | Petrol    | Sports Car      | NULL       | 1993         | 33        | 35                   | Auto         | Available | 53.8        | 6      | 33             | 1.585
1341104    | Honda          | Shuttle             | 1.5A G Honda Sensing  | 83888    | 14950        | Petrol    | Stationwagon    | 38000      | 2021         | 5         | 65                   | Auto         | Available | 63.4        | 1      | 61             | 4.309
```"""

# ---------------------------------------------------------------------------
# Section 5: Domain Hints
# ---------------------------------------------------------------------------

SECTION_5_HINTS = """\
### Domain Hints

Rules and gotchas that help the LLM generate correct SQL:

```
=== GENERAL ===
- All monetary values are in Singapore Dollars (SGD). Do not convert currencies.
- price is the asking price, not the sale price.
- depreciation is annual depreciation in SGD/year, not total depreciation.
- This table contains only consumer vehicles. Commercial vehicles (vans, trucks,
  buses) are excluded during the Silver -> Gold transform.
- The table is rebuilt daily. Data is approximate current state, not historical.

=== COE (Certificate of Entitlement) ===
- Singapore's COE system is a quota licence that gives the right to own and drive
  a vehicle on Singapore roads for 10 years.
- COE is obtained through open bidding exercises run by LTA (Land Transport Authority).
  Bidding happens twice per month.
- COE is the SINGLE LARGEST cost component of car ownership in Singapore.
- Current COE prices (May 2026, approximate):
    Cat A (cars ≤1600cc & ≤97kW/130bhp, or EV ≤110kW): ~$124,790
    Cat B (cars >1600cc or >97kW/130bhp, or EV >110kW): ~$126,236
    Cat E (open category, can be used for any vehicle): similar to Cat B
- coe column: the COE premium paid when the car was first registered (SGD).
  This is a historical value -- COE prices fluctuate significantly over time.
- coe_remaining_months: months until the COE expires (range: 0–120).
  When COE expires, the car MUST be deregistered (scrapped/exported) OR the COE
  must be renewed for another 5 or 10 years at the prevailing COE price.
- COE renewal: car owners can renew COE for 5 or 10 years at the Prevailing Quota
  Premium (PQP), which is the moving average of COE prices over the last 3 months.
- "COE expiring soon" = coe_remaining_months < 24. Cars with low COE remaining
  are cheaper but closer to mandatory deregistration or expensive COE renewal.
- "COE car" or "PREP car": a used car with very low COE remaining (often <2 years),
  priced below its paper value because the buyer will soon face COE renewal costs.
- The `coe` column value may be NULL for older listings where COE data was not scraped.

=== OMV (Open Market Value) ===
- OMV is the customs-assessed value of the vehicle: cost + insurance + freight (CIF).
  It is the BASE value before any Singapore taxes are applied.
- OMV is assessed at the time of first registration and does not change over the
  car's lifetime.
- Typical OMV ranges: mass-market cars $15K-$30K, luxury cars $40K-$100K+,
  supercars $200K-$765K.

=== ARF (Additional Registration Fee) ===
- ARF is a PROGRESSIVE TAX on OMV, paid once at vehicle registration.
- Current ARF tiers (Budget 2023, still current):
    First $20,000 of OMV  →  100% tax →  $20,000 ARF
    Next  $30,000 ($20,001–$50,000)  →  140% tax →  $42,000 ARF
    Next  $30,000 ($50,001–$80,000)  →  180% tax →  $54,000 ARF
    Above $80,000  →  320% tax (raised from 220% in Budget 2023)
- Example: A car with $100,000 OMV → ARF = $20K + $42K + $54K + $64K = $180,000.
- For supercars (OMV >$500K), ARF can exceed $1.4 million.
- arf column range: $0 to $980,594 in this dataset.

=== PARF (Preferential Additional Registration Fee) Rebate ===
- When a car is deregistered BEFORE its COE expires, the owner receives a PARF
  rebate -- a percentage of the ARF paid, returned as cash.
- Budget 2026 (Feb 2026) SLASHED PARF rebates:
    Age ≤5 years at deregistration: 75% → 30% of ARF
    Age 9–10 years (end of COE): 50% → 5% of ARF
    PARF cap: $60,000 → $30,000
- This means the "paper value" (PARF + COE rebate) of a car at end-of-life is now
  much lower, making car ownership more expensive overall.

=== DEREG VALUE (Deregistration Value) ===
- dereg_value is the estimated value received when deregistering (scrapping) the car.
  It consists of: COE rebate (pro-rated) + PARF rebate (age-based % of ARF).
- This is the car's "paper value" -- the minimum the car is worth to the owner
  if they deregister it. Used car prices should generally be ABOVE dereg_value.
- price < dereg_value means the car is priced below its paper value (a potential deal).
- After Budget 2026 PARF cuts, dereg_value for newer cars will be significantly lower.

=== PRICE-TO-OMV RATIO ===
- price_to_omv_ratio = asking_price / omv.
- Values < 1.0: car costs less than its original import value (normal depreciation).
- Values 1.0–2.0: typical for recent models, COE cars, or desirable brands.
- Values > 2.0: car costs more than double its OMV -- usually due to high COE premiums,
  low mileage, or strong demand for that model.
- Values > 5.0: extreme -- usually COE renewal cars where the new COE cost far exceeds
  the car's OMV.

=== VALUE SCORE ===
- value_score is a percentile-based composite score (0-100) for Available listings.
  Higher = better value buy.
- Weighting: depreciation (30%), age (20%), mileage (20%), price-to-OMV (15%),
  COE remaining (15%).
- Only populated for status='Available'. NULL for Sold/Closed.

=== DATA QUALITY ===
- mileage_km has ~15% NULL rate (not all listings report mileage).
- vehicle_type has ~3.5% NULL (some listings lack type classification).
- Model names are partial matches. Use LIKE '%Corolla%' not = 'Corolla'.
- For "newest cars" or "recent models", filter by manufactured >= 2023.
- Use SQLite-compatible syntax. No PERCENTILE_CONT -- use subqueries for median.

=== SINGAPORE CAR COST CONTEXT ===
Total cost of a new car in Singapore = OMV + Excise Duty (20% of OMV)
    + ARF (100%-320% progressive on OMV) + GST (9% on all of the above)
    + COE (market price, currently ~$125K) + Registration Fee (~$220)
- This is why the average used car price ($137K) seems high -- Singapore is the most
  expensive country in the world to own a car.
- A "cheap" used car under $20K in Singapore is typically a very old car (15+ years)
  with low COE remaining.
```"""

# ---------------------------------------------------------------------------
# Column names (all 35, in DDL order)
# ---------------------------------------------------------------------------

_COLUMN_NAMES = [
    "listing_id",
    "brand",
    "model",
    "trim",
    "car_name",
    "price",
    "installment",
    "depreciation",
    "dereg_value",
    "manufactured",
    "age_years",
    "mileage_km",
    "engine_cap_cc",
    "transmission",
    "fuel_type",
    "power",
    "curb_weight",
    "reg_date",
    "coe",
    "coe_remaining_months",
    "road_tax",
    "omv",
    "arf",
    "vehicle_type",
    "listing_type",
    "owners",
    "days_on_market",
    "features",
    "accessories",
    "detail_url",
    "price_to_omv_ratio",
    "value_score",
    "status",
    "first_seen_at",
    "last_seen_at",
]

_ALLOWED_TABLES: set[str] = {"sgcarmart_business_table"}

# Separator used between sections
_SECTION_SEPARATOR = "\n\n---\n\n"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_schema_context() -> str:
    """Return the full schema context string (all 5 sections).

    This string is injected into the system prompt of both the Generator
    and Reviewer LLM agents.
    """
    return _SECTION_SEPARATOR.join([
        SECTION_1_DDL,
        SECTION_2_COLUMNS,
        SECTION_3_BRANDS,
        SECTION_4_SAMPLES,
        SECTION_5_HINTS,
    ])


def get_table_ddl() -> str:
    """Return just the CREATE TABLE DDL (Section 1 content)."""
    return """\
CREATE TABLE sgcarmart_business_table (
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
    last_seen_at        TEXT
);"""


def get_column_names() -> list[str]:
    """Return all 35 column names in DDL order."""
    return list(_COLUMN_NAMES)


def get_allowed_tables() -> set[str]:
    """Return the set of tables the NL2SQL system is permitted to query."""
    return set(_ALLOWED_TABLES)
