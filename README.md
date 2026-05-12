# SGCarMart Used Cars Data Pipeline

Scrapes ~15,000 used car listings from sgcarmart.com and transforms them into a business-ready consumer vehicle table. Built with a Medallion architecture (Bronze -> Silver -> Gold) on SQLite, with incremental re-scraping, field validation, SCD Type 2 history, and CI/CD via GitHub Actions + Cloudflare R2.

## Medallion Architecture

```
BRONZE (raw HTML)          SILVER (validated)              GOLD (business-ready)
+---------------+          +------------------+            +---------------------------+
| Listing pages  |  parse   | listings         | transform  | sgcarmart_business_table   |
| Detail pages   | ------>  | quarantine       | ---------> |   (consumer vehicles only) |
| RSC payloads   | validate | scrape_runs      |  enrich    |   brand/model/trim         |
+---------------+          +------------------+            +-----------+---------------+
                                                                     |
                                                          +----------v-----------+
                                                          | NL2SQL Chatbot       |
                                                          | (Claude API, planned)|
                                                          +----------------------+
```

### Pipeline (CI via `ci_run.py`)

```
                         Cloudflare R2
                    (scrapling_listings.db)
                         |       ^
                 download |       | upload
                         v       |
+----------------------------------------------------------------------+
|                      ci_run.py (orchestrator)                        |
|                                                                      |
|  Step 1         Step 2                 Step 3            Step 4      |
|  download_db -> run_daily_scrape  ->  run_transform ->  upload_db    |
|                 (Bronze -> Silver)    (Silver -> Gold)               |
+----------------------------------------------------------------------+
```

Both Silver and Gold live in the **same SQLite file** — single R2 download/upload. The Gold table is rebuilt on each run, always consistent with current Silver.

## Quick Start

```bash
pip install -r requirements.txt

# Scrape (Bronze -> Silver)
python scrape_listing.py scrape              # Phase 1: discover URLs
python scrape_listing.py scrape-details      # Phase 2: scrape all detail pages
python scrape_listing.py stats               # Show field coverage

# Transform (Silver -> Gold)
python transform.py                          # Run Silver-to-Gold transform

# Daily automation (all steps)
python scrape_listing.py run-daily           # Phase 1 + 2
python scrape_listing.py run-daily --track-history
python scrape_listing.py schedule            # Daemon: daily at 03:00 SGT
```

## Scraper Architecture

Two-phase approach using only static HTTP (no browser):

**Phase 1: URL Discovery** — Iterates listing pages (100/page), extracts listing IDs and detail URLs from Next.js RSC payloads. Incremental: skips already-seen listings, stops after 3 empty pages.

**Phase 2: Detail Scraping** — Fetches each detail page, parses 28 fields from RSC payloads. 10 concurrent async requests with retry logic. Optional field validation + quarantine. Optional SCD Type 2 history tracking.

## Silver -> Gold Transform

`transform.py` reads the Silver `listings` table and writes the Gold `sgcarmart_business_table`. It runs as Step 3 in `ci_run.py`, after scraping and before upload.

### What the transform does

1. **Filter** — Exclude commercial vehicles (vans, trucks, buses) by vehicle_type and brand
2. **Extract** — Parse brand, model, trim from `car_name` using brand-specific regex patterns
3. **Normalize** — Clean fuel_type (`Petrol-Electric` -> `Hybrid`), vehicle_type (`Mid-Sized Sedan` -> `Sedan`), status (`Available for sale` -> `Available`)
4. **Parse** — Convert `coe_remaining` text to months, `reg_date` to ISO format
5. **Compute** — Calculate `age_years`, `days_on_market`, `price_to_omv_ratio`
6. **Score** — Percentile-based `value_score` (0-100) weighing depreciation, age, mileage, price-to-OMV, COE remaining
7. **Lifecycle** — Detect Sold/Closed from silver, mark unseen listings as Delisted

### Brand/Model Extraction

Handles 90+ brands including multi-word brands (Mercedes-Benz, Land Rover, Aston Martin, Alfa Romeo, Rolls-Royce). Brand-specific regex patterns for Mercedes (-Class, EQ, AMG GT), BMW (Series, M, X, iX), Tesla, Lexus, Volvo, BYD, Porsche, and Lamborghini.

```
"Mercedes-Benz GLB-Class GLB180 Progressive"
  -> brand: Mercedes-Benz, model: GLB-Class, trim: GLB180 Progressive

"BMW X3 sDrive20i xLine"
  -> brand: BMW, model: X3, trim: sDrive20i xLine

"Volvo XC60 Recharge Plug-in Hybrid T8 Plus"
  -> brand: Volvo, model: XC60, trim: Recharge Plug-in Hybrid T8 Plus
```

### Data Profile (~15,000 Silver listings)

- **Consumer vehicles**: ~13,300 after excluding 1,900 commercial (12.5%)
- **Top brands**: Toyota (2,334), Mercedes-Benz (2,183), BMW (1,734), Honda (1,486)
- **Vehicle types**: SUV (3,503), Luxury Sedan (2,580), Sports Car (2,305), MPV (1,519)
- **Fuel types**: Petrol (10,713), Hybrid (1,925), Diesel (1,763), Electric (834)
- **Price range**: $1,500 - $2,788,000, avg $128,713

### Enum Mappings

| Silver | Gold |
|--------|------|
| Petrol-Electric | Hybrid |
| Mid-Sized Sedan | Sedan |
| Available for sale | Available |
| SOLD | Sold |
| Diesel (Euro 5 Engine and Above) | Diesel |

## Data Model

### Silver Tables

**`listings`** — 28 columns, upserted by listing_id. All scraped fields in raw form.

**`listings_history`** (SCD Type 2) — All 28 columns + `history_id`, `valid_from`, `valid_to`, `is_current`, `scrape_run_id`.

**`quarantine`** — Validation failures: `quarantine_id`, `listing_id`, `field_name`, `field_value`, `rule_name`, `reason`, `quarantined_at`, `resolved`.

**`scrape_runs`** — Execution tracking: `id`, `started_at`, `finished_at`, `pages_fetched`, `listings_count`, `status`.

### Gold Table

**`sgcarmart_business_table`** — Consumer vehicles only, 35 columns:

| Category | Fields |
|----------|--------|
| Identity | listing_id, brand, model, trim, car_name, detail_url |
| Pricing | price, installment, depreciation, dereg_value, price_to_omv_ratio, value_score |
| Specs | manufactured, age_years, mileage_km, engine_cap_cc, transmission, fuel_type, power, curb_weight |
| Registration | reg_date, coe, coe_remaining_months, road_tax, omv, arf |
| Classification | vehicle_type (clean enum), listing_type, owners |
| Metadata | days_on_market, features, accessories |
| Lifecycle | status (Available/Sold/Closed/Reserved/Delisted), first_seen_at, last_seen_at |

Indexed on brand, model, price, fuel_type, vehicle_type, status, depreciation, value_score.

## Project Structure

| File | Purpose |
|------|---------|
| `scrape_listing.py` | Main scraper CLI (Bronze -> Silver) |
| `transform.py` | Silver-to-Gold transformation |
| `ci_run.py` | CI orchestrator (download, scrape, transform, upload) |
| `db.py` | SQLite database module (Silver schema, upsert, queries, quarantine) |
| `db_scd.py` | SCD Type 2 history tracking |
| `validators.py` | Field validation rules |
| `scheduler.py` | APScheduler daily cron |
| `storage.py` | Cloudflare R2 upload/download |
| `plans/` | Implementation plans |
| `tests/` | Unit and integration tests (204 tests) |

## Key Technical Decisions

### Static HTTP over Browser Rendering

Scrapling's `AsyncFetcher` (curl_cffi backend) fetches pages as static HTTP. SGCarMart serves all data in Next.js RSC payloads, making browser rendering unnecessary.

### RSC Payload Parsing

Next.js RSC payloads embed listing data in `<script>` tags. The scraper uses regex with next-field lookahead to handle multi-level JSON escaping. Some fields (like `type_of_vehicle`) are RSC references (e.g., `$15f` pointing to a chunk like `15f:{"text":"SUV","link":"..."}`) — these are resolved by looking up the referenced chunk and extracting the `text` value.

### SQLite Storage

SQLite provides ACID transactions, efficient upsert by listing_id, and query capability. Both Silver and Gold tables live in one file. COALESCE-based upsert preserves existing non-null values during re-scrapes.

### Incremental Re-scraping

Both scraper phases are incremental: Phase 1 skips seen listing IDs, Phase 2 skips listings with complete detail data. Failed requests retry up to 3 times.

### Field Validation

28 fields validated before database write: range checks (price, mileage, engine_cap), enum checks (transmission, fuel_type), regex checks (reg_date, coe_remaining), and pass-through (features, accessories). Invalid fields set to None, failures logged to `quarantine` table.

### SCD Type 2 History

All listing changes tracked over time with `valid_from`/`valid_to`/`is_current` columns. NULL-aware comparison with epsilon for floats. Enabled with `--track-history`.

### Gold Table Rebuild

The Gold table is rebuilt on each transform run (not incremental). Lifecycle state (Sold, Delisted) is preserved by comparing against the previous Gold table before rebuild. This keeps Gold always consistent with Silver.

## Prerequisites

- Python 3.11+
- No API keys required for scraping
