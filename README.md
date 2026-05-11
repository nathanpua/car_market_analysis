# SGCarMart Used Cars Data Scraper

## Overview

Scrapes ~15,000 used car listings from sgcarmart.com using static HTTP requests via Scrapling. Extracts 28 fields per listing by parsing Next.js RSC payloads. Data is stored in SQLite with incremental re-scrape support, field validation, SCD Type 2 history tracking, and daily scheduled scraping.

## Architecture

Two-phase approach using only static HTTP (no browser required):

**Phase 1: URL Discovery** (`scrape` command)
- Iterates listing pages (100 results/page) to extract detail page URLs
- Parses listing IDs and detail URLs from Next.js RSC payloads
- Stores in SQLite; incremental re-runs skip already-seen listings
- Early termination after 3 consecutive pages with no new URLs

**Phase 2: Detail Scraping** (`scrape-details` command)
- Fetches each detail page's static HTML
- Parses all 28 fields from Next.js RSC payloads embedded in the page
- 10 concurrent async requests with retry logic
- Incremental — skips listings that already have complete detail data
- Optional field validation with quarantine for invalid values
- Optional SCD Type 2 history tracking for all changes

## Quick Start

```bash
pip install -r requirements.txt

python scrape_listing.py scrape              # Phase 1: discover all URLs
python scrape_listing.py scrape --pages 3    # Test with first 3 pages
python scrape_listing.py scrape-details      # Phase 2: scrape all detail pages
python scrape_listing.py scrape-details --limit 100  # Test with 100 pages
python scrape_listing.py scrape-details --no-validate # Disable validation
python scrape_listing.py scrape-details --track-history # Enable SCD tracking
python scrape_listing.py stats               # Show field coverage stats
```

## Daily Automation

```bash
# One-shot: run Phase 1 + Phase 2 immediately
python scrape_listing.py run-daily
python scrape_listing.py run-daily --track-history

# Daemon: runs daily at 03:00 (Asia/Singapore) via APScheduler
python scrape_listing.py schedule
python scrape_listing.py schedule --time 06:00 --track-history
```

## History Tracking

```bash
# View change history for a specific listing
python scrape_listing.py history 12345
python scrape_listing.py history 12345 --limit 50
```

## Data Fields (28 columns)

| Category | Fields |
|----------|--------|
| Identity | listing_id, car_name, car_model, detail_url, listing_type |
| Financial | price, installment, depreciation, omv, arf, coe, road_tax, dereg_value |
| Technical | engine_cap_cc, fuel_type, power, transmission, curb_weight |
| History | reg_date, coe_remaining, mileage_km, owners, manufactured, posted_date |
| Metadata | status, vehicle_type, features, accessories |

## Project Structure

| File | Purpose |
|------|---------|
| `scrape_listing.py` | Main scraper CLI (both phases + scheduling commands) |
| `db.py` | SQLite database module (schema, upsert, queries, quarantine) |
| `validators.py` | Field validation rules with quarantine support |
| `db_scd.py` | SCD Type 2 history tracking (change detection, history writes) |
| `scheduler.py` | APScheduler daily cron + run-daily logic |
| `requirements.txt` | Python dependencies |
| `output/scrapling_listings.db` | SQLite database with all listing data |
| `tests/` | Unit and integration tests |

## Key Technical Decisions

### 1. Static HTTP over Browser Rendering

Scrapling's `AsyncFetcher` (curl_cffi backend) fetches pages as static HTTP. No browser overhead. The site serves all data in Next.js RSC payloads embedded in the HTML, making browser rendering unnecessary.

### 2. RSC Payload Parsing

SGCarMart is a Next.js app that embeds all listing data in React Server Component payloads within the HTML. The scraper extracts these payloads and parses field values using regex with next-field lookahead to handle multi-level JSON escaping (e.g., `19"` rims in accessories).

### 3. SQLite Storage

SQLite provides ACID transactions, efficient upsert by listing_id, and query capability without loading the full dataset. The COALESCE-based upsert preserves existing non-null values during re-scrapes.

### 4. Incremental Re-scraping

Both phases are incremental:
- Phase 1 tracks seen listing IDs and skips pages with no new listings
- Phase 2 skips listings that already have complete detail data (non-null price and accessories)
- Failed requests are retried up to 3 times

### 5. Field Validation

Each of the 28 fields is validated before writing to the database:
- **Range checks**: price (1–20M), mileage (0–1M km), engine_cap (1–10K cc), etc.
- **Enum checks**: transmission (Auto/Manual), fuel_type, listing_type, status
- **Regex checks**: reg_date format, coe_remaining format, detail_url prefix
- **Pass-through**: features, accessories (text blobs)
- **Design**: NULL = valid (missing data is not a failure); only present-but-wrong values fail
- Invalid fields are set to None in the record; failures are stored in the `quarantine` table with reason, rule name, and raw value for review
- `--no-validate` flag disables validation when needed

### 6. SCD Type 2 History Tracking

All listing changes are tracked over time using `listings_history` table:
- `valid_from` / `valid_to` / `is_current` columns for each version
- Change detection: NULL-aware comparison with epsilon for float fields (power)
- NULL incoming = no change (COALESCE logic: existing data preserved)
- New listings get an initial history row; changed listings close old row + insert new
- Use `--track-history` flag to enable

### 7. APScheduler over Cron

Pure Python, cross-platform, cron syntax, timezone-aware (Asia/Singapore), no external daemon required. `max_instances=1` prevents concurrent runs.

## Data Model

### `listings` (current state)
All 28 columns, upserted by listing_id.

### `listings_history` (SCD Type 2)
All 28 columns + `history_id` (PK), `valid_from`, `valid_to`, `is_current`, `scrape_run_id`. Indexed on `(listing_id, is_current)` and `(listing_id, valid_from)`.

### `quarantine` (validation failures)
`quarantine_id`, `listing_id`, `field_name`, `field_value`, `rule_name`, `reason`, `raw_record`, `quarantined_at`, `scrape_run_id`, `resolved`.

### `scrape_runs` (execution tracking)
`id`, `started_at`, `finished_at`, `pages_fetched`, `listings_count`, `status`.

## Prerequisites

- Python 3.11+
- No API keys or paid services required
