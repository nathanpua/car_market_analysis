# SGCarMart Used Cars Data Scraper

## Overview

Scrapes ~15,000 used car listings from sgcarmart.com using static HTTP requests via Scrapling. Extracts 28 fields per listing by parsing Next.js RSC payloads. Data is stored in SQLite with incremental re-scrape support.

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

## Quick Start

```bash
pip install -r requirements.txt

python scrape_listing.py scrape              # Phase 1: discover all URLs
python scrape_listing.py scrape --pages 3    # Test with first 3 pages
python scrape_listing.py scrape-details      # Phase 2: scrape all detail pages
python scrape_listing.py scrape-details --limit 100  # Test with 100 pages
python scrape_listing.py stats               # Show field coverage stats
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
| `scrape_listing.py` | Main scraper CLI (both phases) |
| `db.py` | SQLite database module (schema, upsert, queries) |
| `requirements.txt` | Python dependencies |
| `output/scrapling_listings.db` | SQLite database with all listing data |

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

## Prerequisites

- Python 3.11+
- No API keys or paid services required
