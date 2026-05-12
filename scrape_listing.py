"""
SGCarMart Used Cars Listing Scraper using Scrapling.

Two-phase architecture — both phases use static HTTP (AsyncFetcher):
  Phase 1: Discover all listing URLs by iterating listing pages
  Phase 2: Scrape detail pages for ALL data via RSC payload parsing

Phase 1 extracts only listing_id and detail_url from listing pages.
Phase 2 fetches each detail page's static HTML and parses ALL remaining
columns (price, depreciation, mileage, features, etc.) from Next.js RSC payloads.

Setup:
  pip install -r requirements.txt

Usage:
  python scrape_listing.py scrape              # Phase 1: discover all URLs
  python scrape_listing.py scrape --pages 3    # Discover first 3 pages only
  python scrape_listing.py scrape-details      # Phase 2: scrape all detail pages
  python scrape_listing.py scrape-details --limit 100  # Test with 100 pages
  python scrape_listing.py scrape-details --validate   # Enable field validation (default)
  python scrape_listing.py scrape-details --no-validate # Disable field validation
  python scrape_listing.py stats               # Show field coverage stats
  python scrape_listing.py run-daily           # One-shot: Phase 1 + Phase 2
  python scrape_listing.py schedule            # Daemon: daily cron (Asia/Singapore)
  python scrape_listing.py history LISTING_ID  # Show listing history
"""

import argparse
import asyncio
import logging
import os
import re
from datetime import datetime
from pathlib import Path

from tqdm import tqdm

from db import ListingDB, QuarantineDB
from db_scd import ListingHistoryDB
from validators import ListingValidator

logger = logging.getLogger(__name__)

# --- Configuration ---
BASE_URL = "https://www.sgcarmart.com/used-cars/listing"
RESULTS_PER_PAGE = 100
MAX_EMPTY_PAGES = 3
CHECKPOINT_EVERY_N = 5
REQUEST_DELAY = 0.3  # seconds between requests (polite crawling)
MAX_CONCURRENT = 10  # max parallel detail page fetches
MAX_RETRIES = 3      # retries per failed request

OUTPUT_DIR = Path("output")
DB_PATH = OUTPUT_DIR / "scrapling_listings.db"

# Proxy support (optional, set SCRAPER_PROXY env var)
# Format: http://username:password@host:port
_raw_proxy = os.environ.get("SCRAPER_PROXY", "").strip()
PROXY_URL = _raw_proxy or None
if PROXY_URL:
    from urllib.parse import urlparse
    _parsed = urlparse(PROXY_URL)
    if not _parsed.scheme or not _parsed.hostname:
        logger.warning("SCRAPER_PROXY format invalid, expected http://user:pass@host:port")
        PROXY_URL = None
    else:
        logger.info("Proxy configured: %s://%s:%d", _parsed.scheme, _parsed.hostname, _parsed.port or 0)


# ============================================================
# SHARED: RSC payload extraction
# ============================================================

_RSC_PATTERN = re.compile(r'self\.__next_f\.push\(\[1,"(.*?)"\]\)', re.DOTALL)


def _extract_rsc_payloads(html: str) -> str:
    """Concatenate all Next.js RSC push payloads from HTML.

    Returns raw chunk content without unescaping — the field extraction
    regex handles the multi-level JSON escaping directly.
    """
    chunks = []
    for m in _RSC_PATTERN.finditer(html):
        chunks.append(m.group(1))
    return '\n'.join(chunks)


# ============================================================
# PHASE 1: LISTING PAGE PARSER (URL discovery via RSC)
# ============================================================

# Regex to extract listing card entries from raw RSC (uses \\" for JSON quotes)
_LISTING_ID_RE = re.compile(
    r'\\"id\\":(\d+),\\"link\\":\\"https://www\.sgcarmart\.com(/used-cars/info/[^\\"?]+)'
)


def parse_listing_html(html: str) -> list[dict]:
    """Parse a listing page's static HTML to extract listing URLs only.

    Extracts listing data from Next.js RSC payloads (no JS rendering needed).
    Returns list of {listing_id, detail_url} dicts.
    All other fields (car_name, price, etc.) are populated by Phase 2.
    """
    full_rsc = _extract_rsc_payloads(html)

    unique: dict[int, str] = {}
    for m in _LISTING_ID_RE.finditer(full_rsc):
        lid = int(m.group(1))
        if lid not in unique:
            url = m.group(2).replace('\n', '').replace('\r', '').rstrip('/')
            unique[lid] = url

    return [
        {'listing_id': lid, 'detail_url': url}
        for lid, url in unique.items()
    ]


# ============================================================
# PHASE 2: DETAIL PAGE PARSER (RSC-based, static HTML)
# ============================================================

def _parse_monetary(raw: str) -> int | None:
    """Strip $$, commas, '/yr', ' as of today' -> int."""
    if not raw:
        return None
    cleaned = raw.replace('$', '').replace(',', '').replace('/yr', '')
    cleaned = cleaned.split(' as of')[0].strip()
    if cleaned in ('N.A.', 'N.A', ''):
        return None
    try:
        return int(float(cleaned))
    except (ValueError, TypeError):
        logger.debug("Could not parse monetary value: %r", raw)
        return None


def _parse_int(raw: str) -> int | None:
    """Strip non-numeric and convert to int. Takes text before any parenthetical."""
    if not raw:
        return None
    raw = raw.split('(')[0].strip()
    raw = re.sub(r'\s*(km|cc|kg|kW)\s*$', '', raw, flags=re.IGNORECASE)
    cleaned = re.sub(r'[^\d]', '', raw)
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except (ValueError, TypeError):
        logger.debug("Could not parse int value: %r", raw)
        return None


def _parse_float(raw: str) -> float | None:
    """Extract float (e.g. '92 kW' -> 92.0)."""
    if not raw:
        return None
    m = re.search(r'([\d.]+)', raw)
    if m:
        try:
            return float(m.group(1))
        except ValueError:
            return None
    return None


def _parse_coe_remaining(raw: str) -> str | None:
    """Parse COE remaining string like '1yr 5mths 28days COE left'."""
    if not raw:
        return None
    m = re.search(r'(\d+)y(?:rs?)?\s*(\d+)m(?:ths?)?', raw)
    if m:
        return f"{m.group(1)}y {m.group(2)}m"
    m = re.search(r'(\d+)y(?:rs?)?', raw)
    if m:
        return f"{m.group(1)}y"
    if 'New 5-yr' in raw:
        return '5y (renewed)'
    if 'New 10-yr' in raw:
        return '10y (renewed)'
    return None


def _extract_rsc_field(payload: str, field: str) -> str | None:
    """Extract a field value from the raw RSC data payload via regex.

    Raw RSC encoding (JSON-in-JS-string, 2 levels):
      \\"          = JSON string delimiter (field boundary) — 1 bs + quote
      \\\"         = escaped quote in value (literal ") — 3 bs + quote
      \\\\         = escaped backslash in value (literal \\) — 2 bs
      \\n          = newline in value — bs + n
    We use the next-field lookahead \\",\\" to find value boundaries,
    then unescape the captured content.
    """
    # Match value using next-field lookahead: \\"field\\":\\"value\\",\\"
    m = re.search(rf'\\"{field}\\":\\"(.*?)\\",\\"', payload)
    if m:
        val = m.group(1)
        # Unescape: 3-bs+quote -> ", 2-bs -> \, bs+n -> newline
        val = val.replace('\\' * 3 + '"', '"')
        val = val.replace('\\' * 2, '\\')
        val = val.replace('\\n', '\n')
        return val
    # Try value at end of object (last field before }): \\"field\\":\\"value\\"}
    m = re.search(rf'\\"{field}\\":\\"(.*?)\\"}}', payload)
    if m:
        val = m.group(1)
        val = val.replace('\\' * 3 + '"', '"')
        val = val.replace('\\' * 2, '\\')
        val = val.replace('\\n', '\n')
        return val
    # Numeric value: \\"field\\":1234
    m = re.search(rf'\\"{field}\\":(\d+)', payload)
    if m:
        return m.group(1)
    # Nested object: \\"field\\":{\\"text\\":\\"value\\"...}
    m = re.search(rf'\\"{field}\\":\{{\\"text\\":\\"(.*?)\\"', payload)
    if m:
        val = m.group(1)
        val = val.replace('\\' * 3 + '"', '"')
        val = val.replace('\\' * 2, '\\')
        val = val.replace('\\n', '\n')
        return val
    return None


_RSC_DETAIL_FIELDS = [
    'price', 'installment', 'depreciation', 'reg_date',
    'coe_left', 'mileage', 'engine_cap', 'road_tax',
    'dereg_value', 'coe', 'omv', 'arf', 'power',
    'curb_weight', 'owners', 'manufactured',
    'transmission', 'fuel_type', 'features',
    'accessories', 'status', 'posted_on', 'is_direct_owner',
]


def parse_detail_html(html: str) -> dict:
    """Extract ALL fields from static HTML of a car detail page.

    Parses Next.js RSC payloads embedded in <script> tags.
    Works with Fetcher/AsyncFetcher (no browser needed).

    Returns a dict with both listing-level and detail-level DB columns.
    """
    full_rsc = _extract_rsc_payloads(html)

    # Find the structured data line — prefer one with price (full data),
    # fallback to any line with road_tax + features.
    # RSC pushes duplicate data across chunks; some have price, some don't.
    # Field names appear as \\"field\\" in raw RSC encoding.
    data_line = None
    fallback_line = None
    for line in full_rsc.split('\n'):
        if 'road_tax' in line and 'features' in line:
            m = re.match(r'[0-9a-f]+:(.*)', line.strip(), re.DOTALL)
            if m:
                candidate = m.group(1)
                if 'price' in candidate:
                    data_line = candidate
                    break
                elif fallback_line is None:
                    fallback_line = candidate
    if not data_line:
        data_line = fallback_line

    if not data_line:
        return {}

    # Extract raw values for all fields
    raw: dict[str, str | None] = {}
    for field in _RSC_DETAIL_FIELDS:
        val = _extract_rsc_field(data_line, field)
        if val is not None:
            raw[field] = val

    # type_of_vehicle: nested object {\"text\":\"SUV\",...}
    tov = _extract_rsc_field(data_line, 'type_of_vehicle')
    if tov:
        raw['type_of_vehicle'] = tov

    # car_model from title tag (fallback)
    m = re.search(r'<title>Used \d+ (.*?) for Sale', html)
    if m:
        raw['car_model_title'] = m.group(1).strip()

    # Also extract 'carmodel' from RSC (cleaner than title)
    carmodel = _extract_rsc_field(data_line, 'carmodel')
    if carmodel:
        raw['car_model'] = carmodel

    # Map to DB columns with type parsing
    result: dict = {}

    # --- Listing-level fields ---
    if 'price' in raw:
        result['price'] = _parse_monetary(raw['price'])
    if 'installment' in raw:
        result['installment'] = _parse_monetary(raw['installment'])
    if 'depreciation' in raw:
        result['depreciation'] = _parse_monetary(raw['depreciation'])
    if 'reg_date' in raw:
        result['reg_date'] = raw['reg_date']
    if 'coe_left' in raw:
        result['coe_remaining'] = _parse_coe_remaining(raw['coe_left'])
    if 'posted_on' in raw:
        result['posted_date'] = raw['posted_on']
    if 'is_direct_owner' in raw:
        val = raw['is_direct_owner']
        if val == '1':
            result['listing_type'] = 'Direct Owner'
        else:
            result['listing_type'] = 'Dealer'

    # --- Detail-level fields ---
    # car_name and car_model both come from the detail page
    if 'car_model' in raw:
        result['car_name'] = raw['car_model']
        result['car_model'] = raw['car_model']
    elif 'car_model_title' in raw:
        result['car_name'] = raw['car_model_title']
        result['car_model'] = raw['car_model_title']

    if 'road_tax' in raw:
        result['road_tax'] = _parse_monetary(raw['road_tax'])
    if 'omv' in raw:
        result['omv'] = _parse_monetary(raw['omv'])
    if 'arf' in raw:
        result['arf'] = _parse_monetary(raw['arf'])
    if 'coe' in raw:
        result['coe'] = _parse_monetary(raw['coe'])
    if 'dereg_value' in raw:
        result['dereg_value'] = _parse_monetary(raw['dereg_value'])
    if 'power' in raw:
        result['power'] = _parse_float(raw['power'])
    if 'curb_weight' in raw:
        result['curb_weight'] = _parse_int(raw['curb_weight'])
    if 'mileage' in raw:
        result['mileage_km'] = _parse_int(raw['mileage'])
    if 'engine_cap' in raw:
        result['engine_cap_cc'] = _parse_int(raw['engine_cap'])
    if 'owners' in raw:
        result['owners'] = _parse_int(raw['owners'])
    if 'manufactured' in raw:
        result['manufactured'] = _parse_int(raw['manufactured'])
    if 'transmission' in raw:
        result['transmission'] = raw['transmission'] or None
    if 'fuel_type' in raw:
        result['fuel_type'] = raw['fuel_type'] or None
    if 'type_of_vehicle' in raw:
        result['vehicle_type'] = raw['type_of_vehicle'] or None
    if 'status' in raw:
        result['status'] = raw['status'] or None
    if 'features' in raw:
        cleaned = re.split(r'View specs of the', raw['features'])[0].strip()
        if cleaned:
            result['features'] = cleaned
    if 'accessories' in raw:
        val = raw['accessories'].strip()
        if val:
            result['accessories'] = val

    return result


# ============================================================
# SHARED: Async fetch with retry + rate limiting
# ============================================================

_semaphore = asyncio.Semaphore(MAX_CONCURRENT)
_last_request_time: float = 0.0


async def _fetch_with_retry(url: str) -> tuple[bool, str]:
    """Fetch a URL with retry and rate limiting. Returns (success, html)."""
    from scrapling.fetchers import AsyncFetcher

    global _last_request_time

    for attempt in range(MAX_RETRIES + 1):
        # Rate limit: ensure minimum delay between requests
        now = asyncio.get_event_loop().time()
        elapsed = now - _last_request_time
        if elapsed < REQUEST_DELAY:
            await asyncio.sleep(REQUEST_DELAY - elapsed)

        try:
            page = await AsyncFetcher.get(url, proxy=PROXY_URL)
            _last_request_time = asyncio.get_event_loop().time()

            if page.status == 200:
                return True, page.html_content
            if page.status in (429, 403):
                wait = 2 ** (attempt + 1)
                logger.warning("Rate limited (%d), waiting %ds: %s", page.status, wait, url)
                await asyncio.sleep(wait)
                continue
            if page.status >= 500:
                logger.warning("Server error %d (attempt %d): %s", page.status, attempt + 1, url)
                await asyncio.sleep(1)
                continue
            # 4xx other than 429: don't retry
            return False, ""

        except Exception as e:
            logger.warning("Fetch failed (attempt %d) %s: %s", attempt + 1, url, e)
            if attempt < MAX_RETRIES:
                await asyncio.sleep(1)

    return False, ""


# ============================================================
# PHASE 1: LISTING PAGE SCRAPER (URL discovery, async)
# ============================================================

async def _fetch_listing_page(page_num: int) -> tuple[int, list[dict] | None]:
    """Fetch and parse a single listing page via static HTTP."""
    url = (f"{BASE_URL}?limit={RESULTS_PER_PAGE}"
           if page_num == 1
           else f"{BASE_URL}?limit={RESULTS_PER_PAGE}&page={page_num}")
    success, html = await _fetch_with_retry(url)
    if not success:
        return page_num, None
    return page_num, parse_listing_html(html)


async def _run_scrape(max_pages: int | None, start_page: int = 1):
    """Async implementation of Phase 1 URL discovery."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with ListingDB(DB_PATH) as db:
        seen_ids = db.get_seen_ids()
        new_count = 0
        pages_with_no_new = 0
        consecutive_failures = 0
        pages_fetched = 0
        pages_failed = 0

        run_id = db.start_run()

        if start_page > 1 and seen_ids:
            print(f"Resumed with {len(seen_ids)} existing listings in DB")

        effective_max = max_pages or 999
        end_page = start_page + effective_max - 1
        print(f"Phase 1: Discovering URLs (pages {start_page}-{end_page})")
        print(f"URL: {BASE_URL}?limit={RESULTS_PER_PAGE}&page=N\n")

        start_time = datetime.now()
        failed = False

        try:
            page_nums = list(range(start_page, start_page + effective_max))
            with tqdm(page_nums, unit="page") as pbar:
                for page_num in pbar:
                    _pg, listings = await _fetch_listing_page(page_num)

                    if listings is None:
                        # Fetch failure (network/server error) — separate counter
                        pages_failed += 1
                        consecutive_failures += 1
                        if consecutive_failures >= MAX_EMPTY_PAGES:
                            pbar.write(f"Early stop: {MAX_EMPTY_PAGES} consecutive failures")
                            break
                        continue

                    # Reset failure counter on successful fetch
                    consecutive_failures = 0

                    if not listings:
                        pages_with_no_new += 1
                        if pages_with_no_new >= MAX_EMPTY_PAGES:
                            pbar.write(f"Early stop: {MAX_EMPTY_PAGES} empty pages")
                            break
                        continue

                    # Add only new listings
                    batch = [
                        car for car in listings
                        if car.get('listing_id') and car['listing_id'] not in seen_ids
                    ]
                    for car in batch:
                        seen_ids.add(car['listing_id'])

                    if batch:
                        db.upsert_listings(batch)
                        new_count += len(batch)

                    pages_with_no_new = 0 if batch else pages_with_no_new + 1
                    pages_fetched += 1
                    total = db.get_count()
                    elapsed = (datetime.now() - start_time).total_seconds()
                    speed = pages_fetched / max(elapsed / 60, 0.01)
                    pbar.set_postfix(new=new_count, total=total, speed=f"{speed:.0f}/m")

                    if pages_fetched > 0 and pages_fetched % CHECKPOINT_EVERY_N == 0:
                        pbar.write(f"  >> Checkpoint: {total} URLs discovered")

        except BaseException as e:
            logger.error("Phase 1 aborted: %s", e)
            failed = True
            raise
        finally:
            if failed:
                db.finish_run(run_id, pages_fetched, new_count, status="failed")
            else:
                total = db.get_count()
                db.finish_run(run_id, pages_fetched, total)

                elapsed = (datetime.now() - start_time).total_seconds()
                print(f"\n{'=' * 50}")
                print(f"  PHASE 1 COMPLETE: URL DISCOVERY")
                print(f"{'=' * 50}")
                print(f"  Pages fetched:  {pages_fetched}")
                print(f"  Pages failed:   {pages_failed}")
                print(f"  New URLs:       {new_count}")
                print(f"  DB total:       {total}")
                print(f"  Elapsed:        {elapsed:.0f}s ({elapsed / 60:.1f} min)")
                if pages_fetched > 0:
                    print(f"  Speed:          {pages_fetched / max(elapsed / 60, 0.01):.1f} pages/min")
                print(f"  Output:         {DB_PATH}")
                print(f"\n  Next: python scrape_listing.py scrape-details")


def scrape(max_pages: int | None, start_page: int = 1):
    """Phase 1: Discover listing URLs by scraping listing pages."""
    asyncio.run(_run_scrape(max_pages, start_page))


# ============================================================
# PHASE 2: DETAIL PAGE SCRAPER (async + batched concurrency)
# ============================================================

DETAIL_BATCH_SIZE = MAX_CONCURRENT
DETAIL_CHECKPOINT_EVERY = 100


async def _fetch_detail_batch(
    batch: list[tuple[int, str]],
) -> list[tuple[int, dict | None]]:
    """Fetch a batch of detail pages concurrently with rate limiting."""
    async def fetch_one(listing_id: int, url: str) -> tuple[int, dict | None]:
        async with _semaphore:
            success, html = await _fetch_with_retry(url)
            if not success:
                return listing_id, None
            detail = parse_detail_html(html)
            detail["listing_id"] = listing_id
            return listing_id, detail

    return await asyncio.gather(*[fetch_one(lid, url) for lid, url in batch])


async def _run_scrape_details(limit: int | None = None, validate: bool = True,
                              track_history: bool = False):
    """Async implementation of Phase 2 detail page scraping."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    with ListingDB(DB_PATH) as db:
        quarantine_db = QuarantineDB(db) if validate else None
        validator = ListingValidator() if validate else None
        scd = ListingHistoryDB(db) if track_history else None

        # When SCD is enabled, re-fetch both missing and active listings
        # to detect price/depreciation changes
        if scd:
            missing = db.get_listings_missing_details(limit=None)
            active = db.get_active_listings_for_rescrape()
            seen_ids: set[int] = set()
            pending: list[tuple[int, str]] = []
            for lid, url in missing + active:
                if lid not in seen_ids:
                    seen_ids.add(lid)
                    pending.append((lid, url))
            if limit:
                pending = pending[:limit]
        else:
            pending = db.get_listings_missing_details(limit=limit)
        total_pending = len(pending)
        if total_pending == 0:
            print("All listings already have detail data.")
            return

        # Normalize URLs
        tasks = []
        for listing_id, detail_url in pending:
            url = detail_url
            if not url.startswith("http"):
                url = f"https://www.sgcarmart.com{detail_url}"
            tasks.append((listing_id, url))

        print(f"Phase 2: Scraping detail pages for {total_pending} listings")
        print(f"Concurrency: {DETAIL_BATCH_SIZE} | Delay: {REQUEST_DELAY}s | Retries: {MAX_RETRIES}")
        if validate:
            print(f"Validation: ON (invalid fields quarantined)")
        if track_history:
            print(f"History: SCD Type 2 tracking enabled")
        print(f"Output: {DB_PATH}\n")

        start_time = datetime.now()
        fetched = 0
        failed = 0
        quarantine_count = 0

        try:
            with tqdm(total=total_pending, unit="page") as pbar:
                for i in range(0, len(tasks), DETAIL_BATCH_SIZE):
                    batch = tasks[i:i + DETAIL_BATCH_SIZE]
                    results = await _fetch_detail_batch(batch)

                    # Process successful results
                    batch_ok = []
                    for listing_id, detail in results:
                        if detail is not None:
                            if validator and quarantine_db:
                                result = validator.validate(detail)
                                quarantine_count += len(result.failures)
                                if result.failures:
                                    dicts = ListingValidator.failures_to_dicts(result.failures)
                                    quarantine_db.insert_failures(dicts)
                                batch_ok.append(result.cleaned)
                            else:
                                batch_ok.append(detail)
                            fetched += 1
                        else:
                            failed += 1

                    if batch_ok:
                        if scd:
                            counts = scd.upsert_with_history(batch_ok)
                            pbar.set_postfix(
                                ok=fetched, err=failed, q=quarantine_count,
                                new=counts["new"], chg=counts["changed"],
                            )
                        else:
                            db.upsert_listings(batch_ok)
                            pbar.set_postfix(ok=fetched, err=failed, q=quarantine_count)

                    pbar.update(len(batch))

                    if fetched > 0 and fetched % DETAIL_CHECKPOINT_EVERY == 0:
                        remaining = db.count_missing_details()
                        pbar.write(
                            f"  >> Checkpoint: {fetched} fetched, "
                            f"{remaining} remaining"
                        )

        except BaseException as e:
            logger.error("Phase 2 aborted: %s", e)
            raise
        finally:
            elapsed = (datetime.now() - start_time).total_seconds()
            remaining = db.count_missing_details()

            print(f"\n{'=' * 50}")
            print(f"  PHASE 2 COMPLETE: DETAIL SCRAPE")
            print(f"{'=' * 50}")
            print(f"  Fetched:    {fetched}")
            print(f"  Failed:     {failed}")
            print(f"  Remaining:  {remaining}")
            if validate:
                print(f"  Quarantined fields: {quarantine_count}")
            print(f"  Elapsed:    {elapsed:.0f}s ({elapsed / 60:.1f} min)")
            if fetched > 0:
                speed = fetched / max(elapsed / 60, 0.01)
                print(f"  Speed:      {speed:.1f} pages/min ({elapsed/fetched:.2f}s/page)")

            print_stats_from_db(db)


def scrape_details(limit: int | None = None, validate: bool = True,
                   track_history: bool = False):
    """Phase 2: Scrape detail pages for ALL data."""
    asyncio.run(_run_scrape_details(
        limit=limit, validate=validate, track_history=track_history,
    ))


def print_stats_from_db(db: ListingDB):
    """Print field coverage stats from the database via SQL aggregation."""
    coverage = db.get_field_coverage()
    if not coverage:
        return
    total = coverage[0][2]
    print(f"\n--- Field Coverage ({total} listings) ---")
    for field, non_null, _ in coverage:
        pct = non_null / total * 100
        print(f"  {field:20s}: {non_null:5d}/{total} ({pct:.0f}%)")

    price_stats = db.get_price_stats()
    if price_stats:
        print(f"\n--- Price Stats ---")
        print(f"  Range: ${price_stats['min']:,} - ${price_stats['max']:,}")
        print(f"  Mean:  ${price_stats['mean']:,}")


# ============================================================
# CLI
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="SGCarMart Used Cars Scraper")
    sub = parser.add_subparsers(dest="command")

    sp = sub.add_parser("scrape", help="Phase 1: discover listing URLs")
    sp.add_argument("--pages", type=int, default=None,
                    help="Max pages to scrape (default: all)")
    sp.add_argument("--resume", action="store_true",
                    help="Resume from last checkpoint")

    sub.add_parser("stats", help="Show field coverage stats")

    dp = sub.add_parser("scrape-details", help="Phase 2: scrape detail pages for all data")
    dp.add_argument("--limit", type=int, default=None,
                    help="Max detail pages to fetch (default: all)")
    dp.add_argument("--validate", action="store_true", default=True,
                    help="Enable field validation (default: on)")
    dp.add_argument("--no-validate", action="store_false", dest="validate",
                    help="Disable field validation")
    dp.add_argument("--track-history", action="store_true", default=False,
                    help="Enable SCD Type 2 history tracking")

    hp = sub.add_parser("history", help="Show listing change history")
    hp.add_argument("listing_id", type=int, help="Listing ID to query")
    hp.add_argument("--limit", type=int, default=20,
                    help="Max history rows to show (default: 20)")

    rdp = sub.add_parser("run-daily", help="One-shot: Phase 1 + Phase 2")
    rdp.add_argument("--max-pages", type=int, default=None,
                     help="Max listing pages for Phase 1 (default: all)")
    rdp.add_argument("--detail-limit", type=int, default=None,
                     help="Max detail pages for Phase 2 (default: all)")
    rdp.add_argument("--no-validate", action="store_true", default=False,
                     help="Disable field validation")
    rdp.add_argument("--track-history", action="store_true", default=False,
                     help="Enable SCD Type 2 history tracking")

    sch = sub.add_parser("schedule", help="Daemon: daily cron (Asia/Singapore)")
    sch.add_argument("--time", type=str, default="03:00",
                     help="Daily run time HH:MM (default: 03:00)")
    sch.add_argument("--max-pages", type=int, default=None,
                     help="Max listing pages for Phase 1 (default: all)")
    sch.add_argument("--detail-limit", type=int, default=None,
                     help="Max detail pages for Phase 2 (default: all)")
    sch.add_argument("--no-validate", action="store_true", default=False,
                     help="Disable field validation")
    sch.add_argument("--track-history", action="store_true", default=False,
                     help="Enable SCD Type 2 history tracking")

    args = parser.parse_args()

    if args.command == "scrape":
        start = 1
        if args.resume:
            with ListingDB(DB_PATH) as db:
                last_run = db.get_last_run()
            if last_run and last_run["pages_fetched"]:
                start = last_run["pages_fetched"] + 1
                print(f"Resuming from page {start} (run {last_run['id']})")
        scrape(max_pages=args.pages, start_page=start)

    elif args.command == "scrape-details":
        scrape_details(
            limit=args.limit,
            validate=args.validate,
            track_history=args.track_history,
        )

    elif args.command == "stats":
        with ListingDB(DB_PATH) as db:
            count = db.get_count()
            if count > 0:
                print(f"Loaded {count} listings from {DB_PATH}")
                print_stats_from_db(db)
            else:
                print("No data found. Run `python scrape_listing.py scrape` first.")

    elif args.command == "history":
        with ListingDB(DB_PATH) as db:
            scd = ListingHistoryDB(db)
            history = scd.get_history(args.listing_id)
            if not history:
                print(f"No history found for listing {args.listing_id}")
                return
            print(f"\nHistory for listing {args.listing_id} ({len(history)} versions)")
            print("-" * 60)
            for row in history[:args.limit]:
                current_marker = " [CURRENT]" if row["is_current"] else ""
                print(
                    f"  {row['valid_from']} -> {row['valid_to'] or 'now'}"
                    f"{current_marker}"
                )
                print(f"    Price: {row.get('price')} | Status: {row.get('status')}")
                print(f"    Mileage: {row.get('mileage_km')} | COE: {row.get('coe_remaining')}")

    elif args.command == "run-daily":
        from scheduler import run_daily_scrape
        run_daily_scrape(
            max_pages=args.max_pages,
            detail_limit=args.detail_limit,
            validate=not args.no_validate,
            track_history=args.track_history,
        )

    elif args.command == "schedule":
        from scheduler import start_scheduler
        start_scheduler(
            time=args.time,
            max_pages=args.max_pages,
            detail_limit=args.detail_limit,
            validate=not args.no_validate,
            track_history=args.track_history,
        )

    else:
        parser.print_help()


if __name__ == '__main__':
    main()
