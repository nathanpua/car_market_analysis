# SGCarMart Cloudflare Scraper — Implementation Plan

## Objective

Scrape **all ~13,800+ used car listings** from [sgcarmart.com/used-cars](https://www.sgcarmart.com/used-cars/) using the **Cloudflare `/crawl` API**, which automatically follows links from listing pages into individual detail pages. Parse every detail page to extract the **complete 40+ field dataset** per car, and store everything in **Polars DataFrames** exported to Parquet.

---

## Why `/crawl` Over `/scrape`

| Concern | `/scrape` (old plan) | `/crawl` (new plan) |
|---|---|---|
| API calls | ~14,500 individual requests | **1 async job** (poll for results) |
| Link discovery | Manual: build URLs, manage pagination | **Automatic**: crawler follows links from rendered pages |
| Rate limit pressure | 14,500 calls at 600/min = ~24 min of throttling | Single job runs server-side; no client rate limits |
| Checkpoint/resume | Client-managed checkpoints | **Server-managed**: results persist 14 days; cursor-based retrieval |
| Complexity | Pagination loop + detail loop + concurrency semaphore | Submit job, poll, parse results |
| Cost | Same browser hours either way | Same, but fewer API round-trips |

---

## Site Architecture (Findings)

- **Tech stack**: Next.js with React Server Components (RSC)
- **Data transport**: Embedded in `self.__next_f.push()` script tags as serialized RSC flight payloads containing structured JSON
- **Pagination**: ~694 pages at 20/page; pagination elements use `page-link` class with `data-value` attributes
- **Total listings**: ~13,862
- **Detail page URL**: `https://www.sgcarmart.com/used-cars/info/{slug}-{id}/?dl={dealer_code}`
- **Legacy URL**: `https://www.sgcarmart.com/used_cars/info.php?ID={id}` (301 redirects)
- **User-Agent**: Crawl endpoint uses fixed `CloudflareBrowserRenderingCrawler/1.0` — must verify site does not block it

### Data on Detail Pages (Primary Source — 40+ fields)

Each detail page (`/used-cars/info/...`) contains the **complete record** for one car. The RSC payload on each detail page carries two key objects:

**`ucInfoDetailData`** (car specifications):

| Field | Example | Notes |
|-------|---------|-------|
| `car_model` | `"Mazda 2 HB 1.5A Deluxe (New 5-yr COE)"` | Full model string |
| `price` | `64500` | Integer SGD |
| `depreciation` | `12890` | Annual depreciation SGD/yr |
| `registration_date` | `"24-May-2016"` | |
| `first_registration_date` | `null` or date string | |
| `original_reg_date` | `null` or date string | |
| `manufactured` | `2015` | Year integer |
| `engine_capacity` | `"1,496 cc"` | |
| `mileage` | `"68,000 km"` or `"N.A."` | |
| `fuel_type` | `"Petrol"` | Petrol / Diesel / Electric / Petrol-Electric / Diesel-Electric |
| `transmission` | `"Auto"` | Auto / Manual |
| `owners` | `"1 Owner"` | |
| `type_of_vehicle` | `{text: "Hatchback", link: "..."}` | Body type |
| `drive_range` | `"N.A."` or km value | EV/hybrid only |
| `coe` | `"$35,501"` or `"N.A."` | COE premium paid |
| `coe_left` | `"5y"` | Remaining COE period |
| `omv` | `"$16,534"` | Open market value |
| `arf` | `"$6,534"` | Additional registration fee |
| `dereg_value` | `"$12,345"` or `"N.A."` | PARF/deregistration value |
| `road_tax` | `"$682 /yr"` | Annual road tax |
| `power` | `"85.0 kW (113 bhp)"` | |
| `curb_weight` | `"1,036 kg"` | |
| `lifespan` | date string or `null` | Vehicle lifespan expiry |
| `features` | free text | Vehicle features |
| `accessories` | free text | Fitted accessories |
| `description` | free text | Seller's listing text |
| `opc_scheme` | `null` or details | Off-Peak Car scheme |
| `electric_motor` | `null` or specs | EV motor specs |
| `warranty` | `null` or details | |
| `sta_evaluation` | `null` or grade | STA inspection grade |
| `posted_on` | `"17-Mar-2026"` | |
| `updated_on` | `"17-Mar-2026"` | |

**`ucInfoDetailTopData`** (pricing & seller):

| Field | Example | Notes |
|-------|---------|-------|
| `price_formatted` | `"$64,500"` | Display string |
| `installment` | `856` | Monthly SGD |
| `max_ltv_ratio` | `70` | Loan-to-value % |
| `coe_left_formatted` | `"5yrs COE left"` | |
| `ad_type` | `"PREMIUM AD"` | Premium / Normal |
| `whatsapp_link` | URL string | Seller contact |

**Dealer/metadata fields** (from listing-level RSC):

| Field | Example | Notes |
|-------|---------|-------|
| `id` | `1483577` | Unique listing ID |
| `make` | `"Mazda"` | Manufacturer |
| `model` | `"2"` | Model name |
| `dealer_code` | `4924` | |
| `dealer_name` | `"ABC Motors"` | Via `dealer_lead.name` |
| `dealer_package` | `"premium"` | Dealer tier |
| `status` | `"a"` | Available / Sold |
| `image` | `"https://i.i-sgcm.com/cars_used/..."` | Primary photo URL |
| `date` | `"17-Mar-2026"` | Date posted on site |
| `is_buysafe` | `true/false` | BuySafe certified |
| `is_star_ad` | `true/false` | Star ad flag |
| `is_spotlight` | `true/false` | Spotlight ad flag |
| `warranty_period` | `"12 months"` or `null` | |
| `vehicle_scheme` | `null` or `"OPC"` | |
| `is_imported_used` | `true/false` | Grey import flag |
| `is_eligible_for_parf_rebate` | `true/false` | |
| `has_10_years_coe` | `true/false` | |
| `inspection_grade` | `""` or grade | |

---

## Implementation Plan

### Phase 0 — Environment Setup

**File**: `cloudflare_scraper.ipynb` (cells 1-2)

1. Install dependencies:
   ```
   pip install httpx polars beautifulsoup4 python-dotenv tqdm
   ```
2. Add Cloudflare credentials to `.env`:
   ```
   CF_ACCOUNT_ID=your-account-id
   CF_API_TOKEN=your-api-token
   ```
   Token needs **Account > Browser Rendering > Edit** permission.
3. Load config, validate credentials by calling a lightweight endpoint (e.g. `/screenshot` on a test URL).

---

### Phase 1 — Crawl API Client

**File**: `cloudflare_scraper.ipynb` (cells 3-4)

The `/crawl` endpoint is **asynchronous** with a 3-step lifecycle:

```
1. POST /crawl          → submit job, get job_id
2. GET  /crawl/{job_id} → poll status + retrieve results (cursor-paginated)
3. DELETE /crawl/{job_id} → cancel a running job (if needed)
```

Build three functions:

#### `submit_crawl(url, limit, depth, include_patterns, ...) → job_id`

```
POST https://api.cloudflare.com/client/v4/accounts/{account_id}/browser-rendering/crawl

Body:
{
  "url": <start_url>,
  "limit": <max_pages>,
  "depth": <max_link_depth>,
  "source": "links",
  "formats": ["html"],
  "render": true,
  "options": {
    "includePatterns": [...],
    "excludePatterns": [...],
    "includeExternalLinks": false,
    "includeSubdomains": false
  },
  "gotoOptions": {
    "waitUntil": "networkidle2",
    "timeout": 60000
  },
  "rejectResourceTypes": ["image", "media", "font", "stylesheet"]
}
```

Returns `result.id` (the `job_id`).

#### `poll_crawl(job_id) → (status, records)`

```
GET /crawl/{job_id}?limit=100&cursor={cursor}
```

- Polls every 30s until `status != "running"`
- Uses cursor-based pagination when results exceed 10 MB
- Returns records with: `url`, `status`, `html`, `metadata`

Terminal statuses: `completed`, `cancelled_due_to_timeout`, `cancelled_due_to_limits`, `cancelled_by_user`, `errored`

#### `cancel_crawl(job_id)`

```
DELETE /crawl/{job_id}
```

Emergency stop for runaway jobs.

---

### Phase 2 — Test Crawl (Validation)

**File**: `cloudflare_scraper.ipynb` (cells 5-6)

Before the full run, submit a small test crawl to validate:

```python
test_job_id = submit_crawl(
    url="https://www.sgcarmart.com/used-cars/listing",
    limit=30,           # ~1 listing page + ~20 detail pages
    depth=2,
    include_patterns=[
        "https://www.sgcarmart.com/used-cars/listing**",
        "https://www.sgcarmart.com/used-cars/info/**",
    ],
    exclude_patterns=[
        "**utm_content**",       # skip ad-tracking variants
    ],
)
```

**Validate:**
1. Did the crawler render the JS-heavy listing page? (check `html` content length > threshold)
2. Did it discover and follow links into `/used-cars/info/...` detail pages?
3. Can we parse the RSC `self.__next_f.push()` payloads from the returned HTML?
4. How much `browserSecondsUsed` for ~30 pages? (extrapolate full cost)
5. Was the `CloudflareBrowserRenderingCrawler/1.0` user-agent blocked by the site?
6. Did `robots.txt` disallow any URLs? (check for `"status": "disallowed"` records)

**If pagination links are NOT discovered** (Next.js client-side routing may not produce real `<a href>` tags), fall back to **Phase 2b**.

---

### Phase 2b — Fallback: Seed URLs via `/scrape` (Only if Needed)

If the `/crawl` test shows the crawler cannot navigate pagination (i.e. it only finds detail links from page 1), use a hybrid approach:

1. Use a single `/scrape` call on the first listing page to extract the RSC payload
2. From the RSC data, read the `total` count and compute `total_pages = ceil(total / 20)`
3. Generate all listing page URLs: `https://www.sgcarmart.com/used-cars/listing?page={1..N}`
4. Submit **one `/crawl` job per listing page** (or batch into groups of 50-100 pages), each with `depth: 1` to follow only the detail links on that page

This is still far better than 14,500 individual `/scrape` calls — it would be ~14 crawl jobs of ~1,000 pages each.

---

### Phase 3 — Full Crawl

**File**: `cloudflare_scraper.ipynb` (cells 7-9)

Submit the production crawl job:

```python
job_id = submit_crawl(
    url="https://www.sgcarmart.com/used-cars/listing",
    limit=15000,
    depth=2,
    include_patterns=[
        "https://www.sgcarmart.com/used-cars/listing**",
        "https://www.sgcarmart.com/used-cars/info/**",
    ],
    exclude_patterns=[
        "**utm_content**",
    ],
)
```

**Crawl behavior:**
- `depth: 0` — the starting listing page
- `depth: 1` — all pages linked from listing pages (detail pages + next listing pages)
- `depth: 2` — pages linked from detail pages (not needed, but ensures pagination links from rendered listing pages are followed)
- `source: "links"` — only follows `<a href>` links found in rendered HTML, not sitemaps
- `render: true` — headless Chromium renders each page (required for Next.js RSC)
- `rejectResourceTypes: ["image", "media", "font", "stylesheet"]` — skip images/CSS/fonts to reduce browser time

**Monitoring loop:**

```python
while True:
    status, finished, total, browser_secs = check_status(job_id)
    print(f"[{status}] {finished}/{total} pages, {browser_secs:.0f}s browser time")
    if status != "running":
        break
    sleep(60)
```

**Result retrieval** (cursor-paginated):

```python
all_records = []
cursor = None
while True:
    batch, cursor = get_results(job_id, cursor=cursor, limit=500)
    all_records.extend(batch)
    if cursor is None:
        break
```

**Server-side persistence**: Crawl results persist for **14 days** after completion. No need for client-side checkpoints — if the notebook restarts, just resume polling/retrieving with the same `job_id`.

---

### Phase 4 — Parse Crawl Results

**File**: `cloudflare_scraper.ipynb` (cells 10-13)

Each crawl record contains `{"url": "...", "status": "completed", "html": "..."}`. Classify each page by URL pattern and parse accordingly.

#### Step 4a — Classify Pages

```python
detail_records = [r for r in all_records if "/used-cars/info/" in r["url"]]
listing_records = [r for r in all_records if "/used-cars/listing" in r["url"]]
other_records = [r for r in all_records if r not in detail_records + listing_records]
```

Expected: ~13,800 detail pages, ~694 listing pages, ~0 other.

#### Step 4b — Parse Detail Pages (Primary Data Source)

Each detail page's HTML contains `<script>` tags with `self.__next_f.push([...])` calls. These carry the RSC flight data as serialized strings containing JSON objects.

**Extraction algorithm:**

```
For each detail page HTML:
  1. Find all <script> tags containing "self.__next_f.push"
  2. Extract the string arguments from each push() call
  3. Scan for JSON objects containing known keys:
     - Look for "ucInfoDetailData" or "ucInfoDetailTopData" keys
     - Or find objects with "car_model", "price", "omv" fields
  4. Parse the JSON objects
  5. Merge ucInfoDetailData + ucInfoDetailTopData + dealer fields
     into a single flat record
```

**Fallback — BeautifulSoup HTML parsing:**

If RSC extraction fails for a page (e.g. payload format changed), parse the rendered DOM:
- Car specs table: `<div>` elements with label/value pairs
- Price: element matching `[class*='price']`
- Dealer info: element matching `[class*='dealer']`

#### Step 4c — Parse Listing Pages (Supplementary — Gap Fill)

Listing pages contain summary data for 20 cars each. Parse these **only** to fill gaps for detail pages that failed or were skipped:

```
For each listing page HTML:
  1. Extract self.__next_f.push() payloads
  2. Find array of listing objects (each with "id", "car_model", "price", ...)
  3. Collect as supplementary records keyed by "id"
```

#### Step 4d — Merge and Deduplicate

```
1. Start with detail_page_records as the primary dataset (fullest data)
2. For any listing IDs present in listing_page_records but missing
   from detail_page_records, add the listing-level record (partial data)
3. Deduplicate on "id"
4. Flag partial records (missing detail-page fields) for review
```

---

### Phase 5 — Data Cleaning with Polars

**File**: `cloudflare_scraper.ipynb` (cells 14-16)

#### Type Casting

| Column | Raw Format | Target Type | Transform |
|--------|-----------|-------------|-----------|
| `id` | `1483577` | `UInt32` | Direct cast |
| `price` | `64500` (int) or `"$64,500"` (str) | `Float64` | Strip `$`, `,` if string |
| `depreciation` | `12890` | `Float64` | Direct cast |
| `omv` | `"$16,534"` | `Float64` | Strip `$`, `,` → parse |
| `arf` | `"$6,534"` | `Float64` | Strip `$`, `,` → parse |
| `dereg_value` | `"$12,345"` or `"N.A."` | `Float64` (nullable) | `"N.A."` → `null` |
| `coe` | `"$35,501"` or `"N.A."` | `Float64` (nullable) | `"N.A."` → `null` |
| `road_tax` | `"$682 /yr"` | `Float64` | Strip `$`, `/yr` → parse |
| `manufactured` | `2015` | `UInt16` | Direct cast |
| `registration_date` | `"24-May-2016"` | `Date` | `strptime("%d-%b-%Y")` |
| `original_reg_date` | date string or `null` | `Date` (nullable) | Same format |
| `lifespan` | date string or `null` | `Date` (nullable) | Same format |
| `posted_on` / `updated_on` | `"17-Mar-2026"` | `Date` | Same format |
| `engine_capacity` | `"1,496 cc"` | `UInt32` | Strip `,`, ` cc` → parse |
| `curb_weight` | `"1,036 kg"` | `UInt32` | Strip `,`, ` kg` → parse |
| `mileage` | `"68,000 km"` or `"N.A."` | `Float64` (nullable) | `"N.A."` → `null` |
| `owners` | `"1 Owner"` | `UInt8` | Extract leading digit |
| `power` | `"85.0 kW (113 bhp)"` | `Utf8` | Keep as-is (compound value) |
| `installment` | `856` | `Float64` | Direct cast |
| `max_ltv_ratio` | `70` | `UInt8` | Direct cast |
| `make`, `model`, `car_model` | strings | `Utf8` | Strip whitespace |
| `fuel_type`, `transmission` | strings | `Utf8` (Categorical) | `cast(pl.Categorical)` |
| `type_of_vehicle` | `{text, link}` or string | `Utf8` | Extract `.text` if dict |
| `description`, `features`, `accessories` | free text | `Utf8` | Strip whitespace |
| `is_buysafe`, `is_star_ad`, etc. | `true/false` | `Boolean` | Direct cast |
| `image` | URL string | `Utf8` | Keep as-is |
| `dealer_code` | `4924` | `UInt32` | Direct cast |
| `dealer_name` | string | `Utf8` | Strip whitespace |
| `coe_left` | `"5y"` | `Utf8` | Keep as-is |
| `status` | `"a"` | `Utf8` (Categorical) | Map: `"a"` → `"available"` |
| `ad_type` | `"p"` or `"PREMIUM AD"` | `Utf8` (Categorical) | Normalize |
| `vehicle_scheme`, `opc_scheme` | string or null | `Utf8` (nullable) | |
| `warranty`, `sta_evaluation` | string or null | `Utf8` (nullable) | |
| `electric_motor`, `drive_range` | string or null | `Utf8` (nullable) | For EV/hybrid only |

#### Validation

```python
assert df.filter(pl.col("id").is_null()).height == 0, "No null IDs"
assert df.filter(pl.col("price") <= 0).height == 0, "No zero/negative prices"
assert df["id"].is_unique().all(), "All IDs unique"

# Field completeness report
for col in df.columns:
    pct = (1 - df[col].null_count() / df.height) * 100
    print(f"  {col:30s} {pct:6.1f}% non-null")

# Flag partial records (crawl missed the detail page)
partial = df.filter(pl.col("omv").is_null() & pl.col("price").is_not_null())
print(f"Partial records (listing-only): {partial.height}")
```

---

### Phase 6 — Export & Summary

**File**: `cloudflare_scraper.ipynb` (cells 17-19)

1. **Save final dataset**:
   ```
   output/sgcarmart_used_cars_full.parquet   (primary — columnar, compressed)
   output/sgcarmart_used_cars_full.csv       (secondary — human-readable)
   ```

2. **Save raw crawl HTML** (optional, for re-parsing):
   ```
   output/raw_crawl_results.parquet          (url + html columns only)
   ```

3. **Summary statistics** (printed in notebook):
   - Total listings scraped (detail pages vs listing-only)
   - Field completeness (% non-null per column)
   - Top 10 brands by count
   - Price distribution (min, P25, median, P75, max)
   - Depreciation distribution
   - Vehicle type breakdown
   - Fuel type breakdown
   - Transmission split (Auto vs Manual)
   - COE value distribution
   - OMV distribution
   - Browser seconds used + estimated cost

---

## Final Schema (44 columns)

| # | Column | Source | Description |
|---|--------|--------|-------------|
| 1 | `id` | detail/listing | Unique listing ID |
| 2 | `make` | detail/listing | Car manufacturer (e.g. "Toyota") |
| 3 | `model` | detail/listing | Model name (e.g. "Harrier") |
| 4 | `car_model` | detail/listing | Full model string |
| 5 | `price` | detail/listing | Asking price SGD |
| 6 | `depreciation` | detail/listing | Annual depreciation SGD/yr |
| 7 | `registration_date` | detail/listing | Registration date |
| 8 | `first_registration_date` | detail | First registration (if different) |
| 9 | `original_reg_date` | detail | Original registration date |
| 10 | `manufactured` | detail | Year of manufacture |
| 11 | `engine_capacity` | detail/listing | Engine displacement cc |
| 12 | `mileage` | detail/listing | Odometer reading km |
| 13 | `fuel_type` | detail/listing | Petrol / Diesel / Electric / Petrol-Electric |
| 14 | `transmission` | detail/listing | Auto / Manual |
| 15 | `owners` | detail/listing | Number of previous owners |
| 16 | `type_of_vehicle` | detail/listing | Body type (Sedan, SUV, etc.) |
| 17 | `drive_range` | detail | EV/hybrid driving range km |
| 18 | `coe` | detail | COE premium paid SGD |
| 19 | `coe_left` | detail/listing | COE remaining (e.g. "5y") |
| 20 | `omv` | detail | Open market value SGD |
| 21 | `arf` | detail | Additional registration fee SGD |
| 22 | `dereg_value` | detail | PARF/deregistration value SGD |
| 23 | `road_tax` | detail | Annual road tax SGD/yr |
| 24 | `power` | detail | Engine power (kW/bhp string) |
| 25 | `curb_weight` | detail | Vehicle weight kg |
| 26 | `lifespan` | detail | Vehicle lifespan expiry date |
| 27 | `features` | detail | Vehicle features (free text) |
| 28 | `accessories` | detail | Accessories (free text) |
| 29 | `description` | detail/listing | Seller description |
| 30 | `opc_scheme` | detail | Off-Peak Car scheme details |
| 31 | `electric_motor` | detail | EV motor specs |
| 32 | `warranty` | detail | Warranty details |
| 33 | `sta_evaluation` | detail | STA inspection grade |
| 34 | `installment` | detail/listing | Monthly installment SGD |
| 35 | `max_ltv_ratio` | detail/listing | Max loan-to-value % |
| 36 | `posted_on` | detail | Date listed |
| 37 | `updated_on` | detail | Date last updated |
| 38 | `status` | listing | available / sold |
| 39 | `ad_type` | detail/listing | Premium / Normal |
| 40 | `image` | listing | Primary image URL |
| 41 | `dealer_code` | detail/listing | Dealer ID |
| 42 | `dealer_name` | detail/listing | Dealer / seller name |
| 43 | `is_buysafe` | listing | BuySafe certified flag |
| 44 | `is_imported_used` | listing | Grey import flag |
| 45 | `is_eligible_for_parf_rebate` | listing | PARF rebate eligible |
| 46 | `has_10_years_coe` | listing | 10-year COE flag |
| 47 | `vehicle_scheme` | listing | OPC / Normal scheme |
| 48 | `inspection_grade` | listing | Inspection grade |
| 49 | `detail_page_url` | crawl | URL of the detail page scraped |

---

## Cloudflare Plan & Cost

**Required plan**: Workers Paid ($5/mo). Free tier is unusable (100 page cap, 5 jobs/day, 10 min/day browser time, 6 req/min).

### Cost Estimate

| Variable | Value |
|----------|-------|
| Total pages to render | ~14,500 (694 listing + 13,800 detail) |
| Browser time per page (est.) | 3-8s (JS render + networkidle2 wait) |
| `rejectResourceTypes` savings | ~30-50% reduction (skip images/CSS/fonts) |

| Scenario | Avg seconds/page | Total browser hours | Included free | Overage @ $0.09/hr | **Total monthly** |
|----------|-----------------|--------------------|--------------|--------------------|------------------|
| Best case | 3s | 12 hrs | 10 hrs | $0.18 | **$5.18** |
| Likely case | 5s | 20 hrs | 10 hrs | $0.90 | **$5.90** |
| Conservative | 8s | 32 hrs | 10 hrs | $1.98 | **$6.98** |
| Worst case | 15s | 60 hrs | 10 hrs | $4.50 | **$9.50** |

### API Call Breakdown

| Step | Method | Calls |
|------|--------|-------|
| Submit crawl job | `POST /crawl` | 1 |
| Poll status | `GET /crawl/{id}?limit=1` | ~50-200 (every 30-60s over hours) |
| Retrieve results | `GET /crawl/{id}?limit=500` | ~30 (cursor pages for 14,500 records) |
| **Total API calls** | | **~80-230** |

Compare: old `/scrape` plan required **~14,500** API calls.

---

## File Structure

```
sgcarmart/
├── .env                              # CF_ACCOUNT_ID, CF_API_TOKEN
├── cloudflare_scraper.ipynb          # Main notebook
├── scraper.ipynb                     # Old TinyFish scraper (reference only)
├── output/
│   ├── sgcarmart_used_cars_full.parquet
│   ├── sgcarmart_used_cars_full.csv
│   └── raw_crawl_results.parquet     # Raw HTML per URL (for re-parsing)
└── SCRAPING_PLAN.md                  # This plan
```

No client-side checkpoint files needed — crawl results persist server-side for 14 days.

---

## Risk Mitigations

| Risk | Mitigation |
|------|------------|
| `robots.txt` blocks the crawler UA | Test crawl (Phase 2) checks for `"status": "disallowed"`. If blocked, fall back to `/scrape` with custom UA. |
| Pagination links not discoverable (client-side JS routing) | Test crawl validates link discovery. If only page 1 detail links found, use Phase 2b (seed URLs via `/scrape`). |
| `/crawl` beta instability | Save `job_id` to disk. Results persist 14 days — retry retrieval if polling fails. Fall back to `/scrape` if jobs consistently error. |
| Crawl job timeout (7-day max) | At ~5s/page for 14,500 pages = ~20 hours < 7-day limit. Well within bounds. |
| RSC payload format changes | Fallback to BeautifulSoup HTML DOM parsing per page. |
| Missing detail pages (link not followed) | Compare detail IDs against listing IDs; re-scrape missing ones individually via `/scrape`. |
| Large result set (>10 MB) | Cursor-based pagination on GET — retrieve in 500-record batches. |
| Site rate-limits or blocks mid-crawl | Cloudflare manages request pacing server-side. The crawler is a real Chromium browser. |
| Memory pressure parsing 14,500 HTML pages | Parse in streaming batches (500 records at a time from cursor); append to Parquet incrementally. |
