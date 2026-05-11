# Implementation Plan: GitHub Actions + Cloudflare R2

Deploy the SGCarMart scraper as a daily GitHub Actions workflow with SQLite database persisted to Cloudflare R2 across ephemeral VM runs.

## Storage Option Evaluation

| Criterion | Cloudflare R2 | Cloudflare D1 | Supabase (PostgreSQL) | Turso (libSQL) |
|-----------|--------------|---------------|----------------------|----------------|
| Code changes to DB layer | **None** | Full rewrite | Full rewrite | Minimal |
| Migration effort | ~1 day | ~2 weeks | ~2 weeks | ~3 days |
| SQLite compatibility | **100%** (same file) | D1 subset | N/A (different engine) | High (SQLite fork) |
| Cost (monthly) | **$0** (10GB free) | $0 (5GB free) | $0 (500MB free) | $0 (9GB free) |
| WAL mode support | **Yes** | No | N/A | Yes |
| ON CONFLICT DO UPDATE | **Yes** | Yes | Different syntax | Yes |
| COALESCE upsert | **Yes** | Yes | Requires rewrite | Yes |
| executescript / executemany | **Yes** | Limited | Different API | Yes |
| Foreign keys | **Yes** | Limited | Yes | Yes |
| DB size limit | 10GB free | 10MB per DB (free) | 500MB free | 9GB free |
| New dependencies | boto3 | D1 REST SDK | psycopg2 / supabase-py | libsql-client |
| Complexity | **Low** | Medium | High | Medium |
| Lock-in | Low (S3 standard) | Medium (REST API) | Medium (PostgreSQL) | Low (SQLite-compatible) |

### Why Cloudflare R2

The scraper relies on SQLite features that are hard to replicate in managed databases:

- `PRAGMA journal_mode=WAL` and `PRAGMA synchronous=NORMAL` in `db.py`
- `INSERT ... ON CONFLICT(listing_id) DO UPDATE SET ... COALESCE(excluded.col, listings.col)`
- `executescript()` for schema initialization
- `executemany()` for batch upserts
- 4 tables with indexes, foreign keys, and migration ALTER TABLEs

R2 treats the DB as a binary file. Download before scrape, upload after. The exact same SQLite file is used — zero SQL dialect changes, zero API changes.

D1 removes WAL/PRAGMA support and has a 10MB per-database limit on the free tier. Supabase requires rewriting all SQL to PostgreSQL. Both are high-effort with no functional benefit for a single-writer daily batch job.

---

## Architecture

```
GitHub Actions (daily cron)
    |
    v
ubuntu-latest VM (ephemeral)
    |
    +--> storage.download_db()     --> R2: fetch latest SQLite DB
    |
    +--> scrape_listing.py         --> Phase 1: discover URLs
    |    run-daily --track-history --> Phase 2: scrape details + SCD
    |
    +--> storage.upload_db()       --> R2: persist updated DB
    |
    v
VM destroyed
```

Subsequent runs download the DB from the previous run, append/update data, and upload the new version. SCD Type 2 history accumulates across runs.

---

## Phase 1: Storage Layer

### Create `storage.py`

R2 download/upload module using boto3 (S3-compatible API).

```python
# storage.py -- public interface

def download_db() -> Path:
    """Download DB from R2. Returns local path.
    If object does not exist (first run), returns path anyway --
    ListingDB.__init__ will create the DB fresh."""

def upload_db() -> None:
    """Upload DB to R2. Overwrites previous version."""

def get_db_info() -> dict:
    """Return metadata: file size, last modified, listing count."""
```

Environment variables (set in GitHub Actions secrets):

| Variable | Example | Required |
|----------|---------|----------|
| `R2_ENDPOINT` | `https://<account_id>.r2.cloudflarestorage.com` | Yes |
| `R2_ACCESS_KEY` | (from R2 API token) | Yes |
| `R2_SECRET_KEY` | (from R2 API token) | Yes |
| `R2_BUCKET` | `sgcarmart-bucket` | Yes |
| `R2_DB_KEY` | `scrapling_listings.db` | No (has default) |

### Update `requirements.txt`

Add `boto3>=1.34.0`.

### Create `tests/test_storage.py`

Unit tests using `moto` (S3 mock library):

- Download existing DB from R2
- Download when DB does not exist (first run)
- Upload DB to R2
- Round-trip: upload then download preserves data

Add `moto[s3]>=5.0.0` as a dev dependency.

---

## Phase 2: CI Entry Point

### Create `ci_run.py`

Thin orchestration wrapper. No business logic changes.

```
download_db()
    |
    v
run_daily_scrape(track_history=True, validate=True)
    |
    v
upload_db()   <-- always runs, even on scrape failure (partial progress)
```

```python
# ci_run.py -- structure

def main():
    storage.download_db()
    try:
        run_daily_scrape(track_history=True, validate=True)
    finally:
        storage.upload_db()  # persist even on failure
```

Supports `--dry-run` flag for local testing without R2 upload.

Existing `run-daily` CLI command remains unchanged for local use.

---

## Phase 3: GitHub Actions Workflow

### Create `.github/workflows/scrape.yml`

```yaml
name: Daily Scrape

on:
  schedule:
    - cron: '0 19 * * *'   # 03:00 SGT = 19:00 UTC
  workflow_dispatch:         # manual trigger for testing

jobs:
  scrape:
    runs-on: ubuntu-latest
    timeout-minutes: 90

    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'

      - run: pip install -r requirements.txt

      - run: python ci_run.py
        env:
          R2_ENDPOINT: ${{ secrets.R2_ENDPOINT }}
          R2_ACCESS_KEY: ${{ secrets.R2_ACCESS_KEY }}
          R2_SECRET_KEY: ${{ secrets.R2_SECRET_KEY }}
          R2_BUCKET: ${{ secrets.R2_BUCKET }}

      - uses: actions/upload-artifact@v4
        if: always()
        with:
          name: scrape-output
          path: output/
          retention-days: 7
```

### Confirm `.gitignore`

Verify `output/` is gitignored so the DB is never committed to the repo.

---

## Phase 4: R2 Setup and GitHub Secrets

### Step 4.1: Create R2 bucket

1. Sign up for Cloudflare (free, no credit card)
2. Go to R2 Object Storage in the dashboard 
3. Create bucket named `sgcarmart-bucket`
4. Go to R2 > Manage R2 API Tokens > Create API Token
5. Select "Object Read & Write" permission
6. Note the endpoint URL, access key ID, and secret access key

### Step 4.2: Add GitHub Secrets

In the GitHub repo: Settings > Secrets and variables > Actions > New repository secret

| Secret | Value |
|--------|-------|
| `R2_ENDPOINT` | `https://<account_id>.r2.cloudflarestorage.com` |
| `R2_ACCESS_KEY` | Access key ID from API token |
| `R2_SECRET_KEY` | Secret access key from API token |
| `R2_BUCKET` | `sgcarmart-bucket` |

---

## Phase 5: Verification

### 5.1 Local integration test

```bash
# Set env vars locally
export R2_ENDPOINT=...
export R2_ACCESS_KEY=...
export R2_SECRET_KEY=...
export R2_BUCKET=sgcarmart-bucket

# Dry run (no upload)
python ci_run.py --dry-run

# Full run
python ci_run.py
```

Verify: DB appears in R2 bucket via Cloudflare dashboard.

### 5.2 Manual workflow trigger

Go to GitHub repo > Actions > Daily Scrape > Run workflow.

Verify: workflow completes, DB uploaded to R2, artifact downloadable from Actions tab.

### 5.3 Idempotency check

Trigger the workflow twice in succession.

Verify: second run downloads the DB from the first run, discovers listings already have data, completes quickly with no data loss.

### 5.4 SCD history across runs

After two daily runs, download the DB and query:

```sql
SELECT listing_id, price, status, valid_from, valid_to, is_current
FROM listings_history
WHERE listing_id = <some_id>
ORDER BY valid_from;
```

Verify: price/status changes are tracked, history chain is intact.

### 5.5 Run test suite

```bash
pytest tests/ -v
```

Verify: all existing tests pass (they do not depend on storage.py or ci_run.py).

---

## Files Summary

| Action | File | Purpose |
|--------|------|---------|
| CREATE | `storage.py` | R2 download/upload functions |
| CREATE | `ci_run.py` | CI orchestration entry point |
| CREATE | `.github/workflows/scrape.yml` | GitHub Actions workflow |
| CREATE | `tests/test_storage.py` | Storage layer unit tests |
| MODIFY | `requirements.txt` | Add `boto3>=1.34.0` |

**Unchanged**: `db.py`, `db_scd.py`, `scrape_listing.py`, `validators.py`, `scheduler.py`, all existing tests.

---

## Risks and Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| GitHub Actions cron drift (5-30 min) | Low | Acceptable for daily scraper |
| DB upload fails after successful scrape | Medium | Retry 3x with backoff; artifact backup via upload-artifact |
| Concurrent runs corrupt DB | Medium | GitHub Actions `concurrency` group prevents parallel runs |
| R2 free tier exceeded | None | ~60 requests/month vs 1M free |
| Network timeout during download/upload | Low | boto3 built-in retries + explicit retry logic |
| DB grows beyond disk limit | None | ~10-50MB after months; limit is 14GB |
| GitHub runner IP blocked by target site | Medium | Monitor; fall back to VPS if needed |

---

## Cost Estimate

| Resource | Monthly Cost |
|----------|-------------|
| GitHub Actions (2,000 free min) | $0 |
| Cloudflare R2 (10GB free) | $0 |
| **Total** | **$0** |

Expected usage: ~1,500 Actions minutes/month, ~60 R2 requests/month, ~10MB R2 storage. All well within free tiers.
