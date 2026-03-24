# SGCarMart Used Cars Data Scraper

## Overview

This project scrapes ~14,000 used car listings from sgcarmart.com using Cloudflare's Browser Rendering API. It extracts 25+ fields per car listing and exports to Parquet/CSV format.

## Architecture

The scraper uses a two-phase approach with Cloudflare Workers Bindings for browser rendering.

**Phase 1: URL Discovery**
- Crawls ~140 listing pages to extract detail page URLs
- Uses `/content` endpoint (respects pagination query parameters)
- Stores URLs in SQLite for incremental re-runs
- Early termination after 3 consecutive pages with no new URLs
- First run: ~13 minutes | Incremental: ~2 minutes

**Phase 2: Detail Crawling**
- Crawls ~14,000 detail pages using Workers Bindings
- 10 concurrent browsers with Puppeteer
- ~36 URLs/min throughput
- Wall-clock time: ~6.6 hours
- Incremental (skips already crawled URLs)

## Key Technical Decisions

### 1. Workers Bindings over /crawl REST API

The `/crawl` REST API is a black-box solution with limited concurrency control. Workers Bindings provides full Puppeteer control, enabling custom wait conditions, error handling, and 2x throughput improvement.

| Aspect | /crawl API | Workers Bindings |
|--------|------------|------------------|
| Throughput | ~20 URLs/min | ~36 URLs/min |
| Control | Black box | Full Puppeteer |
| Customization | None | Scripts, wait conditions |

### 2. /content Endpoint for Phase 1

The `/crawl` endpoint normalizes URLs and strips query parameters, breaking pagination. The `/content` endpoint preserves `?page=N` parameters, making it the correct choice for listing page discovery.

### 3. 10 Concurrent Browsers

Testing showed that 15, 20, 25, and 30 concurrent browsers all hit Cloudflare rate limits. 10 browsers is the stable maximum on the Workers Paid Plan.

### 4. SQLite over Cloudflare KV

SQLite provides 1ms lookups vs 50-100ms network calls to Cloudflare KV. Since notebooks run locally, there is no need for cloud storage. This enables fast incremental re-runs without network latency.

### 5. Browser Cleanup in Finally Block

Unclosed browsers block concurrency slots for up to 60 seconds, causing rate limiting. The worker always closes browsers in a finally block to ensure slots are freed immediately.

### 6. Incremental Crawling

**Phase 1:** SQLite tracks seen URLs, enabling re-runs in ~2 minutes instead of ~13 minutes. Early termination stops after 3 consecutive pages with no new URLs.

**Phase 2:** Skips URLs already in results file. Failed URLs are tracked for retry. Checkpoints saved every 20 batches for resume capability.

## Data Fields

| Category | Fields |
|----------|--------|
| Identity | listing_id, car_model, detail_page_url |
| Financial | price, depreciation, omv, arf, coe, road_tax, dereg_value |
| Technical | engine_cap, fuel_type, power, transmission, curb_weight |
| History | reg_date, manufactured, mileage, owners |
| Metadata | status, vehicle_type, features, accessories |

## Project Structure

| File/Directory | Purpose |
|----------------|---------|
| `01_environment_setup.ipynb` | Validate credentials |
| `02_test_crawl.ipynb` | Test and discover RSC pattern |
| `03_validate_crawl_approach.ipynb` | Validate on sample URLs |
| `04_production_crawl.ipynb` | Phase 1: URL Discovery |
| `05_workers_binding_crawl.ipynb` | Phase 2: Detail crawling |
| `worker/` | Cloudflare Worker configuration |
| `output/seen_urls.db` | SQLite: seen URLs for incremental |
| `output/full_detail_data.parquet` | Final dataset |

## Cost Analysis

| Metric | Value |
|--------|-------|
| Workers Paid Plan | $5.00/month |
| Included duration | 10 hours/month |
| Estimated usage | 10-12 hours (full crawl + incrementals) |
| Overage rate | $0.09/hour |
| Typical monthly cost | ~$5.50-6.50 |
| Cost per 1,000 URLs | ~$0.06 |

See `COST_ANALYSIS.md` for detailed breakdown.

## Efficiency Comparison

| Approach | API Calls | Browser Time | Throughput |
|----------|-----------|--------------|------------|
| Individual scrapes | ~14,000 | ~40 hours | ~6 URLs/min |
| /crawl REST API | ~100 | ~12 hours | ~20 URLs/min |
| Workers Bindings | ~100 | ~6 hours | ~36 URLs/min |

## Prerequisites

- Python 3.8+
- Cloudflare Workers Paid Plan ($5/month) - required for 10 browser concurrency
- API token with Browser Rendering permission
- Node.js 18+ (for worker deployment)
