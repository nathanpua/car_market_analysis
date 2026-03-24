# Cost Analysis: SGCarMart Scraper on Cloudflare Workers Paid Plan

## Overview

This document analyzes the costs of running the SGCarMart scraper using Cloudflare's Workers Paid Plan with Browser Rendering via Workers Bindings.

## Cloudflare Pricing Summary

### Workers Paid Plan ($5/month base)

| Component | Included | Overage Rate |
|-----------|----------|--------------|
| Requests | 10 million/month | $0.30/million |
| CPU Time | 30 million ms/month | $0.02/million ms |
| Duration | Unlimited | Free |

### Browser Rendering (Workers Bindings)

| Component | Included | Overage Rate |
|-----------|----------|--------------|
| Duration | 10 hours/month | $0.09/hour |
| Concurrent Browsers | 10 (monthly avg) | $2.00/browser |

**Source:** [Cloudflare Browser Rendering Pricing](https://developers.cloudflare.com/browser-rendering/platform/pricing/)

---

## Architecture Cost Breakdown

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         COST COMPONENTS                                  │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   PHASE 1: URL Discovery (Incremental with SQLite)                      │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  /content API calls (150 pages max)                              │   │
│   │  ├─ First run: ~5s × 150 pages = 750s ≈ 0.21 hours              │   │
│   │  ├─ Incremental: ~5s × 20 pages = 100s ≈ 0.03 hours             │   │
│   │  └─ Early termination after 3 consecutive empty pages           │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
│   PHASE 2: Detail Crawling (Workers Bindings)                           │
│   ┌─────────────────────────────────────────────────────────────────┐   │
│   │  Worker requests (14,252 URLs)                                   │   │
│   │  ├─ Duration: ~5s per URL × 14,252 = 71,260s ≈ 19.8 hours       │   │
│   │  ├─ Throughput: ~36 URLs/min with 10 browsers                   │   │
│   │  └─ Concurrency: 10 browsers (within included limit)            │   │
│   └─────────────────────────────────────────────────────────────────┘   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Detailed Cost Calculation

### Phase 1: URL Discovery (Incremental with SQLite)

| Metric | Value |
|--------|-------|
| Max listing pages | 150 (with 100 listings each) |
| Early termination | After 3 consecutive pages with no new URLs |
| Duration per page | ~5 seconds |
| **First run duration** | 150 × 5s = 750s = **0.21 hours** |
| **Incremental run duration** | ~20 × 5s = 100s = **0.03 hours** |

**Phase 1 Cost:**
- First run: 0.21 hours (within 10 hours included) = **$0.00**
- Incremental runs: 0.03 hours (within 10 hours included) = **$0.00**

**Note:** Incremental runs typically complete in 1-2 minutes due to early termination and SQLite persistence.

### Phase 2: Detail Crawling (Workers Bindings)

| Metric | Value |
|--------|-------|
| URLs to crawl | 14,252 |
| Duration per URL | ~5 seconds (browser rendering time) |
| **Total duration** | 14,252 × 5s = 71,260s = **19.8 hours** |
| Concurrent browsers | 10 (within included limit) |
| **Throughput** | ~36 URLs/min (tested: 9/10 in 14.9s) |
| **Wall-clock time** | 14,252 ÷ 36 = **~6.6 hours** |

**Phase 2 Cost:**

| Component | Calculation | Cost |
|-----------|-------------|------|
| Duration (included) | 10 hours | $0.00 |
| Duration (overage) | 19.8 - 10 = 9.8 hours × $0.09 | $0.88 |
| Concurrency | 10 browsers (included) | $0.00 |
| **Phase 2 Total** | | **$0.88** |

**Note:** Wall-clock time (~6.6 hours) differs from browser duration (19.8 hours) because 10 browsers run in parallel. Cloudflare bills for total browser rendering time, not wall-clock time.

### Worker Requests

| Metric | Value |
|--------|-------|
| Phase 1 requests | 150 (first run) or ~20 (incremental) |
| Phase 2 requests | 14,252 |
| **Total requests** | **~14,402** (first run) or **~14,272** (incremental) |

**Worker Requests Cost:**
- ~14,402 requests (within 10 million included) = **$0.00**

---

## Monthly Cost Summary

### Scenario 1: One Full Crawl Per Month

| Component | Cost |
|-----------|------|
| Workers Paid Plan | $5.00 |
| Phase 1 Duration (0.21 hrs) | $0.00 (within included) |
| Phase 2 Duration (19.8 hrs) | $0.88 (9.8 hours overage) |
| Concurrency | $0.00 (within included) |
| Requests | $0.00 (within included) |
| **Total** | **$5.88/month** |

**Wall-clock time:** ~7 hours (Phase 1: 13 min + Phase 2: 6.6 hrs)

### Scenario 2: One Full Crawl + Weekly Incremental Updates

| Component | Cost |
|-----------|------|
| Workers Paid Plan | $5.00 |
| Phase 1 (full: 0.21 hrs) | $0.00 |
| Phase 1 (incremental × 4: 0.12 hrs) | $0.00 |
| Phase 2 (full: 19.8 hrs) | $0.88 |
| Phase 2 (new URLs × 4, ~400 each) | ~$0.80 |
| **Total** | **~$6.68/month** |

**Note:** Incremental runs typically find ~400 new URLs per week based on observed data.

### Scenario 3: Daily Incremental Crawls

| Component | Cost |
|-----------|------|
| Workers Paid Plan | $5.00 |
| Phase 1 (daily, ~20 pages = 0.03 hrs × 30) | $0.00 |
| Phase 2 (new URLs, ~60/day × 30) | ~$0.27 |
| **Total** | **~$5.27/month** |

---

## Cost Comparison: Alternative Approaches

### vs. REST API /crawl Endpoint

| Approach | Duration | Cost |
|----------|----------|------|
| REST API /crawl | ~30 hours | $5.00 + (20 hrs × $0.09) = **$6.80** |
| **Workers Bindings** | ~20 hours | $5.00 + (10 hrs × $0.09) = **$5.88** |
| **Savings** | 33% faster | **$0.92/month (13.5%)** |

### vs. Traditional VPS + Puppeteer

| Provider | Instance Type | Monthly Cost |
|----------|---------------|--------------|
| AWS EC2 | t3.medium (2 vCPU, 4GB) | ~$30 |
| DigitalOcean | 2 vCPU, 4GB Droplet | ~$18 |
| Hetzner | CX21 (2 vCPU, 4GB) | ~$6 |
| **Cloudflare Workers** | 10 concurrent browsers | **$5.88** |

**Note:** VPS requires manual setup, maintenance, and doesn't scale automatically.

### vs. Serverless Browser Services

| Service | Rate | Monthly Cost (20 hours) |
|---------|------|-------------------------|
| Browserless.io | $0.06/hour | $5.00 + $1.20 = $6.20 |
| Browserbase | $0.05/hour | $5.00 + $0.50 = $5.50 |
| **Cloudflare Workers** | $0.09/hour (overage only) | **$5.88** |

---

## Cost Optimization Strategies

### 1. Incremental Crawling (Implemented)
- **Phase 1 Savings:** ~13 minutes vs ~1+ hour (if 700 pages)
- **Phase 2 Savings:** Only crawl new URLs, skip already crawled
- SQLite persistence + early termination = re-runs in ~2 minutes

### 2. Early Termination (Implemented)
- Stop after 3 consecutive pages with no new URLs
- Typical incremental run: ~20 pages instead of 150

### 3. Batch Processing
- **Savings:** Reduced browser time
- Process multiple URLs per browser session (not implemented)
- Could reduce Phase 2 duration by 20-30%

### 4. Smart Scheduling
- **Savings:** Stay within included limits
- Schedule full crawls once per month
- Use incremental updates for daily/weekly refreshes

---

## Concurrency Cost Analysis

### Current Setup: 10 Concurrent Browsers

```
Monthly average = 10 browsers
Included = 10 browsers
Additional = 0 browsers
Cost = $0.00
```

### If Scaling to 20 Concurrent Browsers

```
Monthly average = 20 browsers
Included = 10 browsers
Additional = 10 browsers
Cost = 10 × $2.00 = $20.00/month

Total monthly cost = $5.00 + $0.88 + $20.00 = $25.88/month
Throughput increase = 2x (45 → 90 URLs/min)
```

### If Scaling to 30 Concurrent Browsers (Max on Paid Plan)

**Note:** 30 concurrent browsers is the **hard limit** on Workers Paid Plan (can request increase from Cloudflare support).

```
Monthly average = 30 browsers
Included = 10 browsers
Additional = 20 browsers
Cost = 20 × $2.00 = $40.00/month

Total monthly cost = $5.00 + $0.88 + $40.00 = $45.88/month
Throughput increase = 3x (45 → 135 URLs/min)
Phase 2 duration = ~6.6 hours (vs 19.8 hours)
```

**Recommendation:**
- **10 browsers** (default): Best cost-efficiency for regular use (~$5.88/month)
- **30 browsers**: Best for time-critical crawls (~$45.88/month, 3x faster)
- Consider scaling up only when you need results quickly (e.g., urgent data refresh)

---

## Break-Even Analysis

### When Does Cloudflare Become More Expensive Than VPS?

| Monthly Crawl Hours | Cloudflare Cost | VPS Cost ($6/month) |
|--------------------|-----------------|---------------------|
| 10 hours | $5.00 | $6.00 |
| 20 hours | $5.90 | $6.00 |
| 50 hours | $8.60 | $6.00 |
| 100 hours | $13.10 | $6.00 |

**Break-even:** ~67 hours/month

If you need more than 67 hours of browser rendering per month, a VPS may be more cost-effective.

---

## Recommended Usage Pattern

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    RECOMMENDED MONTHLY SCHEDULE                          │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   Week 1: Full Crawl                                                    │
│   ├─ Phase 1: Discover all URLs (~13 min, 150 pages)                   │
│   ├─ Phase 2: Crawl all detail pages (~6.6 hours wall-clock)           │
│   └─ Cost: ~$5.88                                                       │
│                                                                         │
│   Weeks 2-4: Incremental Updates (daily or every other day)             │
│   ├─ Phase 1: Check for new URLs (~2 min each, early termination)      │
│   ├─ Phase 2: Crawl new URLs only (~10-15 min each, ~60 URLs)          │
│   └─ Cost: ~$0.05-0.10 per update                                       │
│                                                                         │
│   Total Monthly Cost: ~$5.50-6.50                                       │
│   Total Browser Hours: ~10-12 hours                                     │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Summary

| Metric | Value |
|--------|-------|
| **Base Plan** | $5.00/month |
| **Typical Monthly Cost** | $5.50-6.50 |
| **Included Duration** | 10 hours/month |
| **Estimated Usage** | 10-12 hours/month (full + incrementals) |
| **Concurrent Browsers** | 10 (included) |
| **Throughput** | ~36 URLs/min |
| **Cost per URL** | ~$0.00006 |
| **Cost per 1,000 URLs** | ~$0.06 |

**Current Workflow:**
- Phase 1: 150 listing pages max, early termination after 3 empty pages
- Phase 2: 14,252 detail URLs at ~36 URLs/min
- Wall-clock time for full crawl: ~7 hours (13 min + 6.6 hrs)
- Incremental re-runs: ~2 minutes for Phase 1, varies for Phase 2

**Verdict:** Cloudflare Workers with Browser Rendering is highly cost-effective for this use case, especially with incremental crawling and early termination implemented. The $5/month Workers Paid Plan covers most scenarios, with minimal overage charges for full crawls.

---

## Sources

- [Cloudflare Workers Pricing](https://developers.cloudflare.com/workers/platform/pricing/)
- [Cloudflare Browser Rendering Pricing](https://developers.cloudflare.com/browser-rendering/platform/pricing/)
