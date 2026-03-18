# SGCarMart Used Cars Data Scraper

## Overview

This project scrapes all ~13,800+ used car listings from [sgcarmart.com/used-cars](https://www.sgcarmart.com/used-cars/) using Cloudflare's asynchronous `/crawl` API to extract a complete 40+ field dataset per car and store everything in Polars DataFrames exported to Parquet format.

## Table of Contents
- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Getting Started](#getting-started)
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Business Requirements](#business-requirements)
- [User Stories](#user-stories)
- [Cost Estimation](#cost-estimation)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Efficient Data Collection**: Uses Cloudflare's `/crawl` API (~80-230 API calls vs ~14,500 with individual scrapes)
- **Automatic Link Discovery**: Crawler follows links from listing pages to detail pages automatically
- **Server-Side Processing**: No client-side rate limits or checkpoint management needed
- **Robust Parsing**: Extracts structured data from Next.js RSC flight payloads with BeautifulSoup fallback
- **Data Quality**: Comprehensive validation, cleaning, and type casting of 49 data fields
- **Resume Capable**: Results persist server-side for 14 days, allowing notebook restarts
- **Multiple Output Formats**: Parquet (primary) and CSV (secondary) exports
- **Comprehensive Analytics**: Built-in summary statistics and cost reporting

## Architecture

The scraper follows a 6-phase implementation approach:

1. **Environment Setup**: Dependency installation and credential validation
2. **Cloudflare API Client**: Functions to submit, poll, and cancel crawl jobs
3. **Test Crawl Validation**: Small-scale test to verify approach before full execution
4. **Full Production Crawl**: Execute the complete crawl job for all listings
5. **Data Parsing & Extraction**: Parse HTML to extract structured car data
6. **Data Cleaning & Validation**: Type casting, data integrity checks, and edge case handling
7. **Export & Summary**: Save datasets and generate summary statistics

## Getting Started

### Prerequisites
- Python 3.8+
- Cloudflare Workers Paid Plan ($5/month) - Free tier is insufficient
- Cloudflare API credentials with Account > Browser Rendering > Edit permission

### Installation
1. Clone this repository
2. Install dependencies:
   ```bash
   pip install httpx polars beautifulsoup4 python-dotenv tqdm
   ```
3. Create a `.env` file with your Cloudflare credentials:
   ```env
   CF_ACCOUNT_ID=your-account-id
   CF_API_TOKEN=your-api-token
   ```

## Usage

The main implementation is in `cloudflare_scraper.ipynb`. Execute the notebook cells in order:

1. **Cells 1-2**: Environment setup and credential validation
2. **Cells 3-4**: Cloudflare API client functions
3. **Cells 5-7**: Test crawl validation (GO/NO-GO decision point)
4. **Cells 8-9**: Fallback approach (only if test crawl fails)
5. **Cells 10-12**: Full production crawl execution
6. **Cells 13-17**: Data parsing and extraction
7. **Cells 18-21**: Data cleaning and validation
8. **Cells 22-24**: Export datasets and generate summary statistics

## Project Structure
```
sgcarmart/
├── .env                      # Cloudflare credentials (NOT committed)
├── cloudflare_scraper.ipynb  # Main implementation notebook
├── scraper.ipynb             # Old TinyFish scraper (reference only)
├── SCRAPING_PLAN.md          # Technical implementation plan
├── BUSINESS_REQUIREMENTS.md  # Business requirements and user stories
├── README.md                 # This file
└── output/
    ├── sgcarmart_used_cars_full.parquet   # Primary dataset
    ├── sgcarmart_used_cars_full.csv       # Secondary dataset
    └── raw_crawl_results.parquet          # Optional: raw HTML for re-parsing
```

## Business Requirements

See [BUSINESS_REQUIREMENTS.md](BUSINESS_REQUIREMENTS.md) for detailed business objectives, functional requirements, non-functional requirements, and success metrics.

## User Stories

See [BUSINESS_REQUIREMENTS.md](BUSINESS_REQUIREMENTS.md#user-stories) for complete user stories covering:
- Data Engineer environment setup and testing
- Full crawl execution and monitoring
- Data parsing, cleaning, and validation
- Data analysis access and cost reporting
- System reliability and maintenance

## Cost Estimation

Based on the scraping plan, estimated monthly costs:

| Scenario | Avg seconds/page | Total browser hours | Included free | Overage @ $0.09/hr | **Total monthly** |
|----------|-----------------|--------------------|--------------|--------------------|------------------|
| Best case | 3s | 12 hrs | 10 hrs | $0.18 | **$5.18** |
| Likely case | 5s | 20 hrs | 10 hrs | $0.90 | **$5.90** |
| Conservative | 8s | 32 hrs | 10 hrs | $1.98 | **$6.98** |
| Worst case | 15s | 60 hrs | 10 hrs | $4.50 | **$9.50** |

**Required Plan**: Workers Paid ($5/mo). Free tier is unusable (100 page cap, 5 jobs/day, 10 min/day browser time, 6 req/min).

## API Call Comparison

| Approach | API Calls |
|----------|-----------|
| Legacy `/scrape` | ~14,500 individual requests |
| Cloudflare `/crawl` | ~80-230 total calls |
| **Reduction** | **>98% fewer API calls** |

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

Please ensure your changes align with the project goals and follow the established patterns.

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Cloudflare Browser Rendering API for enabling efficient large-scale scraping
- sgcarmart.com for providing the used car marketplace data
- The open-source community for tools like Polars, HTTPX, and BeautifulSoup