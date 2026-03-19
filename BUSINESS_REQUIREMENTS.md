# SGCarMart Used Cars Data Scraping Project
## Business Requirements and User Stories

### Project Overview
The goal is to scrape all ~13,800+ used car listings from sgcarmart.com/used-cars using Cloudflare's `/crawl` API to extract a complete 40+ field dataset per car and store it in Polars DataFrames exported to Parquet format.

### Business Objectives
1. **Comprehensive Data Collection**: Capture all available used car listings from Singapore's largest car marketplace
2. **Structured Data Extraction**: Transform unstructured web data into a standardized, analyzable format
3. **Efficient Processing**: Leverage Cloudflare's crawl API to minimize API calls and bypass rate limiting
4. **Data Quality**: Ensure high data completeness and accuracy through validation and cleaning processes
5. **Reproducibility**: Create a maintainable scraping pipeline that can be rerun periodically

### Stakeholders
- **Data Analysts**: Will use the scraped data for market analysis, pricing trends, and vehicle valuation
- **Business Intelligence Team**: Will integrate the data into reporting dashboards and predictive models
- **Product Managers**: Will use insights to understand market dynamics and inform product decisions
- **Engineering Team**: Responsible for implementing and maintaining the scraping pipeline

## Functional Requirements

### 1. Environment Setup
**Description**: Configure the development environment with necessary dependencies and credentials
**Acceptance Criteria**:
- Python packages installed: httpx, polars, beautifulsoup4, python-dotenv, tqdm
- Cloudflare API credentials (.env file) properly configured with Account ID and API Token
- Credential validation successful via lightweight endpoint test

### 2. Cloudflare Crawl API Client
**Description**: Implement functions to interact with Cloudflare's asynchronous `/crawl` API
**Acceptance Criteria**:
- `submit_crawl()` function successfully posts crawl job and returns job_id
- `poll_crawl()` function retrieves status and results with cursor-based pagination
- `cancel_crawl()` function provides emergency stop capability
- Proper error handling for API failures and network issues

### 3. Test Crawl Validation
**Description**: Validate the crawling approach with a small-scale test before full execution
**Acceptance Criteria**:
- Test crawl successfully renders JS-heavy listing pages
- Crawler discovers and follows links to detail pages (`/used-cars/info/...`)
- RSC payload extraction from `self.__next_f.push()` works correctly
- Browser time usage measured for cost extrapolation
- Cloudflare crawler user-agent not blocked by sgcarmart.com
- No URLs disallowed by robots.txt

### 4. Fallback Mechanism (if needed)
**Description**: Hybrid approach using `/scrape` for URL seeding if crawl link discovery fails
**Acceptance Criteria**:
- Single `/scrape` call extracts total count from first listing page
- All listing page URLs generated correctly
- Batch crawl jobs submitted with appropriate depth settings
- Approach reduces API calls compared to individual `/scrape` per page

### 5. Full Production Crawl
**Description**: Execute the complete crawl job for all used car listings
**Acceptance Criteria**:
- Production crawl job submitted with correct parameters (limit=15000, depth=2)
- Monitoring loop tracks progress and browser time usage
- Results retrieved via cursor-paginated GET requests
- Server-side persistence leveraged for 14-day result availability
- Proper handling of terminal job statuses (completed, cancelled, errored)

### 6. Data Parsing and Extraction
**Description**: Parse crawled HTML to extract structured car data from RSC payloads
**Acceptance Criteria**:
- Page classification by URL pattern (detail vs listing pages)
- RSC payload extraction from `self.__next_f.push()` script tags
- JSON object identification for `ucInfoDetailData` and `ucInfoDetailTopData`
- Fallback to BeautifulSoup DOM parsing if RSC extraction fails
- Supplementary data extraction from listing pages for gap filling
- Record merging and deduplication by unique ID
- Flagging of partial records for review

### 7. Data Cleaning and Type Casting
**Description**: Transform raw extracted data into properly typed, clean format
**Acceptance Criteria**:
- All fields cast to appropriate data types (UInt32, Float64, Date, Utf8, Boolean, Categorical)
- Currency values cleaned (removal of $, commas, /yr suffixes)
- Date strings parsed to proper Date format
- Null values handled appropriately (N.A. → null)
- Whitespace stripped from string fields
- Categorical fields properly encoded
- Boolean flags converted from true/false strings

### 8. Data Validation
**Description**: Ensure data quality and integrity through validation checks
**Acceptance Criteria**:
- No null IDs in final dataset
- No zero or negative prices
- All IDs unique
- Field completeness report generated (>90% non-null for critical fields)
- Partial records (listing-only) identified and quantified

### 9. Data Export and Summary
**Description**: Export final dataset and generate summary statistics
**Acceptance Criteria**:
- Primary dataset saved as Parquet: `output/sgcarmart_used_cars_full.parquet`
- Secondary dataset saved as CSV: `output/sgcarmart_used_cars_full.csv`
- Optional raw HTML saved: `output/raw_crawl_results.parquet`
- Summary statistics printed including:
  - Total listings scraped (detail vs listing-only)
  - Field completeness percentages
  - Top 10 brands by count
  - Price, depreciation, OMV distributions
  - Vehicle type, fuel type, transmission breakdowns
  - COE value distribution
  - Browser seconds used and estimated cost

## Non-Functional Requirements

### Performance
- Complete crawl job within Cloudflare's 7-day timeout (estimated 20 hours at 5s/page)
- Efficient memory usage through streaming batch processing (500 records at a time)
- Minimize API calls (~80-230 vs 14,500 individual scrapes)
- Optimize browser time by rejecting resource-heavy types (images, CSS, fonts)

### Reliability
- Server-side result persistence for 14 days eliminates need for client checkpoints
- Resume capability: restart notebook and continue polling/retrieving with same job_id
- Fallback mechanisms for link discovery failures and RSC format changes
- Error handling for network issues, API failures, and parsing problems

### Scalability
- Design accommodates future increases in listing volume
- Modular functions allow for easy adjustment of crawl parameters
- Cursor-based pagination handles large result sets (>10 MB)
- Incremental Parquet append prevents memory pressure

### Security
- No hardcoded credentials; all secrets in `.env` file
- Input validation for URL patterns and API parameters
- Secure handling of HTML content to prevent injection risks
- Compliance with Cloudflare API usage policies

### Maintainability
- Well-documented notebook with clear cell separation by phase
- Modular function design for easy testing and modification
- Clear error messages and logging for troubleshooting
- Configuration externalized to environment variables

## User Stories

---

### US-1: Set Up Scraping Environment
**As a** Data Engineer
**I want to** set up the scraping environment quickly
**So that** I can begin development without delays.

**Priority:** High | **Story Points:** 2 | **Labels:** `data-engineer`, `setup`

#### Acceptance Criteria
- [ ] **AC1:** All required Python packages are installed and importable:
  - `httpx` for HTTP requests
  - `polars` for DataFrame operations
  - `beautifulsoup4` for HTML parsing
  - `python-dotenv` for environment management
  - `tqdm` for progress monitoring
- [ ] **AC2:** `.env` file exists with valid Cloudflare credentials:
  - `CLOUDFLARE_ACCOUNT_ID` is set and non-empty
  - `CLOUDFLARE_API_TOKEN` is set and non-empty
- [ ] **AC3:** Credentials are validated successfully via a lightweight API test call
- [ ] **AC4:** `.env` is listed in `.gitignore` to prevent accidental commits
- [ ] **AC5:** Python version is 3.8 or higher

#### Definition of Done
- Environment setup documented in README
- `pip list` shows all required packages
- Test API call returns successful response

---

### US-2: Test Crawl API Approach on Small Scale
**As a** Data Engineer
**I want to** test the crawl API approach on a small scale
**So that** I can validate the method before committing resources.

**Priority:** High | **Story Points:** 5 | **Labels:** `data-engineer`, `testing`

#### Acceptance Criteria
- [ ] **AC1:** Test crawl job is submitted with limited scope (max 5 pages)
- [ ] **AC2:** JavaScript rendering is verified - dynamic content appears in results
- [ ] **AC3:** Link discovery works - detail page URLs (`/used-cars/info/...`) are found
- [ ] **AC4:** RSC payload extraction from `self.__next_f.push()` succeeds
- [ ] **AC5:** Browser time is measured and recorded for cost estimation
- [ ] **AC6:** Cloudflare crawler user-agent is not blocked by sgcarmart.com
- [ ] **AC7:** No URLs are disallowed by robots.txt for crawling
- [ ] **AC8:** Test results are saved for reference and validation

#### Definition of Done
- Test crawl completes with status "completed"
- At least 3 detail pages discovered and rendered
- Browser time logged and cost estimate calculated
- Test results documented in notebook

---

### US-3: Implement Fallback Approach
**As a** Data Engineer
**I want to** fall back to a hybrid approach if automatic link discovery fails
**So that** I can still complete the scraping task efficiently.

**Priority:** Medium | **Story Points:** 3 | **Labels:** `data-engineer`, `fallback`

#### Acceptance Criteria
- [ ] **AC1:** Single `/scrape` call extracts total listing count from first page
- [ ] **AC2:** All listing page URLs are generated correctly based on pagination
- [ ] **AC3:** Batch crawl jobs are submitted with appropriate parameters (depth=1)
- [ ] **AC4:** Total API calls using fallback approach is <500 (vs 14,500 individual scrapes)
- [ ] **AC5:** Results are processed using the same pipeline as full crawl approach
- [ ] **AC6:** Fallback is automatically triggered when link discovery fails
- [ ] **AC7:** Fallback execution time is within acceptable limits (<24 hours)

#### Definition of Done
- Fallback function implemented and tested
- URL generation produces correct page offsets
- Batch crawl submits successfully
- Cost comparison documented (fallback vs primary approach)

---

### US-4: Execute Full Production Crawl
**As a** Data Engineer
**I want to** execute the full production crawl
**So that** I can collect all available used car listings.

**Priority:** High | **Story Points:** 5 | **Labels:** `data-engineer`, `crawling`

#### Acceptance Criteria
- [ ] **AC1:** Production crawl job is submitted with correct parameters:
  - Target URL: `https://www.sgcarmart.com/used-cars`
  - Limit: 15,000 pages
  - Depth: 2 (listing → detail pages)
- [ ] **AC2:** Monitoring loop tracks job status at regular intervals (every 5 minutes)
- [ ] **AC3:** Browser time usage is logged throughout execution
- [ ] **AC4:** Results are retrieved via cursor-paginated GET requests
- [ ] **AC5:** Server-side persistence is verified (results available for 14 days)
- [ ] **AC6:** Terminal job statuses are handled appropriately:
  - `completed`: proceed to parsing
  - `cancelled`: log reason, optionally restart
  - `errored`: log error details, investigate
- [ ] **AC7:** Job ID is persisted for resume capability

#### Definition of Done
- Crawl job completes with status "completed"
- Total pages crawled >13,000
- Browser time recorded and within budget
- Job ID saved to file for recovery

---

### US-5: Parse Crawled HTML into Structured Data
**As a** Data Engineer
**I want to** parse the crawled HTML into structured data
**So that** I can analyze the used car market.

**Priority:** High | **Story Points:** 8 | **Labels:** `data-engineer`, `parsing`

#### Acceptance Criteria
- [ ] **AC1:** Pages are correctly classified by URL pattern:
  - Detail pages: `/used-cars/info/`
  - Listing pages: `/used-cars/` (with pagination params)
- [ ] **AC2:** RSC payloads are extracted from `self.__next_f.push()` script tags
- [ ] **AC3:** JSON objects for car data are identified and parsed:
  - `ucInfoDetailData`: main car specifications
  - `ucInfoDetailTopData`: pricing and top-level info
- [ ] **AC4:** BeautifulSoup DOM parsing works as fallback when RSC extraction fails
- [ ] **AC5:** Listing page data is extracted for gap filling (dealer info, status)
- [ ] **AC6:** Records are merged and deduplicated by unique car ID
- [ ] **AC7:** Partial records (listing-only, no detail page) are flagged for review
- [ ] **AC8:** All 49 target fields are mapped from source data
- [ ] **AC9:** Parsing success rate is >95% for detail pages

#### Definition of Done
- Polars DataFrame created with all parsed records
- Record count matches crawled page expectations
- Partial records quantified in summary
- Field mapping documented

---

### US-6: Clean and Validate Extracted Data
**As a** Data Engineer
**I want to** clean and validate the extracted data
**So that** I can ensure data quality for analysis.

**Priority:** High | **Story Points:** 5 | **Labels:** `data-engineer`, `validation`

#### Acceptance Criteria
- [ ] **AC1:** All fields are cast to appropriate data types:
  - Integer fields: `UInt32` (id, mileage, engine_capacity, etc.)
  - Float fields: `Float64` (price, depreciation, omv, etc.)
  - Date fields: `Date` (registration_date, manufactured, etc.)
  - String fields: `Utf8` (make, model, description, etc.)
  - Boolean fields: `Boolean` (is_buysafe, is_imported_used, etc.)
  - Categorical fields: `Categorical` (fuel_type, transmission, etc.)
- [ ] **AC2:** Currency values are cleaned:
  - `$` prefix removed
  - Commas removed from numbers
  - `/yr` suffix removed from annual values
- [ ] **AC3:** Date strings are parsed to proper Date format (YYYY-MM-DD)
- [ ] **AC4:** Null values are handled appropriately:
  - `N.A.` converted to `null`
  - Empty strings converted to `null`
- [ ] **AC5:** Whitespace is stripped from all string fields
- [ ] **AC6:** Categorical fields are properly encoded
- [ ] **AC7:** Boolean flags are converted from string representation
- [ ] **AC8:** Validation checks pass:
  - Zero null IDs in final dataset
  - Zero negative or zero prices
  - All IDs are unique
- [ ] **AC9:** Field completeness report is generated
- [ ] **AC10:** Critical fields have >90% completeness (price, make, model, year)

#### Definition of Done
- Clean DataFrame passes all validation checks
- Data quality report generated
- No type errors when loading Parquet file
- Summary statistics are sensible

---

### US-7: Provide Access to Scraped Data
**As a** Data Analyst
**I want to** access the scraped data in a standard format
**So that** I can perform market analysis.

**Priority:** High | **Story Points:** 3 | **Labels:** `data-analyst`, `export`

#### Acceptance Criteria
- [ ] **AC1:** Primary dataset is saved as compressed Parquet file:
  - Path: `output/sgcarmart_used_cars_full.parquet`
  - Compression: `snappy` or `gzip`
  - Schema includes all 49 fields with correct types
- [ ] **AC2:** Secondary dataset is saved as CSV for accessibility:
  - Path: `output/sgcarmart_used_cars_full.csv`
  - UTF-8 encoding
  - Proper handling of special characters
- [ ] **AC3:** Raw HTML is optionally saved for re-parsing:
  - Path: `output/raw_crawl_results.parquet`
  - Contains URL, HTML content, and metadata
- [ ] **AC4:** Summary statistics are generated and displayed:
  - Total listings scraped (detail vs listing-only)
  - Field completeness percentages
  - Top 10 brands by count
  - Price distribution (min, max, mean, median, quartiles)
  - Depreciation distribution
  - OMV distribution
  - Vehicle type breakdown
  - Fuel type breakdown
  - Transmission breakdown
  - COE value distribution
- [ ] **AC5:** Browser seconds used and estimated cost are reported
- [ ] **AC6:** Data is importable into common analysis tools (Pandas, SQL, Excel)

#### Definition of Done
- Both Parquet and CSV files exist in output directory
- Files are loadable without errors
- Summary report is readable and complete
- Cost breakdown is documented

---

### US-8: Understand Cost Implications
**As a** Business Stakeholder
**I want to** understand the cost implications
**So that** I can budget appropriately for the scraping project.

**Priority:** Medium | **Story Points:** 2 | **Labels:** `business-stakeholder`, `cost`

#### Acceptance Criteria
- [ ] **AC1:** Estimated browser time is calculated based on:
  - Total pages to crawl (~15,000)
  - Average time per page (5 seconds)
  - Total estimated browser time (~20 hours)
- [ ] **AC2:** Overage costs are determined:
  - Free tier limit: 50 hours/month
  - Overage rate: $0.05 per minute
  - Expected overage: $0 (within free tier)
- [ ] **AC3:** Monthly cost estimate scenarios are provided:
  | Scenario | Pages | Time/Page | Browser Hours | Monthly Cost |
  |----------|-------|-----------|---------------|--------------|
  | Best Case | 13,000 | 4s | 14.4h | $5.00 |
  | Likely | 14,000 | 5s | 19.4h | $5.00 |
  | Conservative | 15,000 | 6s | 25.0h | $5.00 |
  | Worst Case | 16,000 | 8s | 35.6h | $5.00 |
- [ ] **AC4:** Cost comparison to legacy approach is documented:
  - Legacy: 14,500 individual `/scrape` calls = $40+/month
  - New: 1 `/crawl` call = $5/month
  - Savings: >87%
- [ ] **AC5:** Cost implications of incremental updates are explained
- [ ] **AC6:** Budget recommendations for monthly scraping schedule

#### Definition of Done
- Cost analysis document is complete
- All scenarios have calculated costs
- Comparison table is accurate
- Recommendations are actionable

---

### US-9: Ensure Pipeline Reliability and Maintainability
**As a** System Administrator
**I want** the scraping pipeline to be reliable and maintainable
**So that** I can ensure ongoing operation.

**Priority:** Medium | **Story Points:** 3 | **Labels:** `sysadmin`, `reliability`

#### Acceptance Criteria
- [ ] **AC1:** Server-side persistence eliminates need for client-side checkpoints:
  - Results stored on Cloudflare for 14 days
  - Job ID can be used to retrieve results after interruption
- [ ] **AC2:** Resume capability is implemented:
  - Notebook can be restarted and continue from last job ID
  - No data loss on interruption
  - Progress tracking survives restart
- [ ] **AC3:** Fallback mechanisms exist for common failure modes:
  - Link discovery failure → URL seeding fallback
  - RSC parsing failure → BeautifulSoup fallback
  - API rate limiting → automatic retry with backoff
- [ ] **AC4:** Incremental update capability is designed:
  - Can specify date ranges for new listings
  - Can merge with existing dataset
  - Deduplication handles re-runs
- [ ] **AC5:** Error handling is comprehensive:
  - Network errors are caught and logged
  - API errors include status code and message
  - Parsing errors include problematic URL/content reference
  - Critical errors halt execution with clear message
- [ ] **AC6:** Logging provides visibility:
  - Progress updates at regular intervals
  - Browser time consumption tracked
  - Record counts at each stage
  - Error details with timestamps
- [ ] **AC7:** Documentation is complete:
  - README with setup instructions
  - Notebook cells clearly labeled by phase
  - Function docstrings for all modules
  - Troubleshooting guide for common issues

#### Definition of Done
- Pipeline survives network interruption
- Resume from job ID works correctly
- All fallback paths tested
- Documentation is current and complete
- Error messages are actionable

## Dependencies
1. **Cloudflare Workers Paid Plan** ($5/month) - Required for sufficient browser time and API limits
2. **Python 3.8+** - Core programming language
3. **Required Python Packages**:
   - httpx: HTTP client for API requests
   - polars: DataFrame manipulation and Parquet export
   - beautifulsoup4: HTML parsing fallback
   - python-dotenv: Environment variable management
   - tqdm: Progress bars for monitoring

## Success Metrics
1. **Data Completeness**: >95% of expected ~13,800 listings collected
2. **Field Completeness**: >90% non-null for critical fields (price, make, model, year, mileage)
3. **API Efficiency**: <1% of API calls compared to legacy `/scrape` approach
4. **Cost Effectiveness**: Total monthly cost <$10 (including Cloudflare plan)
5. **Execution Time**: Complete crawl within 24 hours
6. **Data Quality**: Validation checks pass with minimal partial records
7. **Usability**: Data readily importable into analysis tools (Pandas, SQL, etc.)

## Risks and Mitigations
| Risk | Probability | Impact | Mitigation |
|------|-------------|--------|------------|
| Cloudflare crawler blocked by sgcarmart.com | Low | High | Test crawl checks for disallowed status; fallback to custom UA `/scrape` |
| Pagination links not discoverable via JS rendering | Medium | Medium | Test crawl validates link discovery; fallback to Phase 2b URL seeding |
| RSC payload format changes | Low | Medium | BeautifulSoup HTML DOM fallback per page |
| Crawl job timeout/exceeding limits | Low | High | Monitor browser time; well within 7-day limit at estimated rates |
| Large result set memory issues | Low | Medium | Cursor-based pagination and streaming batch processing |
| Site rate blocking mid-crawl | Low | Low | Cloudflare manages pacing server-side with real Chromium browser |

## Open Questions
1. What is the exact frequency for re-running this scrape (daily, weekly, monthly)?
2. Should we implement incremental scraping to only get new/updated listings?
3. Are there specific data fields that require additional validation or enrichment?
4. What downstream systems will consume this data, and what are their format requirements?
5. Should we create a scheduling mechanism (cron job) for automated regular scraping?

## Next Steps
1. Review and approve this business requirements document
2. Set up development environment with required dependencies
3. Obtain Cloudflare API credentials with appropriate permissions
4. Implement and test Phase 0 (Environment Setup)
5. Proceed with Phase 1 (Crawl API Client) implementation
6. Execute Phase 2 (Test Crawl) to validate approach
7. Based on test results, proceed with either full crawl or fallback approach
8. Complete data parsing, cleaning, validation, and export phases
9. Review summary statistics and data quality metrics
10. Deliver final dataset to stakeholders for analysis