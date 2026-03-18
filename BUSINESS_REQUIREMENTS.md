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

### As a Data Engineer
I want to set up the scraping environment quickly so that I can begin development without delays.
**Tasks**:
- Install required Python packages
- Configure Cloudflare API credentials
- Validate credentials with test endpoint

### As a Data Engineer
I want to test the crawl API approach on a small scale so that I can validate the method before committing resources.
**Tasks**:
- Submit test crawl job with limited pages
- Verify JavaScript rendering works
- Confirm link discovery between listing and detail pages
- Validate RSC payload extraction
- Measure browser time for cost estimation
- Check for crawler blocking or robots.txt restrictions

### As a Data Engineer
I want to fall back to a hybrid approach if automatic link discovery fails so that I can still complete the scraping task efficiently.
**Tasks**:
- Use `/scrape` to get total count from first page
- Generate all listing page URLs
- Submit batch crawl jobs with depth=1
- Process results similarly to full crawl approach

### As a Data Engineer
I want to execute the full production crawl so that I can collect all available used car listings.
**Tasks**:
- Submit production crawl job with appropriate parameters
- Monitor job status and progress
- Retrieve results via cursor-paginated requests
- Handle job completion or failure appropriately

### As a Data Engineer
I want to parse the crawled HTML into structured data so that I can analyze the used car market.
**Tasks**:
- Classify pages by URL pattern
- Extract RSC payloads from script tags
- Parse JSON objects for car specifications and pricing data
- Extract dealer/metadata from listing-level RSC
- Use BeautifulSoup fallback if needed
- Extract supplementary data from listing pages
- Merge and deduplicate records by ID
- Flag partial records for review

### As a Data Engineer
I want to clean and validate the extracted data so that I can ensure data quality for analysis.
**Tasks**:
- Cast fields to appropriate data types
- Clean currency and measurement values
- Parse date strings to Date format
- Handle null values appropriately
- Strip whitespace from text fields
- Encode categorical variables
- Convert boolean flags
- Validate no null IDs, negative prices, or duplicate IDs
- Generate field completeness report
- Identify and quantify partial records

### As a Data Analyst
I want to access the scraped data in a standard format so that I can perform market analysis.
**Tasks**:
- Save final dataset as compressed Parquet file
- Provide CSV version for accessibility
- Optionally save raw HTML for re-parsing
- Generate summary statistics for data understanding
- Provide insights on price distributions, vehicle types, etc.

### As a Business Stakeholder
I want to understand the cost implications so that I can budget appropriately for the scraping project.
**Tasks**:
- Calculate estimated browser time based on pages and time-per-page
- Determine overage costs beyond free tier limits
- Provide monthly cost estimate scenarios (best, likely, conservative, worst case)
- Compare to legacy approach cost

### As a System Administrator
I want the scraping pipeline to be reliable and maintainable so that I can ensure ongoing operation.
**Tasks**:
- Implement server-side persistence to eliminate client checkpoints
- Provide resume capability after interruptions
- Include fallback mechanisms for common failure modes
- Design for incremental updates and re-runs
- Ensure proper error handling and logging

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