# GitHub Issues for SGCarMart Used Cars Data Scraper

This file contains all user stories formatted as GitHub issues that can be imported into a GitHub project board.

## Issues

### Environment Setup
**Title**: Set up scraping environment quickly
**Description**: As a Data Engineer, I want to set up the scraping environment quickly so that I can begin development without delays.
**Tasks**:
- Install required Python packages: httpx, polars, beautifulsoup4, python-dotenv, tqdm
- Configure Cloudflare API credentials in .env file
- Validate credentials with test endpoint
**Labels**: enhancement, data-engineer
**Estimate**: 1 hour

### Test Crawl Validation
**Title**: Test crawl API approach on small scale
**Description**: As a Data Engineer, I want to test the crawl API approach on a small scale so that I can validate the method before committing resources.
**Tasks**:
- Submit test crawl job with limited pages (30 pages)
- Verify JavaScript rendering works
- Confirm link discovery between listing and detail pages
- Validate RSC payload extraction
- Measure browser time for cost estimation
- Check for crawler blocking or robots.txt restrictions
**Labels**: enhancement, data-engineer, testing
**Estimate**: 2 hours

### Fallback Mechanism
**Title**: Implement fallback approach if automatic link discovery fails
**Description**: As a Data Engineer, I want to fall back to a hybrid approach if automatic link discovery fails so that I can still complete the scraping task efficiently.
**Tasks**:
- Use `/scrape` to get total count from first page
- Generate all listing page URLs
- Submit batch crawl jobs with depth=1
- Process results similarly to full crawl approach
**Labels**: enhancement, data-engineer, fallback
**Estimate**: 2 hours

### Full Production Crawl
**Title**: Execute full production crawl
**Description**: As a Data Engineer, I want to execute the full production crawl so that I can collect all available used car listings.
**Tasks**:
- Submit production crawl job with appropriate parameters (limit=15000, depth=2)
- Monitor job status and progress
- Retrieve results via cursor-paginated requests
- Handle job completion or failure appropriately
**Labels**: enhancement, data-engineer, crawling
**Estimate**: 2 hours coding + ~20 hours crawl time

### Data Parsing and Extraction
**Title**: Parse crawled HTML into structured data
**Description**: As a Data Engineer, I want to parse the crawled HTML into structured data so that I can analyze the used car market.
**Tasks**:
- Classify pages by URL pattern (detail vs listing pages)
- Extract RSC payloads from script tags
- Parse JSON objects for car specifications and pricing data
- Extract dealer/metadata from listing-level RSC
- Use BeautifulSoup fallback if needed
- Extract supplementary data from listing pages
- Merge and deduplicate records by ID
- Flag partial records for review
**Labels**: enhancement, data-engineer, parsing
**Estimate**: 5 hours

### Data Cleaning and Validation
**Title**: Clean and validate extracted data
**Description**: As a Data Engineer, I want to clean and validate the extracted data so that I can ensure data quality for analysis.
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
**Labels**: enhancement, data-engineer, validation
**Estimate**: 3 hours

### Data Access for Analysis
**Title**: Provide access to scraped data for market analysis
**Description**: As a Data Analyst, I want to access the scraped data in a standard format so that I can perform market analysis.
**Tasks**:
- Save final dataset as compressed Parquet file
- Provide CSV version for accessibility
- Optionally save raw HTML for re-parsing
- Generate summary statistics for data understanding
- Provide insights on price distributions, vehicle types, etc.
**Labels**: enhancement, data-analyst
**Estimate**: 2 hours

### Cost Analysis
**Title**: Understand cost implications for budgeting
**Description**: As a Business Stakeholder, I want to understand the cost implications so that I can budget appropriately for the scraping project.
**Tasks**:
- Calculate estimated browser time based on pages and time-per-page
- Determine overage costs beyond free tier limits
- Provide monthly cost estimate scenarios (best, likely, conservative, worst case)
- Compare to legacy approach cost
**Labels**: enhancement, business-stakeholder
**Estimate**: 1 hour

### System Reliability and Maintenance
**Title**: Ensure scraping pipeline is reliable and maintainable
**Description**: As a System Administrator, I want the scraping pipeline to be reliable and maintainable so that I can ensure ongoing operation.
**Tasks**:
- Implement server-side persistence to eliminate client checkpoints
- Provide resume capability after interruptions
- Include fallback mechanisms for common failure modes
- Design for incremental updates and re-runs
- Ensure proper error handling and logging
**Labels**: enhancement, sysadmin, reliability
**Estimate**: 2 hours

## Suggested GitHub Project Board Columns

1. **Backlog** - All issues start here
2. **To Do** - Issues ready to work on
3. **In Progress** - Currently being worked on
4. **Review** - Waiting for code review or validation
5. **Done** - Completed issues

## Suggested Labels

- **Type**: enhancement, bug, documentation
- **Role**: data-engineer, data-analyst, business-stakeholder, sysadmin
- **Phase**: environment-setup, testing, crawling, parsing, validation, export
- **Priority**: high, medium, low
- **Estimate**: 1hr, 2hrs, 3hrs, 4hrs, 5hrs+, etc.