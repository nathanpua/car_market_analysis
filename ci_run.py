"""CI entry point for GitHub Actions workflow.

Orchestrates: download DB from R2, run daily scrape, upload DB back to R2.
Designed to be called from .github/workflows/scrape.yml.

Usage:
    python ci_run.py              # full run with R2 persistence
    python ci_run.py --dry-run    # run without R2 upload (local testing)
"""

import argparse
import logging
import os
import sys
from datetime import datetime


def main():
    parser = argparse.ArgumentParser(description="CI entry point for SGCarMart scraper")
    parser.add_argument("--dry-run", action="store_true",
                        help="Run without uploading to R2")
    parser.add_argument("--max-pages", type=int, default=None,
                        help="Max listing pages for Phase 1")
    parser.add_argument("--detail-limit", type=int, default=None,
                        help="Max detail pages for Phase 2")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        force=True,
    )

    print(f"\n{'=' * 60}")
    print(f"  CI RUN — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")
    print(f"  Dry run:     {args.dry_run}")
    print(f"  Max pages:   {args.max_pages or 'all'}")
    print(f"  Detail limit: {args.detail_limit or 'all'}\n")

    # Proxy connectivity test
    proxy_url = os.environ.get("SCRAPER_PROXY", "").strip()
    if proxy_url:
        print("=" * 40)
        print("  PROXY: Testing connectivity")
        print("=" * 40)
        from urllib.parse import urlparse
        parsed = urlparse(proxy_url)
        print(f"  Host: {parsed.hostname}:{parsed.port}")
        try:
            from scrapling.fetchers import AsyncFetcher
            import asyncio
            async def _test():
                return await AsyncFetcher.get("https://httpbin.org/ip", proxy=proxy_url, timeout=10000)
            page = asyncio.run(_test())
            if page.status == 200:
                print(f"  Proxy OK: {page.text_content.strip()}")
            else:
                print(f"  Proxy returned status {page.status}")
        except Exception as e:
            print(f"  Proxy test failed: {e}")
        print()
    else:
        print("  Proxy: not configured (direct connection)\n")

    scrape_failed = False

    # Step 1: Download DB from R2
    if not args.dry_run:
        print("=" * 40)
        print("  STORAGE: Download DB from R2")
        print("=" * 40)
        from storage import download_db
        db_path = download_db()
        print(f"  DB path: {db_path}\n")
    else:
        print("  [DRY RUN] Skipping R2 download\n")

    # Step 2: Run daily scrape
    print("=" * 40)
    print("  SCRAPE: Run daily scrape")
    print("=" * 40)
    try:
        from scheduler import run_daily_scrape
        result = run_daily_scrape(
            max_pages=args.max_pages,
            detail_limit=args.detail_limit,
            validate=True,
            track_history=True,
        )
        if "failed" in result.get("phase1", "") or "failed" in result.get("phase2", ""):
            scrape_failed = True
    except Exception as e:
        logging.error("Daily scrape failed: %s", e)
        scrape_failed = True

    # Step 3: Upload DB to R2 (always, even on scrape failure)
    if not args.dry_run:
        print("\n" + "=" * 40)
        print("  STORAGE: Upload DB to R2")
        print("=" * 40)
        from storage import upload_db
        upload_db()
        print("  Upload complete\n")
    else:
        print("  [DRY RUN] Skipping R2 upload\n")

    # Exit with non-zero if scrape failed (so GitHub Actions marks it as failed)
    if scrape_failed:
        print("  STATUS: FAILED (DB uploaded with partial progress)")
        sys.exit(1)
    else:
        print("  STATUS: SUCCESS")


if __name__ == "__main__":
    main()
