"""Daily scheduler for SGCarMart scraper.

Provides:
- run_daily_scrape(): one-shot execution of Phase 1 + Phase 2
- start_scheduler(): daemon with APScheduler cron (timezone-aware)
"""

import logging
import signal
import sys
from datetime import datetime

logger = logging.getLogger(__name__)


def run_daily_scrape(
    max_pages: int | None = None,
    detail_limit: int | None = None,
    validate: bool = True,
    track_history: bool = False,
) -> dict:
    """Run both phases in sequence and print summary.

    Returns a dict with phase1 and phase2 status.
    """
    from scrape_listing import scrape, scrape_details

    print(f"\n{'=' * 60}")
    print(f"  DAILY SCRAPE — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'=' * 60}")
    print(f"  Max pages:      {max_pages or 'all'}")
    print(f"  Detail limit:   {detail_limit or 'all'}")
    print(f"  Validate:       {validate}")
    print(f"  Track history:  {track_history}\n")

    start_time = datetime.now()

    # Phase 1: URL discovery
    print("=" * 40)
    print("  PHASE 1: URL Discovery")
    print("=" * 40)
    try:
        scrape(max_pages=max_pages)
        phase1_status = "completed"
    except Exception as e:
        logger.error("Phase 1 failed: %s", e)
        phase1_status = f"failed: {e}"

    # Phase 2: Detail scraping
    print("\n" + "=" * 40)
    print("  PHASE 2: Detail Scraping")
    print("=" * 40)
    try:
        scrape_details(
            limit=detail_limit,
            validate=validate,
            track_history=track_history,
        )
        phase2_status = "completed"
    except Exception as e:
        logger.error("Phase 2 failed: %s", e)
        phase2_status = f"failed: {e}"

    elapsed = (datetime.now() - start_time).total_seconds()

    # Summary
    print(f"\n{'=' * 60}")
    print(f"  DAILY SCRAPE SUMMARY")
    print(f"{'=' * 60}")
    print(f"  Phase 1: {phase1_status}")
    print(f"  Phase 2: {phase2_status}")
    print(f"  Total elapsed: {elapsed:.0f}s ({elapsed / 60:.1f} min)")
    print(f"  Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    return {"phase1": phase1_status, "phase2": phase2_status, "elapsed": elapsed}


def start_scheduler(
    time: str = "03:00",
    max_pages: int | None = None,
    detail_limit: int | None = None,
    validate: bool = True,
    track_history: bool = False,
):
    """Start APScheduler daemon with daily cron job.

    Args:
        time: HH:MM format for daily run (Asia/Singapore timezone)
        max_pages: max listing pages for Phase 1
        detail_limit: max detail pages for Phase 2
        validate: enable field validation
        track_history: enable SCD Type 2 tracking
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
        from apscheduler.triggers.cron import CronTrigger
    except ImportError:
        print("APScheduler not installed. Run: pip install apscheduler>=3.10.0")
        sys.exit(1)

    parts = time.split(":")
    hour, minute = int(parts[0]), int(parts[1])

    scheduler = BlockingScheduler(timezone="Asia/Singapore")
    scheduler.add_executor("processpool")

    scheduler.add_job(
        run_daily_scrape,
        trigger=CronTrigger(hour=hour, minute=minute, timezone="Asia/Singapore"),
        id="daily_scrape",
        name="SGCarMart Daily Scrape",
        max_instances=1,
        kwargs={
            "max_pages": max_pages,
            "detail_limit": detail_limit,
            "validate": validate,
            "track_history": track_history,
        },
    )

    # Graceful shutdown
    def shutdown(signum, frame):
        print("\nShutting down scheduler...")
        scheduler.shutdown(wait=False)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    print(f"Scheduler started. Daily scrape at {time} (Asia/Singapore)")
    print(f"  Max pages:     {max_pages or 'all'}")
    print(f"  Detail limit:  {detail_limit or 'all'}")
    print(f"  Validate:      {validate}")
    print(f"  Track history: {track_history}")
    print(f"  Press Ctrl+C to stop\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("Scheduler stopped.")
