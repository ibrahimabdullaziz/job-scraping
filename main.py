import asyncio
import sys
import os
import random
import uuid
from aiohttp import web
from loguru import logger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from config.settings import (
    LOG_LEVEL, SCRAPE_INTERVAL_MINUTES, LINKEDIN_KEYWORDS, LINKEDIN_LOCATIONS,
    DEDUP_TTL_DAYS, LINKEDIN_DELAY_MIN, LINKEDIN_DELAY_MAX, LINKEDIN_MAX_CONCURRENCY
)
from database.repository import JobRepository
from scraper.linkedin import scrape_linkedin_jobs
from scraper.remotive import scrape_remotive_jobs
from scraper.himalayas import scrape_himalayas_jobs
from scraper.wuzzuf import scrape_wuzzuf_jobs
from scraper.gulftalent import scrape_gulftalent_jobs
from telegram_bot.notifier import TelegramNotifier
from filters.company_filter import filter_muted_companies

# Configure Loguru
logger.remove()
logger.add(sys.stdout, level=LOG_LEVEL, format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{message}</cyan>")

async def run_scraper_batch(tasks, semaphore, delay=0.5, cooldown_event=None):
    """Run a batch of scraper coroutines with bounded concurrency and delay."""
    results = []
    
    async def bounded(coro):
        async with semaphore:
            # If a global cooldown is active (e.g. LinkedIn 429), wait for it to clear
            if cooldown_event is not None and cooldown_event.is_set():
                logger.debug("Cooldown active — waiting before starting next task...")
                while cooldown_event.is_set():
                    await asyncio.sleep(1.0)
            result = await coro
            await asyncio.sleep(delay)
            return result
    
    wrapped = [bounded(t) for t in tasks]
    raw = await asyncio.gather(*wrapped, return_exceptions=True)
    
    for res in raw:
        if isinstance(res, list):
            results.extend(res)
        elif isinstance(res, Exception):
            logger.error(f"A scraper failed: {res}")
    
    return results

async def scrape_and_notify(repo: JobRepository, notifier: TelegramNotifier):
    """Main pipeline execution for a single scrape cycle."""
    cycle_id = uuid.uuid4().hex[:6]
    logger.info(f"[{cycle_id}] Starting scrape cycle — {len(LINKEDIN_KEYWORDS)} keywords × {len(LINKEDIN_LOCATIONS)} locations")
    
    all_jobs = []
    
    # ── Cleanup expired dedup entries BEFORE scraping ──
    repo.cleanup_old_jobs(days=DEDUP_TTL_DAYS)
    
    # ── Phase 1: LinkedIn (most rate-limited — low concurrency + longer delay) ──
    linkedin_tasks = []
    cooldown_event = asyncio.Event()  # Shared 429 cooldown across all LinkedIn tasks
    for loc in LINKEDIN_LOCATIONS:
        for key in LINKEDIN_KEYWORDS:
            linkedin_tasks.append(scrape_linkedin_jobs(keyword=key, location=loc, max_pages=1, cooldown_event=cooldown_event))
    
    delay = random.uniform(LINKEDIN_DELAY_MIN, LINKEDIN_DELAY_MAX)
    logger.info(f"[{cycle_id}] Phase 1: LinkedIn — {len(linkedin_tasks)} tasks ({LINKEDIN_MAX_CONCURRENCY} concurrent, {delay:.1f}s delay)...")
    linkedin_sem = asyncio.Semaphore(LINKEDIN_MAX_CONCURRENCY)
    jobs = await run_scraper_batch(linkedin_tasks, linkedin_sem, delay=delay, cooldown_event=cooldown_event)
    all_jobs.extend(jobs)
    logger.info(f"[{cycle_id}] LinkedIn done — {len(jobs)} jobs collected.")
    
    # ── Phase 2: Location-aware scrapers (Wuzzuf, GulfTalent) ──
    regional_tasks = []
    for loc in LINKEDIN_LOCATIONS:
        for key in LINKEDIN_KEYWORDS:
            regional_tasks.append(scrape_wuzzuf_jobs(keyword=key, location=loc, max_results=10))
            regional_tasks.append(scrape_gulftalent_jobs(keyword=key, location=loc, max_results=10))
    
    logger.info(f"[{cycle_id}] Phase 2: Regional scrapers — {len(regional_tasks)} tasks (4 concurrent, 1.5s delay)...")
    regional_sem = asyncio.Semaphore(4)
    jobs = await run_scraper_batch(regional_tasks, regional_sem, delay=1.5)
    all_jobs.extend(jobs)
    logger.info(f"[{cycle_id}] Regional done — {len(jobs)} jobs collected.")
    
    # ── Phase 3: Location-agnostic scrapers (Remotive, Himalayas — once per keyword) ──
    remote_tasks = []
    for key in LINKEDIN_KEYWORDS:
        remote_tasks.append(scrape_remotive_jobs(keyword=key, location="Remote", max_results=10))
        remote_tasks.append(scrape_himalayas_jobs(keyword=key, location="Remote", max_results=10))
    
    logger.info(f"[{cycle_id}] Phase 3: Remote scrapers — {len(remote_tasks)} tasks (10 concurrent)...")
    remote_sem = asyncio.Semaphore(10)
    jobs = await run_scraper_batch(remote_tasks, remote_sem, delay=0.3)
    all_jobs.extend(jobs)
    logger.info(f"[{cycle_id}] Remote done — {len(jobs)} jobs collected.")
    
    # ── Summary ──
    logger.info(f"[{cycle_id}] Scraping complete. {len(all_jobs)} total raw jobs collected.")

    # ── Company blocklist filter ──
    all_jobs = filter_muted_companies(all_jobs)
    logger.info(f"[{cycle_id}] After company filter: {len(all_jobs)} jobs remain.")

    if not all_jobs:
        logger.info(f"[{cycle_id}] No jobs found this cycle across any source.")
        return

    # Store & Dedupe
    new_jobs_count = 0
    for job in all_jobs:
        if repo.insert_job(job):
            new_jobs_count += 1
            
    logger.info(f"[{cycle_id}] Deduplication complete. {new_jobs_count} brand new jobs added to database.")

    # Retrieve Pending & Notify
    unsent_jobs = repo.get_unsent_jobs()
    if unsent_jobs:
        await notifier.send_job_alerts(unsent_jobs, repo)
    else:
        logger.info(f"[{cycle_id}] No unsent jobs pending for Telegram.")

async def main():
    repo = JobRepository()
    notifier = TelegramNotifier()

    await notifier.send_startup_message()

    logger.info("Running initial scrape cycle before scheduling...")
    await scrape_and_notify(repo, notifier)

    logger.info(f"Initializing APScheduler. Interval: {SCRAPE_INTERVAL_MINUTES} minutes.")
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        scrape_and_notify,
        'interval',
        minutes=SCRAPE_INTERVAL_MINUTES,
        args=[repo, notifier],
        max_instances=1,       # Prevent overlapping cycles
        coalesce=True,         # If runs are missed, only execute once
        misfire_grace_time=300 # Tolerate up to 5 min of scheduler delay
    )
    
    scheduler.start()
    
    # --- Web Server for UptimeRobot (Render Fix) ---
    async def health_check(request):
        return web.Response(text="Bot is running!")

    app = web.Application()
    app.router.add_get('/', health_check)
    
    runner = web.AppRunner(app)
    await runner.setup()
    
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()
    logger.info(f"Web server started on port {port} for UptimeRobot pings.")
    # -----------------------------------------------

    try:
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Graceful shutdown initiated.")
        scheduler.shutdown()
        await runner.cleanup()
        logger.info("Bot stopped.")

if __name__ == "__main__":
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Exiting...")
