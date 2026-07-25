import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# LinkedIn Scraping Settings (Comma-separated)
_keywords = os.getenv("LINKEDIN_KEYWORDS", "frontend, backend, fullstack, node, react, developer, tester, ui, ux, ai, .net, php, python, java, golang, go, c++, ruby, rust, angular, vue, django, spring, ios, android, flutter, react native, swift, kotlin, devops, machine learning, ml")
LINKEDIN_KEYWORDS = [k.strip() for k in _keywords.split(",") if k.strip()]

_locations = os.getenv("LINKEDIN_LOCATIONS", "Egypt, Saudi Arabia, United States")
LINKEDIN_LOCATIONS = [loc.strip() for loc in _locations.split(",") if loc.strip()]

# Application Settings
try:
    SCRAPE_INTERVAL_MINUTES = int(os.getenv("SCRAPE_INTERVAL_MINUTES", "5"))
except ValueError:
    SCRAPE_INTERVAL_MINUTES = 5

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Deduplication TTL — jobs older than this are purged so they can re-surface
try:
    DEDUP_TTL_DAYS = int(os.getenv("DEDUP_TTL_DAYS", "7"))
except ValueError:
    DEDUP_TTL_DAYS = 7

# LinkedIn rate-limit tuning
try:
    LINKEDIN_DELAY_MIN = float(os.getenv("LINKEDIN_DELAY_MIN", "3.0"))
    LINKEDIN_DELAY_MAX = float(os.getenv("LINKEDIN_DELAY_MAX", "6.0"))
    LINKEDIN_MAX_CONCURRENCY = int(os.getenv("LINKEDIN_MAX_CONCURRENCY", "2"))
except ValueError:
    LINKEDIN_DELAY_MIN = 3.0
    LINKEDIN_DELAY_MAX = 6.0
    LINKEDIN_MAX_CONCURRENCY = 2

# Muted / blocked companies — jobs from these will be silently dropped
# You can override via .env: MUTED_COMPANIES="micro1,Hire Feed,Jobs AI,Hired"
_muted = os.getenv("MUTED_COMPANIES", "micro1, Hire Feed, Jobs AI, Hired, micro1 AI")
MUTED_COMPANIES = [c.strip().lower() for c in _muted.split(",") if c.strip()]

# Database Setting
DB_PATH = "jobs.db"

