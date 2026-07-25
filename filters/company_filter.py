from loguru import logger
from config.settings import MUTED_COMPANIES


def is_company_muted(company: str) -> bool:
    """
    Returns True if the company name matches any entry in MUTED_COMPANIES.
    Matching is case-insensitive and checks if the muted keyword is a
    substring of the company name (e.g. "micro1" matches "micro1 AI Hiring").
    """
    if not company:
        return False
    company_lower = company.lower()
    return any(muted in company_lower for muted in MUTED_COMPANIES)


def filter_muted_companies(jobs: list) -> list:
    """
    Filters out jobs whose company is in the muted list.
    Returns the cleaned list and logs how many were dropped.
    """
    filtered = []
    dropped = 0
    for job in jobs:
        company = job.get("company", "")
        if is_company_muted(company):
            logger.debug(f"🔇 Muted company — skipping: '{company}'")
            dropped += 1
        else:
            filtered.append(job)

    if dropped:
        logger.info(f"🔇 Company filter: dropped {dropped} job(s) from muted companies.")

    return filtered
