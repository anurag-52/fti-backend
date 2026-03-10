"""
Daily Automated Scheduler
4:30 PM — BSE Bhav Copy pull
5:00 PM — Signal engine run
5:30 PM — 5 PM confirmation emails
6:00 PM — 6 PM recommendation emails
"""

import logging
from datetime import date
from database import SessionLocal
from services.data_service import daily_data_pull
from services.engine import run_signal_engine
from services.email_service import send_recommendation_emails

logger = logging.getLogger(__name__)

BSE_HOLIDAYS_2025_2026 = {
    date(2025, 1, 26), date(2025, 3, 14), date(2025, 4, 10),
    date(2025, 4, 14), date(2025, 4, 18), date(2025, 5, 1),
    date(2025, 8, 15), date(2025, 8, 27), date(2025, 10, 2),
    date(2025, 10, 2), date(2025, 10, 24), date(2025, 11, 5),
    date(2025, 12, 25),
    date(2026, 1, 26), date(2026, 3, 20), date(2026, 4, 3),
    date(2026, 4, 14), date(2026, 8, 15), date(2026, 10, 2),
    date(2026, 12, 25),
}

def is_trading_day(d: date = None) -> bool:
    """Returns True if d is a BSE trading day (Mon-Fri, not holiday)"""
    if d is None:
        d = date.today()
    if d.weekday() >= 5:     # Saturday=5, Sunday=6
        return False
    return d not in BSE_HOLIDAYS_2025_2026

async def run_daily_jobs():
    """Main daily job — runs at 4:30 PM IST"""
    today = date.today()
    if not is_trading_day(today):
        logger.info(f"Skipping — {today} is not a BSE trading day")
        return

    logger.info(f"=== Starting daily jobs for {today} ===")
    db = SessionLocal()

    try:
        # Step 1: Data pull
        logger.info("Step 1: BSE data pull...")
        pull_result = daily_data_pull(db, today)
        logger.info(f"Data pull: {pull_result}")

        # Step 2: Signal engine (runs at ~5 PM)
        logger.info("Step 2: Running signal engine...")
        signal_count = run_signal_engine(db, today)
        logger.info(f"Signals generated: {signal_count}")

    except Exception as e:
        logger.error(f"Daily jobs failed: {e}")
    finally:
        db.close()

async def run_recommendation_emails():
    """Runs at 6:00 PM IST"""
    today = date.today()
    if not is_trading_day(today):
        return

    db = SessionLocal()
    try:
        result = send_recommendation_emails(db, today)
        logger.info(f"Recommendation emails: {result}")
    except Exception as e:
        logger.error(f"Recommendation emails failed: {e}")
    finally:
        db.close()
