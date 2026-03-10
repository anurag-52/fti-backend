"""
BSE Data Service
Primary:  BSE Bhav Copy (free, official, no API key)
Fallback: yfinance .BO suffix (free, no API key)
"""

import requests
import zipfile
import io
import pandas as pd
import logging
from datetime import date, timedelta
from sqlalchemy.orm import Session
from typing import Optional, List

from database import OHLCV, BSEStock

logger = logging.getLogger(__name__)

BSE_BHAV_URL = "https://www.bseindia.com/download/BhavCopy/Equity/EQ{dd}{mm}{yy}_CSV.zip"

BSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://www.bseindia.com"
}

# ─── BSE BHAV COPY ───────────────────────────────────────────────────────────

def download_bhav_copy(target_date: date) -> Optional[pd.DataFrame]:
    """Download and parse BSE Bhav Copy for a given date"""
    dd = target_date.strftime("%d")
    mm = target_date.strftime("%m")
    yy = target_date.strftime("%y")

    url = BSE_BHAV_URL.format(dd=dd, mm=mm, yy=yy)
    logger.info(f"Downloading BSE Bhav Copy: {url}")

    try:
        r = requests.get(url, headers=BSE_HEADERS, timeout=30)
        if r.status_code != 200:
            logger.warning(f"Bhav Copy HTTP {r.status_code} for {target_date}")
            return None

        z = zipfile.ZipFile(io.BytesIO(r.content))
        csv_name = [n for n in z.namelist() if n.endswith(".CSV") or n.endswith(".csv")]
        if not csv_name:
            return None

        df = pd.read_csv(z.open(csv_name[0]))
        df.columns = [c.strip().upper() for c in df.columns]

        # Standardise column names
        col_map = {
            "SCRIP_CD": "bse_code", "SC_CODE": "bse_code",
            "SCRIP_NAME": "company_name", "SC_NAME": "company_name",
            "OPEN": "open", "HIGH": "high", "LOW": "low",
            "CLOSE": "close", "PREVCLOSE": "prev_close",
            "NO_OF_SHRS": "volume", "TOTTRDQTY": "volume",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        df["date"] = target_date
        df["source"] = "bhav_copy"

        required = ["bse_code", "open", "high", "low", "close"]
        if not all(c in df.columns for c in required):
            logger.error(f"Missing columns in Bhav Copy. Got: {list(df.columns)}")
            return None

        df = df[["bse_code", "open", "high", "low", "close", "volume", "date", "source"]].copy()
        df["bse_code"] = df["bse_code"].astype(str).str.strip()
        for col in ["open", "high", "low", "close"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")
        df = df.dropna(subset=["open", "high", "low", "close"])

        logger.info(f"Bhav Copy loaded: {len(df)} records for {target_date}")
        return df

    except Exception as e:
        logger.error(f"Bhav Copy download failed: {e}")
        return None

# ─── YFINANCE FALLBACK ───────────────────────────────────────────────────────

def fetch_yfinance(symbol: str, start: date, end: date) -> Optional[pd.DataFrame]:
    """Fetch OHLCV via yfinance .BO suffix — compatible with yfinance >= 1.0"""
    try:
        import yfinance as yf
        ticker = f"{symbol}.BO"
        raw = yf.download(ticker, start=str(start), end=str(end + timedelta(days=1)),
                          interval="1d", auto_adjust=True, progress=False)
        if raw is None or raw.empty:
            return None

        # Flatten MultiIndex columns (yfinance >= 0.2 returns MultiIndex)
        if isinstance(raw.columns, pd.MultiIndex):
            raw.columns = [col[0].lower() for col in raw.columns]
        else:
            raw.columns = [str(c).lower() for c in raw.columns]

        df = raw.reset_index()

        # Rename date column
        date_col = next((c for c in df.columns if "date" in c.lower()), None)
        if date_col and date_col != "date":
            df = df.rename(columns={date_col: "date"})

        df["date"] = pd.to_datetime(df["date"]).dt.date
        df["source"] = "yfinance"

        # Ensure all required columns exist
        for col in ["open", "high", "low", "close", "volume"]:
            if col not in df.columns:
                df[col] = 0.0

        df = df[["date", "open", "high", "low", "close", "volume", "source"]].copy()
        df = df.dropna(subset=["open", "high", "low", "close"])
        return df

    except Exception as e:
        logger.error(f"yfinance failed for {symbol}: {e}")
        return None

# ─── SEED HISTORICAL DATA (ON USER ONBOARDING) ───────────────────────────────

def seed_historical_data(stock: BSEStock, db: Session, days: int = 90) -> int:
    """
    Seed historical OHLCV for a stock using yfinance.
    Returns count of records inserted.
    """
    end_date   = date.today()
    start_date = end_date - timedelta(days=days + 30)  # extra buffer

    # Check what we already have
    existing = db.query(OHLCV.date).filter(OHLCV.stock_id == stock.id).all()
    existing_dates = {r.date for r in existing}

    df = fetch_yfinance(stock.symbol, start_date, end_date)
    if df is None or df.empty:
        logger.warning(f"No yfinance data for {stock.symbol}")
        return 0

    inserted = 0
    for _, row in df.iterrows():
        if row["date"] in existing_dates:
            continue
        ohlcv = OHLCV(
            stock_id = stock.id,
            date     = row["date"],
            open     = float(row["open"]),
            high     = float(row["high"]),
            low      = float(row["low"]),
            close    = float(row["close"]),
            volume   = float(row.get("volume", 0) or 0),
            source   = "yfinance"
        )
        db.add(ohlcv)
        inserted += 1

    db.commit()
    logger.info(f"Seeded {inserted} records for {stock.symbol}")
    return inserted

# ─── DAILY DATA PULL (ALL WATCHED STOCKS) ────────────────────────────────────

def daily_data_pull(db: Session, target_date: date = None) -> dict:
    """
    Main daily data pull — runs at 4:30 PM IST.
    1. Try BSE Bhav Copy
    2. Fallback to yfinance per stock
    Returns summary dict
    """
    if target_date is None:
        target_date = date.today()

    summary = {"date": str(target_date), "bhav_copy": 0, "yfinance": 0, "failed": 0}

    # Step 1: BSE Bhav Copy
    bhav_df = download_bhav_copy(target_date)

    # Get all active stocks that are on at least one watchlist
    from database import Watchlist
    watched_stock_ids = {wl.stock_id for wl in db.query(Watchlist).all()}
    if not watched_stock_ids:
        return summary

    stocks = db.query(BSEStock).filter(BSEStock.id.in_(watched_stock_ids)).all()

    for stock in stocks:
        # Check if we already have today's data
        existing = db.query(OHLCV).filter(
            OHLCV.stock_id == stock.id,
            OHLCV.date     == target_date
        ).first()
        if existing:
            continue

        inserted = False

        # Try Bhav Copy first
        if bhav_df is not None:
            row = bhav_df[bhav_df["bse_code"] == stock.bse_code]
            if not row.empty:
                r = row.iloc[0]
                db.add(OHLCV(
                    stock_id = stock.id,
                    date     = target_date,
                    open     = float(r["open"]),
                    high     = float(r["high"]),
                    low      = float(r["low"]),
                    close    = float(r["close"]),
                    volume   = float(r.get("volume", 0) or 0),
                    source   = "bhav_copy"
                ))
                summary["bhav_copy"] += 1
                inserted = True

        # Fallback: yfinance
        if not inserted:
            df = fetch_yfinance(stock.symbol, target_date, target_date)
            if df is not None and not df.empty:
                r = df.iloc[0]
                db.add(OHLCV(
                    stock_id = stock.id,
                    date     = target_date,
                    open     = float(r["open"]),
                    high     = float(r["high"]),
                    low      = float(r["low"]),
                    close    = float(r["close"]),
                    volume   = float(r.get("volume", 0) or 0),
                    source   = "yfinance"
                ))
                summary["yfinance"] += 1
                inserted = True

        if not inserted:
            summary["failed"] += 1
            logger.warning(f"Failed to get data for {stock.symbol} on {target_date}")

    db.commit()
    logger.info(f"Daily data pull complete: {summary}")
    return summary

# ─── SEARCH BSE STOCKS ───────────────────────────────────────────────────────

def search_bse_stocks(query: str, db: Session, limit: int = 20) -> List[BSEStock]:
    """Search stocks by name or BSE code"""
    return db.query(BSEStock).filter(
        BSEStock.is_active == True,
        (BSEStock.company_name.ilike(f"%{query}%") |
         BSEStock.bse_code.ilike(f"%{query}%") |
         BSEStock.symbol.ilike(f"%{query}%"))
    ).limit(limit).all()
