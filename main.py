"""
FTI Trading App — Main Application Entry Point
Courtney Smith Channel Breakout + 5-Day Condition System
BSE India | FastAPI + SQLite
"""

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import logging

from database import engine, Base
from routers import auth, admin, users, stocks, signals, portfolio, backtest, email_routes
from services.scheduler import run_daily_jobs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create all DB tables on startup
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables created/verified")
    # Seed admin user on startup
    from database import SessionLocal, User
    from auth import hash_password
    try:
        db = SessionLocal()
        if not db.query(User).filter(User.email == "admin@ftitrading.com").first():
            db.add(User(name="Super Admin", email="admin@ftitrading.com",
                        hashed_password=hash_password("Admin2026"),
                        is_admin=True, is_active=True, onboarding_done=True))
            db.commit()
            logger.info("Admin user created")
        else:
            logger.info("Admin user already exists")
        db.close()
    except Exception as e:
        logger.info(f"Seed error: {e}")


    # Schedule daily jobs (IST timezone)
    scheduler.add_job(run_daily_jobs, "cron", hour=16, minute=30, id="daily_data_pull")
    scheduler.start()
    logger.info("Scheduler started — daily jobs at 4:30 PM IST")

    yield

    scheduler.shutdown()
    logger.info("Scheduler stopped")

app = FastAPI(
    title="FTI Trading App",
    description="Freedom Trader Intensive — Courtney Smith Channel Breakout System",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount static files
app.mount("/static", StaticFiles(directory="static"), name="static")

# Include all routers
app.include_router(auth.router,        prefix="/api/auth",      tags=["Authentication"])
app.include_router(admin.router,       prefix="/api/admin",     tags=["Super Admin"])
app.include_router(users.router,       prefix="/api/users",     tags=["Users"])
app.include_router(stocks.router,      prefix="/api/stocks",    tags=["Stocks & Watchlist"])
app.include_router(signals.router,     prefix="/api/signals",   tags=["Signals"])
app.include_router(portfolio.router,   prefix="/api/portfolio", tags=["Portfolio"])
app.include_router(backtest.router,    prefix="/api/backtest",  tags=["Backtest"])
app.include_router(email_routes.router,prefix="/api/email",     tags=["Email"])

@app.get("/")
async def root(request: Request):
    return FileResponse("../frontend/index.html")

@app.get("/health")
async def health():
    return {"status": "ok", "app": "FTI Trading App v1.0"}
