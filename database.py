"""
Database Configuration — SQLite (dev) / PostgreSQL (prod)
SQLAlchemy ORM Models
"""

from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean, DateTime, Date, ForeignKey, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import os

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fti_trading.db")

# Handle PostgreSQL URL format from Railway
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

connect_args = {"check_same_thread": False} if "sqlite" in DATABASE_URL else {}
engine = create_engine(DATABASE_URL, connect_args=connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ─── MODELS ──────────────────────────────────────────────────────────────────

class User(Base):
    __tablename__ = "users"
    id              = Column(Integer, primary_key=True, index=True)
    name            = Column(String(100), nullable=False)
    email           = Column(String(150), unique=True, index=True, nullable=False)
    hashed_password = Column(String(255), nullable=False)
    mobile          = Column(String(20))
    investment_amount = Column(Float, default=0.0)    # Total INR allocated
    is_admin        = Column(Boolean, default=False)
    is_active       = Column(Boolean, default=True)
    onboarding_done = Column(Boolean, default=False)
    created_at      = Column(DateTime, default=datetime.utcnow)
    updated_at      = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    watchlist   = relationship("Watchlist",   back_populates="user", cascade="all, delete")
    portfolio   = relationship("Position",    back_populates="user", cascade="all, delete")
    trades      = relationship("Trade",       back_populates="user", cascade="all, delete")
    confirmations = relationship("TradeConfirmation", back_populates="user", cascade="all, delete")

class BSEStock(Base):
    __tablename__ = "bse_stocks"
    id          = Column(Integer, primary_key=True, index=True)
    bse_code    = Column(String(20), unique=True, index=True, nullable=False)
    symbol      = Column(String(20), nullable=False)
    company_name= Column(String(200), nullable=False)
    sector      = Column(String(100))
    is_active   = Column(Boolean, default=True)

    ohlcv       = relationship("OHLCV", back_populates="stock", cascade="all, delete")
    signals     = relationship("Signal", back_populates="stock", cascade="all, delete")

class Watchlist(Base):
    __tablename__ = "watchlist"
    id          = Column(Integer, primary_key=True, index=True)
    user_id     = Column(Integer, ForeignKey("users.id"), nullable=False)
    stock_id    = Column(Integer, ForeignKey("bse_stocks.id"), nullable=False)
    added_at    = Column(DateTime, default=datetime.utcnow)

    user        = relationship("User", back_populates="watchlist")
    stock       = relationship("BSEStock")

class OHLCV(Base):
    __tablename__ = "ohlcv"
    id          = Column(Integer, primary_key=True, index=True)
    stock_id    = Column(Integer, ForeignKey("bse_stocks.id"), nullable=False)
    date        = Column(Date, nullable=False)
    open        = Column(Float)
    high        = Column(Float)
    low         = Column(Float)
    close       = Column(Float)
    volume      = Column(Float)
    source      = Column(String(20), default="bhav_copy")  # bhav_copy | yfinance

    stock       = relationship("BSEStock", back_populates="ohlcv")

    class Config:
        unique_together = [("stock_id", "date")]

class Signal(Base):
    __tablename__ = "signals"
    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("bse_stocks.id"), nullable=False)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False)
    date            = Column(Date, nullable=False)           # Signal generated for this date
    signal_type     = Column(String(10), nullable=False)     # BUY | EXIT | HOLD
    channel_55_high = Column(Float)
    channel_20_low  = Column(Float)
    five_day_count  = Column(Integer)
    adx_value       = Column(Float)
    adx_rising      = Column(Boolean)
    buy_stop_price  = Column(Float)
    stop_loss_price = Column(Float)
    quantity        = Column(Integer)
    capital_required= Column(Float)
    max_risk        = Column(Float)
    exit_reason     = Column(String(50))   # rejection_rule | adx_exit | trailing_stop
    exit_price      = Column(Float)
    estimated_pnl   = Column(Float)
    is_overridden   = Column(Boolean, default=False)
    override_reason = Column(Text)
    email_sent      = Column(Boolean, default=False)
    created_at      = Column(DateTime, default=datetime.utcnow)

    stock           = relationship("BSEStock", back_populates="signals")

class TradeConfirmation(Base):
    """User's daily YES/NO confirmation for BUY recommendations"""
    __tablename__ = "trade_confirmations"
    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False)
    signal_id       = Column(Integer, ForeignKey("signals.id"), nullable=False)
    stock_id        = Column(Integer, ForeignKey("bse_stocks.id"), nullable=False)
    date            = Column(Date, nullable=False)
    hit             = Column(Boolean, default=False)    # Did stock cross buy stop?
    purchased       = Column(Boolean)                   # NULL=pending, True=YES, False=NO
    actual_buy_price= Column(Float)
    quantity        = Column(Integer)
    submitted_at    = Column(DateTime)
    created_at      = Column(DateTime, default=datetime.utcnow)

    user            = relationship("User", back_populates="confirmations")

class Position(Base):
    """Currently open portfolio positions"""
    __tablename__ = "positions"
    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False)
    stock_id        = Column(Integer, ForeignKey("bse_stocks.id"), nullable=False)
    entry_date      = Column(Date, nullable=False)
    entry_price     = Column(Float, nullable=False)
    quantity        = Column(Integer, nullable=False)
    stop_loss_price = Column(Float)          # Current trailing stop
    signal_id       = Column(Integer, ForeignKey("signals.id"))
    is_open         = Column(Boolean, default=True)
    created_at      = Column(DateTime, default=datetime.utcnow)

    user            = relationship("User", back_populates="portfolio")
    stock           = relationship("BSEStock")

class Trade(Base):
    """Completed (closed) trades history"""
    __tablename__ = "trades"
    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False)
    stock_id        = Column(Integer, ForeignKey("bse_stocks.id"), nullable=False)
    entry_date      = Column(Date)
    entry_price     = Column(Float)
    exit_date       = Column(Date)
    exit_price      = Column(Float)
    quantity        = Column(Integer)
    realised_pnl    = Column(Float)
    exit_reason     = Column(String(50))
    days_held       = Column(Integer)
    created_at      = Column(DateTime, default=datetime.utcnow)

    user            = relationship("User", back_populates="trades")
    stock           = relationship("BSEStock")

class EmailLog(Base):
    __tablename__ = "email_logs"
    id          = Column(Integer, primary_key=True, index=True)
    user_id     = Column(Integer, ForeignKey("users.id"))
    email_type  = Column(String(30))    # confirmation_5pm | recommendation_6pm
    date        = Column(Date)
    status      = Column(String(20))    # sent | failed | pending
    error_msg   = Column(Text)
    sent_at     = Column(DateTime)

class SystemSettings(Base):
    __tablename__ = "system_settings"
    id          = Column(Integer, primary_key=True, index=True)
    key         = Column(String(100), unique=True, nullable=False)
    value       = Column(Text)
    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
