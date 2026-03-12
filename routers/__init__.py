"""
All API Routers — FTI Trading App
"""

# ─── IMPORTS ─────────────────────────────────────────────────────────────────
from fastapi import APIRouter, Depends, HTTPException, Body, BackgroundTasks
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session
from pydantic import BaseModel, EmailStr
from typing import Optional, List
from datetime import date, timedelta
import pandas as pd

from database import get_db, User, BSEStock, Watchlist, OHLCV, Signal, Position, Trade, TradeConfirmation, EmailLog
from auth import (hash_password, verify_password, create_access_token,
                  create_reset_token, verify_reset_token, get_current_user,
                  get_current_admin, generate_temp_password)
from services.engine import run_signal_engine, run_backtest
from services.data_service import search_bse_stocks, seed_historical_data
from services.email_service import send_email, build_confirmation_email

# ─── SCHEMAS ─────────────────────────────────────────────────────────────────

class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    is_admin: bool
    name: str

class UserCreate(BaseModel):
    name: str
    email: EmailStr
    mobile: Optional[str] = None
    investment_amount: float = 0.0

class UserUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    mobile: Optional[str] = None
    investment_amount: Optional[float] = None
    is_active: Optional[bool] = None
    password: Optional[str] = None

class PasswordChange(BaseModel):
    current_password: str
    new_password: str

class ForgotPassword(BaseModel):
    email: EmailStr

class ResetPassword(BaseModel):
    token: str
    new_password: str

class OnboardingSetup(BaseModel):
    investment_amount: float
    stock_ids: List[int]

class ConfirmTrade(BaseModel):
    confirmation_id: int
    purchased: bool
    actual_buy_price: Optional[float] = None

class OverrideSignal(BaseModel):
    signal_id: int
    override_reason: str

class BacktestRequest(BaseModel):
    stock_id: int
    start_date: date
    end_date: date
    starting_capital: Optional[float] = None
    risk_pct: float = 0.01

class PortfolioSimRequest(BaseModel):
    stock_ids: List[int]                  # 1–20 BSEStock IDs
    start_date: date
    end_date: date
    starting_capital: Optional[float] = None
    risk_pct: float = 0.01
    max_concurrent_positions: int = 10    # cap on simultaneous open positions

# ═══════════════════════════════════════════════════════════════════════════════
# AUTH ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

auth_router = APIRouter()

@auth_router.post("/login", response_model=LoginResponse)
def login(form: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == form.username, User.is_active == True).first()
    if not user or not verify_password(form.password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = create_access_token({"sub": user.email})
    return LoginResponse(access_token=token, is_admin=user.is_admin, name=user.name)

@auth_router.post("/forgot-password")
def forgot_password(body: ForgotPassword, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == body.email).first()
    if user:
        token   = create_reset_token(user.email)
        import os
        app_url = os.getenv("APP_URL", "http://localhost:8000")
        reset_link = f"{app_url}/reset-password?token={token}"
        html = f"""<p>Hi {user.name},</p>
        <p>Click the link below to reset your FTI Trading App password. Valid for 30 minutes.</p>
        <p><a href="{reset_link}" style="background:#1B4F8A;color:#fff;padding:10px 20px;text-decoration:none;border-radius:5px">Reset Password</a></p>
        <p>If you didn't request this, ignore this email.</p>"""
        send_email(user.email, "[FTI] Password Reset Request", html)
    return {"message": "If that email exists, a reset link has been sent."}

@auth_router.post("/reset-password")
def reset_password(body: ResetPassword, db: Session = Depends(get_db)):
    email = verify_reset_token(body.token)
    if not email:
        raise HTTPException(status_code=400, detail="Invalid or expired reset token")
    if len(body.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    user = db.query(User).filter(User.email == email).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.hashed_password = hash_password(body.new_password)
    db.commit()
    return {"message": "Password reset successfully"}

@auth_router.post("/change-password")
def change_password(body: PasswordChange, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if not verify_password(body.current_password, current_user.hashed_password):
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    if len(body.new_password) < 8:
        raise HTTPException(status_code=400, detail="Password must be at least 8 characters")
    current_user.hashed_password = hash_password(body.new_password)
    db.commit()
    return {"message": "Password changed successfully"}

# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

admin_router = APIRouter()

@admin_router.get("/users")
def list_users(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    users = db.query(User).filter(User.is_admin == False).all()
    result = []
    for u in users:
        positions = db.query(Position).filter(Position.user_id == u.id, Position.is_open == True).all()
        invested  = sum(p.entry_price * p.quantity for p in positions)
        available = max(0, u.investment_amount - invested)
        result.append({
            "id": u.id, "name": u.name, "email": u.email, "mobile": u.mobile,
            "investment_amount": u.investment_amount, "available_funds": available,
            "is_active": u.is_active, "onboarding_done": u.onboarding_done,
            "created_at": str(u.created_at), "open_positions": len(positions),
            "watchlist_count": db.query(Watchlist).filter(Watchlist.user_id == u.id).count()
        })
    return result

@admin_router.post("/users")
def create_user(body: UserCreate, background_tasks: BackgroundTasks, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(status_code=400, detail="Email already registered")
    temp_pw = generate_temp_password()
    user = User(
        name=body.name, email=body.email, mobile=body.mobile,
        investment_amount=body.investment_amount,
        hashed_password=hash_password(temp_pw), is_admin=False
    )
    db.add(user)
    db.commit()
    db.refresh(user)
    import os
    app_url = os.getenv("APP_URL", "http://localhost:8000")
    html = f"""<p>Hi {user.name},</p>
    <p>Your FTI Trading App account has been created.</p>
    <p><strong>Email:</strong> {user.email}<br><strong>Temporary Password:</strong> {temp_pw}</p>
    <p><a href="{app_url}/login" style="background:#1B4F8A;color:#fff;padding:10px 20px;text-decoration:none;border-radius:5px">Login Now</a></p>
    <p>Please change your password after first login.</p>"""
    background_tasks.add_task(send_email, user.email, "[FTI] Your Trading App Account", html)
    return {"id": user.id, "name": user.name, "email": user.email, "temp_password": temp_pw}

@admin_router.put("/users/{user_id}")
def update_user(user_id: int, body: UserUpdate, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    if body.name is not None: user.name = body.name
    if body.mobile is not None: user.mobile = body.mobile
    if body.investment_amount is not None: user.investment_amount = body.investment_amount
    if body.is_active is not None: user.is_active = body.is_active
    if body.email is not None: user.email = body.email
    if body.password is not None: user.hashed_password = hash_password(body.password)
    db.commit()
    return {"message": "User updated"}

@admin_router.delete("/users/{user_id}")
def delete_user(user_id: int, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    user.is_active = False
    db.commit()
    return {"message": "User deactivated"}

@admin_router.get("/portfolios")
def all_portfolios(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    users = db.query(User).filter(User.is_admin == False, User.is_active == True).all()
    result = []
    for u in users:
        positions = db.query(Position).filter(Position.user_id == u.id, Position.is_open == True).all()
        pos_data = []
        for p in positions:
            latest = db.query(OHLCV).filter(OHLCV.stock_id == p.stock_id).order_by(OHLCV.date.desc()).first()
            curr_price = latest.close if latest else p.entry_price
            pnl = (curr_price - p.entry_price) * p.quantity
            pos_data.append({
                "stock": p.stock.company_name, "bse_code": p.stock.bse_code,
                "entry_price": p.entry_price, "current_price": curr_price,
                "quantity": p.quantity, "pnl": round(pnl, 2),
                "entry_date": str(p.entry_date)
            })
        result.append({"user": u.name, "email": u.email,
                        "investment": u.investment_amount, "positions": pos_data})
    return result

@admin_router.post("/override-signal")
def override_signal(body: OverrideSignal, db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    signal = db.query(Signal).filter(Signal.id == body.signal_id).first()
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")
    signal.is_overridden  = True
    signal.override_reason = body.override_reason
    db.commit()
    return {"message": "Signal overridden"}

@admin_router.post("/run-engine")
def manual_engine_run(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    count = run_signal_engine(db)
    return {"message": f"Engine run complete. {count} signals generated."}

@admin_router.get("/email-logs")
def email_logs(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    logs = db.query(EmailLog).order_by(EmailLog.sent_at.desc()).limit(200).all()
    return [{"id": l.id, "user_id": l.user_id, "type": l.email_type,
             "date": str(l.date), "status": l.status, "error": l.error_msg} for l in logs]

# ═══════════════════════════════════════════════════════════════════════════════
# USER ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

users_router = APIRouter()

@users_router.get("/me")
def get_me(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    from services.engine import get_available_funds
    available = get_available_funds(current_user, db)
    watchlist_count = db.query(Watchlist).filter(Watchlist.user_id == current_user.id).count()
    return {
        "id": current_user.id, "name": current_user.name, "email": current_user.email,
        "mobile": current_user.mobile, "investment_amount": current_user.investment_amount,
        "available_funds": round(available, 2), "is_admin": current_user.is_admin,
        "onboarding_done": current_user.onboarding_done, "watchlist_count": watchlist_count,
        "max_risk_per_trade": round(current_user.investment_amount * 0.01, 2)
    }

@users_router.post("/onboarding")
def complete_onboarding(body: OnboardingSetup, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    if len(body.stock_ids) > 30:
        raise HTTPException(status_code=400, detail="Maximum 30 stocks allowed in watchlist")
    current_user.investment_amount = body.investment_amount
    db.query(Watchlist).filter(Watchlist.user_id == current_user.id).delete()
    for sid in body.stock_ids:
        stock = db.query(BSEStock).filter(BSEStock.id == sid).first()
        if stock:
            db.add(Watchlist(user_id=current_user.id, stock_id=sid))
            seed_historical_data(stock, db, days=90)
    current_user.onboarding_done = True
    db.commit()
    return {"message": "Onboarding complete. You will receive your first recommendations at 6 PM."}

# ═══════════════════════════════════════════════════════════════════════════════
# STOCKS ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

stocks_router = APIRouter()

@stocks_router.get("/search")
def search_stocks(q: str, db: Session = Depends(get_db), current_user: User = Depends(get_current_user)):
    stocks = search_bse_stocks(q, db)
    return [{"id": s.id, "bse_code": s.bse_code, "symbol": s.symbol,
             "company_name": s.company_name, "sector": s.sector} for s in stocks]

@stocks_router.get("/watchlist")
def get_watchlist(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    wl = db.query(Watchlist).filter(Watchlist.user_id == current_user.id).all()
    return [{"id": w.id, "stock_id": w.stock.id, "bse_code": w.stock.bse_code,
             "symbol": w.stock.symbol, "company_name": w.stock.company_name} for w in wl]

@stocks_router.post("/watchlist/{stock_id}")
def add_to_watchlist(stock_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    count = db.query(Watchlist).filter(Watchlist.user_id == current_user.id).count()
    if count >= 30:
        raise HTTPException(status_code=400, detail="Maximum 30 stocks allowed")
    existing = db.query(Watchlist).filter(Watchlist.user_id == current_user.id, Watchlist.stock_id == stock_id).first()
    if existing:
        raise HTTPException(status_code=400, detail="Stock already in watchlist")
    stock = db.query(BSEStock).filter(BSEStock.id == stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")
    db.add(Watchlist(user_id=current_user.id, stock_id=stock_id))
    seed_historical_data(stock, db, days=90)
    db.commit()
    return {"message": f"{stock.company_name} added to watchlist"}

@stocks_router.delete("/watchlist/{stock_id}")
def remove_from_watchlist(stock_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    wl = db.query(Watchlist).filter(Watchlist.user_id == current_user.id, Watchlist.stock_id == stock_id).first()
    if not wl:
        raise HTTPException(status_code=404, detail="Stock not in watchlist")
    db.delete(wl)
    db.commit()
    return {"message": "Removed from watchlist"}

# ═══════════════════════════════════════════════════════════════════════════════
# SIGNALS ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

signals_router = APIRouter()

@signals_router.get("/today")
def todays_signals(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    today = date.today()
    signals = db.query(Signal).filter(
        Signal.user_id == current_user.id,
        Signal.date == today
    ).all()
    return [{
        "id": s.id, "signal_type": s.signal_type,
        "stock": s.stock.company_name, "bse_code": s.stock.bse_code, "stock_id": s.stock_id,
        "channel_55_high": s.channel_55_high, "channel_20_low": s.channel_20_low,
        "five_day_count": s.five_day_count, "adx_value": s.adx_value, "adx_rising": s.adx_rising,
        "buy_stop_price": s.buy_stop_price, "stop_loss_price": s.stop_loss_price,
        "quantity": s.quantity, "capital_required": s.capital_required, "max_risk": s.max_risk,
        "exit_reason": s.exit_reason, "exit_price": s.exit_price, "estimated_pnl": s.estimated_pnl,
        "is_overridden": s.is_overridden
    } for s in signals if not s.is_overridden]

# ═══════════════════════════════════════════════════════════════════════════════
# PORTFOLIO ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

portfolio_router = APIRouter()

@portfolio_router.get("/")
def get_portfolio(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    positions = db.query(Position).filter(
        Position.user_id == current_user.id, Position.is_open == True
    ).all()

    pos_data   = []
    total_value = 0
    total_cost  = 0

    today = date.today()
    for p in positions:
        latest = db.query(OHLCV).filter(OHLCV.stock_id == p.stock_id).order_by(OHLCV.date.desc()).first()
        curr_price = latest.close if latest else p.entry_price
        cost       = p.entry_price * p.quantity
        value      = curr_price * p.quantity
        pnl        = value - cost
        pnl_pct    = (pnl / cost * 100) if cost else 0
        days_held  = (today - p.entry_date).days

        # Current trailing stop
        stop_data = db.query(OHLCV).filter(
            OHLCV.stock_id == p.stock_id,
            OHLCV.date >= today - timedelta(days=30)
        ).order_by(OHLCV.date.desc()).limit(20).all()
        trail_stop = min(s.low for s in stop_data) if stop_data else p.stop_loss_price

        # Check for exit signal today
        exit_signal = db.query(Signal).filter(
            Signal.user_id == current_user.id,
            Signal.stock_id == p.stock_id,
            Signal.date == today,
            Signal.signal_type == "EXIT"
        ).first()

        pos_data.append({
            "position_id": p.id, "stock": p.stock.company_name,
            "bse_code": p.stock.bse_code, "stock_id": p.stock_id,
            "entry_date": str(p.entry_date), "entry_price": p.entry_price,
            "current_price": round(curr_price, 2), "quantity": p.quantity,
            "cost": round(cost, 2), "current_value": round(value, 2),
            "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2),
            "trailing_stop": round(trail_stop, 2) if trail_stop else None,
            "days_held": days_held,
            "exit_signal": exit_signal.exit_reason if exit_signal else None
        })
        total_value += value
        total_cost  += cost

    from services.engine import get_available_funds
    available = get_available_funds(current_user, db)

    closed_trades = db.query(Trade).filter(Trade.user_id == current_user.id).order_by(Trade.exit_date.desc()).limit(50).all()
    closed_data = [{
        "stock": t.stock.company_name, "bse_code": t.stock.bse_code,
        "entry_date": str(t.entry_date), "entry_price": t.entry_price,
        "exit_date": str(t.exit_date), "exit_price": t.exit_price,
        "quantity": t.quantity, "realised_pnl": t.realised_pnl,
        "exit_reason": t.exit_reason, "days_held": t.days_held
    } for t in closed_trades]

    all_pnls = [t.realised_pnl for t in closed_trades if t.realised_pnl]
    wins = [p for p in all_pnls if p > 0]
    win_rate = round(len(wins) / len(all_pnls) * 100, 1) if all_pnls else 0

    return {
        "summary": {
            "investment_amount": current_user.investment_amount,
            "total_cost": round(total_cost, 2),
            "portfolio_value": round(total_value, 2),
            "unrealised_pnl": round(total_value - total_cost, 2),
            "unrealised_pnl_pct": round((total_value - total_cost) / total_cost * 100, 2) if total_cost else 0,
            "available_funds": round(available, 2),
            "total_realised_pnl": round(sum(all_pnls), 2),
            "win_rate": win_rate,
            "total_closed_trades": len(closed_trades),
        },
        "open_positions": pos_data,
        "closed_trades": closed_data
    }

@portfolio_router.post("/confirm-trade")
def confirm_trade(body: ConfirmTrade, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    from datetime import datetime
    conf = db.query(TradeConfirmation).filter(
        TradeConfirmation.id == body.confirmation_id,
        TradeConfirmation.user_id == current_user.id
    ).first()
    if not conf:
        raise HTTPException(status_code=404, detail="Confirmation not found")

    conf.purchased   = body.purchased
    conf.submitted_at = datetime.utcnow()

    if body.purchased and body.actual_buy_price:
        conf.actual_buy_price = body.actual_buy_price
        signal = db.query(Signal).filter(Signal.id == conf.signal_id).first()
        if signal:
            qty = signal.quantity or 1
            pos = Position(
                user_id     = current_user.id,
                stock_id    = conf.stock_id,
                entry_date  = date.today(),
                entry_price = body.actual_buy_price,
                quantity    = qty,
                stop_loss_price = signal.stop_loss_price,
                signal_id   = signal.id
            )
            db.add(pos)

    db.commit()
    return {"message": "Trade confirmation saved", "purchased": body.purchased}

@portfolio_router.post("/confirm-exit/{position_id}")
def confirm_exit(position_id: int, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    pos = db.query(Position).filter(
        Position.id == position_id, Position.user_id == current_user.id, Position.is_open == True
    ).first()
    if not pos:
        raise HTTPException(status_code=404, detail="Position not found")

    latest = db.query(OHLCV).filter(OHLCV.stock_id == pos.stock_id).order_by(OHLCV.date.desc()).first()
    exit_price = latest.close if latest else pos.entry_price

    pnl = (exit_price - pos.entry_price) * pos.quantity
    days_held = (date.today() - pos.entry_date).days

    trade = Trade(
        user_id    = current_user.id,
        stock_id   = pos.stock_id,
        entry_date = pos.entry_date,
        entry_price= pos.entry_price,
        exit_date  = date.today(),
        exit_price = exit_price,
        quantity   = pos.quantity,
        realised_pnl = round(pnl, 2),
        exit_reason  = "user_confirmed",
        days_held    = days_held
    )
    db.add(trade)
    pos.is_open = False
    db.commit()
    return {"message": "Position closed", "realised_pnl": round(pnl, 2)}

# ═══════════════════════════════════════════════════════════════════════════════
# BACKTEST ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

backtest_router = APIRouter()

@backtest_router.post("/run")
def run_backtest_api(body: BacktestRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    stock = db.query(BSEStock).filter(BSEStock.id == body.stock_id).first()
    if not stock:
        raise HTTPException(status_code=404, detail="Stock not found")

    rows = db.query(OHLCV).filter(
        OHLCV.stock_id == body.stock_id,
        OHLCV.date >= body.start_date,
        OHLCV.date <= body.end_date
    ).order_by(OHLCV.date).all()

    if len(rows) < 60:
        from services.data_service import fetch_yfinance
        yf_df = fetch_yfinance(stock.symbol, body.start_date, body.end_date)
        if yf_df is not None and len(yf_df) >= 60:
            df = yf_df
        else:
            raise HTTPException(status_code=400, detail=f"Insufficient data — {len(rows)} days found, minimum 60 required")
    else:
        df = pd.DataFrame([{
            "date": r.date, "open": r.open, "high": r.high,
            "low": r.low, "close": r.close, "volume": r.volume
        } for r in rows])

    capital = body.starting_capital or current_user.investment_amount
    result  = run_backtest(df, capital, body.risk_pct)
    result["stock"] = {"name": stock.company_name, "bse_code": stock.bse_code}
    result["params"] = {"start_date": str(body.start_date), "end_date": str(body.end_date),
                        "starting_capital": capital, "risk_pct": body.risk_pct}
    return result

@backtest_router.post("/portfolio")
def run_portfolio_sim(
    body: PortfolioSimRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """
    Portfolio Simulation Engine — multi-stock backtest with shared capital.

    Selects up to 20 stocks, runs the Channel Breakout strategy across all of
    them simultaneously, allocating capital from a single pool.  Entries are
    prioritised by five_day_count (most mature first); max_concurrent_positions
    caps how many stocks can be held at once.
    """
    if len(body.stock_ids) < 1:
        raise HTTPException(status_code=400, detail="At least 1 stock is required")
    if len(body.stock_ids) > 20:
        raise HTTPException(status_code=400, detail="Maximum 20 stocks allowed")
    if body.max_concurrent_positions < 1 or body.max_concurrent_positions > 20:
        raise HTTPException(status_code=400, detail="max_concurrent_positions must be 1–20")

    from services.data_service import fetch_yfinance
    from services.engine import run_portfolio_backtest

    stock_dfs:   dict = {}
    stock_names: dict = {}
    skipped:     list = []

    for stock_id in body.stock_ids:
        stock = db.query(BSEStock).filter(BSEStock.id == stock_id).first()
        if not stock:
            skipped.append({"stock_id": stock_id, "reason": "not found"})
            continue

        rows = db.query(OHLCV).filter(
            OHLCV.stock_id == stock_id,
            OHLCV.date     >= body.start_date,
            OHLCV.date     <= body.end_date,
        ).order_by(OHLCV.date).all()

        if len(rows) >= 60:
            df = pd.DataFrame([{
                "date": r.date, "open": r.open, "high": r.high,
                "low": r.low, "close": r.close, "volume": r.volume,
            } for r in rows])
        else:
            df = fetch_yfinance(stock.symbol, body.start_date, body.end_date)
            if df is None or len(df) < 60:
                skipped.append({"stock_id": stock_id, "symbol": stock.symbol,
                                 "reason": f"insufficient data ({len(rows)} rows in DB)"})
                continue

        sid = str(stock_id)
        stock_dfs[sid]   = df
        stock_names[sid] = stock.company_name

    if not stock_dfs:
        raise HTTPException(
            status_code=400,
            detail="No stocks had sufficient data. Need >= 60 trading days in the date range."
        )

    capital = body.starting_capital or current_user.investment_amount
    result  = run_portfolio_backtest(
        stock_dfs   = stock_dfs,
        stock_names = stock_names,
        starting_capital = capital,
        risk_pct    = body.risk_pct,
        max_positions = body.max_concurrent_positions,
    )

    result["params"] = {
        "stocks_requested":        len(body.stock_ids),
        "stocks_with_data":        len(stock_dfs),
        "stocks_skipped":          skipped,
        "start_date":              str(body.start_date),
        "end_date":                str(body.end_date),
        "starting_capital":        capital,
        "risk_pct":                body.risk_pct,
        "max_concurrent_positions": body.max_concurrent_positions,
    }
    return result

# ═══════════════════════════════════════════════════════════════════════════════
# EMAIL ROUTES ROUTER
# ═══════════════════════════════════════════════════════════════════════════════

email_router = APIRouter()

@email_router.post("/send-confirmation")
def trigger_confirmation_emails(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Admin trigger for 5 PM confirmation emails"""
    from services.email_service import send_recommendation_emails
    today = date.today()
    result = send_recommendation_emails(db, today)
    return result

@email_router.post("/send-recommendations")
def trigger_recommendation_emails(db: Session = Depends(get_db), admin: User = Depends(get_current_admin)):
    """Admin trigger for 6 PM recommendation emails"""
    from services.email_service import send_recommendation_emails
    today = date.today()
    result = send_recommendation_emails(db, today)
    return result

# ─── WIRE UP ROUTERS ─────────────────────────────────────────────────────────

# Create router module stubs so main.py imports work
import sys, types

def _make_router_module(name, router):
    mod = types.ModuleType(name)
    mod.router = router
    return mod

sys.modules["routers.auth"]         = _make_router_module("routers.auth", auth_router)
sys.modules["routers.admin"]        = _make_router_module("routers.admin", admin_router)
sys.modules["routers.users"]        = _make_router_module("routers.users", users_router)
sys.modules["routers.stocks"]       = _make_router_module("routers.stocks", stocks_router)
sys.modules["routers.signals"]      = _make_router_module("routers.signals", signals_router)
sys.modules["routers.portfolio"]    = _make_router_module("routers.portfolio", portfolio_router)
sys.modules["routers.backtest"]     = _make_router_module("routers.backtest", backtest_router)
sys.modules["routers.email_routes"] = _make_router_module("routers.email_routes", email_router)
