"""
FTI Signal Calculation Engine
Implements exactly:
  1. Channel Breakout (55-day entry, 20-day trailing stop)
  2. 5-Day Condition Filter
  3. ADX(20) filter
  4. Rejection Rule
  5. Position sizing (1% risk)
"""

import pandas as pd
import numpy as np
from typing import Optional, Tuple, Dict, List
from sqlalchemy.orm import Session
from datetime import date, timedelta
import logging

from database import OHLCV, Signal, Position, User, BSEStock, Watchlist

logger = logging.getLogger(__name__)

# ─── ADX CALCULATION (Wilder Smoothing) ─────────────────────────────────────

def calculate_adx(df: pd.DataFrame, period: int = 20) -> pd.Series:
    """Calculate ADX using Wilder's smoothing method"""
    high  = df["high"]
    low   = df["low"]
    close = df["close"]

    plus_dm  = high.diff()
    minus_dm = -low.diff()
    plus_dm[plus_dm  < 0] = 0
    minus_dm[minus_dm < 0] = 0
    plus_dm[(plus_dm < minus_dm)] = 0
    minus_dm[(minus_dm < plus_dm)] = 0

    tr = pd.concat([
        high - low,
        (high - close.shift()).abs(),
        (low  - close.shift()).abs()
    ], axis=1).max(axis=1)

    # Wilder smoothing
    atr      = tr.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    plus_di  = 100 * (plus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean() / atr)
    minus_di = 100 * (minus_dm.ewm(alpha=1/period, min_periods=period, adjust=False).mean() / atr)

    dx  = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di)
    adx = dx.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    return adx

# ─── CHANNEL CALCULATIONS ────────────────────────────────────────────────────

def calculate_channels(df: pd.DataFrame) -> pd.DataFrame:
    """Add all channel and indicator columns to OHLCV dataframe"""
    df = df.copy().sort_values("date").reset_index(drop=True)

    # 55-day highest high (entry channel)
    df["ch55_high"]   = df["high"].rolling(55, min_periods=55).max()
    # 20-day lowest low (trailing stop)
    df["ch20_low"]    = df["low"].rolling(20, min_periods=20).min()
    # ADX 20-period
    df["adx"]         = calculate_adx(df, period=20)
    df["adx_rising"]  = df["adx"] > df["adx"].shift(1)

    # 5-Day Condition: count consecutive days where 55-day high is flat or declining
    df["ch55_flat_declining"] = df["ch55_high"] <= df["ch55_high"].shift(1)
    five_day = []
    counter = 0
    for val in df["ch55_flat_declining"]:
        if pd.isna(val):
            counter = 0
        elif val:
            counter += 1
        else:
            counter = 0
        five_day.append(counter)
    df["five_day_count"] = five_day

    return df

# ─── POSITION SIZING ─────────────────────────────────────────────────────────

def calculate_position_size(
    investment_amount: float,
    buy_stop_price: float,
    stop_loss_price: float,
    available_funds: float,
    risk_pct: float = 0.01
) -> Tuple[int, float, float]:
    """
    Returns (quantity, capital_required, max_risk)
    Never exceeds 1% risk or available_funds
    Returns (0, 0, 0) if trade is not viable
    """
    if buy_stop_price <= stop_loss_price:
        return 0, 0.0, 0.0

    max_risk     = investment_amount * risk_pct
    risk_per_share = buy_stop_price - stop_loss_price
    quantity     = int(max_risk / risk_per_share)  # floor division

    if quantity <= 0:
        return 0, 0.0, 0.0

    capital_required = quantity * buy_stop_price

    # Reduce if not enough available funds
    if capital_required > available_funds:
        quantity = int(available_funds / buy_stop_price)
        if quantity <= 0:
            return 0, 0.0, 0.0
        capital_required = quantity * buy_stop_price

    return quantity, round(capital_required, 2), round(max_risk, 2)

# ─── AVAILABLE FUNDS ─────────────────────────────────────────────────────────

def get_available_funds(user: User, db: Session) -> float:
    """Investment amount minus cost of all open positions"""
    positions = db.query(Position).filter(
        Position.user_id == user.id,
        Position.is_open == True
    ).all()

    invested = sum(p.entry_price * p.quantity for p in positions)
    return max(0.0, user.investment_amount - invested)

# ─── SIGNAL GENERATION FOR ONE STOCK / ONE USER ──────────────────────────────

def generate_signal_for_stock(
    stock: BSEStock,
    user: User,
    df: pd.DataFrame,
    db: Session,
    today: date
) -> Optional[Dict]:
    """
    Generates signal dict for one stock/user combination.
    Returns None if no signal or insufficient data.
    """
    if df is None or len(df) < 60:
        return None

    df = calculate_channels(df)
    if df.empty:
        return None

    latest = df.iloc[-1]
    prev   = df.iloc[-2] if len(df) > 1 else None

    ch55_high      = latest.get("ch55_high")
    ch20_low       = latest.get("ch20_low")
    adx_val        = latest.get("adx")
    adx_rising     = bool(latest.get("adx_rising", False))
    five_day_count = int(latest.get("five_day_count", 0))

    if pd.isna(ch55_high) or pd.isna(adx_val):
        return None

    # ── Check if user holds this stock ──────────────────────────────────────
    open_position = db.query(Position).filter(
        Position.user_id  == user.id,
        Position.stock_id == stock.id,
        Position.is_open  == True
    ).first()

    # ── EXIT SIGNAL CHECK (if in position) ──────────────────────────────────
    if open_position:
        exit_reason = None
        today_close = float(latest["close"])
        today_low   = float(latest["low"])
        entry_date  = open_position.entry_date
        days_held   = (today - entry_date).days

        # 1. Rejection Rule (days 1 and 2, and ongoing)
        if today_close < ch55_high:
            exit_reason = "rejection_rule"

        # 2. ADX exit — was above 40, now declining
        if not exit_reason and prev is not None:
            prev_adx = float(prev.get("adx", 0))
            if prev_adx >= 40 and not adx_rising:
                exit_reason = "adx_exit"

        # 3. Trailing stop — 20-day low
        if not exit_reason and ch20_low and today_low <= ch20_low:
            exit_reason = "trailing_stop"

        if exit_reason:
            estimated_pnl = (today_close - open_position.entry_price) * open_position.quantity
            return {
                "signal_type":     "EXIT",
                "channel_55_high": float(ch55_high),
                "channel_20_low":  float(ch20_low) if ch20_low else None,
                "five_day_count":  five_day_count,
                "adx_value":       round(float(adx_val), 2),
                "adx_rising":      adx_rising,
                "exit_reason":     exit_reason,
                "exit_price":      round(today_close, 2),
                "estimated_pnl":   round(estimated_pnl, 2),
                "quantity":        open_position.quantity,
            }
        else:
            return {"signal_type": "HOLD", "channel_55_high": float(ch55_high),
                    "channel_20_low": float(ch20_low) if ch20_low else None,
                    "five_day_count": five_day_count, "adx_value": round(float(adx_val), 2),
                    "adx_rising": adx_rising}

    # ── BUY SIGNAL CHECK (not in position) ───────────────────────────────────
    # All 4 conditions must be TRUE
    if five_day_count < 5:
        return {"signal_type": "STAND_ASIDE", "reason": f"5-Day Condition not met ({five_day_count}/5)",
                "five_day_count": five_day_count, "adx_value": round(float(adx_val), 2), "adx_rising": adx_rising,
                "channel_55_high": float(ch55_high)}

    if not adx_rising:
        return {"signal_type": "STAND_ASIDE", "reason": "ADX declining",
                "five_day_count": five_day_count, "adx_value": round(float(adx_val), 2), "adx_rising": adx_rising,
                "channel_55_high": float(ch55_high)}

    # BUY STOP price = 55-day high + 0.25% buffer
    buy_stop_price  = round(ch55_high * 1.0025, 2)
    stop_loss_price = round(float(latest["low"]), 2)   # Last bar low

    available_funds = get_available_funds(user, db)
    quantity, capital_required, max_risk = calculate_position_size(
        user.investment_amount, buy_stop_price, stop_loss_price, available_funds
    )

    if quantity == 0:
        return {"signal_type": "STAND_ASIDE", "reason": "Insufficient funds or zero quantity",
                "five_day_count": five_day_count, "adx_value": round(float(adx_val), 2), "adx_rising": adx_rising,
                "channel_55_high": float(ch55_high)}

    return {
        "signal_type":      "BUY",
        "channel_55_high":  float(ch55_high),
        "channel_20_low":   float(ch20_low) if ch20_low else None,
        "five_day_count":   five_day_count,
        "adx_value":        round(float(adx_val), 2),
        "adx_rising":       adx_rising,
        "buy_stop_price":   buy_stop_price,
        "stop_loss_price":  stop_loss_price,
        "quantity":         quantity,
        "capital_required": capital_required,
        "max_risk":         max_risk,
    }

# ─── RUN ENGINE FOR ALL USERS × ALL WATCHLIST STOCKS ─────────────────────────

def run_signal_engine(db: Session, target_date: date = None) -> int:
    """
    Full signal engine run.
    Returns count of signals generated.
    """
    if target_date is None:
        target_date = date.today()

    users = db.query(User).filter(User.is_active == True, User.onboarding_done == True).all()
    total_signals = 0

    for user in users:
        watchlist = db.query(Watchlist).filter(Watchlist.user_id == user.id).all()

        for wl in watchlist:
            stock = wl.stock
            # Fetch last 90 days of OHLCV
            cutoff = target_date - timedelta(days=120)
            rows = db.query(OHLCV).filter(
                OHLCV.stock_id == stock.id,
                OHLCV.date     >= cutoff,
                OHLCV.date     <= target_date
            ).order_by(OHLCV.date).all()

            if len(rows) < 60:
                logger.warning(f"Insufficient data for {stock.symbol} ({len(rows)} rows)")
                continue

            df = pd.DataFrame([{
                "date": r.date, "open": r.open, "high": r.high,
                "low": r.low, "close": r.close, "volume": r.volume
            } for r in rows])

            signal_data = generate_signal_for_stock(stock, user, df, db, target_date)
            if signal_data is None:
                continue

            # Save signal to DB
            signal = Signal(
                stock_id        = stock.id,
                user_id         = user.id,
                date            = target_date,
                signal_type     = signal_data.get("signal_type"),
                channel_55_high = signal_data.get("channel_55_high"),
                channel_20_low  = signal_data.get("channel_20_low"),
                five_day_count  = signal_data.get("five_day_count"),
                adx_value       = signal_data.get("adx_value"),
                adx_rising      = signal_data.get("adx_rising"),
                buy_stop_price  = signal_data.get("buy_stop_price"),
                stop_loss_price = signal_data.get("stop_loss_price"),
                quantity        = signal_data.get("quantity"),
                capital_required= signal_data.get("capital_required"),
                max_risk        = signal_data.get("max_risk"),
                exit_reason     = signal_data.get("exit_reason"),
                exit_price      = signal_data.get("exit_price"),
                estimated_pnl   = signal_data.get("estimated_pnl"),
            )
            db.add(signal)
            total_signals += 1

        db.commit()

    logger.info(f"Signal engine completed: {total_signals} signals generated for {target_date}")
    return total_signals

# ─── BACKTEST ENGINE ─────────────────────────────────────────────────────────

def run_backtest(
    df: pd.DataFrame,
    starting_capital: float,
    risk_pct: float = 0.01
) -> Dict:
    """
    Full backtest simulation of the Channel Breakout + 5-Day Condition system.
    Returns complete performance metrics and trade list.
    """
    df = calculate_channels(df)
    df = df.dropna(subset=["ch55_high", "ch20_low", "adx"]).reset_index(drop=True)

    capital   = starting_capital
    position  = None   # {"entry_price", "quantity", "entry_idx", "entry_date", "stop_loss"}
    trades    = []
    equity_curve = []

    entry_candidates = 0
    for i in range(1, len(df)):
        row  = df.iloc[i]
        prev = df.iloc[i-1]

        today_date  = row["date"]
        today_close = float(row["close"])
        today_low   = float(row["low"])
        today_high  = float(row["high"])
        ch55        = float(row["ch55_high"])
        ch20        = float(row["ch20_low"])
        adx         = float(row["adx"])
        adx_rising  = bool(row["adx_rising"])
        five_count  = int(row["five_day_count"])

        # ── Manage open position ───────────────────────────────────────
        if position:
            days_held   = i - position["entry_idx"]
            exit_reason = None
            exit_price  = today_close

            # 1. Rejection Rule
            if today_close < ch55:
                exit_reason = "rejection_rule"
            # 2. ADX exit
            elif float(prev["adx"]) >= 40 and not adx_rising:
                exit_reason = "adx_exit"
            # 3. Trailing Stop
            elif today_low <= ch20:
                exit_reason = "trailing_stop"
                exit_price  = ch20

            if exit_reason:
                pnl = (exit_price - position["entry_price"]) * position["quantity"]
                capital += position["entry_price"] * position["quantity"] + pnl
                trades.append({
                    "entry_date":   position["entry_date"],
                    "entry_price":  position["entry_price"],
                    "exit_date":    str(today_date),
                    "exit_price":   round(exit_price, 2),
                    "quantity":     position["quantity"],
                    "pnl":          round(pnl, 2),
                    "exit_reason":  exit_reason,
                    "days_held":    days_held,
                    "result":       "WIN" if pnl > 0 else "LOSS"
                })
                position = None

        # ── Check for new BUY entry ─────────────────────────────────────
        elif int(prev["five_day_count"]) >= 5 and adx_rising:
            entry_candidates += 1
            prev_ch55 = float(prev["ch55_high"])
            buy_stop  = round(prev_ch55 * 1.0025, 2)
            # Stop loss: entry bar low, but must be below buy_stop
            raw_stop  = float(row["low"])
            stop_loss = raw_stop if raw_stop < buy_stop else round(buy_stop * 0.98, 2)

            if today_high >= buy_stop:
                qty, cap, _ = calculate_position_size(
                    starting_capital, buy_stop, stop_loss, capital, risk_pct
                )
                if qty > 0 and cap <= capital:
                    capital -= cap
                    position = {
                        "entry_price": buy_stop,
                        "quantity":    qty,
                        "entry_idx":   i,
                        "entry_date":  str(today_date),
                        "stop_loss":   stop_loss
                    }

        # Portfolio value = cash + open position value
        port_val = capital + (today_close * position["quantity"] if position else 0)
        equity_curve.append({"date": str(today_date), "value": round(port_val, 2)})

    import logging; logging.getLogger(__name__).warning(f"Backtest debug: {len(df)} rows, entry_candidates={entry_candidates}, trades={len(trades)}"); print(f"BACKTEST: {len(df)} rows, candidates={entry_candidates}, trades={len(trades)}", flush=True)
    # Close any open position at last bar
    if position:
        last = df.iloc[-1]
        exit_price = float(last["close"])
        pnl = (exit_price - position["entry_price"]) * position["quantity"]
        trades.append({
            "entry_date":  position["entry_date"],
            "entry_price": position["entry_price"],
            "exit_date":   str(last["date"]),
            "exit_price":  round(exit_price, 2),
            "quantity":    position["quantity"],
            "pnl":         round(pnl, 2),
            "exit_reason": "end_of_data",
            "days_held":   len(df) - 1 - position["entry_idx"],
            "result":      "WIN" if pnl > 0 else "LOSS"
        })

    # ── Performance metrics ─────────────────────────────────────────────────
    if not trades:
        return {"trades": [], "metrics": {}, "equity_curve": equity_curve,
                "message": "No trades generated in selected date range"}

    pnls       = [t["pnl"] for t in trades]
    wins       = [p for p in pnls if p > 0]
    losses     = [p for p in pnls if p <= 0]
    total_pnl  = sum(pnls)
    win_rate   = len(wins) / len(trades) * 100 if trades else 0
    avg_win    = sum(wins)   / len(wins)   if wins   else 0
    avg_loss   = sum(losses) / len(losses) if losses else 0
    expectancy = (win_rate/100 * avg_win) + ((1 - win_rate/100) * avg_loss)

    # Max drawdown
    peak = starting_capital
    max_dd = 0
    for point in equity_curve:
        val = point["value"]
        if val > peak:
            peak = val
        dd = (peak - val) / peak * 100
        if dd > max_dd:
            max_dd = dd

    return {
        "trades": trades,
        "equity_curve": equity_curve,
        "metrics": {
            "total_trades":     len(trades),
            "win_rate":         round(win_rate, 1),
            "total_pnl":        round(total_pnl, 2),
            "total_return_pct": round(total_pnl / starting_capital * 100, 2),
            "max_drawdown_pct": round(max_dd, 2),
            "avg_win":          round(avg_win, 2),
            "avg_loss":         round(avg_loss, 2),
            "expectancy":       round(expectancy, 2),
            "largest_win":      round(max(pnls), 2),
            "largest_loss":     round(min(pnls), 2),
            "starting_capital": starting_capital,
            "final_capital":    round(starting_capital + total_pnl, 2),
        }
    }


# ─── PORTFOLIO SIMULATION ENGINE ─────────────────────────────────────────────

def run_portfolio_backtest(
    stock_dfs: Dict[str, pd.DataFrame],
    stock_names: Dict[str, str],
    starting_capital: float,
    risk_pct: float = 0.01,
    max_positions: int = 10,
) -> Dict:
    """
    Multi-stock portfolio backtest with shared capital allocation.

    Algorithm (per day):
      1. Process exits for all open positions (freeing capital).
      2. Find new entry candidates: five_day_count >= 5, adx_rising,
         buy stop price triggered today.
      3. Rank candidates by five_day_count desc (most mature first),
         then ADX desc as tiebreak.
      4. Allocate capital to candidates until max_positions reached
         or capital exhausted.

    Args:
        stock_dfs:         {str(stock_id): OHLCV DataFrame}
        stock_names:       {str(stock_id): company_name}
        starting_capital:  total INR to simulate with
        risk_pct:          fraction of starting_capital risked per trade (default 1%)
        max_positions:     max concurrent open positions (default 10)

    Returns dict with: trades, equity_curve, metrics, stock_breakdown
    """
    # Pre-compute channels; skip stocks with < 60 rows
    computed: Dict[str, pd.DataFrame] = {}
    for sid, df in stock_dfs.items():
        if len(df) < 60:
            logger.warning(f"Skipping stock {sid}: only {len(df)} rows")
            continue
        c = calculate_channels(df.copy())
        c = c.dropna(subset=["ch55_high", "adx"]).reset_index(drop=True)
        if len(c) > 0:
            computed[sid] = c

    if not computed:
        return {
            "trades": [], "equity_curve": [], "metrics": {},
            "message": "No stocks have sufficient data (need >= 60 rows each)",
        }

    # Build {date → row_index} lookup per stock for O(1) daily access
    date_idx: Dict[str, Dict] = {}
    for sid, df in computed.items():
        date_idx[sid] = {row["date"]: i for i, row in df.iterrows()}

    all_dates = sorted(set(d for di in date_idx.values() for d in di))

    capital   = starting_capital
    positions: Dict[str, Dict] = {}   # sid → {entry_price, qty, entry_date, stop_loss}
    trades:    List[Dict]       = []
    equity_curve: List[Dict]    = []

    today_rows: Dict[str, any] = {}   # kept for end-of-data close

    for today in all_dates:
        today_rows = {}
        prev_rows  = {}
        for sid, df in computed.items():
            if today in date_idx[sid]:
                idx = date_idx[sid][today]
                today_rows[sid] = df.iloc[idx]
                if idx > 0:
                    prev_rows[sid] = df.iloc[idx - 1]

        # ── Step 1: Process exits ────────────────────────────────────────────
        for sid in list(positions.keys()):
            if sid not in today_rows:
                continue
            row  = today_rows[sid]
            prev = prev_rows.get(sid)
            pos  = positions[sid]

            ch55        = row.get("ch55_high")
            ch20        = row.get("ch20_low")
            adx_rising  = bool(row.get("adx_rising", False))
            today_close = float(row["close"])
            today_low   = float(row["low"])

            if pd.isna(ch55):
                continue

            exit_reason = None
            exit_price  = today_close

            # 1. Rejection rule
            if today_close < float(ch55):
                exit_reason = "rejection_rule"
            # 2. ADX exit
            elif prev is not None:
                prev_adx = float(prev.get("adx", 0) or 0)
                if prev_adx >= 40 and not adx_rising:
                    exit_reason = "adx_exit"
            # 3. Trailing stop
            if not exit_reason and not pd.isna(ch20) and today_low <= float(ch20):
                exit_reason = "trailing_stop"
                exit_price  = float(ch20)

            if exit_reason:
                pnl = (exit_price - pos["entry_price"]) * pos["qty"]
                capital += pos["entry_price"] * pos["qty"] + pnl
                entry_date = pos["entry_date"]
                days_held  = (today - entry_date).days if isinstance(today, date) and isinstance(entry_date, date) else 0
                trades.append({
                    "stock_id":    sid,
                    "stock_name":  stock_names.get(sid, sid),
                    "entry_date":  str(entry_date),
                    "entry_price": pos["entry_price"],
                    "exit_date":   str(today),
                    "exit_price":  round(exit_price, 2),
                    "quantity":    pos["qty"],
                    "pnl":         round(pnl, 2),
                    "exit_reason": exit_reason,
                    "days_held":   days_held,
                    "result":      "WIN" if pnl > 0 else "LOSS",
                })
                del positions[sid]

        # ── Step 2: Find entry candidates ────────────────────────────────────
        entry_candidates = []
        for sid, row in today_rows.items():
            if sid in positions:
                continue
            prev = prev_rows.get(sid)
            if prev is None:
                continue

            five_count = int(row.get("five_day_count", 0) or 0)
            adx_rising = bool(row.get("adx_rising", False))
            ch55_prev  = prev.get("ch55_high")

            if pd.isna(ch55_prev) or five_count < 5 or not adx_rising:
                continue

            buy_stop   = round(float(ch55_prev) * 1.0025, 2)
            today_high = float(row["high"])

            if today_high < buy_stop:
                continue  # buy stop not triggered

            stop_loss = float(row["low"])
            if stop_loss >= buy_stop:
                stop_loss = round(buy_stop * 0.98, 2)

            entry_candidates.append({
                "sid":        sid,
                "buy_stop":   buy_stop,
                "stop_loss":  stop_loss,
                "five_count": five_count,
                "adx":        float(row.get("adx", 0) or 0),
            })

        # Most-mature 5-day condition first; ADX as tiebreak
        entry_candidates.sort(key=lambda x: (x["five_count"], x["adx"]), reverse=True)

        # ── Step 3: Allocate capital ─────────────────────────────────────────
        for cand in entry_candidates:
            if len(positions) >= max_positions:
                break
            qty, cap, _ = calculate_position_size(
                starting_capital, cand["buy_stop"], cand["stop_loss"], capital, risk_pct
            )
            if qty <= 0 or cap > capital:
                continue
            capital -= cap
            positions[cand["sid"]] = {
                "entry_price": cand["buy_stop"],
                "qty":         qty,
                "entry_date":  today,
                "stop_loss":   cand["stop_loss"],
            }

        # ── Equity snapshot ──────────────────────────────────────────────────
        pos_value = sum(
            float(today_rows[sid]["close"]) * pos["qty"]
            for sid, pos in positions.items()
            if sid in today_rows
        )
        equity_curve.append({
            "date":            str(today),
            "value":           round(capital + pos_value, 2),
            "cash":            round(capital, 2),
            "open_positions":  len(positions),
        })

    # Close any positions still open at last bar
    last_date = all_dates[-1] if all_dates else None
    for sid, pos in list(positions.items()):
        if sid in today_rows:
            exit_price = float(today_rows[sid]["close"])
            pnl        = (exit_price - pos["entry_price"]) * pos["qty"]
            entry_date = pos["entry_date"]
            days_held  = (last_date - entry_date).days if last_date and isinstance(entry_date, date) else 0
            trades.append({
                "stock_id":    sid,
                "stock_name":  stock_names.get(sid, sid),
                "entry_date":  str(entry_date),
                "entry_price": pos["entry_price"],
                "exit_date":   str(last_date),
                "exit_price":  round(exit_price, 2),
                "quantity":    pos["qty"],
                "pnl":         round(pnl, 2),
                "exit_reason": "end_of_data",
                "days_held":   days_held,
                "result":      "WIN" if pnl > 0 else "LOSS",
            })

    # ── Aggregate metrics ────────────────────────────────────────────────────
    if not trades:
        return {
            "trades": [], "equity_curve": equity_curve,
            "metrics": {}, "stock_breakdown": [],
            "message": "No trades generated in selected date range",
        }

    pnls      = [t["pnl"] for t in trades]
    wins      = [p for p in pnls if p > 0]
    losses    = [p for p in pnls if p <= 0]
    total_pnl = sum(pnls)
    win_rate  = len(wins) / len(trades) * 100
    avg_win   = sum(wins)   / len(wins)   if wins   else 0
    avg_loss  = sum(losses) / len(losses) if losses else 0

    peak   = starting_capital
    max_dd = 0.0
    for pt in equity_curve:
        val = pt["value"]
        if val > peak:
            peak = val
        dd = (peak - val) / peak * 100 if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd

    # Per-stock breakdown
    stock_agg: Dict[str, Dict] = {}
    for t in trades:
        sid = t["stock_id"]
        if sid not in stock_agg:
            stock_agg[sid] = {"name": t["stock_name"], "trades": 0, "pnl": 0.0, "wins": 0}
        stock_agg[sid]["trades"] += 1
        stock_agg[sid]["pnl"]    += t["pnl"]
        if t["pnl"] > 0:
            stock_agg[sid]["wins"] += 1

    stock_breakdown = sorted([
        {
            "stock_name": v["name"],
            "trades":     v["trades"],
            "total_pnl":  round(v["pnl"], 2),
            "win_rate":   round(v["wins"] / v["trades"] * 100, 1) if v["trades"] > 0 else 0,
        }
        for v in stock_agg.values()
    ], key=lambda x: x["total_pnl"], reverse=True)

    return {
        "trades":          trades,
        "equity_curve":    equity_curve,
        "stock_breakdown": stock_breakdown,
        "metrics": {
            "total_trades":       len(trades),
            "win_rate":           round(win_rate, 1),
            "total_pnl":          round(total_pnl, 2),
            "total_return_pct":   round(total_pnl / starting_capital * 100, 2),
            "max_drawdown_pct":   round(max_dd, 2),
            "avg_win":            round(avg_win, 2),
            "avg_loss":           round(avg_loss, 2),
            "starting_capital":   starting_capital,
            "final_capital":      round(starting_capital + total_pnl, 2),
            "stocks_tested":      len(computed),
            "stocks_with_trades": len(stock_agg),
        },
    }
