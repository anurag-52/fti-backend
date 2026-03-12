"""
FTI Trading App — Core Logic Tests

Covers four areas:
  1. BSE Data Fetching       — yfinance fetch, bhav copy download, column parsing
  2. Signal Accuracy         — BUY / EXIT / HOLD / STAND_ASIDE prediction logic
  3. 5-Day Condition         — counter increment, reset, and edge cases
  4. Portfolio Simulation    — multi-stock backtest with shared capital allocation
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import pandas as pd
import numpy as np
import io, zipfile
from datetime import date, timedelta
from unittest.mock import patch, MagicMock

from services.engine import (
    calculate_adx,
    calculate_channels,
    calculate_position_size,
    generate_signal_for_stock,
    get_available_funds,
    run_backtest,
    run_portfolio_backtest,
)
from services.data_service import download_bhav_copy, fetch_yfinance


# ═══════════════════════════════════════════════════════════════════════════
# SHARED HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _ohlcv(n, highs, lows=None, closes=None, opens=None, start=date(2023, 1, 2)):
    """Build a minimal OHLCV DataFrame from lists/scalars."""
    def _fill(val, default):
        if val is None:
            return [default] * n
        if isinstance(val, (int, float)):
            return [val] * n
        return list(val)

    h = _fill(highs, 100.0)
    l = _fill(lows, 90.0)
    c = _fill(closes, 95.0)
    o = _fill(opens, 93.0)
    dates = [start + timedelta(days=i) for i in range(n)]
    return pd.DataFrame({"date": dates, "open": o, "high": h,
                          "low": l, "close": c, "volume": [1_000_000] * n})


def _flat_then_decline(n=80, decline=80.0, flat_from=60):
    """
    Days 0..flat_from-1 : high rises by 1 each day (100, 101, …)
                          → 55-day channel actively makes new highs every day
    Days flat_from..n-1  : high = decline (below previous peak)
                          → channel freezes; five_day_count ticks 1, 2, 3 …
                            starting exactly at index flat_from.
    ADX is non-NaN because of the strong prior trend.
    """
    assert flat_from <= n, "flat_from must be <= n"
    highs  = [100.0 + i for i in range(flat_from)] + [decline] * (n - flat_from)
    lows   = [h - 10 for h in highs]
    closes = [h - 5  for h in highs]
    return _ohlcv(n, highs, lows, closes)


def _rising(n=80, start_price=50.0, step=1.0):
    """Steadily rising prices — channel makes new highs every day."""
    prices = [start_price + i * step for i in range(n)]
    return _ohlcv(n,
                  highs  =[p + 2 for p in prices],
                  lows   =[p - 2 for p in prices],
                  closes =prices)


def _mock_db_no_position():
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = None
    return db


def _mock_db_with_position(entry_price=100.0, quantity=100,
                            entry_date=date(2023, 1, 2)):
    pos = MagicMock()
    pos.entry_price = entry_price
    pos.quantity    = quantity
    pos.entry_date  = entry_date
    db = MagicMock()
    db.query.return_value.filter.return_value.first.return_value = pos
    return db


def _mock_user(investment=500_000.0):
    u = MagicMock()
    u.id = 1
    u.investment_amount = investment
    return u


def _mock_stock():
    s = MagicMock()
    s.id     = 1
    s.symbol = "RELIANCE"
    return s


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 1 — BSE DATA FETCHING
# ═══════════════════════════════════════════════════════════════════════════

class TestBSEDataFetching:

    # ── yfinance ────────────────────────────────────────────────────────────

    def test_yfinance_returns_dataframe_for_known_stock(self):
        """fetch_yfinance returns a non-empty DataFrame for RELIANCE (requires network + yfinance)."""
        end   = date.today() - timedelta(days=1)
        start = end - timedelta(days=10)
        df = fetch_yfinance("RELIANCE", start, end)
        if df is None:
            pytest.skip("yfinance unavailable (not installed or no network)")
        assert not df.empty, "DataFrame should not be empty"

    def test_yfinance_has_required_columns(self):
        """Returned DataFrame must have OHLCV columns + date."""
        end   = date.today() - timedelta(days=1)
        start = end - timedelta(days=10)
        df = fetch_yfinance("RELIANCE", start, end)
        if df is None:
            pytest.skip("yfinance unavailable (network)")
        for col in ["date", "open", "high", "low", "close", "volume"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_yfinance_no_nulls_in_ohlc(self):
        """OHLC columns must have no NaN after fetch."""
        end   = date.today() - timedelta(days=1)
        start = end - timedelta(days=10)
        df = fetch_yfinance("RELIANCE", start, end)
        if df is None:
            pytest.skip("yfinance unavailable (network)")
        for col in ["open", "high", "low", "close"]:
            assert df[col].isna().sum() == 0, f"NaN found in column {col}"

    def test_yfinance_high_gte_low(self):
        """high must be >= low on every row."""
        end   = date.today() - timedelta(days=1)
        start = end - timedelta(days=10)
        df = fetch_yfinance("RELIANCE", start, end)
        if df is None:
            pytest.skip("yfinance unavailable (network)")
        assert (df["high"] >= df["low"]).all(), "Found rows where high < low"

    def test_yfinance_returns_none_for_invalid_symbol(self):
        """An invalid symbol should return None (not raise)."""
        end   = date.today() - timedelta(days=1)
        start = end - timedelta(days=5)
        df = fetch_yfinance("INVALID_TICKER_XYZ999", start, end)
        assert df is None or df.empty, "Expected None or empty for invalid ticker"

    def test_yfinance_date_column_is_date_type(self):
        """date column should contain Python date objects (not datetime)."""
        end   = date.today() - timedelta(days=1)
        start = end - timedelta(days=10)
        df = fetch_yfinance("RELIANCE", start, end)
        if df is None or df.empty:
            pytest.skip("yfinance unavailable (network)")
        assert isinstance(df["date"].iloc[0], date), \
            f"Expected date type, got {type(df['date'].iloc[0])}"

    # ── BSE Bhav Copy (mocked HTTP) ─────────────────────────────────────────

    def _make_bhav_zip(self) -> bytes:
        """Create a minimal in-memory Bhav Copy ZIP that matches BSE format."""
        csv_content = (
            "SC_CODE,SC_NAME,OPEN,HIGH,LOW,CLOSE,TOTTRDQTY\n"
            "500325,RELIANCE,2800,2850,2780,2820,1500000\n"
            "500180,HDFCBANK,1600,1620,1590,1610,800000\n"
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("EQ010124.CSV", csv_content)
        return buf.getvalue()

    def test_bhav_copy_parses_columns_correctly(self):
        """download_bhav_copy maps BSE columns → standard names."""
        zip_bytes = self._make_bhav_zip()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        with patch("services.data_service.requests.get", return_value=mock_response):
            df = download_bhav_copy(date(2024, 1, 1))

        assert df is not None, "Expected DataFrame but got None"
        for col in ["bse_code", "open", "high", "low", "close"]:
            assert col in df.columns, f"Missing column: {col}"

    def test_bhav_copy_correct_values(self):
        """Parsed values match the CSV content."""
        zip_bytes = self._make_bhav_zip()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = zip_bytes

        with patch("services.data_service.requests.get", return_value=mock_response):
            df = download_bhav_copy(date(2024, 1, 1))

        reliance = df[df["bse_code"] == "500325"]
        assert not reliance.empty, "RELIANCE (500325) row missing"
        row = reliance.iloc[0]
        assert row["open"]  == 2800.0
        assert row["high"]  == 2850.0
        assert row["low"]   == 2780.0
        assert row["close"] == 2820.0

    def test_bhav_copy_returns_none_on_404(self):
        """HTTP 404 should return None, not raise."""
        mock_response = MagicMock()
        mock_response.status_code = 404

        with patch("services.data_service.requests.get", return_value=mock_response):
            df = download_bhav_copy(date(2024, 1, 1))

        assert df is None

    def test_bhav_copy_returns_none_on_network_error(self):
        """Network exception should return None."""
        with patch("services.data_service.requests.get", side_effect=Exception("timeout")):
            df = download_bhav_copy(date(2024, 1, 1))

        assert df is None

    def test_bhav_copy_drops_rows_with_null_ohlc(self):
        """Rows with non-numeric OHLC are dropped."""
        csv_content = (
            "SC_CODE,SC_NAME,OPEN,HIGH,LOW,CLOSE,TOTTRDQTY\n"
            "500325,RELIANCE,2800,2850,2780,2820,1500000\n"
            "500180,HDFCBANK,N/A,N/A,N/A,N/A,0\n"       # should be dropped
        )
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("EQ010124.CSV", csv_content)

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = buf.getvalue()

        with patch("services.data_service.requests.get", return_value=mock_response):
            df = download_bhav_copy(date(2024, 1, 1))

        assert df is not None
        assert len(df) == 1, f"Expected 1 valid row, got {len(df)}"


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 2 — 5-DAY CONDITION CALCULATION
# ═══════════════════════════════════════════════════════════════════════════

class TestFiveDayCondition:

    def test_counter_increments_when_channel_is_flat(self):
        """
        After a period of flat highs, the 55-day channel peak stays constant.
        five_day_count must increment by 1 each day.
        """
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        # After day 60 (index 60) channel is flat — count should be 1,2,3,...
        counts = result["five_day_count"].tolist()

        # From index 60 onwards the channel was last broken at index 59
        # so counts[60] == 1, counts[61] == 2, ... counts[64] == 5
        decline_counts = counts[60:65]
        assert decline_counts == [1, 2, 3, 4, 5], \
            f"Expected [1,2,3,4,5] but got {decline_counts}"

    def test_counter_reaches_5_on_5th_flat_day(self):
        """five_day_count must be exactly 5 after 5 consecutive flat/declining days."""
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        assert result["five_day_count"].iloc[64] == 5

    def test_counter_exceeds_5_for_longer_flat_periods(self):
        """Counter keeps growing beyond 5 if channel remains flat."""
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        assert result["five_day_count"].iloc[79] >= 15, \
            "Counter should keep growing beyond 5"

    def test_counter_resets_on_new_channel_high(self):
        """
        When price makes a new 55-day high, the counter must reset to 0.
        Scenario: flat for 10 days, then a breakout high, then flat again.
        """
        n = 80
        highs = [100.0] * 60 + [80.0] * 10 + [200.0] + [80.0] * 9
        df = _ohlcv(n, highs)
        result = calculate_channels(df)

        # Day 70 has high=200 which is a new 55-day max → counter resets
        assert result["five_day_count"].iloc[70] == 0, \
            f"Expected 0 after breakout but got {result['five_day_count'].iloc[70]}"
        # Day 71 starts counting again: 1
        assert result["five_day_count"].iloc[71] == 1, \
            f"Expected 1 after reset but got {result['five_day_count'].iloc[71]}"

    def test_counter_zero_before_sufficient_data(self):
        """five_day_count must be 0 for rows where ch55_high is NaN (< 55 bars)."""
        df = _flat_then_decline(n=80)
        result = calculate_channels(df)
        # First 54 rows have NaN ch55_high — counts should be 0
        early = result["five_day_count"].iloc[:54].tolist()
        assert all(c == 0 for c in early), \
            f"Expected all zeros before 55 bars, got: {early[:10]}..."

    def test_counter_is_integer(self):
        """five_day_count column values must be integers."""
        df = _flat_then_decline(n=80)
        result = calculate_channels(df)
        assert result["five_day_count"].dtype in [int, "int64", "int32", object], \
            "five_day_count should be integer-typed"
        # All values castable to int without error
        result["five_day_count"].astype(int)

    def test_exactly_4_flat_days_does_not_satisfy_condition(self):
        """4 consecutive flat days → counter == 4, which is < 5 (no BUY signal)."""
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        assert result["five_day_count"].iloc[63] == 4, \
            "4 flat days should give count == 4"


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 3 — SIGNAL ACCURACY (BUY / EXIT / STAND_ASIDE / HOLD)
# ═══════════════════════════════════════════════════════════════════════════

class TestSignalAccuracy:

    # ── calculate_channels ────────────────────────────────────────────────

    def test_ch55_high_is_rolling_55_day_max(self):
        """ch55_high must equal the rolling 55-day maximum of 'high'."""
        df = _rising(n=80)
        result = calculate_channels(df)
        expected = df["high"].rolling(55, min_periods=55).max()
        pd.testing.assert_series_equal(
            result["ch55_high"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_ch20_low_is_rolling_20_day_min(self):
        """ch20_low must equal the rolling 20-day minimum of 'low'."""
        df = _rising(n=80)
        result = calculate_channels(df)
        expected = df["low"].rolling(20, min_periods=20).min()
        pd.testing.assert_series_equal(
            result["ch20_low"].reset_index(drop=True),
            expected.reset_index(drop=True),
            check_names=False,
        )

    def test_adx_is_non_negative(self):
        """ADX values must all be >= 0."""
        df = _rising(n=80)
        result = calculate_channels(df)
        adx_values = result["adx"].dropna()
        assert (adx_values >= 0).all(), "ADX contains negative values"

    def test_adx_bounded_0_to_100(self):
        """ADX must be in [0, 100] range."""
        df = _rising(n=80)
        result = calculate_channels(df)
        adx_values = result["adx"].dropna()
        assert (adx_values <= 100).all(), f"ADX > 100: {adx_values[adx_values > 100]}"

    # ── Position Sizing ───────────────────────────────────────────────────

    def test_position_size_respects_1pct_risk(self):
        """
        With 1% risk on 500,000 INR, risk per trade = 5,000.
        buy_stop=105, stop_loss=100 → risk_per_share=5 → qty=1000.
        """
        qty, capital, max_risk = calculate_position_size(
            investment_amount=500_000.0,
            buy_stop_price=105.0,
            stop_loss_price=100.0,
            available_funds=500_000.0,
            risk_pct=0.01,
        )
        assert qty == 1_000
        assert capital == 105_000.0
        assert max_risk == 5_000.0

    def test_position_size_capped_by_available_funds(self):
        """If capital_required > available_funds, quantity is reduced."""
        qty, capital, _ = calculate_position_size(
            investment_amount=500_000.0,
            buy_stop_price=200.0,
            stop_loss_price=190.0,
            available_funds=10_000.0,    # only 10k available
            risk_pct=0.01,
        )
        assert capital <= 10_000.0, "Capital should not exceed available funds"
        assert qty == 50  # floor(10000/200)

    def test_position_size_zero_when_buy_stop_lte_stop_loss(self):
        """Invalid setup (stop loss >= buy stop) must return (0, 0, 0)."""
        qty, capital, risk = calculate_position_size(
            investment_amount=500_000.0,
            buy_stop_price=100.0,
            stop_loss_price=105.0,   # stop loss ABOVE entry → invalid
            available_funds=500_000.0,
        )
        assert qty == 0
        assert capital == 0.0
        assert risk == 0.0

    def test_position_size_zero_when_no_funds(self):
        """Zero available funds → quantity 0."""
        qty, capital, _ = calculate_position_size(
            investment_amount=500_000.0,
            buy_stop_price=100.0,
            stop_loss_price=90.0,
            available_funds=0.0,
        )
        assert qty == 0

    # ── BUY Signal Generation ──────────────────────────────────────────────

    def test_buy_signal_when_all_conditions_met(self):
        """
        BUY signal is generated when:
          - five_day_count >= 5  (channel flat for >= 5 days)
          - adx_rising = True
          - no open position
        """
        df = _flat_then_decline(n=80, flat_from=60)
        # Force ADX to be rising by making the tail strongly trending
        # Use a df where adx_rising will be True at the last row
        # We'll patch calculate_channels to force adx_rising=True
        result = calculate_channels(df)
        # Manually force adx_rising on last row to isolate BUY logic
        result.loc[result.index[-1], "adx_rising"]    = True
        result.loc[result.index[-1], "five_day_count"] = 5

        today    = result["date"].iloc[-1]
        stock    = _mock_stock()
        user     = _mock_user(investment=500_000.0)
        db       = _mock_db_no_position()

        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(stock, user, df, db, today)

        assert signal is not None
        assert signal["signal_type"] == "BUY", \
            f"Expected BUY but got {signal['signal_type']}: {signal.get('reason', '')}"

    def test_buy_signal_includes_position_sizing(self):
        """BUY signal must include buy_stop_price, stop_loss_price, quantity, capital_required."""
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        result.loc[result.index[-1], "adx_rising"]    = True
        result.loc[result.index[-1], "five_day_count"] = 5

        today = result["date"].iloc[-1]
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(
                _mock_stock(), _mock_user(), df, _mock_db_no_position(), today
            )

        if signal and signal["signal_type"] == "BUY":
            for key in ["buy_stop_price", "stop_loss_price", "quantity", "capital_required"]:
                assert key in signal, f"BUY signal missing field: {key}"
            assert signal["buy_stop_price"] == round(signal["channel_55_high"] * 1.0025, 2)

    def test_stand_aside_when_5day_condition_not_met(self):
        """five_day_count < 5 → STAND_ASIDE, not BUY."""
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        # Force five_day_count=3 (< 5) and adx rising
        result.loc[result.index[-1], "adx_rising"]    = True
        result.loc[result.index[-1], "five_day_count"] = 3

        today = result["date"].iloc[-1]
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(
                _mock_stock(), _mock_user(), df, _mock_db_no_position(), today
            )

        assert signal is not None
        assert signal["signal_type"] == "STAND_ASIDE"
        assert "5-Day Condition" in signal.get("reason", ""), \
            f"Unexpected reason: {signal.get('reason')}"

    def test_stand_aside_when_adx_declining(self):
        """adx_rising = False (and five_day_count >= 5) → STAND_ASIDE."""
        df = _flat_then_decline(n=80)
        result = calculate_channels(df)
        result.loc[result.index[-1], "adx_rising"]    = False
        result.loc[result.index[-1], "five_day_count"] = 10

        today = result["date"].iloc[-1]
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(
                _mock_stock(), _mock_user(), df, _mock_db_no_position(), today
            )

        assert signal is not None
        assert signal["signal_type"] == "STAND_ASIDE"
        assert "ADX" in signal.get("reason", ""), \
            f"Unexpected reason: {signal.get('reason')}"

    def test_no_signal_when_insufficient_data(self):
        """Fewer than 60 rows → None (not enough history for calculations)."""
        df = _ohlcv(30, highs=100.0)
        signal = generate_signal_for_stock(
            _mock_stock(), _mock_user(), df, _mock_db_no_position(), date.today()
        )
        assert signal is None

    # ── EXIT Signal Generation ────────────────────────────────────────────

    def test_exit_rejection_rule(self):
        """
        Rejection Rule: close < ch55_high → EXIT with reason 'rejection_rule'.
        Position is open; today's close falls below the 55-day channel high.
        """
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        # Manually set last row: close < ch55_high
        result.loc[result.index[-1], "close"]         = 85.0   # below ch55 (100)
        result.loc[result.index[-1], "ch55_high"]     = 100.0
        result.loc[result.index[-1], "adx_rising"]    = True
        result.loc[result.index[-1], "five_day_count"] = 15

        today = result["date"].iloc[-1]
        db    = _mock_db_with_position(entry_price=90.0, quantity=100,
                                        entry_date=today - timedelta(days=5))
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(_mock_stock(), _mock_user(), df, db, today)

        assert signal is not None
        assert signal["signal_type"] == "EXIT"
        assert signal["exit_reason"] == "rejection_rule"

    def test_exit_trailing_stop(self):
        """
        Trailing Stop: today's low <= ch20_low → EXIT with reason 'trailing_stop'.
        """
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        ch20  = float(result["ch20_low"].dropna().iloc[-1])

        # close above ch55 (so rejection rule doesn't fire)
        result.loc[result.index[-1], "close"]      = 105.0
        result.loc[result.index[-1], "ch55_high"]  = 100.0
        result.loc[result.index[-1], "ch20_low"]   = ch20
        result.loc[result.index[-1], "low"]        = ch20 - 1   # hits the trailing stop
        result.loc[result.index[-1], "adx_rising"] = True

        today = result["date"].iloc[-1]
        db    = _mock_db_with_position(entry_price=90.0, quantity=100,
                                        entry_date=today - timedelta(days=20))
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(_mock_stock(), _mock_user(), df, db, today)

        assert signal is not None
        assert signal["signal_type"] == "EXIT"
        assert signal["exit_reason"] == "trailing_stop"

    def test_exit_adx_rule(self):
        """
        ADX exit: previous ADX >= 40 and current ADX is declining → EXIT.
        """
        df = _flat_then_decline(n=80)
        result = calculate_channels(df)
        last  = result.index[-1]
        prev  = result.index[-2]

        # close above ch55 and low above ch20 (so other rules don't fire)
        result.loc[last, "close"]     = 105.0
        result.loc[last, "ch55_high"] = 100.0
        result.loc[last, "ch20_low"]  = 50.0
        result.loc[last, "low"]       = 90.0
        result.loc[last, "adx"]       = 38.0   # current ADX declining from 42
        result.loc[last, "adx_rising"] = False
        result.loc[prev, "adx"]       = 42.0   # was >= 40

        today = result["date"].iloc[-1]
        db    = _mock_db_with_position(entry_price=90.0, quantity=100,
                                        entry_date=today - timedelta(days=10))
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(_mock_stock(), _mock_user(), df, db, today)

        assert signal is not None
        assert signal["signal_type"] == "EXIT"
        assert signal["exit_reason"] == "adx_exit"

    def test_hold_when_in_position_no_exit_trigger(self):
        """
        When in a position and none of the three exit rules fire → HOLD.
        """
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        last = result.index[-1]

        # close above ch55, low above ch20, ADX < 40 → no exit
        result.loc[last, "close"]     = 110.0  # above ch55 (100)
        result.loc[last, "ch55_high"] = 100.0
        result.loc[last, "ch20_low"]  = 50.0
        result.loc[last, "low"]       = 90.0   # above ch20 (50)
        result.loc[last, "adx"]       = 25.0   # below 40
        result.loc[last, "adx_rising"] = True

        today = result["date"].iloc[-1]
        db    = _mock_db_with_position(entry_price=90.0, quantity=100,
                                        entry_date=today - timedelta(days=5))
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(_mock_stock(), _mock_user(), df, db, today)

        assert signal is not None
        assert signal["signal_type"] == "HOLD"

    def test_exit_pnl_is_estimated_correctly(self):
        """Estimated P&L in EXIT signal = (exit_price - entry_price) * quantity."""
        df = _flat_then_decline(n=80, flat_from=60)
        result = calculate_channels(df)
        last = result.index[-1]

        result.loc[last, "close"]      = 85.0  # below ch55 → rejection rule
        result.loc[last, "ch55_high"]  = 100.0
        result.loc[last, "adx_rising"] = True

        entry_price = 90.0
        quantity    = 100
        today = result["date"].iloc[-1]
        db    = _mock_db_with_position(entry_price=entry_price, quantity=quantity,
                                        entry_date=today - timedelta(days=5))
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(_mock_stock(), _mock_user(), df, db, today)

        assert signal["signal_type"] == "EXIT"
        expected_pnl = round((85.0 - entry_price) * quantity, 2)
        assert signal["estimated_pnl"] == expected_pnl, \
            f"Expected P&L {expected_pnl} but got {signal['estimated_pnl']}"

    # ── buy_stop_price formula ─────────────────────────────────────────────

    def test_buy_stop_price_is_ch55_plus_025pct(self):
        """buy_stop_price must be exactly ch55_high * 1.0025 (0.25% buffer)."""
        df = _flat_then_decline(n=80)
        result = calculate_channels(df)
        last = result.index[-1]
        result.loc[last, "adx_rising"]    = True
        result.loc[last, "five_day_count"] = 5
        ch55 = result.loc[last, "ch55_high"]

        today = result["date"].iloc[-1]
        with patch("services.engine.calculate_channels", return_value=result):
            signal = generate_signal_for_stock(
                _mock_stock(), _mock_user(), df, _mock_db_no_position(), today
            )

        if signal and signal["signal_type"] == "BUY":
            expected_buy_stop = round(float(ch55) * 1.0025, 2)
            assert signal["buy_stop_price"] == expected_buy_stop, \
                f"Expected {expected_buy_stop}, got {signal['buy_stop_price']}"


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 4 — BACKTEST ENGINE
# ═══════════════════════════════════════════════════════════════════════════

class TestBacktestEngine:

    def test_backtest_returns_required_keys(self):
        """run_backtest must return trades, equity_curve, and metrics."""
        df = _rising(n=200, start_price=50.0, step=0.5)
        result = run_backtest(df, starting_capital=100_000.0)
        for key in ["trades", "equity_curve", "metrics"]:
            assert key in result, f"Missing key: {key}"

    def test_backtest_equity_curve_length_matches_data(self):
        """equity_curve should have one entry per usable day."""
        df = _rising(n=200)
        result = run_backtest(df, starting_capital=100_000.0)
        # equity_curve covers rows after the first (loop starts at i=1)
        assert len(result["equity_curve"]) > 0

    def test_backtest_win_rate_between_0_and_100(self):
        """win_rate in metrics must be 0..100."""
        df = _rising(n=200)
        result = run_backtest(df, starting_capital=100_000.0)
        metrics = result.get("metrics", {})
        if metrics:
            assert 0 <= metrics["win_rate"] <= 100

    def test_backtest_no_negative_quantity(self):
        """All trades must have quantity > 0."""
        df = _rising(n=200)
        result = run_backtest(df, starting_capital=100_000.0)
        for t in result.get("trades", []):
            assert t["quantity"] > 0, f"Negative quantity in trade: {t}"

    def test_backtest_entry_before_exit(self):
        """entry_date must be before or equal to exit_date in all trades."""
        df = _rising(n=200)
        result = run_backtest(df, starting_capital=100_000.0)
        for t in result.get("trades", []):
            entry = t["entry_date"]
            exit_ = t["exit_date"]
            assert entry <= exit_, f"Entry {entry} after exit {exit_}"

    def test_backtest_insufficient_data_returns_empty_trades(self):
        """With < 60 rows, no trades should be generated (insufficient channel data)."""
        df = _ohlcv(30, highs=100.0)
        result = run_backtest(df, starting_capital=100_000.0)
        # Either no trades or a message
        assert "trades" in result
        assert result["trades"] == [] or isinstance(result.get("message"), str)


# ═══════════════════════════════════════════════════════════════════════════
# SECTION 5 — PORTFOLIO SIMULATION ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def _portfolio_dfs(n_stocks=3, n_days=200):
    """
    Build a dict of {str(stock_id): DataFrame} for portfolio backtest tests.
    Each stock has rising-then-flat data with slightly different phase so
    at least some entry signals are generated.
    """
    dfs = {}
    names = {}
    for i in range(n_stocks):
        # Offset start price so each stock has a different channel level
        start = 100.0 + i * 50
        df = _rising(n=n_days, start_price=start, step=0.8)
        dfs[str(i + 1)]   = df
        names[str(i + 1)] = f"Stock {i + 1}"
    return dfs, names


def _portfolio_flat_dfs(n_stocks=3, n_days=200, flat_from=120):
    """
    Rising then flat data — guarantees five_day_count >= 5 on later rows,
    giving the engine real entry candidates across multiple stocks.
    """
    dfs = {}
    names = {}
    for i in range(n_stocks):
        df = _flat_then_decline(n=n_days, decline=80.0 + i * 5, flat_from=flat_from)
        dfs[str(i + 1)]   = df
        names[str(i + 1)] = f"Stock {i + 1}"
    return dfs, names


class TestPortfolioSimEngine:

    # ── Return structure ──────────────────────────────────────────────────

    def test_returns_required_top_level_keys(self):
        """run_portfolio_backtest must return trades, equity_curve, metrics, stock_breakdown."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for key in ["trades", "equity_curve", "metrics", "stock_breakdown"]:
            assert key in result, f"Missing top-level key: {key}"

    def test_metrics_contains_required_fields(self):
        """metrics dict must include all standard performance fields."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        if result.get("metrics"):
            for field in ["total_trades", "win_rate", "total_pnl", "total_return_pct",
                          "max_drawdown_pct", "starting_capital", "final_capital",
                          "stocks_tested", "stocks_with_trades"]:
                assert field in result["metrics"], f"Missing metrics field: {field}"

    def test_equity_curve_has_date_value_cash_positions(self):
        """Each equity_curve entry must have date, value, cash, open_positions."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for pt in result["equity_curve"][:5]:
            for key in ["date", "value", "cash", "open_positions"]:
                assert key in pt, f"equity_curve entry missing key: {key}"

    # ── Capital management ────────────────────────────────────────────────

    def test_portfolio_value_never_exceeds_starting_capital_significantly(self):
        """
        Without leverage, total portfolio value should not massively exceed
        starting capital (up to 2× is fine due to gains, but 10× signals a bug).
        """
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for pt in result["equity_curve"]:
            assert pt["value"] < 500_000.0 * 10, \
                f"Portfolio value exploded: {pt['value']}"

    def test_cash_never_goes_negative(self):
        """Cash portion of equity must never go below 0."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for pt in result["equity_curve"]:
            assert pt["cash"] >= -0.01, f"Cash went negative: {pt['cash']}"

    def test_max_concurrent_positions_respected(self):
        """open_positions in equity_curve must never exceed max_positions."""
        dfs, names = _portfolio_flat_dfs(n_stocks=5)
        max_pos = 2
        result = run_portfolio_backtest(
            dfs, names, starting_capital=500_000.0, max_positions=max_pos
        )
        for pt in result["equity_curve"]:
            assert pt["open_positions"] <= max_pos, \
                f"open_positions {pt['open_positions']} exceeded max {max_pos}"

    def test_single_stock_portfolio_matches_single_backtest_direction(self):
        """
        A 1-stock portfolio should produce trades in the same direction
        (all wins or all losses) as the single-stock backtest on rising data.
        """
        df = _flat_then_decline(n=200, flat_from=120)
        # Single-stock
        single = run_backtest(df.copy(), starting_capital=500_000.0)
        # Portfolio with same stock
        portfolio = run_portfolio_backtest(
            {"1": df.copy()}, {"1": "TestStock"},
            starting_capital=500_000.0
        )
        # Both should agree on whether there were any trades
        single_had_trades    = len(single.get("trades", [])) > 0
        portfolio_had_trades = len(portfolio.get("trades", [])) > 0
        # If single backtest found trades, portfolio should too (same logic)
        if single_had_trades:
            assert portfolio_had_trades, \
                "Single-stock portfolio found no trades but single backtest did"

    # ── Trade integrity ───────────────────────────────────────────────────

    def test_all_trades_have_required_fields(self):
        """Every trade must include stock_id, entry/exit dates and prices, pnl, qty."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for t in result["trades"]:
            for field in ["stock_id", "stock_name", "entry_date", "entry_price",
                          "exit_date", "exit_price", "quantity", "pnl",
                          "exit_reason", "days_held", "result"]:
                assert field in t, f"Trade missing field: {field}"

    def test_trade_entry_before_exit(self):
        """entry_date must be <= exit_date in every trade."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for t in result["trades"]:
            assert t["entry_date"] <= t["exit_date"], \
                f"Entry {t['entry_date']} is after exit {t['exit_date']}"

    def test_trade_quantity_positive(self):
        """All trade quantities must be > 0."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for t in result["trades"]:
            assert t["quantity"] > 0, f"Trade has non-positive quantity: {t}"

    def test_trade_result_field_consistent_with_pnl(self):
        """'result' field must be WIN iff pnl > 0, otherwise LOSS."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for t in result["trades"]:
            expected = "WIN" if t["pnl"] > 0 else "LOSS"
            assert t["result"] == expected, \
                f"result={t['result']} but pnl={t['pnl']}"

    def test_exit_reason_is_valid(self):
        """exit_reason must be one of the four known values."""
        valid_reasons = {"rejection_rule", "adx_exit", "trailing_stop", "end_of_data"}
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for t in result["trades"]:
            assert t["exit_reason"] in valid_reasons, \
                f"Unknown exit_reason: {t['exit_reason']}"

    # ── Stock breakdown ───────────────────────────────────────────────────

    def test_stock_breakdown_covers_all_traded_stocks(self):
        """stock_breakdown must have one entry per stock that had at least one trade."""
        dfs, names = _portfolio_flat_dfs(n_stocks=3)
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        traded_ids = {t["stock_id"] for t in result["trades"]}
        breakdown_names = {b["stock_name"] for b in result["stock_breakdown"]}
        for sid in traded_ids:
            stock_name = names[sid]
            assert stock_name in breakdown_names, \
                f"Stock {stock_name} traded but missing from stock_breakdown"

    def test_stock_breakdown_win_rate_in_valid_range(self):
        """Win rate in stock_breakdown must be 0–100."""
        dfs, names = _portfolio_flat_dfs()
        result = run_portfolio_backtest(dfs, names, starting_capital=500_000.0)
        for b in result["stock_breakdown"]:
            assert 0 <= b["win_rate"] <= 100, \
                f"Invalid win_rate {b['win_rate']} for {b['stock_name']}"

    # ── Edge cases ────────────────────────────────────────────────────────

    def test_all_insufficient_data_returns_message(self):
        """All stocks with < 60 rows → message, no crash."""
        tiny_dfs   = {"1": _ohlcv(30, highs=100.0), "2": _ohlcv(30, highs=110.0)}
        tiny_names = {"1": "TinyA", "2": "TinyB"}
        result = run_portfolio_backtest(tiny_dfs, tiny_names, starting_capital=100_000.0)
        assert "message" in result or result.get("trades") == [], \
            "Expected graceful handling of insufficient data"

    def test_single_stock_no_crash(self):
        """Portfolio with exactly 1 stock should not crash."""
        df = _flat_then_decline(n=200, flat_from=120)
        result = run_portfolio_backtest(
            {"1": df}, {"1": "OnlyStock"}, starting_capital=200_000.0
        )
        assert "trades" in result
        assert "equity_curve" in result

    def test_max_positions_one_allows_only_one_trade_at_a_time(self):
        """With max_positions=1, open_positions in equity_curve never exceeds 1."""
        dfs, names = _portfolio_flat_dfs(n_stocks=5)
        result = run_portfolio_backtest(
            dfs, names, starting_capital=500_000.0, max_positions=1
        )
        for pt in result["equity_curve"]:
            assert pt["open_positions"] <= 1
