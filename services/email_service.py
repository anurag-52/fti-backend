import os
import logging
import urllib.request
import urllib.error
import json
from typing import List, Dict
from datetime import date
from sqlalchemy.orm import Session
from database import User, Signal, Position, BSEStock, EmailLog

logger = logging.getLogger(__name__)

BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")
FROM_EMAIL    = os.getenv("FROM_EMAIL", "freedomtraderindia@gmail.com")
APP_URL       = os.getenv("APP_URL", "http://localhost:8000")

def send_email(to_email: str, subject: str, html_body: str) -> bool:
    if not BREVO_API_KEY:
        logger.warning("BREVO_API_KEY not set")
        return False
    try:
        payload = json.dumps({
            "sender": {"name": "FTI Trading", "email": FROM_EMAIL},
            "to": [{"email": to_email}],
            "subject": subject,
            "htmlContent": html_body
        }).encode("utf-8")
        req = urllib.request.Request(
            "https://api.brevo.com/v3/smtp/email",
            data=payload,
            headers={
                "api-key": BREVO_API_KEY,
                "Content-Type": "application/json",
                "Accept": "application/json"
            },
            method="POST"
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            logger.info(f"Email sent to {to_email} | messageId={result.get('messageId')}")
            return True
    except urllib.error.HTTPError as e:
        logger.error(f"Brevo API error {e.code}: {e.read().decode()}")
        return False
    except Exception as e:
        logger.error(f"Email failed to {to_email}: {e}")
        return False

EMAIL_STYLE = """
<style>
  body { font-family: Arial, sans-serif; background: #f4f6fa; margin: 0; padding: 0; }
  .container { max-width: 700px; margin: 20px auto; background: #fff; border-radius: 10px;
               box-shadow: 0 2px 12px rgba(0,0,0,0.08); overflow: hidden; }
  .header { background: #1B4F8A; color: #fff; padding: 28px 32px; }
  .header h1 { margin: 0; font-size: 22px; }
  .header p  { margin: 6px 0 0; font-size: 14px; opacity: 0.85; }
  .body { padding: 28px 32px; }
  .section-title { font-size: 16px; font-weight: bold; color: #1B4F8A;
                   border-bottom: 2px solid #1B4F8A; padding-bottom: 6px; margin: 24px 0 14px; }
  table { width: 100%; border-collapse: collapse; font-size: 13px; }
  th { background: #1B4F8A; color: #fff; padding: 10px 8px; text-align: left; }
  td { padding: 9px 8px; border-bottom: 1px solid #eee; }
  tr:nth-child(even) td { background: #f4f8ff; }
  .hit   { color: #1E8449; font-weight: bold; }
  .nohit { color: #C0392B; font-weight: bold; }
  .btn { display: inline-block; padding: 10px 22px; background: #1B4F8A;
         color: #fff !important; text-decoration: none; border-radius: 6px;
         font-size: 14px; font-weight: bold; margin: 8px 4px; }
  .btn-yes { background: #1E8449; }
  .btn-no  { background: #C0392B; }
  .footer  { background: #f4f6fa; padding: 18px 32px; font-size: 12px; color: #888;
             text-align: center; }
  .badge-buy  { background: #d4edda; color: #1E8449; padding: 3px 8px;
                border-radius: 12px; font-weight: bold; font-size: 12px; }
  .badge-exit { background: #fad7d7; color: #C0392B; padding: 3px 8px;
                border-radius: 12px; font-weight: bold; font-size: 12px; }
  .empty-msg { color: #888; font-style: italic; padding: 12px 0; }
  @media (max-width: 600px) {
    .body, .header { padding: 18px 16px; }
    table { font-size: 11px; }
    th, td { padding: 7px 5px; }
  }
</style>
"""

# ─── 5 PM CONFIRMATION EMAIL ──────────────────────────────────────────────────

def build_confirmation_email(
    user: User,
    confirmations: List[Dict],   # [{stock, bse_code, rec_price, today_high, hit, conf_id}]
    today: date
) -> str:
    date_str = today.strftime("%d %b %Y")
    rows = ""
    for c in confirmations:
        hit_badge = f'<span class="hit">✅ HIT</span>' if c["hit"] else f'<span class="nohit">❌ NOT HIT</span>'
        action = ""
        if c["hit"]:
            yes_url = f"{APP_URL}/confirm?id={c['conf_id']}&action=yes"
            no_url  = f"{APP_URL}/confirm?id={c['conf_id']}&action=no"
            action  = f'<a href="{yes_url}" class="btn btn-yes">YES, I Bought</a> <a href="{no_url}" class="btn btn-no">NO, I Didn\'t</a>'
        else:
            action = '<span style="color:#888">—</span>'
        rows += f"""
        <tr>
          <td><strong>{c['stock']}</strong><br><small style="color:#888">{c['bse_code']}</small></td>
          <td>₹{c['rec_price']:,.2f}</td>
          <td>₹{c['today_high']:,.2f}</td>
          <td>{hit_badge}</td>
          <td>{c['qty']} shares</td>
          <td>{action}</td>
        </tr>"""

    if not rows:
        rows = '<tr><td colspan="6" class="empty-msg">No pending BUY recommendations from yesterday.</td></tr>'

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
{EMAIL_STYLE}</head><body>
<div class="container">
  <div class="header">
    <h1>🔔 Action Required — Confirm Your Trades</h1>
    <p>Freedom Trader Intensive | {date_str} | Please confirm before 6:00 PM IST</p>
  </div>
  <div class="body">
    <p>Hi <strong>{user.name}</strong>,</p>
    <p>Below are yesterday's BUY recommendations. Please confirm which stocks you were able to purchase on your brokerage.</p>

    <div class="section-title">Yesterday's BUY Recommendations — Hit Status</div>
    <table>
      <tr><th>Stock</th><th>Rec. Buy Price</th><th>Today's High</th><th>Status</th><th>Qty</th><th>Did You Buy?</th></tr>
      {rows}
    </table>

    <p style="margin-top:20px">
      <a href="{APP_URL}/dashboard" class="btn">📊 Open Full Dashboard</a>
    </p>
    <p style="color:#C0392B; font-size:13px">
      ⚠️ You must submit your confirmation before <strong>6:00 PM IST</strong> to receive tonight's recommendations.
    </p>
  </div>
  <div class="footer">FTI Trading App | Based on Courtney Smith's Channel Breakout System<br>
  <a href="{APP_URL}">Open App</a> | This is not financial advice. Trade at your own risk.</div>
</div></body></html>"""

# ─── 6 PM RECOMMENDATION EMAIL ────────────────────────────────────────────────

def build_recommendation_email(
    user: User,
    buy_signals: List[Dict],   # [{stock, bse_code, buy_stop, stop_loss, qty, capital, max_risk, five_day, adx}]
    exit_signals: List[Dict],  # [{stock, bse_code, entry_price, exit_price, qty, pnl, exit_reason, days_held}]
    today: date
) -> str:
    date_str = today.strftime("%d %b %Y")

    # BUY signals table
    buy_rows = ""
    for s in buy_signals:
        buy_rows += f"""
        <tr>
          <td><strong>{s['stock']}</strong><br><small style="color:#888">{s['bse_code']}</small></td>
          <td><strong>₹{s['buy_stop']:,.2f}</strong></td>
          <td>₹{s['stop_loss']:,.2f}</td>
          <td><strong>{s['qty']}</strong></td>
          <td>₹{s['capital']:,.0f}</td>
          <td>₹{s['max_risk']:,.0f}</td>
          <td>{s['five_day']}d</td>
          <td>{s['adx']:.1f} {'↑' if s['adx_rising'] else '↓'}</td>
        </tr>"""
    if not buy_rows:
        buy_rows = '<tr><td colspan="8" class="empty-msg">No BUY signals for tomorrow. All conditions not met for any watched stock.</td></tr>'

    # EXIT signals table
    exit_rows = ""
    for s in exit_signals:
        pnl_color = "#1E8449" if s['pnl'] >= 0 else "#C0392B"
        pnl_str   = f'+₹{s["pnl"]:,.0f}' if s['pnl'] >= 0 else f'-₹{abs(s["pnl"]):,.0f}'
        reason_map = {"rejection_rule": "Rejection Rule", "adx_exit": "ADX Exit (40+)",
                      "trailing_stop": "Trailing Stop (20-Day)"}
        exit_rows += f"""
        <tr>
          <td><strong>{s['stock']}</strong><br><small style="color:#888">{s['bse_code']}</small></td>
          <td>₹{s['entry_price']:,.2f}</td>
          <td><strong>₹{s['exit_price']:,.2f}</strong></td>
          <td>{s['qty']}</td>
          <td style="color:{pnl_color}; font-weight:bold">{pnl_str}</td>
          <td>{reason_map.get(s['exit_reason'], s['exit_reason'])}</td>
          <td>{s['days_held']}d</td>
        </tr>"""
    if not exit_rows:
        exit_rows = '<tr><td colspan="7" class="empty-msg">No EXIT signals for tomorrow. All open positions are within hold parameters.</td></tr>'

    return f"""<!DOCTYPE html><html><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
{EMAIL_STYLE}</head><body>
<div class="container">
  <div class="header">
    <h1>📈 Trading Recommendations for Tomorrow</h1>
    <p>Freedom Trader Intensive | {date_str} | Place orders tomorrow on your brokerage</p>
  </div>
  <div class="body">
    <p>Hi <strong>{user.name}</strong>,</p>
    <p>Here are your personalised recommendations based on your ₹{user.investment_amount:,.0f} portfolio and 1% risk per trade.</p>

    <div class="section-title">🟢 BUY Signals — Place as Buy Stop Orders Tomorrow</div>
    <div style="overflow-x:auto">
    <table>
      <tr><th>Stock</th><th>Buy Stop ₹</th><th>Stop Loss ₹</th><th>Qty</th><th>Capital ₹</th><th>Max Risk ₹</th><th>5-Day</th><th>ADX</th></tr>
      {buy_rows}
    </table>
    </div>
    <p style="font-size:12px; color:#888">💡 Place as Buy Stop order. If price doesn't reach Buy Stop, order won't trigger.</p>

    <div class="section-title">🔴 EXIT / SELL Signals — Exit Tomorrow at Open</div>
    <div style="overflow-x:auto">
    <table>
      <tr><th>Stock</th><th>Entry ₹</th><th>Exit ₹</th><th>Qty</th><th>Est. P&L</th><th>Reason</th><th>Held</th></tr>
      {exit_rows}
    </table>
    </div>
    <p style="font-size:12px; color:#888">💡 Exit at market open tomorrow. Use limit order near the exit price.</p>

    <p style="margin-top:24px">
      <a href="{APP_URL}/dashboard" class="btn">📊 Open My Dashboard</a>
      <a href="{APP_URL}/confirm" class="btn" style="background:#555">✅ Confirm Today's Trades</a>
    </p>
    <p style="color:#888; font-size:12px; margin-top:16px">
      Remember: Confirm today's trade purchases before <strong>6:00 PM IST tomorrow</strong> to receive the next day's recommendations.
    </p>
  </div>
  <div class="footer">FTI Trading App | Based on Courtney D. Smith's Channel Breakout System<br>
  <a href="{APP_URL}">Open App</a> | This is not financial advice. Always verify before trading.</div>
</div></body></html>"""

# ─── SEND TO ALL USERS ────────────────────────────────────────────────────────

def send_recommendation_emails(db: Session, target_date: date) -> Dict:
    """Send 6 PM recommendation emails to all active users"""
    users = db.query(User).filter(
        User.is_active == True,
        User.is_admin  == False,
        User.onboarding_done == True
    ).all()

    summary = {"sent": 0, "failed": 0}

    for user in users:
        # Get BUY signals for this user today
        buy_sigs = db.query(Signal).join(BSEStock).filter(
            Signal.user_id     == user.id,
            Signal.date        == target_date,
            Signal.signal_type == "BUY",
            Signal.is_overridden == False
        ).all()

        exit_sigs = db.query(Signal).join(BSEStock).filter(
            Signal.user_id     == user.id,
            Signal.date        == target_date,
            Signal.signal_type == "EXIT",
            Signal.is_overridden == False
        ).all()

        buy_list = [{
            "stock":      s.stock.company_name,
            "bse_code":   s.stock.bse_code,
            "buy_stop":   s.buy_stop_price or 0,
            "stop_loss":  s.stop_loss_price or 0,
            "qty":        s.quantity or 0,
            "capital":    s.capital_required or 0,
            "max_risk":   s.max_risk or 0,
            "five_day":   s.five_day_count or 0,
            "adx":        s.adx_value or 0,
            "adx_rising": s.adx_rising or False,
        } for s in buy_sigs]

        exit_list = []
        for s in exit_sigs:
            pos = db.query(Position).filter(
                Position.user_id  == user.id,
                Position.stock_id == s.stock_id,
                Position.is_open  == True
            ).first()
            if pos:
                days_held = (target_date - pos.entry_date).days
                exit_list.append({
                    "stock":       s.stock.company_name,
                    "bse_code":    s.stock.bse_code,
                    "entry_price": pos.entry_price,
                    "exit_price":  s.exit_price or 0,
                    "qty":         pos.quantity,
                    "pnl":         s.estimated_pnl or 0,
                    "exit_reason": s.exit_reason or "",
                    "days_held":   days_held,
                })

        html   = build_recommendation_email(user, buy_list, exit_list, target_date)
        result = send_email(user.email, f"[FTI] Recommendations for {target_date.strftime('%d %b %Y')}", html)

        # Log
        log = EmailLog(user_id=user.id, email_type="recommendation_6pm", date=target_date,
                       status="sent" if result else "failed")
        db.add(log)
        if result:
            summary["sent"] += 1
        else:
            summary["failed"] += 1

    db.commit()
    return summary
