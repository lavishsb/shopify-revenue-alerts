"""
Shopify Revenue Alert System
Compares current hour revenue vs same hour yesterday/last_week.
Sends HTML email alert via Gmail SMTP if revenue drops by X%.
"""

import os
import json
import smtplib
import logging
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path

import pytz
import requests

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Config from environment variables
# ---------------------------------------------------------------------------
SHOPIFY_STORE     = os.environ["SHOPIFY_STORE"]          # e.g. mystore.myshopify.com
SHOPIFY_TOKEN     = os.environ["SHOPIFY_TOKEN"]          # Admin API access token
EMAIL_FROM        = os.environ["EMAIL_FROM"]
EMAIL_TO          = os.environ["EMAIL_TO"]               # comma-separated for multiple
SMTP_USER         = os.environ["SMTP_USER"]
SMTP_PASS         = os.environ["SMTP_PASS"]

ALERT_THRESHOLD   = float(os.environ.get("ALERT_THRESHOLD", "20"))   # % drop
COMPARE_TO        = os.environ.get("COMPARE_TO", "yesterday")        # yesterday | last_week
TIMEZONE          = os.environ.get("TIMEZONE", "Asia/Kolkata")
COOLDOWN_HOURS    = float(os.environ.get("COOLDOWN_HOURS", "2"))
STORE_NAME        = os.environ.get("STORE_NAME", SHOPIFY_STORE)
COOLDOWN_FILE     = os.environ.get("COOLDOWN_FILE", "/tmp/last_alert.json")

API_VERSION       = "2024-01"
VALID_STATUSES    = {"paid", "partially_paid", "pending"}

# ---------------------------------------------------------------------------
# Shopify helpers
# ---------------------------------------------------------------------------

def _shopify_headers() -> dict:
    return {
        "X-Shopify-Access-Token": SHOPIFY_TOKEN,
        "Content-Type": "application/json",
    }


def fetch_orders_for_window(start: datetime, end: datetime) -> list[dict]:
    """
    Fetch all paid/partially_paid/pending orders in [start, end].
    Handles cursor-based pagination via Link header.
    """
    url = (
        f"https://{SHOPIFY_STORE}/admin/api/{API_VERSION}/orders.json"
    )
    params = {
        "status": "any",
        "created_at_min": start.isoformat(),
        "created_at_max": end.isoformat(),
        "limit": 250,
        "fields": "id,total_price,financial_status,created_at",
    }

    orders: list[dict] = []
    page = 1

    while url:
        log.info("  Fetching page %d → %s", page, url)
        resp = requests.get(url, headers=_shopify_headers(), params=params, timeout=30)
        resp.raise_for_status()

        batch = resp.json().get("orders", [])
        log.info("  Got %d orders in batch", len(batch))

        for order in batch:
            if order.get("financial_status") in VALID_STATUSES:
                orders.append(order)

        # Parse Link header for next cursor
        url = _parse_next_link(resp.headers.get("Link", ""))
        params = None  # params only on first request; subsequent use cursor URL
        page += 1

    return orders


def _parse_next_link(link_header: str) -> str | None:
    """Extract the 'next' URL from a Shopify Link header."""
    if not link_header:
        return None
    for part in link_header.split(","):
        part = part.strip()
        if 'rel="next"' in part:
            url_part = part.split(";")[0].strip()
            return url_part.strip("<>")
    return None


def calc_revenue(orders: list[dict]) -> float:
    return sum(float(o.get("total_price", 0)) for o in orders)


# ---------------------------------------------------------------------------
# Time window helpers
# ---------------------------------------------------------------------------

def get_last_completed_hour(tz: pytz.BaseTzInfo) -> tuple[datetime, datetime]:
    """Returns (start, end) for the last fully completed hour in local tz."""
    now = datetime.now(tz)
    end   = now.replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=1)
    return start, end


def get_comparison_window(current_start: datetime, compare_to: str) -> tuple[datetime, datetime]:
    """Returns the same 1-hour window shifted by -1 day or -7 days."""
    if compare_to == "last_week":
        delta = timedelta(weeks=1)
    else:  # default: yesterday
        delta = timedelta(days=1)

    ref_start = current_start - delta
    ref_end   = ref_start + timedelta(hours=1)
    return ref_start, ref_end


# ---------------------------------------------------------------------------
# Cooldown system
# ---------------------------------------------------------------------------

def _load_cooldown() -> dict:
    path = Path(COOLDOWN_FILE)
    if path.exists():
        try:
            with path.open() as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def _save_cooldown(data: dict) -> None:
    path = Path(COOLDOWN_FILE)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w") as f:
        json.dump(data, f, indent=2)


def is_in_cooldown(window_key: str) -> bool:
    """Returns True if an alert for this window_key was sent within COOLDOWN_HOURS."""
    data = _load_cooldown()
    last_sent_str = data.get(window_key)
    if not last_sent_str:
        return False
    last_sent = datetime.fromisoformat(last_sent_str)
    elapsed = (datetime.utcnow() - last_sent).total_seconds() / 3600
    log.info("Cooldown check: last alert %.2fh ago (limit %.1fh)", elapsed, COOLDOWN_HOURS)
    return elapsed < COOLDOWN_HOURS


def record_alert(window_key: str) -> None:
    data = _load_cooldown()
    data[window_key] = datetime.utcnow().isoformat()
    _save_cooldown(data)
    log.info("Cooldown recorded for key: %s", window_key)


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def _fmt_window(start: datetime, end: datetime) -> str:
    fmt = "%d %b %Y %I:%M %p %Z"
    return f"{start.strftime(fmt)} → {end.strftime(fmt)}"


def _fmt_inr(amount: float) -> str:
    return f"₹{amount:,.2f}"


def build_html_email(
    current_start: datetime, current_end: datetime, current_rev: float,
    ref_start: datetime, ref_end: datetime, ref_rev: float,
    drop_pct: float,
    sent_at: datetime,
) -> str:
    drop_color = "#d93025"  # red
    threshold_note = f"{ALERT_THRESHOLD:.0f}%"

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<style>
  body      {{ font-family: Arial, sans-serif; background: #f4f4f4; margin: 0; padding: 20px; }}
  .card     {{ background: #ffffff; border-radius: 8px; max-width: 620px;
               margin: 0 auto; padding: 30px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
  h2        {{ color: #d93025; margin-top: 0; }}
  table     {{ width: 100%; border-collapse: collapse; margin-top: 16px; }}
  th        {{ background: #f0f0f0; text-align: left; padding: 10px 12px;
               font-size: 13px; color: #555; border: 1px solid #ddd; }}
  td        {{ padding: 10px 12px; border: 1px solid #ddd; font-size: 14px; }}
  .drop     {{ color: {drop_color}; font-weight: bold; font-size: 18px; }}
  .footer   {{ margin-top: 24px; font-size: 12px; color: #888; text-align: center; }}
  .badge    {{ display: inline-block; background: #fff3f3; border: 1px solid #d93025;
               border-radius: 4px; padding: 2px 8px; color: #d93025;
               font-weight: bold; font-size: 13px; }}
</style>
</head>
<body>
<div class="card">
  <h2>⚠️ Revenue Drop Alert — {STORE_NAME}</h2>
  <p>
    Revenue has dropped by <span class="badge">{drop_pct:.1f}%</span>, exceeding the
    configured threshold of <strong>{threshold_note}</strong>.
  </p>

  <table>
    <thead>
      <tr>
        <th>Metric</th>
        <th>Value</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td><strong>Current Hour Window</strong></td>
        <td>{_fmt_window(current_start, current_end)}</td>
      </tr>
      <tr>
        <td><strong>Current Hour Revenue</strong></td>
        <td><strong>{_fmt_inr(current_rev)}</strong></td>
      </tr>
      <tr>
        <td><strong>Comparison Window ({COMPARE_TO.replace("_", " ")})</strong></td>
        <td>{_fmt_window(ref_start, ref_end)}</td>
      </tr>
      <tr>
        <td><strong>Comparison Revenue</strong></td>
        <td>{_fmt_inr(ref_rev)}</td>
      </tr>
      <tr>
        <td><strong>Revenue Drop</strong></td>
        <td><span class="drop">▼ {drop_pct:.1f}%</span></td>
      </tr>
    </tbody>
  </table>

  <div class="footer">
    Alert generated at {sent_at.strftime("%d %b %Y %I:%M:%S %p %Z")} &nbsp;|&nbsp;
    Threshold: {threshold_note} &nbsp;|&nbsp; Store: {STORE_NAME}
  </div>
</div>
</body>
</html>"""


def send_email(subject: str, html_body: str) -> None:
    recipients = [r.strip() for r in EMAIL_TO.split(",") if r.strip()]

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_FROM
    msg["To"]      = ", ".join(recipients)
    msg.attach(MIMEText(html_body, "html"))

    log.info("Connecting to Gmail SMTP…")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(EMAIL_FROM, recipients, msg.as_string())
    log.info("Email sent to: %s", recipients)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    tz = pytz.timezone(TIMEZONE)

    # 1. Determine time windows
    curr_start, curr_end = get_last_completed_hour(tz)
    ref_start,  ref_end  = get_comparison_window(curr_start, COMPARE_TO)

    window_key = curr_start.strftime("%Y-%m-%dT%H")
    log.info("Current window  : %s → %s", curr_start, curr_end)
    log.info("Reference window: %s → %s", ref_start, ref_end)

    # 2. Cooldown check
    if is_in_cooldown(window_key):
        log.info("Within cooldown period for %s — skipping.", window_key)
        return

    # 3. Fetch revenue
    log.info("Fetching current hour orders…")
    curr_orders = fetch_orders_for_window(curr_start, curr_end)
    curr_rev    = calc_revenue(curr_orders)
    log.info("Current revenue : ₹%.2f (%d orders)", curr_rev, len(curr_orders))

    log.info("Fetching comparison hour orders…")
    ref_orders  = fetch_orders_for_window(ref_start, ref_end)
    ref_rev     = calc_revenue(ref_orders)
    log.info("Reference revenue: ₹%.2f (%d orders)", ref_rev, len(ref_orders))

    # 4. Calculate drop
    if ref_rev == 0:
        log.warning("Reference revenue is ₹0 — cannot calculate drop. Skipping alert.")
        return

    drop_pct = ((ref_rev - curr_rev) / ref_rev) * 100
    log.info("Revenue change  : %.2f%%  (threshold: %.0f%%)", drop_pct, ALERT_THRESHOLD)

    if drop_pct < ALERT_THRESHOLD:
        log.info("No alert needed (drop %.2f%% < threshold %.0f%%).", drop_pct, ALERT_THRESHOLD)
        return

    # 5. Send alert
    log.warning("ALERT: Revenue dropped %.2f%% — sending email.", drop_pct)
    sent_at   = datetime.now(tz)
    subject   = (
        f"⚠️ [{STORE_NAME}] Revenue Alert: ▼{drop_pct:.1f}% drop "
        f"at {curr_start.strftime('%I %p %Z')}"
    )
    html_body = build_html_email(
        curr_start, curr_end, curr_rev,
        ref_start,  ref_end,  ref_rev,
        drop_pct, sent_at,
    )
    send_email(subject, html_body)

    # 6. Record cooldown
    record_alert(window_key)
    log.info("Done.")


if __name__ == "__main__":
    main()
