"""
Provenance Pulse Scraper
Clicks 3m / 1m / 1w on https://provenance.io/pulse and appends
all 12 metrics to provenance_pulse.csv in the repo root.
"""

import re
import csv
import asyncio
from datetime import datetime, timezone
from pathlib import Path

CSV_PATH = Path("provenance_pulse.csv")

TIME_PERIODS = ["3m", "1m", "1w"]

FIELDNAMES = [
    "captured_at", "time_period",
    "tvl_usd", "trading_tvl_usd",
    "total_participants", "total_committed_value_usd",
    "total_loan_balance_usd", "total_loans",
    "chain_transactions", "chain_fees_usd",
    "loan_amount_funded_usd", "loans_funded",
    "loan_amount_paid_usd", "loans_paid",
    "scrape_status", "error_message",
]

LABEL_MAP = {
    "tvl":                      "tvl_usd",
    "trading tvl":              "trading_tvl_usd",
    "total participants":       "total_participants",
    "total committed value":    "total_committed_value_usd",
    "total loan balance":       "total_loan_balance_usd",
    "total loans":              "total_loans",
    "chain transactions":       "chain_transactions",
    "chain fees":               "chain_fees_usd",
    "loan amount funded":       "loan_amount_funded_usd",
    "loans funded":             "loans_funded",
    "loan amount paid":         "loan_amount_paid_usd",
    "loans paid":               "loans_paid",
}

PERIOD_BUTTONS = {
    "3m": ["3m", "3M", "3 Months", "3mo"],
    "1m": ["1m", "1M", "1 Month",  "1mo"],
    "1w": ["1w", "1W", "1 Week",   "7d"],
}


def parse_value(text: str):
    if not text:
        return None
    t = text.strip().replace(",", "").replace("$", "").replace(" ", "")
    mult = 1
    if t.upper().endswith("T"): mult = 1_000_000_000_000; t = t[:-1]
    elif t.upper().endswith("B"): mult = 1_000_000_000;   t = t[:-1]
    elif t.upper().endswith("M"): mult = 1_000_000;       t = t[:-1]
    elif t.upper().endswith("K"): mult = 1_000;           t = t[:-1]
    try:
        return float(t) * mult
    except ValueError:
        return None


def normalise_label(raw: str) -> str:
    s = raw.lower().strip()
    s = re.sub(r"^(\d+\s*(months?|weeks?|days?|hours?|h)\s*)", "", s)
    return s.strip()


def ensure_csv_header():
    if not CSV_PATH.exists():
        with open(CSV_PATH, "w", newline="") as f:
            csv.DictWriter(f, fieldnames=FIELDNAMES).writeheader()
        print(f"[CSV] Created {CSV_PATH}")


def append_row(row: dict):
    with open(CSV_PATH, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES, extrasaction="ignore")
        writer.writerow(row)
    print(f"[CSV] Appended [{row['time_period']}] at {row['captured_at']}")


async def extract_metrics(page) -> dict:
    body_text = await page.inner_text("body")
    lines = [l.strip() for l in body_text.split("\n") if l.strip()]
    metrics = {}
    i = 0
    while i < len(lines):
        norm = normalise_label(lines[i])
        if norm in LABEL_MAP:
            col = LABEL_MAP[norm]
            for j in range(i + 1, min(i + 5, len(lines))):
                val_str = lines[j].strip()
                if re.match(r"^\$?[\d.,]+[TBMKtbmk]?$", val_str):
                    parsed = parse_value(val_str)
                    if parsed is not None and col not in metrics:
                        metrics[col] = parsed
                    break
        i += 1
    return metrics


async def click_period(page, period: str) -> bool:
    for label in PERIOD_BUTTONS[period]:
        try:
            btn = page.get_by_role("button", name=re.compile(f"^{re.escape(label)}$", re.IGNORECASE))
            if await btn.count() > 0:
                await btn.first.click()
                print(f"[Scraper] Clicked [{period}] via role=button '{label}'")
                return True
        except Exception:
            pass
        try:
            for tag in ("button", "span", "div", "li", "a"):
                els = await page.query_selector_all(f"{tag}:has-text('{label}')")
                for el in els:
                    txt = (await el.inner_text()).strip()
                    if txt.lower() == label.lower():
                        await el.click()
                        print(f"[Scraper] Clicked [{period}] via <{tag}>")
                        return True
        except Exception:
            pass
    print(f"[Scraper] WARNING: Could not find button for [{period}]")
    return False


async def scrape_all_periods() -> dict:
    from playwright.async_api import async_playwright
    results = {}
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(viewport={"width": 1440, "height": 900})
        print("[Scraper] Loading https://provenance.io/pulse ...")
        await page.goto("https://provenance.io/pulse", wait_until="networkidle", timeout=90_000)
        await asyncio.sleep(5)
        for period in TIME_PERIODS:
            await click_period(page, period)
            await asyncio.sleep(3)
            metrics = await extract_metrics(page)
            found = sum(1 for v in metrics.values() if v is not None)
            print(f"[Scraper] [{period}] {found}/12 metrics found")
            results[period] = metrics
        await browser.close()
    return results


def run():
    ensure_csv_header()
    captured_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    print(f"\n{'='*55}\n[Scraper] Run at {captured_at}\n{'='*55}")
    try:
        all_periods = asyncio.run(scrape_all_periods())
        for period, metrics in all_periods.items():
            append_row({"captured_at": captured_at, "time_period": period,
                        "scrape_status": "ok", "error_message": "", **metrics})
        print("[Scraper] ✓ Done.")
    except Exception as e:
        print(f"[Scraper] ERROR: {e}")
        for period in TIME_PERIODS:
            append_row({"captured_at": captured_at, "time_period": period,
                        "scrape_status": "error", "error_message": str(e)})
        raise


if __name__ == "__main__":
    run()
