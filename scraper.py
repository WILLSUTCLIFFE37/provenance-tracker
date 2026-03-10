"""
Provenance Pulse Scraper
Clicks 3m / 1m / 1w on the PROVENANCE BLOCKCHAIN METRICS selector
(the second/lower selector on the page — not the Hash Metrics one at the top)
and appends all 12 metrics to provenance_pulse.csv.
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

# Map our keys to the button text visible on the page
PERIOD_BUTTON_TEXT = {
    "3m": ["3m", "3M"],
    "1m": ["1m", "1M"],
    "1w": ["1w", "1W"],
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


async def extract_metrics_from_section(page) -> dict:
    """Extract metrics from the Provenance Blockchain Metrics section only."""
    body_text = await page.inner_text("body")
    lines = [l.strip() for l in body_text.split("\n") if l.strip()]

    # Find the start of the Provenance Blockchain Metrics section
    # so we only parse lines below that heading
    section_start = 0
    for i, line in enumerate(lines):
        if "provenance blockchain metrics" in line.lower():
            section_start = i
            break

    # Only parse lines from that section onwards
    relevant_lines = lines[section_start:]

    metrics = {}
    i = 0
    while i < len(relevant_lines):
        norm = normalise_label(relevant_lines[i])
        if norm in LABEL_MAP:
            col = LABEL_MAP[norm]
            for j in range(i + 1, min(i + 5, len(relevant_lines))):
                val_str = relevant_lines[j].strip()
                if re.match(r"^\$?[\d.,]+[TBMKtbmk]?$", val_str):
                    parsed = parse_value(val_str)
                    if parsed is not None and col not in metrics:
                        metrics[col] = parsed
                    break
        i += 1
    return metrics


async def click_blockchain_period(page, period: str) -> bool:
    """
    Click the period button in the SECOND selector group (Provenance Blockchain Metrics).
    The page has two identical 24h/1w/1m/3m selectors — we want the lower one.
    Strategy: find ALL matching buttons and click the LAST one.
    """
    for label in PERIOD_BUTTON_TEXT[period]:
        try:
            # get_by_role finds all buttons with this text — .last targets the second selector
            btn = page.get_by_role("button", name=re.compile(f"^{re.escape(label)}$", re.IGNORECASE))
            count = await btn.count()
            if count >= 2:
                await btn.last.click()
                print(f"[Scraper] Clicked [{period}] — button {count}/{count} (Blockchain Metrics selector)")
                return True
            elif count == 1:
                # Only one found — still click it
                await btn.last.click()
                print(f"[Scraper] Clicked [{period}] — only 1 button found, clicking it")
                return True
        except Exception as e:
            print(f"[Scraper] role=button attempt failed: {e}")

        # Fallback: query all elements with matching text, click the last one
        try:
            for tag in ("button", "span", "div", "li", "a"):
                els = await page.query_selector_all(f"{tag}:has-text('{label}')")
                matching = []
                for el in els:
                    txt = (await el.inner_text()).strip()
                    if txt.lower() == label.lower():
                        matching.append(el)
                if len(matching) >= 1:
                    await matching[-1].click()  # last = Blockchain Metrics selector
                    print(f"[Scraper] Clicked [{period}] via last <{tag}> matching '{label}' ({len(matching)} found)")
                    return True
        except Exception as e:
            print(f"[Scraper] fallback attempt failed: {e}")

    print(f"[Scraper] WARNING: Could not find period button for [{period}]")
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
            await click_blockchain_period(page, period)
            await asyncio.sleep(3)

            metrics = await extract_metrics_from_section(page)
            found = sum(1 for v in metrics.values() if v is not None)
            print(f"[Scraper] [{period}] {found}/12 metrics found: {metrics}")
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
