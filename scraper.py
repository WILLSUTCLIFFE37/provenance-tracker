"""
Provenance Pulse Scraper
Handles all time-period label variants:
  3m: "3 Months Chain Transactions"
  1m: "Month's Chain Transactions"  
  1w: "Week's Chain Transactions"
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

# Maps period key → the exact button text visible on the page
PERIOD_BUTTON_TEXT = {
    "3m": "3m",
    "1m": "1m",
    "1w": "1w",
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
    """
    Strip ALL time-period prefixes so labels match LABEL_MAP keys.

    Handles:
      "3 Months Chain Transactions"  -> "chain transactions"
      "3 Months Loan Amount Funded"  -> "loan amount funded"
      "Month's Chain Fees"           -> "chain fees"
      "Month's Loans Funded"         -> "loans funded"
      "Week's Chain Transactions"    -> "chain transactions"
      "Week's Loan Amount Paid"      -> "loan amount paid"
    """
    s = raw.lower().strip()

    # Remove patterns like: "3 months ", "3months", "1 month ", "1week "
    s = re.sub(r"^\d+\s*(months?|weeks?|days?|hours?)\s*", "", s)

    # Remove possessive patterns like: "month's ", "week's ", "months' "
    s = re.sub(r"^(months?'s?|weeks?'s?|days?'s?)\s*", "", s)

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
    """Parse only lines below the 'Provenance Blockchain Metrics' heading."""
    body_text = await page.inner_text("body")
    lines = [l.strip() for l in body_text.split("\n") if l.strip()]

    section_start = 0
    for i, line in enumerate(lines):
        if "provenance blockchain metrics" in line.lower():
            section_start = i
            break

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
    Use JavaScript to find all period-selector containers on the page,
    take the LAST one (Blockchain Metrics), and click the target period within it.
    """
    target = PERIOD_BUTTON_TEXT[period]  # "3m", "1m", or "1w"

    result = await page.evaluate(f"""
        () => {{
            const target = '{target}';

            // Find every element whose DIRECT children include buttons/spans
            // with text matching the four period options
            const periodSet = new Set(['24h', '1w', '1m', '3m']);
            const containers = [];

            for (const el of document.querySelectorAll('*')) {{
                const directChildTexts = Array.from(el.children)
                    .map(c => c.innerText?.trim().toLowerCase())
                    .filter(Boolean);
                const hits = directChildTexts.filter(t => periodSet.has(t));
                if (hits.length >= 3) {{
                    containers.push(el);
                }}
            }}

            if (containers.length === 0) {{
                return 'ERROR: no selector containers found';
            }}

            // The LAST container is the Blockchain Metrics one
            const blockchainContainer = containers[containers.length - 1];

            // Click the child whose text matches our target
            for (const child of blockchainContainer.children) {{
                if (child.innerText?.trim().toLowerCase() === target) {{
                    child.click();
                    return `OK: clicked '${{target}}' in container ${{containers.length}} of ${{containers.length}}`;
                }}
            }}

            // If direct children didn't match, try descendants
            for (const desc of blockchainContainer.querySelectorAll('*')) {{
                if (desc.innerText?.trim().toLowerCase() === target) {{
                    desc.click();
                    return `OK: clicked '${{target}}' in descendant of last container`;
                }}
            }}

            return `ERROR: target '${{target}}' not found in last container`;
        }}
    """)

    print(f"[Scraper] [{period}] {result}")
    return str(result).startswith("OK")


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
            print(f"\n[Scraper] --- Period: {period} ---")
            await click_blockchain_period(page, period)
            await asyncio.sleep(3)

            metrics = await extract_metrics_from_section(page)
            found = sum(1 for v in metrics.values() if v is not None)
            print(f"[Scraper] [{period}] {found}/12 metrics found")
            for k, v in metrics.items():
                print(f"           {k}: {v}")
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
        print("\n[Scraper] ✓ All done.")
    except Exception as e:
        print(f"[Scraper] ERROR: {e}")
        for period in TIME_PERIODS:
            append_row({"captured_at": captured_at, "time_period": period,
                        "scrape_status": "error", "error_message": str(e)})
        raise


if __name__ == "__main__":
    run()
