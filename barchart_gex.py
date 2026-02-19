"""
Barchart GEX Scraper — Playwright (Headless Chromium)
Fetches EXACT gamma flip, call wall, put wall from Barchart.
Values are JS-rendered → needs real browser.

Install on Railway:
  pip install playwright
  playwright install --with-deps chromium
"""

import asyncio
import re
import logging
import time

logger = logging.getLogger(__name__)

# Cache to avoid hammering Barchart
_cache = {}  # ticker -> {levels, timestamp}
CACHE_TTL = 55  # seconds

ETF_TICKERS = {"QQQ", "SPY", "IWM", "DIA", "GLD", "SLV", "TLT", "XLF", "XLE", "VOO"}


def _get_url(ticker):
    asset_type = "etfs-funds" if ticker.upper() in ETF_TICKERS else "stocks"
    return f"https://www.barchart.com/{asset_type}/quotes/{ticker.upper()}/gamma-exposure"


async def fetch_barchart_gex_async(ticker="QQQ"):
    """
    Fetch GEX levels from Barchart using Playwright.
    Returns dict with gamma_flip, call_wall, put_wall, spot, source
    or None on failure.
    """
    ticker = ticker.upper()

    # Check cache
    cached = _cache.get(ticker)
    if cached and (time.time() - cached['timestamp']) < CACHE_TTL:
        logger.info(f"Barchart cache hit for {ticker} (age: {time.time() - cached['timestamp']:.0f}s)")
        return cached['levels']

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright not installed! pip install playwright && playwright install --with-deps chromium")
        return None

    levels = {}

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-extensions',
                    '--disable-background-networking',
                    '--no-first-run',
                ]
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
            )

            page = await context.new_page()
            url = _get_url(ticker)

            logger.info(f"Barchart Playwright: loading {ticker}...")
            await page.goto(url, wait_until='domcontentloaded', timeout=30000)

            # Wait for gamma flip text to appear (JS renders it)
            try:
                await page.wait_for_function(
                    "document.body.innerText.includes('gamma flip point is')",
                    timeout=20000
                )
                logger.info(f"Barchart Playwright: gamma flip text rendered")
            except Exception:
                logger.info(f"Barchart Playwright: waiting extra for JS...")
                await asyncio.sleep(5)

            # Get rendered text (all JS executed)
            text = await page.evaluate("document.body.innerText")
            logger.info(f"Barchart Playwright: got {len(text)} chars")

            await browser.close()

            # ── Parse values ──
            # "GLD gamma flip point is 391.72"
            gf_match = re.search(
                rf'{ticker}\s+gamma\s+flip\s+point\s+is\s+(\d+\.?\d*)',
                text, re.IGNORECASE
            )
            if gf_match:
                levels['gamma_flip'] = float(gf_match.group(1))
                logger.info(f"Barchart: Gamma Flip = {levels['gamma_flip']}")

            # "GLD put wall is 450.00"
            pw_match = re.search(
                rf'{ticker}\s+put\s+wall\s+is\s+(\d+\.?\d*)',
                text, re.IGNORECASE
            )
            if pw_match:
                levels['put_wall'] = float(pw_match.group(1))
                logger.info(f"Barchart: Put Wall = {levels['put_wall']}")

            # "GLD call wall is 475.00"
            cw_match = re.search(
                rf'{ticker}\s+call\s+wall\s+is\s+(\d+\.?\d*)',
                text, re.IGNORECASE
            )
            if cw_match:
                levels['call_wall'] = float(cw_match.group(1))
                logger.info(f"Barchart: Call Wall = {levels['call_wall']}")

            # Spot price
            spot_match = re.search(r'Last Price\s*\$?([\d,]+\.?\d*)', text)
            if not spot_match:
                spot_match = re.search(r'(\d{2,4}\.\d{2})\s+[+-]?\d+\.\d+\s+[+-]?\d+\.\d+%', text)
            if spot_match:
                levels['spot'] = float(spot_match.group(1).replace(',', ''))

    except Exception as e:
        logger.error(f"Barchart Playwright error: {e}")
        return None

    if 'gamma_flip' in levels:
        if 'spot' in levels:
            levels['gamma_regime'] = "Positiv" if levels['spot'] > levels['gamma_flip'] else "Negativ"
        else:
            levels['gamma_regime'] = "N/A"
        levels['source'] = 'barchart'

        # Update cache
        _cache[ticker] = {'levels': levels, 'timestamp': time.time()}

        logger.info(f"Barchart SUCCESS {ticker}: GF={levels.get('gamma_flip')} "
                     f"CW={levels.get('call_wall')} PW={levels.get('put_wall')}")
        return levels

    logger.warning(f"Barchart Playwright: could not parse GEX for {ticker}")
    return None


def fetch_barchart_gex(ticker="QQQ"):
    """Synchronous wrapper — for non-async callers."""
    try:
        return asyncio.run(fetch_barchart_gex_async(ticker))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(fetch_barchart_gex_async(ticker))
        loop.close()
        return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    async def test():
        for t in ["QQQ", "GLD"]:
            print(f"\n{'='*40}")
            print(f"  {t} GEX from Barchart (Playwright)")
            print(f"{'='*40}")
            result = await fetch_barchart_gex_async(t)
            if result:
                for k, v in result.items():
                    print(f"  {k}: {v}")
            else:
                print("  FAILED")

    asyncio.run(test())
