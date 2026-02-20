"""
ChartExchange Dark Pool Scraper — Playwright (Headless Chromium)
Fetches EXACT dark pool levels from ChartExchange.
Values are JS-rendered → needs real browser.

Replaces the broken POST API approach (ChartExchange blocks server requests).
"""

import asyncio
import re
import logging
import time

logger = logging.getLogger(__name__)

# Cache to avoid hammering
_cache = {}  # ticker -> {levels, timestamp}
CACHE_TTL = 300  # 5 minutes

# ✅ FIX: GLD/SLV/SPY/IWM sind alle NYSE Arca (ETFs!)
# Primary exchange per ticker
EXCHANGE_MAP = {
    'QQQ': 'nasdaq',
    'SPY': 'nyse_arca',
    'IWM': 'nyse_arca',
    'GLD': 'amex',        # ✅ amex funktioniert, nyse_arca gibt 404
    'SLV': 'amex',
    'AAPL': 'nasdaq', 'MSFT': 'nasdaq', 'AMZN': 'nasdaq',
    'NVDA': 'nasdaq', 'TSLA': 'nasdaq', 'META': 'nasdaq',
    'AMD': 'nasdaq', 'GOOGL': 'nasdaq',
}

# Fallback-Kette falls primary 404 gibt
EXCHANGE_FALLBACKS = {
    'GLD': ['amex', 'nyse_arca', 'nyse'],
    'SLV': ['amex', 'nyse_arca', 'nyse'],
    'SPY': ['nyse_arca', 'nyse'],
    'IWM': ['nyse_arca', 'nyse'],
}


def _normalize(ticker):
    """Normalize ticker aliases → canonical ChartExchange ticker."""
    aliases = {'GOLD': 'GLD', 'SILVER': 'SLV', 'NASDAQ': 'QQQ'}
    return aliases.get(ticker.upper(), ticker.upper())


def _get_urls(ticker):
    """Returns list of URLs to try in order."""
    ticker = _normalize(ticker)
    exchanges = EXCHANGE_FALLBACKS.get(ticker.upper())
    if not exchanges:
        exchanges = [EXCHANGE_MAP.get(ticker.upper(), 'nasdaq')]
    return [
        f"https://chartexchange.com/symbol/{ex}-{ticker.lower()}/exchange-volume/"
        for ex in exchanges
    ]


async def fetch_dp_playwright(ticker="QQQ", max_levels=15):
    """
    Fetch dark pool levels from ChartExchange using Playwright.
    Returns list of {'price': float, 'volume': int, 'trades': int}
    or empty list on failure.
    """
    ticker = _normalize(ticker.upper())

    # Check cache
    cached = _cache.get(ticker)
    if cached and (time.time() - cached['timestamp']) < CACHE_TTL:
        logger.info(f"ChartExchange DP cache hit for {ticker}")
        return cached['levels']

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright not installed!")
        return []

    levels = []

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
            urls_to_try = _get_urls(ticker)
            rows = []

            # Try each URL until one works
            for url in urls_to_try:
                logger.info(f"ChartExchange Playwright: trying {ticker} DP URL={url}")
                await page.goto(url, wait_until='networkidle', timeout=45000)
                await asyncio.sleep(3)

                title = await page.title()
                logger.info(f"ChartExchange DP: page title = '{title}'")

                if '404' in title or 'not found' in title.lower():
                    logger.warning(f"ChartExchange DP: 404 at {url}, trying next...")
                    continue

                # Wait for Dark Pool Levels table
                try:
                    await page.wait_for_selector(
                        '#darkpoollevels table tbody tr, [id*="darkpool"] table tbody tr, table tbody tr',
                        timeout=20000
                    )
                    logger.info(f"ChartExchange DP: table found at {url}")
                except Exception:
                    logger.warning(f"ChartExchange DP: no table at {url}, trying next...")
                    continue

                # Scroll to dark pool section
                await page.evaluate("document.querySelector('#darkpoollevels')?.scrollIntoView()")
                await asyncio.sleep(2)
                break  # Valid page found — extract below

            # Try extracting from DataTable via JS
            rows = await page.evaluate("""() => {
                const results = [];
                // Find all tables on page, look for one with Level/Trades/Volume headers
                const tables = document.querySelectorAll('table');
                for (const table of tables) {
                    const headers = table.querySelectorAll('thead th');
                    const headerTexts = Array.from(headers).map(h => h.textContent.trim().toLowerCase());
                    
                    if (headerTexts.includes('level') && headerTexts.includes('volume')) {
                        const tbody = table.querySelector('tbody');
                        if (!tbody) continue;
                        
                        const trs = tbody.querySelectorAll('tr');
                        for (const tr of trs) {
                            const tds = tr.querySelectorAll('td');
                            if (tds.length >= 3) {
                                const level = tds[0]?.textContent.trim();
                                const trades = tds[1]?.textContent.trim();
                                const volume = tds[2]?.textContent.trim() || tds[3]?.textContent.trim();
                                results.push({level, trades, volume});
                            }
                        }
                        if (results.length > 0) break;
                    }
                }
                return results;
            }""")

            logger.info(f"ChartExchange Playwright: extracted {len(rows)} rows")

            # If JS extraction failed, try text parsing as fallback
            if not rows:
                text = await page.evaluate("document.body.innerText")
                logger.info(f"ChartExchange Playwright: fallback text parsing ({len(text)} chars)")
                
                # Format: "459.94    405    216,874    99.75M"
                pattern = r'(\d{2,4}\.\d{2})\s+(\d[\d,]*)\s+(\d[\d,]*)\s+'
                matches = re.findall(pattern, text)
                for m in matches[:max_levels]:
                    try:
                        price = float(m[0])
                        trades = int(m[1].replace(',', ''))
                        volume = int(m[2].replace(',', ''))
                        if volume > 100:
                            levels.append({
                                'price': price,
                                'volume': volume,
                                'trades': trades,
                            })
                    except (ValueError, IndexError):
                        continue

            await browser.close()

            # Parse JS-extracted rows
            if rows and not levels:
                for row in rows[:max_levels]:
                    try:
                        price_str = re.sub(r'[^\d.]', '', row.get('level', ''))
                        vol_str = re.sub(r'[^\d]', '', row.get('volume', ''))
                        trades_str = re.sub(r'[^\d]', '', row.get('trades', ''))
                        
                        price = float(price_str) if price_str else 0
                        volume = int(vol_str) if vol_str else 0
                        trades = int(trades_str) if trades_str else 0
                        
                        if price > 0 and volume > 0:
                            levels.append({
                                'price': price,
                                'volume': volume,
                                'trades': trades,
                            })
                    except (ValueError, IndexError):
                        continue

    except Exception as e:
        logger.error(f"ChartExchange Playwright error: {e}")
        return []

    # Sort by volume descending
    levels.sort(key=lambda x: x['volume'], reverse=True)
    levels = levels[:max_levels]

    if levels:
        _cache[ticker] = {'levels': levels, 'timestamp': time.time()}
        logger.info(f"ChartExchange DP SUCCESS {ticker}: {len(levels)} levels, "
                     f"top: ${levels[0]['price']} vol={levels[0]['volume']}")
    else:
        logger.warning(f"ChartExchange Playwright: no DP levels found for {ticker}")

    return levels


def fetch_dp_sync(ticker="QQQ", max_levels=15):
    """Synchronous wrapper."""
    try:
        return asyncio.run(fetch_dp_playwright(ticker, max_levels))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(fetch_dp_playwright(ticker, max_levels))
        loop.close()
        return result


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    async def test():
        for t in ["QQQ", "GLD"]:
            print(f"\n{'='*40}")
            print(f"  {t} Dark Pool Levels (Playwright)")
            print(f"{'='*40}")
            result = await fetch_dp_playwright(t)
            if result:
                for i, lvl in enumerate(result[:10], 1):
                    print(f"  {i}. ${lvl['price']:.2f}  Vol: {lvl['volume']:,}  Trades: {lvl['trades']}")
            else:
                print("  FAILED")

    asyncio.run(test())
