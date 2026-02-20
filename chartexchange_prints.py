"""
ChartExchange Dark Pool Prints Scraper — Playwright
Fetches top block trades with direction (Bid/Ask/Mid) from ChartExchange.
T+1 data — yesterday's prints.
"""

import asyncio
import re
import logging
import time

logger = logging.getLogger(__name__)

_cache = {}
CACHE_TTL = 600  # 10 min

# Exchange prefixes — prints page may differ from DP levels page!
# Format: primary first, then fallbacks
EXCHANGE_MAP = {
    'QQQ': ['nasdaq'],
    'SPY': ['nyse_arca', 'nyse'],
    'IWM': ['nyse_arca', 'nyse'],
    'GLD': ['amex', 'nyse', 'nyse_arca', 'arca'],  # nyse_arca gives 404 for prints!
    'SLV': ['amex', 'nyse', 'nyse_arca'],
}


def _get_urls(ticker):
    """Returns list of URLs to try, in order."""
    exchanges = EXCHANGE_MAP.get(ticker.upper(), ['nasdaq', 'nyse'])
    return [
        f"https://chartexchange.com/symbol/{ex}-{ticker.lower()}/exchange-volume/dark-pool-prints/"
        for ex in exchanges
    ]


async def fetch_prints_playwright(ticker="QQQ", min_size=100000, max_prints=15):
    ticker = ticker.upper()

    cached = _cache.get(ticker)
    if cached and (time.time() - cached['timestamp']) < CACHE_TTL:
        logger.info(f"DP Prints cache hit for {ticker}")
        return cached['prints']

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        logger.error("Playwright not installed!")
        return []

    prints = []
    urls_to_try = _get_urls(ticker)

    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                    '--disable-gpu',
                    '--disable-extensions',
                    '--no-first-run',
                ]
            )

            context = await browser.new_context(
                user_agent='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                viewport={'width': 1920, 'height': 1080},
            )

            page = await context.new_page()
            rows = []

            # Try each URL until one works (not 404)
            for url in urls_to_try:
                logger.info(f"DP Prints Playwright: trying URL={url}")
                await page.goto(url, wait_until='networkidle', timeout=45000)
                await asyncio.sleep(3)

                title = await page.title()
                logger.info(f"DP Prints: page title = '{title}'")

                # 404 check
                if '404' in title or 'not found' in title.lower():
                    logger.warning(f"DP Prints: 404 at {url}, trying next...")
                    continue

                # Valid page — wait for table
                try:
                    await page.wait_for_selector('table tbody tr', timeout=20000)
                    logger.info(f"DP Prints: table found at {url}")
                except Exception:
                    logger.warning(f"DP Prints: no table at {url}, trying next...")
                    continue

                # Extract rows
                rows = await page.evaluate("""() => {
                    const results = [];
                    const tables = document.querySelectorAll('table');
                    for (const table of tables) {
                        const headers = table.querySelectorAll('thead th');
                        const headerTexts = Array.from(headers).map(h => h.textContent.trim().toLowerCase());

                        if (headerTexts.includes('time') && headerTexts.includes('price') && headerTexts.includes('size')) {
                            const timeIdx = headerTexts.indexOf('time');
                            const priceIdx = headerTexts.indexOf('price');
                            const sizeIdx = headerTexts.indexOf('size');
                            const premiumIdx = headerTexts.indexOf('premium');
                            const sideIdx = headerTexts.indexOf('side');
                            const exchangeIdx = headerTexts.indexOf('exchange');

                            const tbody = table.querySelector('tbody');
                            if (!tbody) continue;

                            const trs = tbody.querySelectorAll('tr');
                            for (const tr of trs) {
                                const tds = tr.querySelectorAll('td');
                                if (tds.length >= 4) {
                                    results.push({
                                        time: timeIdx >= 0 ? tds[timeIdx]?.textContent.trim() : '',
                                        price: priceIdx >= 0 ? tds[priceIdx]?.textContent.trim() : '',
                                        size: sizeIdx >= 0 ? tds[sizeIdx]?.textContent.trim() : '',
                                        premium: premiumIdx >= 0 ? tds[premiumIdx]?.textContent.trim() : '',
                                        side: sideIdx >= 0 ? tds[sideIdx]?.textContent.trim() : '',
                                        exchange: exchangeIdx >= 0 ? tds[exchangeIdx]?.textContent.trim() : '',
                                    });
                                }
                            }
                            if (results.length > 0) break;
                        }
                    }
                    return results;
                }""")

                logger.info(f"DP Prints Playwright: extracted {len(rows)} rows from {url}")
                if rows:
                    break  # Success — stop trying other URLs

            await browser.close()

            # Parse rows
            for row in rows:
                try:
                    price = float(re.sub(r'[^\d.]', '', row.get('price', '0')))
                    size_str = re.sub(r'[^\d]', '', row.get('size', '0'))
                    size = int(size_str) if size_str else 0
                    side = row.get('side', '').strip()
                    time_str = row.get('time', '').strip()
                    exchange = row.get('exchange', '').strip()
                    premium_str = row.get('premium', '')

                    premium = 0
                    pm = re.match(r'([\d.]+)\s*([MKB]?)', premium_str)
                    if pm:
                        val = float(pm.group(1))
                        suffix = pm.group(2)
                        if suffix == 'M':
                            premium = val * 1_000_000
                        elif suffix == 'K':
                            premium = val * 1_000
                        elif suffix == 'B':
                            premium = val * 1_000_000_000
                        else:
                            premium = val

                    if size >= min_size and price > 0:
                        prints.append({
                            'time': time_str,
                            'price': price,
                            'size': size,
                            'premium': premium,
                            'side': side,
                            'exchange': exchange,
                        })
                except (ValueError, IndexError):
                    continue

    except Exception as e:
        logger.error(f"DP Prints Playwright error: {e}")
        return []

    prints.sort(key=lambda x: x['size'], reverse=True)
    prints = prints[:max_prints]

    if prints:
        _cache[ticker] = {'prints': prints, 'timestamp': time.time()}
        logger.info(f"DP Prints SUCCESS {ticker}: {len(prints)} trades, "
                    f"top: {prints[0]['size']:,} @ ${prints[0]['price']:.2f} ({prints[0]['side']})")
    else:
        logger.warning(f"DP Prints: no prints found for {ticker} (tried {len(urls_to_try)} URLs)")

    return prints


def fetch_prints_sync(ticker="QQQ", min_size=100000, max_prints=15):
    try:
        return asyncio.run(fetch_prints_playwright(ticker, min_size, max_prints))
    except RuntimeError:
        loop = asyncio.new_event_loop()
        result = loop.run_until_complete(fetch_prints_playwright(ticker, min_size, max_prints))
        loop.close()
        return result


def format_prints_discord(prints, ticker="QQQ", ratio=6.37):
    is_gold = ticker.upper() in ("GLD", "GOLD")
    etf_label = "GLD" if is_gold else ticker.upper()
    cfd_label = "XAUUSD" if is_gold else "NAS100 CFD"
    title = "BullNet Block Trades - GOLD" if is_gold else f"BullNet Block Trades - {ticker.upper()}"

    if not prints:
        return f"```\n{title}\n{'='*44}\n  Keine Block Trades gefunden.\n{'='*44}\n```"

    bid_count = sum(1 for p in prints if 'bid' in p['side'].lower())
    ask_count = sum(1 for p in prints if 'ask' in p['side'].lower())
    mid_count = sum(1 for p in prints if 'mid' in p['side'].lower())
    bid_vol = sum(p['size'] for p in prints if 'bid' in p['side'].lower())
    ask_vol = sum(p['size'] for p in prints if 'ask' in p['side'].lower())

    if bid_vol > ask_vol * 1.2:
        bias = "BULLISH 🟢"
    elif ask_vol > bid_vol * 1.2:
        bias = "BEARISH 🔴"
    else:
        bias = "NEUTRAL ⚪"

    min_label = "5K" if is_gold else "100K"
    lines = [f"```", title, "=" * 44,
             f"  Top Block Trades (>{min_label} Shares)",
             f"  Bid: {bid_count} ({bid_vol:,}) | Ask: {ask_count} ({ask_vol:,}) | Mid: {mid_count}",
             f"  Block Trade Bias: {bias}", ""]

    for i, p in enumerate(prints[:10], 1):
        side_icon = "🟢" if 'bid' in p['side'].lower() else "🔴" if 'ask' in p['side'].lower() else "⚪"
        cfd_price = round(p['price'] * ratio, 2)
        premium_str = ""
        if p['premium'] >= 1_000_000:
            premium_str = f" ${p['premium']/1_000_000:.1f}M"
        elif p['premium'] >= 1_000:
            premium_str = f" ${p['premium']/1_000:.0f}K"

        lines.append(f"  {side_icon} {p['price']:.2f} {etf_label} = {cfd_price:.0f} {cfd_label}")
        lines.append(f"     {p['size']:,} Shares | {p['side']}{premium_str}")
        lines.append(f"     {p['time']}  [{p['exchange']}]")
        lines.append("")

    lines += ["=" * 44,
              f"  Ratio: {ratio:.4f} | Daten: Vortag (T+1)",
              f"  → !dp {ticker} fuer DP Zonen im Indikator",
              "```"]

    return "\n".join(lines)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

    async def test():
        for t in ["QQQ", "GLD"]:
            print(f"\n{'='*40}\n  {t} Dark Pool Prints\n{'='*40}")
            result = await fetch_prints_playwright(t, min_size=5000 if t == "GLD" else 100000)
            if result:
                for i, p in enumerate(result[:5], 1):
                    side_icon = "BUY" if 'bid' in p['side'].lower() else "SELL" if 'ask' in p['side'].lower() else "MID"
                    print(f"  {i}. ${p['price']:.3f} x {p['size']:,}  [{side_icon}]  {p['time']}")
            else:
                print("  FAILED")

    asyncio.run(test())
