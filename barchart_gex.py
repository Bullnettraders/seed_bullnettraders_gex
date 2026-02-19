"""
Barchart GEX Scraper — No Selenium!
Uses requests.Session to get cookies, then calls internal API.
Falls back to parsing page text for gamma flip / call wall / put wall.

Barchart calculates GEX levels from:
- ALL contracts (all expirations)
- Open Interest
- 1% move
- End-of-Day data (updated ~8:30pm ET)
"""

import requests
import re
import logging
import time

logger = logging.getLogger(__name__)

# Barchart page URLs
BARCHART_PAGE = "https://www.barchart.com/{asset_type}/quotes/{ticker}/gamma-exposure"

# Barchart internal API
BARCHART_API = "https://www.barchart.com/proxies/core-api/v1/options/get"

# ETFs vs stocks
ETF_TICKERS = {"QQQ", "SPY", "IWM", "DIA", "GLD", "SLV", "TLT", "XLF", "XLE", "VOO"}

# Browser-like headers
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

API_HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/144.0.0.0 Safari/537.36',
    'Accept': 'application/json',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
}


def fetch_barchart_gex(ticker="QQQ"):
    """
    Fetch GEX levels from Barchart.
    
    Strategy:
    1. Open session, GET the gamma-exposure page → get cookies
    2. Parse page HTML for gamma flip / call wall / put wall text
    3. If text parsing fails, call internal API with cookies → calculate ourselves
    
    Returns: dict with gamma_flip, call_wall, put_wall, spot, source
             or None on failure
    """
    ticker = ticker.upper()
    asset_type = "etfs-funds" if ticker in ETF_TICKERS else "stocks"
    page_url = BARCHART_PAGE.format(asset_type=asset_type, ticker=ticker)

    session = requests.Session()
    session.headers.update(HEADERS)

    levels = {}

    try:
        # ── Step 1: Get page + cookies ──
        logger.info(f"Barchart: fetching page for {ticker}...")
        resp = session.get(page_url, timeout=30)
        resp.raise_for_status()
        page_text = resp.text
        logger.info(f"Barchart: page loaded ({len(page_text)} chars)")

        # ── Step 2: Parse gamma flip from page text ──
        # Barchart renders: "QQQ gamma flip point is 618.62"
        gf_match = re.search(
            rf'{ticker}\s+gamma\s+flip\s+point\s+is\s+(\d+\.?\d*)',
            page_text, re.IGNORECASE
        )
        if gf_match:
            levels['gamma_flip'] = float(gf_match.group(1))
            logger.info(f"Barchart: Gamma Flip = {levels['gamma_flip']}")

        # "QQQ put wall is 600.00. QQQ call wall is 630.00"
        pw_match = re.search(
            rf'{ticker}\s+put\s+wall\s+is\s+(\d+\.?\d*)',
            page_text, re.IGNORECASE
        )
        if pw_match:
            levels['put_wall'] = float(pw_match.group(1))
            logger.info(f"Barchart: Put Wall = {levels['put_wall']}")

        cw_match = re.search(
            rf'{ticker}\s+call\s+wall\s+is\s+(\d+\.?\d*)',
            page_text, re.IGNORECASE
        )
        if cw_match:
            levels['call_wall'] = float(cw_match.group(1))
            logger.info(f"Barchart: Call Wall = {levels['call_wall']}")

        # ── Step 3: If page text worked, we're done ──
        if 'gamma_flip' in levels:
            # Try to get spot price
            spot_match = re.search(
                r'Last\s*Price[:\s]*\$?(\d+\.?\d*)', page_text, re.IGNORECASE
            )
            if not spot_match:
                spot_match = re.search(
                    r'"lastPrice":\s*(\d+\.?\d*)', page_text
                )
            if spot_match:
                levels['spot'] = float(spot_match.group(1))

            # Derive regime
            if 'spot' in levels:
                levels['gamma_regime'] = "Positiv" if levels['spot'] > levels['gamma_flip'] else "Negativ"
            
            levels['source'] = 'barchart'
            logger.info(f"Barchart TEXT PARSE SUCCESS: GF={levels.get('gamma_flip')} CW={levels.get('call_wall')} PW={levels.get('put_wall')}")
            return levels

        # ── Step 4: Text parse failed — try API with session cookies ──
        logger.info("Barchart: text parse incomplete, trying API...")
        api_levels = _fetch_via_api(session, ticker)
        if api_levels:
            return api_levels

        # ── Step 5: Nothing worked ──
        if levels:
            levels['source'] = 'barchart-partial'
            return levels

        logger.warning("Barchart: all methods failed")
        return None

    except Exception as e:
        logger.error(f"Barchart error: {e}")
        return None


def _fetch_via_api(session, ticker):
    """
    Call Barchart internal options API with session cookies.
    Fetch ALL expirations and calculate GEX ourselves.
    """
    try:
        # Update headers for API call
        api_session_headers = {
            'Accept': 'application/json',
            'Referer': f'https://www.barchart.com/etfs-funds/quotes/{ticker}/gamma-exposure',
        }

        # Get XSRF token from cookies
        xsrf = session.cookies.get('XSRF-TOKEN', '')
        if xsrf:
            # URL-decode the token
            import urllib.parse
            xsrf_decoded = urllib.parse.unquote(xsrf)
            api_session_headers['x-xsrf-token'] = xsrf_decoded

        params = {
            'symbols': ticker,
            'raw': '1',
            'fields': 'symbol,strikePrice,optionType,baseLastPrice,dailyGamma,gamma,dailyOpenInterest,openInterest,daysToExpiration,expirationDate',
            'groupBy': 'strikePrice',
        }

        logger.info(f"Barchart API: fetching options for {ticker}...")
        resp = session.get(BARCHART_API, params=params, headers=api_session_headers, timeout=30)

        if resp.status_code != 200:
            logger.warning(f"Barchart API: status {resp.status_code}")
            return None

        data = resp.json()

        # Data structure: {"data": [{"raw": {...}}, ...]}
        raw_data = data.get('data', [])
        if not raw_data:
            logger.warning("Barchart API: no data returned")
            return None

        logger.info(f"Barchart API: got {len(raw_data)} records")

        # Parse and calculate GEX
        return _calculate_gex_from_barchart(raw_data, ticker)

    except Exception as e:
        logger.error(f"Barchart API error: {e}")
        return None


def _calculate_gex_from_barchart(raw_data, ticker):
    """
    Calculate Gamma Flip, Call Wall, Put Wall from Barchart options data.
    Uses same formula as Barchart:
    GEX = Gamma × Open Interest × Spot² × 0.01
    (per 1% move)
    """
    import numpy as np

    spot = 0
    strikes_data = {}  # strike -> {call_gex, put_gex, net_gex}

    for item in raw_data:
        raw = item.get('raw', item)  # handle both formats

        strike = float(raw.get('strikePrice', 0))
        opt_type = raw.get('optionType', '').lower()
        gamma = float(raw.get('dailyGamma', 0) or raw.get('gamma', 0) or 0)
        oi = int(raw.get('dailyOpenInterest', 0) or raw.get('openInterest', 0) or 0)
        base_price = float(raw.get('baseLastPrice', 0) or raw.get('baseDailyLastPrice', 0) or 0)

        if base_price > 0:
            spot = base_price

        if strike <= 0 or gamma <= 0 or oi <= 0:
            continue

        # GEX per 1% move: Gamma * OI * 100 * Spot * Spot * 0.01
        gex = gamma * oi * 100 * spot * spot * 0.01

        # Dealer GEX: calls positive, puts negative
        if 'call' in opt_type:
            dealer_gex = gex
        else:
            dealer_gex = -gex

        if strike not in strikes_data:
            strikes_data[strike] = {'call_gex': 0, 'put_gex': 0, 'net_gex': 0}

        if 'call' in opt_type:
            strikes_data[strike]['call_gex'] += dealer_gex
        else:
            strikes_data[strike]['put_gex'] += dealer_gex
        strikes_data[strike]['net_gex'] += dealer_gex

    if not strikes_data or spot <= 0:
        logger.warning("Barchart calc: insufficient data")
        return None

    # Sort by strike
    sorted_strikes = sorted(strikes_data.keys())
    net_gex_values = [strikes_data[s]['net_gex'] for s in sorted_strikes]

    # Find Gamma Flip (where net GEX crosses zero)
    gamma_flip = None
    min_dist = float('inf')
    for i in range(len(net_gex_values) - 1):
        if net_gex_values[i] * net_gex_values[i + 1] < 0:
            # Linear interpolation
            ratio = abs(net_gex_values[i]) / (abs(net_gex_values[i]) + abs(net_gex_values[i + 1]))
            fp = sorted_strikes[i] + ratio * (sorted_strikes[i + 1] - sorted_strikes[i])
            dist = abs(fp - spot)
            if dist < min_dist:
                min_dist = dist
                gamma_flip = fp

    # Find Call Wall (strike with highest positive call GEX)
    call_wall = None
    max_call_gex = 0
    for s in sorted_strikes:
        if strikes_data[s]['call_gex'] > max_call_gex:
            max_call_gex = strikes_data[s]['call_gex']
            call_wall = s

    # Find Put Wall (strike with most negative put GEX)
    put_wall = None
    min_put_gex = 0
    for s in sorted_strikes:
        if strikes_data[s]['put_gex'] < min_put_gex:
            min_put_gex = strikes_data[s]['put_gex']
            put_wall = s

    levels = {
        'spot': spot,
        'source': 'barchart-api',
    }

    if gamma_flip is not None:
        levels['gamma_flip'] = round(gamma_flip, 2)
        levels['gamma_regime'] = "Positiv" if spot > gamma_flip else "Negativ"
    if call_wall is not None:
        levels['call_wall'] = call_wall
    if put_wall is not None:
        levels['put_wall'] = put_wall

    logger.info(f"Barchart CALC: GF={levels.get('gamma_flip')} CW={call_wall} PW={put_wall} Spot={spot}")
    return levels


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    
    for t in ["QQQ", "GLD"]:
        print(f"\n{'='*40}")
        print(f"  {t} GEX from Barchart")
        print(f"{'='*40}")
        result = fetch_barchart_gex(t)
        if result:
            for k, v in result.items():
                print(f"  {k}: {v}")
        else:
            print("  FAILED")
