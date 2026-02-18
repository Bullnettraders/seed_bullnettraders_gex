import requests
import numpy as np
import pandas as pd
from datetime import datetime
from scipy.stats import norm
import json
import logging
import re
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"
RISK_FREE_RATE = 0.045
DIVIDEND_YIELD = 0.005
MAX_EXPIRATIONS = 12
STRIKE_RANGE_PCT = 0.20


# ═══════════════════════════════════════════════════════════
#  GEX SOURCE: CBOE API (Direct)
# ═══════════════════════════════════════════════════════════

def bs_gamma(S, K, T, r, q, sigma):
    if T <= 0 or sigma <= 0 or S <= 0:
        return 0.0
    d1 = (np.log(S / K) + (r - q + 0.5 * sigma**2) * T) / (sigma * np.sqrt(T))
    return np.exp(-q * T) * norm.pdf(d1) / (S * sigma * np.sqrt(T))


def parse_option_symbol(symbol):
    match = re.search(r'(\d{6})([CP])(\d{8})$', symbol)
    if not match:
        return None
    date_str = match.group(1)
    opt_type = 'call' if match.group(2) == 'C' else 'put'
    strike = int(match.group(3)) / 1000.0
    try:
        exp_date = datetime.strptime(date_str, '%y%m%d')
    except:
        return None
    return exp_date, opt_type, strike


def fetch_cboe_options(ticker="QQQ"):
    url = CBOE_URL.format(ticker=ticker)
    headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    spot = data.get('data', {}).get('current_price', None)
    if spot is None:
        spot = data.get('data', {}).get('close', None)
    if spot is None:
        for key in ['last_trade_price', 'prev_day_close', 'bid', 'ask']:
            val = data.get('data', {}).get(key)
            if val and val > 0:
                spot = val
                break
    options = data.get('data', {}).get('options', [])
    logger.info(f"CBOE: Spot ${spot:.2f} | Contracts: {len(options)}")
    return spot, options


def parse_options(spot, options):
    records = []
    now = datetime.now()
    skipped = {'no_symbol': 0, 'no_strike': 0, 'out_of_range': 0, 'expired': 0, 'no_iv': 0}

    for opt in options:
        try:
            symbol = opt.get('option', '')
            if not symbol:
                skipped['no_symbol'] += 1
                continue
            parsed = parse_option_symbol(symbol)
            if parsed is None:
                skipped['no_symbol'] += 1
                continue
            exp_date, opt_type, strike = parsed
            if strike <= 0:
                skipped['no_strike'] += 1
                continue
            if abs(strike - spot) / spot > STRIKE_RANGE_PCT:
                skipped['out_of_range'] += 1
                continue
            dte = (exp_date - now).days
            if dte < 0:
                skipped['expired'] += 1
                continue
            T = max(dte / 365.0, 1 / 365.0)
            oi = opt.get('open_interest', 0) or 0
            volume = opt.get('volume', 0) or 0
            iv = opt.get('iv', 0) or 0
            gamma_raw = opt.get('gamma', 0) or 0
            bid = opt.get('bid', 0) or 0
            ask = opt.get('ask', 0) or 0
            if iv <= 0:
                if bid <= 0 and ask <= 0:
                    skipped['no_iv'] += 1
                    continue
                iv = 0.20
            records.append({
                'strike': strike, 'type': opt_type, 'expiration': exp_date,
                'dte': dte, 'T': T, 'oi': int(oi), 'volume': int(volume),
                'iv': iv, 'gamma': gamma_raw, 'bid': bid, 'ask': ask,
            })
        except:
            continue

    df = pd.DataFrame(records)
    logger.info(f"CBOE: Parsed {len(df)} contracts | Skipped: {skipped}")
    return df


def calculate_gex(spot, df):
    if df.empty:
        return pd.DataFrame()
    exp_dates = sorted(df['expiration'].unique())[:MAX_EXPIRATIONS]
    df = df[df['expiration'].isin(exp_dates)].copy()
    gammas = []
    for _, row in df.iterrows():
        if row['gamma'] > 0:
            gammas.append(row['gamma'])
        else:
            gammas.append(bs_gamma(spot, row['strike'], row['T'], RISK_FREE_RATE, DIVIDEND_YIELD, row['iv']))
    df['calc_gamma'] = gammas
    df['gex'] = df['calc_gamma'] * df['oi'] * 100 * spot * spot * 0.01
    df['dealer_gex'] = df.apply(lambda r: r['gex'] if r['type'] == 'call' else -r['gex'], axis=1)
    gex_by_strike = df.groupby('strike').agg(
        call_gex=('dealer_gex', lambda x: x[df.loc[x.index, 'type'] == 'call'].sum()),
        put_gex=('dealer_gex', lambda x: x[df.loc[x.index, 'type'] == 'put'].sum()),
        net_gex=('dealer_gex', 'sum'),
        total_oi=('oi', 'sum'),
        total_volume=('volume', 'sum'),
    ).reset_index().sort_values('strike')
    return gex_by_strike


def find_key_levels(spot, gex_df):
    levels = {}
    if gex_df.empty:
        return levels
    call_data = gex_df[gex_df['call_gex'] > 0]
    if not call_data.empty:
        idx = call_data['call_gex'].idxmax()
        levels['call_wall'] = call_data.loc[idx, 'strike']
    put_data = gex_df[gex_df['put_gex'] < 0]
    if not put_data.empty:
        idx = put_data['put_gex'].idxmin()
        levels['put_wall'] = put_data.loc[idx, 'strike']
    sorted_gex = gex_df.sort_values('strike')
    net_vals = sorted_gex['net_gex'].values
    strikes = sorted_gex['strike'].values
    gamma_flip = None
    min_dist = float('inf')
    for i in range(len(net_vals) - 1):
        if net_vals[i] * net_vals[i + 1] < 0:
            ratio = abs(net_vals[i]) / (abs(net_vals[i]) + abs(net_vals[i + 1]))
            fp = strikes[i] + ratio * (strikes[i + 1] - strikes[i])
            dist = abs(fp - spot)
            if dist < min_dist:
                min_dist = dist
                gamma_flip = fp
    if gamma_flip is not None:
        levels['gamma_flip'] = round(gamma_flip, 2)
        levels['gamma_regime'] = "Positiv" if spot > gamma_flip else "Negativ"
    if gex_df['total_volume'].sum() > 0:
        idx = gex_df['total_volume'].idxmax()
        levels['hvl'] = gex_df.loc[idx, 'strike']
    gex_df['abs_gex'] = gex_df['call_gex'].abs() + gex_df['put_gex'].abs()
    idx = gex_df['abs_gex'].idxmax()
    levels['abs_gamma_strike'] = gex_df.loc[idx, 'strike']
    return levels


# ═══════════════════════════════════════════════════════════
#  DISCORD FORMAT
# ═══════════════════════════════════════════════════════════

def format_discord_message(spot, levels, ratio=41.33, ticker="QQQ"):
    def to_cfd(p):
        return round(p * ratio, 2)
    now = datetime.now().strftime("%d.%m.%Y %H:%M")
    gf = levels.get('gamma_flip', 0)
    cw = levels.get('call_wall', 0)
    pw = levels.get('put_wall', 0)
    hvl = levels.get('hvl', 0)
    regime = levels.get('gamma_regime', 'N/A')
    source = levels.get('source', 'cboe')

    is_gold = ticker.upper() in ("GLD", "GOLD")
    etf_label = "GLD" if is_gold else "QQQ"
    cfd_label = "XAUUSD" if is_gold else "NAS100 CFD"
    title = f"BullNet GEX Report - {'GOLD' if is_gold else ticker.upper()}"

    hint = "UEBER Flip = Range/Magnet" if regime == "Positiv" else "UNTER Flip = Acceleration"

    lines = []
    lines.append(title)
    lines.append("=" * 44)
    lines.append(f"  {now}  |  {etf_label}: ${spot:.2f}  |  Source: {source}")
    lines.append("")
    lines.append(f"  Gamma Regime: {regime.upper()}")
    lines.append(f"  {hint}")
    lines.append("")
    lines.append("--- KEY LEVELS ---")
    lines.append(f"  Gamma Flip:   {gf:.2f} {etf_label}  =  {to_cfd(gf):.2f} {cfd_label}")
    lines.append(f"  Call Wall:    {cw:.2f} {etf_label}  =  {to_cfd(cw):.2f} {cfd_label}")
    lines.append(f"  Put Wall:     {pw:.2f} {etf_label}  =  {to_cfd(pw):.2f} {cfd_label}")
    if hvl:
        lines.append(f"  HVL:          {hvl:.2f} {etf_label}  =  {to_cfd(hvl):.2f} {cfd_label}")
    lines.append("")
    lines.append(f"--- {cfd_label} INPUT ---")
    lines.append(f"  Gamma Flip:    {to_cfd(gf):.2f}")
    lines.append(f"  Call Wall:     {to_cfd(cw):.2f}")
    lines.append(f"  Put Wall:      {to_cfd(pw):.2f}")
    if hvl:
        lines.append(f"  HVL:           {to_cfd(hvl):.2f}")
    lines.append("")
    lines.append("=" * 44)
    lines.append(f"  Ratio: {ratio:.4f} | {source.upper()} (15min delayed)")
    return "```\n" + "\n".join(lines) + "\n```"


# ═══════════════════════════════════════════════════════════
#  MAIN — Barchart first, CBOE fallback
# ═══════════════════════════════════════════════════════════

def run(ticker="QQQ", ratio=41.33):
    """
    Get GEX levels.
    Source: CBOE API with own Gamma Flip calculation.
    (Barchart removed — no Selenium/Chrome in Docker)
    """
    spot = None
    levels = None
    gex_df = None

    try:
        logger.info(f"Fetching CBOE options for {ticker}...")
        spot, options = fetch_cboe_options(ticker)
        if not options:
            logger.error(f"CBOE: No options data for {ticker}")
            return None, None, None
        df = parse_options(spot, options)
        if df.empty:
            logger.error(f"CBOE: Empty DataFrame for {ticker}")
            return None, None, None
        gex_df = calculate_gex(spot, df)
        levels = find_key_levels(spot, gex_df)
        levels['source'] = 'cboe'
        levels['spot'] = spot
        logger.info(f"CBOE SUCCESS {ticker}: GF={levels.get('gamma_flip')} CW={levels.get('call_wall')} PW={levels.get('put_wall')} HVL={levels.get('hvl')}")
        return spot, levels, gex_df
    except Exception as e:
        logger.error(f"CBOE failed for {ticker}: {e}")
        raise


if __name__ == "__main__":
    spot, levels, _ = run()
    if levels:
        print(format_discord_message(spot, levels))
