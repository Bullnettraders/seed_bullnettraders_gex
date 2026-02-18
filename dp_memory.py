"""
BullNet Dark Pool Level Memory
Tracks "sticky" DP levels that persist until price reaches them.
Big prints (high volume) stay on the chart until actually hit.
"""

import json
import os
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Persistent storage file (survives bot restarts on Railway via volume mount)
MEMORY_FILE = os.getenv('DP_MEMORY_FILE', 'dp_memory.json')

# How close price must get to "hit" a level (0.15% tolerance)
HIT_TOLERANCE = 0.0015

# Max age in days before a level expires even if not hit
MAX_AGE_DAYS = 14

# Minimum volume to be remembered (only BIG prints)
MIN_VOLUME_TO_REMEMBER = 250000

# Max levels to add per day (only the top N by volume)
MAX_NEW_PER_DAY = 3


def load_memory():
    """Load persistent DP level memory."""
    if os.path.exists(MEMORY_FILE):
        try:
            with open(MEMORY_FILE, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"DP Memory load failed: {e}")
    return {"QQQ": [], "GLD": []}


def save_memory(memory):
    """Save DP level memory to disk."""
    try:
        with open(MEMORY_FILE, 'w') as f:
            json.dump(memory, f, indent=2)
    except Exception as e:
        logger.error(f"DP Memory save failed: {e}")


def update_levels(ticker, new_levels, current_price):
    """
    Update level memory with new DP data.
    
    1. Load existing unvisited levels
    2. Mark any levels that price has now reached as "hit"
    3. Add new high-volume levels
    4. Remove expired levels (>14 days old)
    5. Return combined active levels (unvisited)
    
    Args:
        ticker: "QQQ" or "GLD"
        new_levels: list of {'strike': float, 'volume': int, ...} from today's DP scan
        current_price: current ETF spot price
    
    Returns:
        list of active (unvisited) levels, sorted by volume desc
    """
    memory = load_memory()
    ticker = ticker.upper()
    if ticker not in memory:
        memory[ticker] = []
    
    existing = memory[ticker]
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d')
    
    # ── Step 1: Mark hit levels ──
    active = []
    hit_count = 0
    expired_count = 0
    
    for lvl in existing:
        price = lvl['price']
        added = lvl.get('added', now_str)
        
        # Check if expired
        try:
            added_date = datetime.strptime(added, '%Y-%m-%d')
            age_days = (now - added_date).days
        except:
            age_days = 0
        
        if age_days > MAX_AGE_DAYS:
            expired_count += 1
            logger.info(f"DP Memory: expired {ticker} {price:.2f} (age: {age_days}d)")
            continue
        
        # Check if price has reached this level
        if current_price and current_price > 0:
            distance = abs(current_price - price) / current_price
            if distance < HIT_TOLERANCE:
                hit_count += 1
                logger.info(f"DP Memory: HIT {ticker} {price:.2f} (spot: {current_price:.2f}, dist: {distance:.4f})")
                continue
        
        # Still active
        active.append(lvl)
    
    if hit_count > 0:
        logger.info(f"DP Memory: {hit_count} levels hit for {ticker}")
    if expired_count > 0:
        logger.info(f"DP Memory: {expired_count} levels expired for {ticker}")
    
    # ── Step 2: Add new high-volume levels ──
    # Only add TOP 3 levels by volume that aren't already tracked
    existing_prices = set(round(l['price'], 2) for l in active)
    new_count = 0
    
    # Sort new levels by volume descending — biggest prints first
    candidates = sorted(new_levels, key=lambda x: x.get('volume', 0), reverse=True)
    
    for lvl in candidates:
        strike = lvl.get('strike', lvl.get('price', 0))
        volume = lvl.get('volume', 0)
        
        if volume < MIN_VOLUME_TO_REMEMBER:
            continue
        
        strike_r = round(strike, 2)
        if strike_r in existing_prices:
            # Update volume if same level seen again (accumulation!)
            for a in active:
                if round(a['price'], 2) == strike_r:
                    old_vol = a.get('volume', 0)
                    if volume > old_vol:
                        a['volume'] = volume
                        a['last_seen'] = now_str
                    # Track how many days this level appeared
                    a['seen_count'] = a.get('seen_count', 1) + 1
                    break
            continue
        
        # Stop if we already added enough new levels today
        if new_count >= MAX_NEW_PER_DAY:
            continue
        
        # New level
        active.append({
            'price': strike,
            'volume': volume,
            'trades': lvl.get('trades', 0),
            'type': lvl.get('type', 'DP Level'),
            'added': now_str,
            'last_seen': now_str,
            'seen_count': 1,
        })
        existing_prices.add(strike_r)
        new_count += 1
        logger.info(f"DP Memory: NEW {ticker} {strike:.2f} Vol: {volume:,}")
    
    # ── Step 3: Sort by volume (biggest prints first) ──
    active.sort(key=lambda x: x.get('volume', 0), reverse=True)
    
    # Keep max 20 levels per ticker
    active = active[:20]
    
    # ── Step 4: Save ──
    memory[ticker] = active
    save_memory(memory)
    
    logger.info(f"DP Memory: {ticker} = {len(active)} active levels ({new_count} new, {hit_count} hit, {expired_count} expired)")
    
    return active


def get_active_levels(ticker, current_price=None):
    """
    Get currently active (unvisited) DP levels.
    Optionally filters out levels that current price has now reached.
    """
    memory = load_memory()
    ticker = ticker.upper()
    levels = memory.get(ticker, [])
    
    if not current_price:
        return levels
    
    # Filter out hit levels
    active = []
    for lvl in levels:
        distance = abs(current_price - lvl['price']) / current_price
        if distance >= HIT_TOLERANCE:
            active.append(lvl)
    
    return active


def get_top_zones(ticker, n=4, current_price=None):
    """
    Get top N unvisited DP zones for Pine Script indicator.
    Returns levels sorted by price (ascending).
    """
    active = get_active_levels(ticker, current_price)
    
    # Take top N by volume
    top = sorted(active, key=lambda x: x.get('volume', 0), reverse=True)[:n]
    
    # Sort by price for zone ordering
    top.sort(key=lambda x: x['price'])
    
    return top


def format_memory_discord(ticker, current_price=None):
    """Format active DP levels for Discord display."""
    active = get_active_levels(ticker, current_price)
    
    if not active:
        return f"Keine aktiven DP Levels für {ticker}."
    
    lines = []
    lines.append(f"**BullNet DP Memory — {ticker}**")
    lines.append(f"Aktive (nicht erreichte) Levels: **{len(active)}**")
    lines.append("```")
    
    for i, lvl in enumerate(active[:12], 1):
        price = lvl['price']
        vol = lvl.get('volume', 0)
        added = lvl.get('added', '?')
        seen = lvl.get('seen_count', 1)
        age = ''
        try:
            added_date = datetime.strptime(added, '%Y-%m-%d')
            days = (datetime.now() - added_date).days
            age = f"{days}d"
        except:
            age = "?"
        
        # Distance from current price
        dist_str = ""
        if current_price and current_price > 0:
            dist_pct = (price - current_price) / current_price * 100
            arrow = "↑" if dist_pct > 0 else "↓"
            dist_str = f" | {arrow}{abs(dist_pct):.2f}%"
        
        repeat = f" x{seen}" if seen > 1 else ""
        lines.append(f"  {i:2d}. {price:>8.2f}  Vol: {vol:>10,}  | {age}{repeat}{dist_str}")
    
    lines.append("```")
    
    return "\n".join(lines)


if __name__ == "__main__":
    # Test
    test_levels = [
        {'strike': 601.07, 'volume': 1209524, 'trades': 697, 'type': 'Block Trade'},
        {'strike': 601.83, 'volume': 427961, 'trades': 292, 'type': 'DP Support'},
        {'strike': 613.00, 'volume': 850000, 'trades': 400, 'type': 'DP Resistance'},
        {'strike': 600.07, 'volume': 363714, 'trades': 587, 'type': 'DP Support'},
        {'strike': 595.00, 'volume': 50000, 'trades': 100, 'type': 'DP Support'},  # Below threshold
    ]
    
    # Day 1: Add levels, price at 601
    active = update_levels("QQQ", test_levels, current_price=601.0)
    print(f"Day 1: {len(active)} active levels")
    for l in active:
        print(f"  {l['price']:.2f} - Vol: {l['volume']:,}")
    
    # Day 2: Price moved to 601.83 — should hit that level
    active = update_levels("QQQ", [], current_price=601.83)
    print(f"\nDay 2 (price=601.83): {len(active)} active levels")
    for l in active:
        print(f"  {l['price']:.2f} - Vol: {l['volume']:,}")
    
    # 613 should still be there
    print(f"\n613 still active: {any(round(l['price'],0) == 613 for l in active)}")
    
    # Format for Discord
    print(f"\n{format_memory_discord('QQQ', 601.83)}")
    
    # Cleanup
    if os.path.exists(MEMORY_FILE):
        os.remove(MEMORY_FILE)
