import os
import asyncio
import logging
import traceback
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks
from discord import Embed

from gex_calculator import run as run_gex, format_discord_message
from darkpool import get_dark_pool_levels, format_dp_discord, get_top_dp_zones
from pine_seeds import push_gex_to_github, push_dp_to_github, ensure_symbol_info
from dp_memory import update_levels as dp_memory_update, get_top_zones, format_memory_discord

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

# ═══════════════════════════════════════════════════════════
#  CONFIG
# ═══════════════════════════════════════════════════════════

TOKEN = os.getenv('DISCORD_TOKEN', '')

# Legacy Channel (für GEX + !all + Fallback)
CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID', '0'))

# ✅ NEU: Dedizierte Dark Pool Channels
CHANNEL_DP_NASDAQ = int(os.getenv('CHANNEL_DP_NASDAQ', '0'))
CHANNEL_DP_GOLD = int(os.getenv('CHANNEL_DP_GOLD', '0'))

RATIO = float(os.getenv('QQQ_CFD_RATIO', '41.33'))
GOLD_RATIO = float(os.getenv('GLD_XAUUSD_RATIO', '10.97'))
SCHEDULE_ENABLED = os.getenv('SCHEDULE_ENABLED', 'true').lower() == 'true'

# Discord Post Zeiten in Berliner Zeit
SCHEDULE_TIMES_DE = [(9, 0), (13, 0), (14, 30), (20, 0)]

# Marker damit purge() alte DP Posts sauber erkennt
DP_MARKER = "BullNet Dark Pool"


# ═══════════════════════════════════════════════════════════
#  PRICE / RATIO HELPERS
# ═══════════════════════════════════════════════════════════

def _yahoo_price(ticker, headers=None, timeout=10):
    """Fetch regularMarketPrice from Yahoo Finance. Returns None on failure."""
    import requests as req
    if headers is None:
        headers = {'User-Agent': 'Mozilla/5.0'}
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=1d&interval=1d"
    try:
        r = req.get(url, headers=headers, timeout=timeout)
        result = r.json().get('chart', {}).get('result')
        if result and len(result) > 0:
            price = result[0].get('meta', {}).get('regularMarketPrice')
            if price and price > 0:
                return float(price)
    except Exception as e:
        logger.debug(f"Yahoo {ticker} failed: {e}")
    return None


def _get_broker_gold():
    """Fetch Gold price as quoted by Eightcap / Yahoo GC=F."""
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        price = _yahoo_price('GC%3DF', headers)
        if price and 4000 < price < 7000:
            logger.info(f"Broker Gold (GC=F): {price}")
            return float(price)
    except Exception as e:
        logger.warning(f"GC=F failed: {e}")
    return None


def auto_update_ratios():
    """Auto-calculate ratios from live market data."""
    global RATIO, GOLD_RATIO
    headers = {'User-Agent': 'Mozilla/5.0'}

    try:
        qqq_price = _yahoo_price('QQQ', headers)
        nas_price = _yahoo_price('NQ%3DF', headers)
        if qqq_price and nas_price:
            new_ratio = round(nas_price / qqq_price, 2)
            if 30 < new_ratio < 55:
                RATIO = new_ratio
                logger.info(f"Auto-Ratio NAS/QQQ: {RATIO}")
    except Exception as e:
        logger.warning(f"NAS ratio failed: {e}")

    try:
        gld_price = _yahoo_price('GLD', headers)
        gold_price = _get_broker_gold()
        if gld_price and gold_price:
            new_gold = round(gold_price / gld_price, 4)
            if 8.0 < new_gold < 15.0:
                GOLD_RATIO = new_gold
                logger.info(f"Auto-Ratio Gold/GLD: {GOLD_RATIO}")
    except Exception as e:
        logger.warning(f"Gold ratio failed: {e}")


# ═══════════════════════════════════════════════════════════
#  TICKER META — Single source of truth
# ═══════════════════════════════════════════════════════════

def ticker_meta(ticker):
    """Gibt {ratio, etf, cfd, title, channel_id, min_print_size} für ticker zurück."""
    t = ticker.upper()
    is_gold = t in ("GLD", "GOLD")
    if is_gold:
        return {
            'ticker': 'GLD',
            'is_gold': True,
            'ratio': GOLD_RATIO,
            'etf': 'GLD',
            'cfd': 'XAUUSD',
            'title': 'GOLD',
            'channel_id': CHANNEL_DP_GOLD,
            'min_print_size': 5000,
            'color': 0xFFD700,
        }
    return {
        'ticker': t,
        'is_gold': False,
        'ratio': RATIO,
        'etf': 'QQQ',
        'cfd': 'NAS100',
        'title': t,
        'channel_id': CHANNEL_DP_NASDAQ,
        'min_print_size': 100000,
        'color': 0x7B68EE,
    }


# ═══════════════════════════════════════════════════════════
#  DISCORD INIT
# ═══════════════════════════════════════════════════════════

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)


# ═══════════════════════════════════════════════════════════
#  DARK POOL — CORE POST HELPER
# ═══════════════════════════════════════════════════════════

async def purge_old_dp_posts(channel, limit=500):
    """Löscht ALLE alten Bot-Messages im DP Channel.
    Da es dedizierte DP Channels sind: komplett plattmachen.

    Zwei Phasen:
      1. Bulk delete für Messages < 14 Tage (schnell)
      2. Einzel-delete für ältere Messages (Discord API Limit)
    """
    if channel is None:
        return 0

    def is_bot_msg(msg):
        return msg.author.id == bot.user.id

    total_deleted = 0

    # Phase 1: Bulk purge (nur <14 Tage alt)
    try:
        deleted = await channel.purge(limit=limit, check=is_bot_msg, bulk=True)
        total_deleted += len(deleted)
        logger.info(f"Bulk-purged {len(deleted)} Bot-Messages in #{channel.name}")
    except discord.Forbidden:
        logger.warning(f"❌ Manage Messages Permission fehlt in #{channel.name}")
        return 0
    except Exception as e:
        logger.warning(f"Bulk purge failed in #{channel.name}: {e}")

    # Phase 2: Einzel-delete für >14 Tage alte Bot-Messages
    try:
        old_count = 0
        async for msg in channel.history(limit=limit):
            if msg.author.id == bot.user.id:
                try:
                    await msg.delete()
                    old_count += 1
                    await asyncio.sleep(0.3)  # Rate limit freundlich
                except discord.NotFound:
                    pass
                except Exception as e:
                    logger.debug(f"Einzel-delete skip: {e}")
        if old_count > 0:
            logger.info(f"Einzel-deleted {old_count} alte (>14d) Messages in #{channel.name}")
            total_deleted += old_count
    except Exception as e:
        logger.warning(f"Einzel-delete phase failed: {e}")

    logger.info(f"✅ Purge #{channel.name}: {total_deleted} Messages gesamt gelöscht")
    return total_deleted


def build_dp_embed(dp, meta):
    """Baut das Dark Pool Embed für ein Ticker."""
    levels = dp.get('levels', [])
    finra = dp.get('finra')
    r = meta['ratio']

    embed = Embed(
        title=f"{DP_MARKER} - {meta['title']}",
        description=f"Source: {dp.get('source', 'N/A')} | {len(levels)} Levels",
        color=meta['color'],
        timestamp=datetime.now(timezone.utc),
    )

    for lvl in levels[:6]:
        strike = lvl['strike']
        tp = lvl['type']
        vol = lvl.get('volume', 0)
        num = lvl.get('num_levels', 1)
        cluster_tag = f" ({num}x)" if num > 1 else ""
        embed.add_field(
            name=f"{tp}{cluster_tag}",
            value=f"`{strike:.2f}` {meta['etf']}\n`{strike*r:.0f}` {meta['cfd']}\nVol: {vol:,}",
            inline=True,
        )

    if finra:
        embed.add_field(
            name="FINRA Short %",
            value=f"`{finra['short_percent']}%`\n{finra['date']}",
            inline=True,
        )

    embed.set_footer(text=f"Ratio: {r:.4f} | TradingView ✅ | BULLNET")
    return embed


async def post_dp_report(ticker, channel=None, purge=True, include_prints=True):
    """
    Zentraler Dark Pool Report Poster.

    Flow:
      1. Channel auflösen (Argument > meta > None)
      2. Alte DP Posts im Channel löschen (optional)
      3. GEX → Spot → DP Levels holen
      4. Text-Block + Embed posten
      5. Block Trades (Prints) posten
      6. TradingView Push
    """
    meta = ticker_meta(ticker)
    t = meta['ticker']

    # Channel auflösen
    if channel is None and meta['channel_id']:
        channel = bot.get_channel(meta['channel_id'])
    if channel is None:
        logger.warning(f"Kein Channel für {t} — setze CHANNEL_DP_{'GOLD' if meta['is_gold'] else 'NASDAQ'}")
        return False

    logger.info(f"post_dp_report: {t} → #{channel.name}")

    # 1. Alte Posts löschen
    if purge:
        await purge_old_dp_posts(channel)

    # 2. Daten holen
    try:
        spot, _, gex_df = await asyncio.to_thread(run_gex, t, meta['ratio'])
        dp = await asyncio.to_thread(get_dark_pool_levels, t, spot, gex_df)
    except Exception as e:
        logger.error(f"DP fetch failed for {t}: {e}")
        await channel.send(f"⚠️ {t} Dark Pool Fetch Fehler: {e}")
        return False

    if not dp.get('levels'):
        await channel.send(f"⚠️ {t}: Keine Dark Pool Daten verfügbar.")
        return False

    # 3. Text-Block + Embed posten
    try:
        msg = format_dp_discord(dp, meta['ratio'], t)
        if len(msg) > 1900:
            msg = msg[:1900] + "\n```"
        await channel.send(msg)
        await channel.send(embed=build_dp_embed(dp, meta))
    except Exception as e:
        logger.error(f"DP post failed for {t}: {e}")

    # 4. Block Trades
    if include_prints:
        try:
            from chartexchange_prints import fetch_prints_sync, format_prints_discord
            prints = await asyncio.to_thread(
                fetch_prints_sync, t, meta['min_print_size'], 15
            )
            if prints:
                pmsg = format_prints_discord(prints, t, meta['ratio'])
                if len(pmsg) > 1950:
                    pmsg = pmsg[:1950] + "\n```"
                await channel.send(pmsg)

                # BT push zu TradingView
                asyncio.create_task(_push_bt_to_tradingview(t, prints))
        except Exception as e:
            logger.warning(f"Prints failed for {t}: {e}")

    # 5. DP Memory + TradingView push
    await _push_dp_to_tradingview(t, dp, spot)

    return True


# ═══════════════════════════════════════════════════════════
#  GEX REPORT (unchanged)
# ═══════════════════════════════════════════════════════════

async def get_gex_report(ticker="QQQ", ratio=None):
    is_gold = ticker.upper() in ("GLD", "GOLD")
    if is_gold:
        ticker = "GLD"
    r = ratio or (GOLD_RATIO if is_gold else RATIO)

    spot = None
    levels = None
    gex_df = None

    try:
        from barchart_gex import fetch_barchart_gex_async
        logger.info(f"Trying Barchart Playwright for {ticker}...")
        bc_levels = await fetch_barchart_gex_async(ticker)
        if bc_levels and 'gamma_flip' in bc_levels:
            spot = bc_levels.get('spot', 0)
            levels = bc_levels
            logger.info(f"Barchart Playwright SUCCESS: GF={levels.get('gamma_flip')}")

            if 'hvl' not in levels:
                try:
                    from gex_calculator import fetch_cboe_options, parse_options, calculate_gex, find_key_levels
                    cboe_spot, options = await asyncio.to_thread(fetch_cboe_options, ticker)
                    if options:
                        df = parse_options(cboe_spot or spot, options)
                        if not df.empty:
                            gex_result = calculate_gex(cboe_spot or spot, df)
                            cboe_levels = find_key_levels(cboe_spot or spot, gex_result)
                            if 'hvl' in cboe_levels:
                                levels['hvl'] = cboe_levels['hvl']
                            if not spot or spot == 0:
                                spot = cboe_spot
                                levels['spot'] = spot
                except Exception as e:
                    logger.warning(f"CBOE HVL supplement failed: {e}")
        else:
            levels = None
    except Exception as e:
        logger.warning(f"Barchart Playwright failed: {e}")

    if not levels or 'gamma_flip' not in levels:
        try:
            spot, levels, gex_df = await asyncio.to_thread(run_gex, ticker, r)
        except Exception as e:
            logger.error(f"GEX error: {e}")
            return None, None, str(e) + "\n" + traceback.format_exc()[-500:]

    if not levels:
        return None, None, "Levels leer"

    gf = levels.get('gamma_flip', 0)
    if levels.get('gamma_regime', 'N/A') == 'N/A' and spot and spot > 0 and gf > 0:
        levels['gamma_regime'] = "Positiv" if spot > gf else "Negativ"

    text_msg = format_discord_message(spot, levels, r, ticker)
    gf = levels.get('gamma_flip', 0)
    cw = levels.get('call_wall', 0)
    pw = levels.get('put_wall', 0)
    hvl = levels.get('hvl', 0)
    regime = levels.get('gamma_regime', 'N/A')
    source = levels.get('source', 'cboe')
    color = 0x00FF88 if regime == "Positiv" else 0xFF3B3B if regime == "Negativ" else 0x808080

    etf_label = "GLD" if is_gold else "QQQ"
    cfd_label = "XAUUSD" if is_gold else "CFD"
    title = "BullNet GEX - GOLD" if is_gold else "BullNet GEX - " + ticker

    embed = Embed(
        title=title,
        description=f"Regime: {regime.upper()}\nSpot: ${spot:.2f} {etf_label}\nSource: {source}",
        color=color, timestamp=datetime.now(timezone.utc)
    )
    embed.add_field(name="Gamma Flip", value=f"`{gf:.2f}` {etf_label}\n`{gf*r:.2f}` {cfd_label}", inline=True)
    embed.add_field(name="Call Wall", value=f"`{cw:.2f}` {etf_label}\n`{cw*r:.2f}` {cfd_label}", inline=True)
    embed.add_field(name="Put Wall", value=f"`{pw:.2f}` {etf_label}\n`{pw*r:.2f}` {cfd_label}", inline=True)
    if hvl:
        embed.add_field(name="HVL", value=f"`{hvl:.2f}` {etf_label}\n`{hvl*r:.2f}` {cfd_label}", inline=True)
    embed.set_footer(text=f"Ratio: {r:.4f} | {source.upper()} | BULLNET")

    try:
        await asyncio.to_thread(push_gex_to_github, ticker, levels, spot)
    except Exception as e:
        logger.warning(f"Pine seeds push failed: {e}")

    return text_msg, embed, None


# ═══════════════════════════════════════════════════════════
#  TRADINGVIEW PUSH HELPERS
# ═══════════════════════════════════════════════════════════

async def _push_dp_to_tradingview(ticker, dp, spot):
    dp_ticker = "GLD" if ticker.upper() in ("GLD", "GOLD") else ticker.upper()
    try:
        if dp.get('levels'):
            active_levels = await asyncio.to_thread(dp_memory_update, dp_ticker, dp['levels'], spot)
            logger.info(f"DP Memory updated: {len(active_levels)} active levels for {dp_ticker}")

            zones = get_top_dp_zones(dp['levels'])
            if zones.get('dp1', 0) > 0:
                await asyncio.to_thread(push_dp_to_github, dp_ticker, None, zones)
                logger.info(f"DP TradingView push OK {dp_ticker}: {zones}")
    except Exception as e:
        logger.warning(f"DP TradingView push failed: {e}")


async def _push_bt_to_tradingview(ticker, prints_data):
    try:
        from pine_seeds import push_bt_to_github
        bt_ticker = "GLD" if ticker.upper() in ("GLD", "GOLD") else ticker.upper()
        push_bt_to_github(bt_ticker, prints_data)
        logger.info(f"BT TradingView push OK {bt_ticker}")
    except Exception as e:
        logger.warning(f"BT TradingView push failed: {e}")


# ═══════════════════════════════════════════════════════════
#  LOOP 1 — Alle 30 Min: Still zu TradingView pushen
# ═══════════════════════════════════════════════════════════

@tasks.loop(minutes=30)
async def auto_push_tradingview():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5 or not (13 <= now.hour <= 22):
        return

    logger.info(f"Auto TradingView Sync: {now.strftime('%H:%M')} UTC")

    try:
        await asyncio.to_thread(auto_update_ratios)
    except Exception as e:
        logger.warning(f"Auto-ratio failed: {e}")

    for ticker in ("QQQ", "GLD"):
        try:
            r = GOLD_RATIO if ticker == "GLD" else RATIO
            spot, levels, gex_df = await asyncio.to_thread(run_gex, ticker, r)
            if levels and 'gamma_flip' in levels:
                await asyncio.to_thread(push_gex_to_github, ticker, levels, spot)
            dp = await asyncio.to_thread(get_dark_pool_levels, ticker, spot, gex_df)
            await _push_dp_to_tradingview(ticker, dp, spot)
        except Exception as e:
            logger.warning(f"Auto-push {ticker} failed: {e}")


@auto_push_tradingview.before_loop
async def before_auto_push():
    await bot.wait_until_ready()


# ═══════════════════════════════════════════════════════════
#  LOOP 2 — Geplante Discord Posts
# ═══════════════════════════════════════════════════════════

@tasks.loop(minutes=1)
async def scheduled_gex():
    import pytz
    now_utc = datetime.now(timezone.utc)
    if now_utc.weekday() >= 5:
        return

    de_tz = pytz.timezone('Europe/Berlin')
    now_de = now_utc.astimezone(de_tz)
    h_de, m_de = now_de.hour, now_de.minute

    is_post_time = any(
        h_de == h and m_de in (m, m + 1)
        for h, m in SCHEDULE_TIMES_DE
    )
    if not is_post_time:
        return

    post_key = f"{h_de:02d}:{m_de:02d}"
    if not hasattr(scheduled_gex, '_last_post'):
        scheduled_gex._last_post = ""
    if scheduled_gex._last_post == post_key:
        return
    scheduled_gex._last_post = post_key

    logger.info(f"Scheduled Discord Post: {h_de:02d}:{m_de:02d} Berliner Zeit")

    # ── GEX in legacy channel ──
    gex_channel = bot.get_channel(CHANNEL_ID) if CHANNEL_ID else None
    if gex_channel:
        for tkr in ("QQQ", "GLD"):
            result = await get_gex_report(tkr)
            if result[0]:
                await gex_channel.send(result[0])
                await gex_channel.send(embed=result[1])

    # ── DP Posts in dedizierte Channels (löschen + neu) ──
    if (h_de == 9 and m_de <= 5) or (h_de == 14 and 28 <= m_de <= 35):
        logger.info("Scheduled DP Posts — splitting QQQ/GLD into dedicated channels")
        await post_dp_report("QQQ")
        await post_dp_report("GLD")


@scheduled_gex.before_loop
async def before_schedule():
    await bot.wait_until_ready()


# ═══════════════════════════════════════════════════════════
#  COMMANDS — GEX
# ═══════════════════════════════════════════════════════════

@bot.command(name='gex')
async def cmd_gex(ctx, ticker: str = "QQQ"):
    async with ctx.typing():
        result = await get_gex_report(ticker.upper())
    if result[0]:
        await ctx.send(result[0])
        await ctx.send(embed=result[1])
    else:
        await ctx.send(f"Keine Daten fuer {ticker}\nFehler: {result[2]}"[:1900])


@bot.command(name='gold')
async def cmd_gold(ctx):
    async with ctx.typing():
        result = await get_gex_report("GLD")
    if result[0]:
        await ctx.send(result[0])
        await ctx.send(embed=result[1])
    else:
        await ctx.send(f"Keine Gold Daten\nFehler: {result[2]}"[:1900])


@bot.command(name='gamma')
async def cmd_gamma(ctx):
    async with ctx.typing():
        try:
            spot, levels, _ = await asyncio.to_thread(run_gex, "QQQ", RATIO)
        except Exception as e:
            await ctx.send(f"Fehler: {e}")
            return
    if levels:
        regime = levels.get('gamma_regime', 'N/A')
        gf = levels.get('gamma_flip', 0)
        pos = "Oberhalb" if spot > gf else "Unterhalb"
        await ctx.send(f"Gamma: {regime.upper()} | Flip: {gf:.2f} | Spot: {spot:.2f} | {pos}")


# ═══════════════════════════════════════════════════════════
#  COMMANDS — DARK POOL (benutzen jetzt post_dp_report)
# ═══════════════════════════════════════════════════════════

@bot.command(name='darkpool')
async def cmd_darkpool(ctx, ticker: str = "QQQ"):
    """Dark Pool Report. Postet in den dedizierten DP Channel,
    löscht alte Posts vorher. Bei Aufruf aus anderem Channel:
    Info-Message + Post im Ziel-Channel."""
    meta = ticker_meta(ticker)
    target = bot.get_channel(meta['channel_id']) if meta['channel_id'] else None

    if target is None:
        # Kein dedizierter Channel gesetzt → im aktuellen Channel posten, kein purge
        async with ctx.typing():
            ok = await post_dp_report(meta['ticker'], channel=ctx.channel, purge=False)
        if not ok:
            await ctx.send(f"⚠️ Dark Pool für {meta['ticker']} nicht verfügbar.")
        return

    async with ctx.typing():
        ok = await post_dp_report(meta['ticker'], channel=target, purge=True)

    if ok and ctx.channel.id != target.id:
        await ctx.send(f"✅ {meta['title']} Dark Pool gepostet in {target.mention}")
    elif not ok:
        await ctx.send(f"⚠️ Dark Pool Fehler für {meta['ticker']}")


@bot.command(name='dp')
async def cmd_dp(ctx, ticker: str = "QQQ"):
    await cmd_darkpool(ctx, ticker)


@bot.command(name='dpall')
async def cmd_dpall(ctx):
    """Beide Dark Pools (QQQ + GLD) in ihre Channels posten."""
    async with ctx.typing():
        ok_q = await post_dp_report("QQQ")
        ok_g = await post_dp_report("GLD")
    status = f"QQQ: {'✅' if ok_q else '❌'} | GLD: {'✅' if ok_g else '❌'}"
    await ctx.send(f"Dark Pool Split Post: {status}")


@bot.command(name='prints')
async def cmd_prints(ctx, ticker: str = "QQQ"):
    meta = ticker_meta(ticker)
    async with ctx.typing():
        try:
            from chartexchange_prints import fetch_prints_sync, format_prints_discord
            prints = await asyncio.to_thread(
                fetch_prints_sync, meta['ticker'], meta['min_print_size'], 15
            )
            msg = format_prints_discord(prints, meta['ticker'], meta['ratio'])
        except Exception as e:
            await ctx.send(f"Prints Fehler: {e}")
            return

    if len(msg) > 1950:
        msg = msg[:1950] + "\n```"
    await ctx.send(msg)
    asyncio.create_task(_push_bt_to_tradingview(meta['ticker'], prints))


# ═══════════════════════════════════════════════════════════
#  COMMANDS — DP MEMORY
# ═══════════════════════════════════════════════════════════

@bot.command(name='dpmem')
async def cmd_dpmem(ctx, ticker: str = "QQQ"):
    meta = ticker_meta(ticker)
    async with ctx.typing():
        try:
            spot, _, _ = await asyncio.to_thread(run_gex, meta['ticker'], meta['ratio'])
        except:
            spot = None
        msg = format_memory_discord(meta['ticker'], spot)
    await ctx.send(msg)


@bot.command(name='dpadd')
async def cmd_dpadd(ctx, price: float = 0, volume: int = 200000, ticker: str = "QQQ"):
    if price <= 0:
        await ctx.send("Syntax: `!dpadd 613.00 850000` oder `!dpadd 613.00 850000 GLD`")
        return
    meta = ticker_meta(ticker)
    try:
        spot, _, _ = await asyncio.to_thread(run_gex, meta['ticker'], meta['ratio'])
    except:
        spot = None
    manual = [{'strike': price, 'volume': volume, 'trades': 0, 'type': 'Manual DP'}]
    active = await asyncio.to_thread(dp_memory_update, meta['ticker'], manual, spot)
    dist_str = ""
    if spot and spot > 0:
        pct = (price - spot) / spot * 100
        arrow = "↑" if pct > 0 else "↓"
        dist_str = f" ({arrow}{abs(pct):.2f}% von Spot)"
    await ctx.send(f"✅ **{meta['ticker']} DP Level hinzugefügt:** {price:.2f} | Vol: {volume:,}{dist_str}\n"
                   f"Aktive Levels: {len(active)}")


@bot.command(name='dpremove')
async def cmd_dpremove(ctx, price: float = 0, ticker: str = "QQQ"):
    if price <= 0:
        await ctx.send("Syntax: `!dpremove 601.07` oder `!dpremove 450.00 GLD`")
        return
    meta = ticker_meta(ticker)
    from dp_memory import load_memory, save_memory
    memory = load_memory()
    levels = memory.get(meta['ticker'], [])
    before = len(levels)
    levels = [l for l in levels if abs(l['price'] - price) > 0.05]
    if before == len(levels):
        await ctx.send(f"❌ Level {price:.2f} nicht gefunden für {meta['ticker']}.")
        return
    memory[meta['ticker']] = levels
    save_memory(memory)
    await ctx.send(f"✅ **{meta['ticker']} DP Level entfernt:** {price:.2f} | Verbleibend: {len(levels)}")


# ═══════════════════════════════════════════════════════════
#  COMMANDS — UTILS / RATIOS / LEVELS
# ═══════════════════════════════════════════════════════════

@bot.command(name='goldlevels')
async def cmd_goldlevels(ctx):
    async with ctx.typing():
        try:
            spot, levels, _ = await asyncio.to_thread(run_gex, "GLD", GOLD_RATIO)
        except Exception as e:
            await ctx.send(f"Fehler: {e}")
            return
    if levels:
        gf = levels.get('gamma_flip', 0)
        cw = levels.get('call_wall', 0)
        pw = levels.get('put_wall', 0)
        hvl = levels.get('hvl', 0)
        r = GOLD_RATIO
        msg = (
            "Gold / XAUUSD Levels\n"
            "-----------------------------------\n"
            f"Gamma Flip:    {gf*r:.2f}  (GLD {gf:.2f})\n"
            f"Call Wall:     {cw*r:.2f}  (GLD {cw:.2f})\n"
            f"Put Wall:      {pw*r:.2f}  (GLD {pw:.2f})\n"
            f"HVL:           {hvl*r:.2f}  (GLD {hvl:.2f})\n"
            "-----------------------------------\n"
            f"GLD Spot: ${spot:.2f} | Ratio: {r:.4f}"
        )
        await ctx.send("```\n" + msg + "\n```")


@bot.command(name='levels')
async def cmd_levels(ctx):
    async with ctx.typing():
        try:
            spot, levels, _ = await asyncio.to_thread(run_gex, "QQQ", RATIO)
        except Exception as e:
            await ctx.send(f"Fehler: {e}")
            return
    if levels:
        gf = levels.get('gamma_flip', 0)
        cw = levels.get('call_wall', 0)
        pw = levels.get('put_wall', 0)
        hvl = levels.get('hvl', 0)
        msg = (
            "TradingView Input\n"
            "-----------------------------------\n"
            f"Gamma Flip:    {gf:.2f}\n"
            f"Call Wall:     {cw:.2f}\n"
            f"Put Wall:      {pw:.2f}\n"
            f"HVL:           {hvl:.2f}\n"
            "-----------------------------------\n"
            f"Ratio: {RATIO:.2f} | Spot: ${spot:.2f}"
        )
        await ctx.send("```\n" + msg + "\n```")


@bot.command(name='ratio')
async def cmd_ratio(ctx, action: str = None):
    global RATIO, GOLD_RATIO
    if action == "auto":
        await ctx.send("Berechne Ratios aus Live-Daten...")
        await asyncio.to_thread(auto_update_ratios)
        await ctx.send(f"✅ **Auto-Ratio:**\nNAS/QQQ: **{RATIO:.2f}**\nXAUUSD/GLD: **{GOLD_RATIO:.4f}**")
    elif action and action.replace('.', '').isdigit():
        RATIO = float(action)
        await ctx.send(f"NAS Ratio gesetzt: {RATIO:.2f}")
    else:
        await ctx.send(f"**Aktuelle Ratios:**\nNAS/QQQ: **{RATIO:.2f}**\nXAUUSD/GLD: **{GOLD_RATIO:.4f}**\n"
                       f"`!ratio auto` = Live berechnen")


@bot.command(name='goldratio')
async def cmd_goldratio(ctx, new_ratio: float = None):
    global GOLD_RATIO
    if new_ratio:
        GOLD_RATIO = new_ratio
        await ctx.send(f"✅ Gold Ratio gesetzt: **{GOLD_RATIO:.4f}**")
    else:
        await ctx.send(f"Gold Ratio: **{GOLD_RATIO:.4f}**")


@bot.command(name='setgex')
async def cmd_setgex(ctx, ticker: str = None, gf: float = None, cw: float = None, pw: float = None, hvl: float = None):
    if not ticker or not gf or not cw or not pw:
        await ctx.send("```\n!setgex <ticker> <gf> <cw> <pw> [hvl]\nBsp: !setgex QQQ 618.62 630 600\n```")
        return
    ticker = ticker.upper()
    if hvl is None:
        hvl = cw
    try:
        from gex_calculator import fetch_cboe_options
        spot, _ = await asyncio.to_thread(fetch_cboe_options, ticker)
    except:
        spot = 0
    regime = "Positiv" if spot and spot > gf else "Negativ"
    levels = {
        'gamma_flip': gf, 'call_wall': cw, 'put_wall': pw, 'hvl': hvl,
        'gamma_regime': regime, 'source': 'barchart-manual', 'spot': spot,
    }
    try:
        await asyncio.to_thread(push_gex_to_github, ticker, levels, spot or 0)
        push_ok = True
    except Exception as e:
        logger.warning(f"setgex push failed: {e}")
        push_ok = False
    meta = ticker_meta(ticker)
    lines = [
        f"GEX Levels gesetzt — {ticker}",
        "=" * 40,
        f"  Gamma Flip:  {gf:.2f} {meta['etf']}  =  {gf*meta['ratio']:.2f} {meta['cfd']}",
        f"  Call Wall:   {cw:.2f} {meta['etf']}  =  {cw*meta['ratio']:.2f} {meta['cfd']}",
        f"  Put Wall:    {pw:.2f} {meta['etf']}  =  {pw*meta['ratio']:.2f} {meta['cfd']}",
        f"  HVL:         {hvl:.2f} {meta['etf']}  =  {hvl*meta['ratio']:.2f} {meta['cfd']}",
        "", f"  Regime: {regime.upper()}",
        f"  TradingView Push: {'✅' if push_ok else '❌'}",
        "=" * 40,
    ]
    await ctx.send("```\n" + "\n".join(lines) + "\n```")


@bot.command(name='all')
async def cmd_all(ctx):
    """Full report: GEX in aktuellem Channel, DP split in ihre Channels."""
    async with ctx.typing():
        for tkr in ("QQQ", "GLD"):
            result = await get_gex_report(tkr)
            if result[0]:
                await ctx.send(result[0])
                await ctx.send(embed=result[1])
        await post_dp_report("QQQ")
        await post_dp_report("GLD")
    await ctx.send("✅ Full Report — GEX hier, DP in dedizierten Channels")


@bot.command(name='test')
async def cmd_test(ctx):
    import requests
    await ctx.send("Teste Verbindungen...")
    try:
        url = "https://cdn.cboe.com/api/global/delayed_quotes/options/QQQ.json"
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
        resp = requests.get(url, headers=headers, timeout=30)
        spot = resp.json().get('data', {}).get('close', 'N/A')
        opts = len(resp.json().get('data', {}).get('options', []))
        await ctx.send(f"CBOE: {resp.status_code} | Spot: {spot} | Options: {opts}")
    except Exception as e:
        await ctx.send(f"CBOE Fehler: {e}")

    # Channel Check
    lines = ["**DP Channels:**"]
    for label, cid in (("NASDAQ", CHANNEL_DP_NASDAQ), ("GOLD", CHANNEL_DP_GOLD)):
        ch = bot.get_channel(cid) if cid else None
        if ch:
            lines.append(f"  {label}: ✅ #{ch.name}")
        else:
            lines.append(f"  {label}: ❌ (ID: {cid or 'nicht gesetzt'})")
    await ctx.send("\n".join(lines))


@bot.command(name='hilfe')
async def cmd_help_de(ctx):
    msg = (
        "BullNet Bot - Befehle\n"
        "===================================\n"
        "  GEX\n"
        "  !gex / !gold / !gamma / !levels\n"
        "  !goldlevels / !setgex\n"
        "-----------------------------------\n"
        "  DARK POOL (split channels)\n"
        "  !dp [QQQ|GLD]  → dedizierter Channel\n"
        "  !dpall         → beide auf einmal\n"
        "  !prints [t]    → Block Trades\n"
        "-----------------------------------\n"
        "  DP MEMORY\n"
        "  !dpmem / !dpadd / !dpremove\n"
        "-----------------------------------\n"
        "  UTILS\n"
        "  !ratio / !goldratio / !all / !test\n"
        "===================================\n"
        "Auto-Posts: 09:00, 13:00, 14:30, 20:00 DE\n"
        "DP löscht alte Posts vor neuem"
    )
    await ctx.send("```\n" + msg + "\n```")


# ═══════════════════════════════════════════════════════════
#  STARTUP
# ═══════════════════════════════════════════════════════════

@bot.event
async def on_ready():
    logger.info(f"Bot ready: {bot.user}")

    try:
        await asyncio.to_thread(auto_update_ratios)
        logger.info(f"Ratios: NAS/QQQ={RATIO} | XAUUSD/GLD={GOLD_RATIO}")
    except Exception as e:
        logger.warning(f"Auto-ratio on startup failed: {e}")

    try:
        await asyncio.to_thread(ensure_symbol_info)
    except Exception as e:
        logger.warning(f"symbol_info check failed: {e}")

    # Channel Config Check
    logger.info(f"DP Channels: NASDAQ={CHANNEL_DP_NASDAQ} GOLD={CHANNEL_DP_GOLD}")
    if CHANNEL_DP_NASDAQ == 0:
        logger.warning("⚠️ CHANNEL_DP_NASDAQ nicht gesetzt!")
    if CHANNEL_DP_GOLD == 0:
        logger.warning("⚠️ CHANNEL_DP_GOLD nicht gesetzt!")

    auto_push_tradingview.start()
    logger.info("Auto TradingView Sync gestartet (alle 30 Min)")

    if SCHEDULE_ENABLED and (CHANNEL_ID or CHANNEL_DP_NASDAQ or CHANNEL_DP_GOLD):
        scheduled_gex.start()
        logger.info("Discord Scheduler gestartet: 09:00, 13:00, 14:30, 20:00 DE")


if __name__ == "__main__":
    if not TOKEN:
        spot, levels, gex_df = run_gex("QQQ", RATIO)
        if levels:
            print(format_discord_message(spot, levels, RATIO))
        dp = get_dark_pool_levels("QQQ", spot, gex_df)
        print(format_dp_discord(dp, RATIO))
    else:
        bot.run(TOKEN)
