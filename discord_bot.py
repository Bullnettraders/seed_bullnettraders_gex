import os
import asyncio
import logging
import traceback
from datetime import datetime, timezone

import discord
from discord.ext import commands, tasks
from discord import Embed

from gex_calculator import run as run_gex, format_discord_message
from darkpool import get_dark_pool_levels, format_dp_discord
from pine_seeds import push_gex_to_github, push_dp_to_github
from dp_memory import update_levels as dp_memory_update, get_top_zones, format_memory_discord

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

TOKEN = os.getenv('DISCORD_TOKEN', '')
CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID', '0'))
RATIO = float(os.getenv('QQQ_CFD_RATIO', '41.33'))
GOLD_RATIO = float(os.getenv('GLD_XAUUSD_RATIO', '10.97'))
SCHEDULE_ENABLED = os.getenv('SCHEDULE_ENABLED', 'true').lower() == 'true'
SCHEDULE_HOURS = [14, 17, 20]


def auto_update_ratios():
    """Auto-calculate ratios from live market data."""
    global RATIO, GOLD_RATIO
    import requests as req
    try:
        # Fetch NAS100 CFD and QQQ
        headers = {'User-Agent': 'Mozilla/5.0'}
        # Use Yahoo Finance API for quick quotes
        qqq_url = "https://query1.finance.yahoo.com/v8/finance/chart/QQQ?range=1d&interval=1d"
        nas_url = "https://query1.finance.yahoo.com/v8/finance/chart/NQ=F?range=1d&interval=1d"
        gld_url = "https://query1.finance.yahoo.com/v8/finance/chart/GLD?range=1d&interval=1d"
        gc_url  = "https://query1.finance.yahoo.com/v8/finance/chart/GC=F?range=1d&interval=1d"

        qqq_r = req.get(qqq_url, headers=headers, timeout=10)
        nas_r = req.get(nas_url, headers=headers, timeout=10)
        gld_r = req.get(gld_url, headers=headers, timeout=10)
        gc_r  = req.get(gc_url, headers=headers, timeout=10)

        qqq_price = qqq_r.json()['chart']['result'][0]['meta']['regularMarketPrice']
        nas_price = nas_r.json()['chart']['result'][0]['meta']['regularMarketPrice']
        gld_price = gld_r.json()['chart']['result'][0]['meta']['regularMarketPrice']
        gc_price  = gc_r.json()['chart']['result'][0]['meta']['regularMarketPrice']

        if qqq_price > 0 and nas_price > 0:
            new_ratio = round(nas_price / qqq_price, 2)
            if 30 < new_ratio < 55:  # Sanity check
                RATIO = new_ratio
                logger.info(f"Auto-Ratio NAS/QQQ: {RATIO} (NAS={nas_price}, QQQ={qqq_price})")

        if gld_price > 0 and gc_price > 0:
            new_gold = round(gc_price / gld_price, 2)
            if 5 < new_gold < 15:  # Sanity check
                GOLD_RATIO = new_gold
                logger.info(f"Auto-Ratio XAUUSD/GLD: {GOLD_RATIO} (GC={gc_price}, GLD={gld_price})")

    except Exception as e:
        logger.warning(f"Auto-ratio failed (using defaults): {e}")

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)


async def get_gex_report(ticker="QQQ", ratio=None):
    is_gold = ticker.upper() in ("GLD", "GOLD")
    if is_gold:
        ticker = "GLD"
    r = ratio or (GOLD_RATIO if is_gold else RATIO)
    
    spot = None
    levels = None
    gex_df = None
    
    # ── 1. Try Barchart Playwright (exact values) ──
    try:
        from barchart_gex import fetch_barchart_gex_async
        logger.info(f"Trying Barchart Playwright for {ticker}...")
        bc_levels = await fetch_barchart_gex_async(ticker)
        if bc_levels and 'gamma_flip' in bc_levels:
            spot = bc_levels.get('spot', 0)
            levels = bc_levels
            logger.info(f"Barchart Playwright SUCCESS: GF={levels.get('gamma_flip')} CW={levels.get('call_wall')} PW={levels.get('put_wall')}")
            
            # Get HVL from CBOE if missing
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
            logger.info("Barchart Playwright returned no gamma_flip, falling back...")
            levels = None
    except Exception as e:
        logger.warning(f"Barchart Playwright failed: {e}, falling back to API/CBOE")

    # ── 2. Fallback: Barchart API + CBOE (sync in thread) ──
    if not levels or 'gamma_flip' not in levels:
        try:
            spot, levels, gex_df = await asyncio.to_thread(run_gex, ticker, r)
        except Exception as e:
            logger.error(f"GEX error: {e}")
            logger.error(traceback.format_exc())
            return None, None, str(e) + "\n" + traceback.format_exc()[-500:]
    
    if not levels:
        return None, None, "Levels leer - keine Daten berechnet"
    
    # Fix regime if N/A but we have spot + gamma_flip
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

    # Auto-push to GitHub pine_seeds for TradingView
    try:
        await asyncio.to_thread(push_gex_to_github, ticker, levels, spot)
    except Exception as e:
        logger.warning(f"Pine seeds push failed: {e}")

    return text_msg, embed, None


@tasks.loop(minutes=30)
async def scheduled_gex():
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:
        return
    if now.hour not in SCHEDULE_HOURS or now.minute > 5:
        return
    channel = bot.get_channel(CHANNEL_ID)
    if not channel:
        return
    
    # Auto-update ratios
    try:
        await asyncio.to_thread(auto_update_ratios)
    except:
        pass
    
    # Post GEX
    result = await get_gex_report()
    text_msg, embed = result[0], result[1]
    if text_msg:
        await channel.send(text_msg)
        await channel.send(embed=embed)
    # Post Dark Pool at first schedule only (14:00 UTC)
    if now.hour == 14:
        try:
            spot, _, gex_df = await asyncio.to_thread(run_gex, "QQQ", RATIO)
            dp = await asyncio.to_thread(get_dark_pool_levels, "QQQ", spot, gex_df)
            dp_msg = format_dp_discord(dp, RATIO)
            await channel.send(dp_msg)
            # Update DP Memory
            if dp.get('levels'):
                await asyncio.to_thread(dp_memory_update, "QQQ", dp['levels'], spot)
            # Push to GitHub (using memory for sticky levels)
            if dp.get('levels'):
                top_zones = get_top_zones("QQQ", n=4, current_price=spot)
                if top_zones:
                    mem_dp = dict(dp)
                    mem_dp['levels'] = [{'strike': z['price'], 'volume': z.get('volume', 0), 'type': z.get('type', 'DP Level')} for z in top_zones]
                    await asyncio.to_thread(push_dp_to_github, "QQQ", mem_dp)
            # Also do GLD
            try:
                gld_spot, _, gld_gex = await asyncio.to_thread(run_gex, "GLD", GOLD_RATIO)
                gld_dp = await asyncio.to_thread(get_dark_pool_levels, "GLD", gld_spot, gld_gex)
                if gld_dp.get('levels'):
                    await asyncio.to_thread(dp_memory_update, "GLD", gld_dp['levels'], gld_spot)
                if gld_dp.get('levels'):
                    gld_zones = get_top_zones("GLD", n=4, current_price=gld_spot)
                    if gld_zones:
                        mem_gld = dict(gld_dp)
                        mem_gld['levels'] = [{'strike': z['price'], 'volume': z.get('volume', 0), 'type': z.get('type', 'DP Level')} for z in gld_zones]
                        await asyncio.to_thread(push_dp_to_github, "GLD", mem_gld)
            except Exception as e:
                logger.error(f"Scheduled GLD DP error: {e}")
        except Exception as e:
            logger.error(f"Scheduled DP error: {e}")


@scheduled_gex.before_loop
async def before_schedule():
    await bot.wait_until_ready()


@bot.command(name='gex')
async def cmd_gex(ctx, ticker: str = "QQQ"):
    ticker = ticker.upper()
    async with ctx.typing():
        result = await get_gex_report(ticker)
    text_msg, embed, error = result[0], result[1], result[2]
    if text_msg:
        await ctx.send(text_msg)
        await ctx.send(embed=embed)
    else:
        err_msg = f"Keine Daten fuer {ticker}\nFehler: {error}"
        if len(err_msg) > 1900:
            err_msg = err_msg[:1900]
        await ctx.send(err_msg)


@bot.command(name='gold')
async def cmd_gold(ctx):
    """Gold (GLD) GEX Report with XAUUSD conversion."""
    async with ctx.typing():
        result = await get_gex_report("GLD")
    text_msg, embed, error = result[0], result[1], result[2]
    if text_msg:
        await ctx.send(text_msg)
        await ctx.send(embed=embed)
    else:
        err_msg = f"Keine Gold Daten\nFehler: {error}"
        if len(err_msg) > 1900:
            err_msg = err_msg[:1900]
        await ctx.send(err_msg)


@bot.command(name='goldlevels')
async def cmd_goldlevels(ctx):
    """Gold levels formatted for TradingView / XAUUSD."""
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
        msg = "Gold / XAUUSD Levels\n"
        msg += "-----------------------------------\n"
        msg += f"Gamma Flip:    {gf*r:.2f}  (GLD {gf:.2f})\n"
        msg += f"Call Wall:     {cw*r:.2f}  (GLD {cw:.2f})\n"
        msg += f"Put Wall:      {pw*r:.2f}  (GLD {pw:.2f})\n"
        msg += f"HVL:           {hvl*r:.2f}  (GLD {hvl:.2f})\n"
        msg += "-----------------------------------\n"
        msg += f"GLD Spot: ${spot:.2f} | Ratio: {r:.4f}"
        await ctx.send("```\n" + msg + "\n```")


@bot.command(name='goldratio')
async def cmd_goldratio(ctx, new_ratio: float = None):
    """View or set the GLD→XAUUSD ratio."""
    global GOLD_RATIO
    if new_ratio:
        GOLD_RATIO = new_ratio
        await ctx.send(f"Gold Ratio gesetzt: {GOLD_RATIO:.4f}")
    else:
        await ctx.send(f"Gold Ratio: {GOLD_RATIO:.4f}\nBeispiel: GLD $260 x {GOLD_RATIO:.4f} = XAUUSD ${260*GOLD_RATIO:.2f}")


@bot.command(name='setgex')
async def cmd_setgex(ctx, ticker: str = None, gf: float = None, cw: float = None, pw: float = None, hvl: float = None):
    """Manually set GEX levels from Barchart and push to TradingView.
    Usage: !setgex QQQ 618.62 630 600
           !setgex GLD 391.72 475 450
           !setgex GLD 391.72 475 450 460  (with HVL)
    """
    if not ticker or not gf or not cw or not pw:
        await ctx.send("```\n"
            "!setgex <ticker> <gamma_flip> <call_wall> <put_wall> [hvl]\n\n"
            "Beispiel:\n"
            "  !setgex QQQ 618.62 630 600\n"
            "  !setgex GLD 391.72 475 450\n"
            "  !setgex GLD 391.72 475 450 460\n\n"
            "Werte von Barchart Gamma Exposure Seite kopieren.\n"
            "Pushed automatisch zu TradingView!\n```")
        return

    ticker = ticker.upper()
    if hvl is None:
        hvl = cw  # Default HVL to call wall if not given

    # Determine regime
    try:
        # Try to get current spot for regime calculation
        from gex_calculator import fetch_cboe_options
        spot, _ = await asyncio.to_thread(fetch_cboe_options, ticker)
    except:
        spot = 0

    regime = "Positiv" if spot and spot > gf else "Negativ"

    # Build levels dict
    levels = {
        'gamma_flip': gf,
        'call_wall': cw,
        'put_wall': pw,
        'hvl': hvl,
        'gamma_regime': regime,
        'source': 'barchart-manual',
        'spot': spot,
    }

    # Push to GitHub for TradingView auto-import
    try:
        await asyncio.to_thread(push_gex_to_github, ticker, levels, spot or 0)
        push_ok = True
    except Exception as e:
        logger.warning(f"setgex push failed: {e}")
        push_ok = False

    # Determine labels
    is_gold = ticker in ("GLD", "GOLD")
    etf_label = "GLD" if is_gold else ticker
    r = GOLD_RATIO if is_gold else RATIO
    cfd_label = "XAUUSD" if is_gold else "NAS100"

    # Build response
    lines = [
        f"GEX Levels gesetzt — {ticker}",
        "=" * 40,
        f"  Gamma Flip:  {gf:.2f} {etf_label}  =  {gf*r:.2f} {cfd_label}",
        f"  Call Wall:   {cw:.2f} {etf_label}  =  {cw*r:.2f} {cfd_label}",
        f"  Put Wall:    {pw:.2f} {etf_label}  =  {pw*r:.2f} {cfd_label}",
        f"  HVL:         {hvl:.2f} {etf_label}  =  {hvl*r:.2f} {cfd_label}",
        "",
        f"  Regime: {regime.upper()}",
        f"  GitHub Push: {'✅' if push_ok else '❌ Fehler'}",
        f"  Source: Barchart (manuell)",
        "=" * 40,
    ]
    await ctx.send("```\n" + "\n".join(lines) + "\n```")


@bot.command(name='darkpool')
async def cmd_darkpool(ctx, ticker: str = "QQQ"):
    """Dark Pool levels from previous day."""
    ticker = ticker.upper()
    is_gold = ticker in ("GLD", "GOLD")
    r = GOLD_RATIO if is_gold else RATIO
    etf_label = "GLD" if is_gold else "QQQ"
    cfd_label = "XAUUSD" if is_gold else "CFD"
    
    async with ctx.typing():
        try:
            spot, _, gex_df = await asyncio.to_thread(run_gex, ticker, r)
            dp = await asyncio.to_thread(get_dark_pool_levels, ticker, spot, gex_df)
            msg = format_dp_discord(dp, r, ticker)
            
            # Update DP Memory — track unvisited levels
            dp_ticker = "GLD" if is_gold else ticker
            if dp.get('levels'):
                active_levels = await asyncio.to_thread(dp_memory_update, dp_ticker, dp['levels'], spot)
                logger.info(f"DP Memory: {len(active_levels)} active levels for {dp_ticker}")
            
            # Push DP levels to GitHub for Pine Script auto-import
            # Uses MEMORY (sticky levels) instead of just today's data
            if dp.get('levels'):
                top_zones = get_top_zones(dp_ticker, n=4, current_price=spot)
                if top_zones:
                    # Override dp data with memory-based top zones for push
                    mem_dp = dict(dp)
                    mem_dp['levels'] = [{'strike': z['price'], 'volume': z.get('volume', 0), 'type': z.get('type', 'DP Level')} for z in top_zones]
                    await asyncio.to_thread(push_dp_to_github, dp_ticker, mem_dp)
        except Exception as e:
            await ctx.send(f"Dark Pool Fehler: {e}")
            return
    if len(msg) > 1900:
        msg = msg[:1900] + "\n```"
    await ctx.send(msg)

    # Also send embed
    levels = dp.get('levels', [])
    finra = dp.get('finra')
    if levels:
        color = 0xFFD700 if is_gold else 0x7B68EE
        embed = Embed(
            title="BullNet Dark Pool - " + ("GOLD" if is_gold else ticker),
            description=f"Source: {dp.get('source', 'N/A')} | {len(levels)} Levels",
            color=color, timestamp=datetime.now(timezone.utc)
        )
        for lvl in levels[:6]:
            strike = lvl['strike']
            tp = lvl['type']
            vol = lvl.get('volume', 0)
            embed.add_field(
                name=f"{tp}",
                value=f"`{strike:.2f}` {etf_label}\n`{strike*r:.0f}` {cfd_label}\nVol: {vol:,}",
                inline=True
            )
        if finra:
            embed.add_field(
                name="FINRA Short %",
                value=f"`{finra['short_percent']}%`\n{finra['date']}",
                inline=True
            )
        embed.set_footer(text="Ratio: " + f"{r:.2f}" + " | BULLNET")
        await ctx.send(embed=embed)


@bot.command(name='dp')
async def cmd_dp(ctx, ticker: str = "QQQ"):
    """Shortcut for !darkpool."""
    await cmd_darkpool(ctx, ticker)


@bot.command(name='dpmem')
async def cmd_dpmem(ctx, ticker: str = "QQQ"):
    """Show active (unvisited) Dark Pool levels from memory."""
    ticker = ticker.upper()
    is_gold = ticker in ("GLD", "GOLD")
    dp_ticker = "GLD" if is_gold else ticker
    r = GOLD_RATIO if is_gold else RATIO
    
    async with ctx.typing():
        try:
            spot, _, _ = await asyncio.to_thread(run_gex, ticker, r)
        except:
            spot = None
        
        msg = format_memory_discord(dp_ticker, spot)
    
    await ctx.send(msg)


@bot.command(name='dpadd')
async def cmd_dpadd(ctx, price: float = 0, volume: int = 200000, ticker: str = "QQQ"):
    """Manuell ein DP Level zur Memory hinzufügen. Syntax: !dpadd 613.00 850000"""
    if price <= 0:
        await ctx.send("Syntax: `!dpadd 613.00 850000` oder `!dpadd 613.00 850000 GLD`")
        return
    
    ticker = ticker.upper()
    is_gold = ticker in ("GLD", "GOLD")
    dp_ticker = "GLD" if is_gold else ticker
    
    # Get current spot for context
    r = GOLD_RATIO if is_gold else RATIO
    try:
        spot, _, _ = await asyncio.to_thread(run_gex, ticker, r)
    except:
        spot = None
    
    # Add to memory
    manual_level = [{'strike': price, 'volume': volume, 'trades': 0, 'type': 'Manual DP'}]
    active = await asyncio.to_thread(dp_memory_update, dp_ticker, manual_level, spot)
    
    dist_str = ""
    if spot and spot > 0:
        dist_pct = (price - spot) / spot * 100
        arrow = "↑" if dist_pct > 0 else "↓"
        dist_str = f" ({arrow}{abs(dist_pct):.2f}% von Spot)"
    
    await ctx.send(f"✅ **{dp_ticker} DP Level hinzugefügt:** {price:.2f} | Vol: {volume:,}{dist_str}\n"
                   f"Aktive Levels: {len(active)} | Bleibt bis Preis es erreicht (max 14 Tage)")


@bot.command(name='dpremove')
async def cmd_dpremove(ctx, price: float = 0, ticker: str = "QQQ"):
    """Manuell ein DP Level aus Memory entfernen. Syntax: !dpremove 601.07"""
    if price <= 0:
        await ctx.send("Syntax: `!dpremove 601.07` oder `!dpremove 450.00 GLD`")
        return
    
    ticker = ticker.upper()
    is_gold = ticker in ("GLD", "GOLD")
    dp_ticker = "GLD" if is_gold else ticker
    
    from dp_memory import load_memory, save_memory
    memory = load_memory()
    levels = memory.get(dp_ticker, [])
    
    before = len(levels)
    levels = [l for l in levels if abs(l['price'] - price) > 0.05]
    after = len(levels)
    
    if before == after:
        await ctx.send(f"❌ Level {price:.2f} nicht in Memory gefunden für {dp_ticker}.")
        return
    
    memory[dp_ticker] = levels
    save_memory(memory)
    await ctx.send(f"✅ **{dp_ticker} DP Level entfernt:** {price:.2f} | Verbleibend: {after} Levels")


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


@bot.command(name='ratio')
async def cmd_ratio(ctx, action: str = None):
    """Zeige oder aktualisiere Ratios. !ratio auto = Live berechnen"""
    global RATIO, GOLD_RATIO
    if action == "auto":
        await ctx.send("Berechne Ratios aus Live-Daten...")
        await asyncio.to_thread(auto_update_ratios)
        await ctx.send(f"✅ **Auto-Ratio:**\nNAS/QQQ: **{RATIO:.2f}**\nXAUUSD/GLD: **{GOLD_RATIO:.2f}**")
    elif action and action.replace('.','').isdigit():
        RATIO = float(action)
        await ctx.send(f"NAS Ratio gesetzt: {RATIO:.2f}")
    else:
        await ctx.send(f"**Aktuelle Ratios:**\nNAS/QQQ: **{RATIO:.2f}**\nXAUUSD/GLD: **{GOLD_RATIO:.2f}**\n\n"
                       f"`!ratio auto` = Live berechnen\n`!ratio 41.33` = Manuell setzen")


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
        msg = "TradingView Input\n"
        msg += "-----------------------------------\n"
        msg += f"Gamma Flip:    {gf:.2f}\n"
        msg += f"Call Wall:     {cw:.2f}\n"
        msg += f"Put Wall:      {pw:.2f}\n"
        msg += f"HVL:           {hvl:.2f}\n"
        msg += "-----------------------------------\n"
        msg += f"Ratio: {RATIO:.2f} | Spot: ${spot:.2f}"
        await ctx.send("```\n" + msg + "\n```")


@bot.command(name='all')
async def cmd_all(ctx):
    """Full report: GEX + Dark Pool + Gold combined."""
    async with ctx.typing():
        # Nasdaq GEX
        result = await get_gex_report("QQQ")
        text_msg, embed, error = result[0], result[1], result[2]

        # Dark Pool
        try:
            spot, _, gex_df = await asyncio.to_thread(run_gex, "QQQ", RATIO)
            dp = await asyncio.to_thread(get_dark_pool_levels, "QQQ", spot, gex_df)
            dp_msg = format_dp_discord(dp, RATIO)
        except:
            dp_msg = None

        # Gold GEX
        gold_result = await get_gex_report("GLD")
        gold_msg, gold_embed, gold_error = gold_result[0], gold_result[1], gold_result[2]

    if text_msg:
        await ctx.send(text_msg)
        await ctx.send(embed=embed)
    if dp_msg:
        if len(dp_msg) > 1900:
            dp_msg = dp_msg[:1900] + "\n```"
        await ctx.send(dp_msg)
    if gold_msg:
        await ctx.send(gold_msg)
        await ctx.send(embed=gold_embed)


@bot.command(name='test')
async def cmd_test(ctx):
    import requests
    await ctx.send("Teste CBOE Verbindung...")
    try:
        url = "https://cdn.cboe.com/api/global/delayed_quotes/options/QQQ.json"
        headers = {'User-Agent': 'Mozilla/5.0', 'Accept': 'application/json'}
        resp = requests.get(url, headers=headers, timeout=30)
        await ctx.send(f"Status: {resp.status_code} | Size: {len(resp.text)} bytes")
        data = resp.json()
        spot = data.get('data', {}).get('close', 'N/A')
        opts = len(data.get('data', {}).get('options', []))
        await ctx.send(f"Spot: {spot} | Options: {opts}")
    except Exception as e:
        await ctx.send(f"CBOE Fehler: {e}")


@bot.command(name='hilfe')
async def cmd_help_de(ctx):
    msg = "BullNet GEX Bot - Befehle\n"
    msg += "===================================\n"
    msg += "  NASDAQ / QQQ\n"
    msg += "-----------------------------------\n"
    msg += "!gex         Voller GEX Report\n"
    msg += "!levels      TradingView Werte\n"
    msg += "!gamma       Schnellcheck Regime\n"
    msg += "!darkpool    Dark Pool Levels\n"
    msg += "!dp          Kurzform darkpool\n"
    msg += "!ratio       Ratio anzeigen/setzen\n"
    msg += "-----------------------------------\n"
    msg += "  GOLD / XAUUSD\n"
    msg += "-----------------------------------\n"
    msg += "!gold        Gold GEX Report\n"
    msg += "!goldlevels  XAUUSD Werte\n"
    msg += "!goldratio   Gold Ratio setzen\n"
    msg += "-----------------------------------\n"
    msg += "  KOMBI\n"
    msg += "-----------------------------------\n"
    msg += "!all         NAS + DP + Gold\n"
    msg += "!test        CBOE Verbindung\n"
    msg += "!hilfe       Diese Hilfe\n"
    msg += "===================================\n"
    msg += "Auto: 14:00, 17:00, 20:30 UTC"
    await ctx.send("```\n" + msg + "\n```")


@bot.event
async def on_ready():
    logger.info(f"Bot ready: {bot.user}")
    # Auto-calculate ratios from live market data
    try:
        await asyncio.to_thread(auto_update_ratios)
        logger.info(f"Ratios: NAS/QQQ={RATIO} | XAUUSD/GLD={GOLD_RATIO}")
    except Exception as e:
        logger.warning(f"Auto-ratio on startup failed: {e}")
    if SCHEDULE_ENABLED and CHANNEL_ID > 0:
        scheduled_gex.start()


if __name__ == "__main__":
    if not TOKEN:
        spot, levels, gex_df = run_gex("QQQ", RATIO)
        if levels:
            print(format_discord_message(spot, levels, RATIO))
        dp = get_dark_pool_levels("QQQ", spot, gex_df)
        print(format_dp_discord(dp, RATIO))
    else:
        bot.run(TOKEN)
