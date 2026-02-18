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
from pine_seeds import push_gex_to_github

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

TOKEN = os.getenv('DISCORD_TOKEN', '')
CHANNEL_ID = int(os.getenv('DISCORD_CHANNEL_ID', '0'))
RATIO = float(os.getenv('QQQ_CFD_RATIO', '41.33'))
GOLD_RATIO = float(os.getenv('GLD_XAUUSD_RATIO', '10.97'))
SCHEDULE_ENABLED = os.getenv('SCHEDULE_ENABLED', 'true').lower() == 'true'
SCHEDULE_HOURS = [14, 17, 20]

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix='!', intents=intents)


async def get_gex_report(ticker="QQQ", ratio=None):
    is_gold = ticker.upper() in ("GLD", "GOLD")
    if is_gold:
        ticker = "GLD"
    r = ratio or (GOLD_RATIO if is_gold else RATIO)
    try:
        spot, levels, gex_df = await asyncio.to_thread(run_gex, ticker, r)
    except Exception as e:
        logger.error(f"GEX error: {e}")
        logger.error(traceback.format_exc())
        return None, None, str(e) + "\n" + traceback.format_exc()[-500:]
    if not levels:
        return None, None, "Levels leer - keine Daten berechnet"
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


@bot.command(name='darkpool')
async def cmd_darkpool(ctx, ticker: str = "QQQ"):
    """Dark Pool levels from previous day."""
    ticker = ticker.upper()
    async with ctx.typing():
        try:
            spot, _, gex_df = await asyncio.to_thread(run_gex, ticker, RATIO)
            dp = await asyncio.to_thread(get_dark_pool_levels, ticker, spot, gex_df)
            msg = format_dp_discord(dp, RATIO)
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
        color = 0x7B68EE
        embed = Embed(
            title="BullNet Dark Pool - " + ticker,
            description=f"Source: {dp.get('source', 'N/A')} | {len(levels)} Levels",
            color=color, timestamp=datetime.now(timezone.utc)
        )
        for lvl in levels[:6]:
            strike = lvl['strike']
            tp = lvl['type']
            vol = lvl.get('volume', 0)
            embed.add_field(
                name=f"{tp}",
                value=f"`{strike:.2f}` QQQ\n`{strike*RATIO:.0f}` CFD\nVol: {vol:,}",
                inline=True
            )
        if finra:
            embed.add_field(
                name="FINRA Short %",
                value=f"`{finra['short_percent']}%`\n{finra['date']}",
                inline=True
            )
        embed.set_footer(text="Ratio: " + f"{RATIO:.2f}" + " | BULLNET")
        await ctx.send(embed=embed)


@bot.command(name='dp')
async def cmd_dp(ctx, ticker: str = "QQQ"):
    """Shortcut for !darkpool."""
    await cmd_darkpool(ctx, ticker)


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
async def cmd_ratio(ctx, new_ratio: float = None):
    global RATIO
    if new_ratio:
        RATIO = new_ratio
        await ctx.send(f"Ratio gesetzt: {RATIO:.2f}")
    else:
        await ctx.send(f"Aktueller Ratio: {RATIO:.2f}")


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
