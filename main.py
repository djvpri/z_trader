"""
Z Trader — Backend
===================
Stack  : FastAPI + uvicorn
Data   : yfinance (gratis)
AI     : DeepSeek (RSI Bot), Gemini (Google), Qwen (MA Bot) — multi-saham independen
Push   : WebSocket broadcast ke semua client yang terhubung
"""

import asyncio
import json
import os
import time as time_module
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from datetime import datetime, date, time, timezone, timedelta
from typing import Optional
from urllib.parse import quote

import httpx
import yfinance as yf
from dotenv import load_dotenv
import database as db
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

load_dotenv()

# ─── Konfigurasi API key ──────────────────────────────────────────────────────
DEEPSEEK_API_KEY = os.getenv("DEEPSEEK_API_KEY", "")
GEMINI_API_KEY   = os.getenv("GEMINI_API_KEY", "")
QWEN_API_KEY     = os.getenv("QWEN_API_KEY", "")

# ─── Konstanta ────────────────────────────────────────────────────────────────
MODAL           = 100_000_000   # Rp 100 juta modal awal per AI
POLL_INTERVAL   = 5             # detik antar fetch harga
LAYER1_EVERY    = 2             # Layer 1 setiap N tick
GEMINI_COOLDOWN = 6             # minimum tick jeda antar panggilan Gemini
NEWS_EVERY      = 6             # update berita setiap N tick
LOT_SIZE         = 100    # 1 lot = 100 lembar
BUY_FRACTION     = 0.35   # pakai 35% kas per BUY
SNAPSHOT_EVERY   = 12     # simpan snapshot portofolio setiap N tick (~60 detik)
PRICE_SAVE_EVERY = 6      # simpan harga ke DB setiap N tick (~30 detik)
RESULT_EVERY     = 60     # simpan hasil kompetisi setiap N tick (~5 menit)

DEEPSEEK_URL = "https://api.deepseek.com/v1/chat/completions"
GEMINI_URL   = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
QWEN_URL     = "https://dashscope.aliyuncs.com/compatible-mode/v1/chat/completions"

GEMINI_SYSTEM = (
    "Kamu adalah AI trader saham IDX dengan portofolio multi-saham. "
    "Pilih saham terbaik untuk ditrade berdasarkan data teknikal dan berita. "
    "Respond HANYA dengan JSON: "
    '{"ticker":"KODE.JK","action":"BUY"|"SELL"|"HOLD","confidence":0-100,"reason":"maks 20 kata"}'
)

FUNDAMENTAL = {
    "BBCA.JK": {"PE": "23.4x", "PBV": "4.1x", "ROE": "18.2%", "DY": "2.1%", "rating": "Strong Buy"},
    "TLKM.JK": {"PE": "13.2x", "PBV": "2.3x", "ROE": "17.5%", "DY": "5.4%", "rating": "Hold"},
    "ASII.JK": {"PE": "11.8x", "PBV": "1.6x", "ROE": "14.8%", "DY": "4.2%", "rating": "Buy"},
    "BBRI.JK": {"PE": "14.1x", "PBV": "2.2x", "ROE": "15.9%", "DY": "5.8%", "rating": "Buy"},
    "GOTO.JK": {"PE": "N/A",   "PBV": "3.8x", "ROE": "-4.2%", "DY": "0%",   "rating": "Speculative"},
}
TICKERS = list(FUNDAMENTAL.keys())
WIB = timezone(timedelta(hours=7))

# ─── Konfigurasi RSS ──────────────────────────────────────────────────────────
TICKER_QUERIES = {
    "BBCA.JK": "BBCA BCA bank saham IDX",
    "TLKM.JK": "TLKM Telkom saham IDX",
    "ASII.JK": "ASII Astra saham IDX",
    "BBRI.JK": "BBRI BRI bank saham IDX",
    "GOTO.JK": "GOTO GoTo Gojek saham IDX",
    "XAU":     "harga emas XAU gold price",
}

POSITIVE_KW = {"naik","tumbuh","profit","laba","untung","meningkat","positif","bullish",
               "rekor","kuat","optimis","rally","gain","rise","growth","strong","upgrade",
               "dividen","ekspansi","melampaui","bagikan","catat","menang","lonjakan"}
NEGATIVE_KW = {"turun","jatuh","rugi","kerugian","menurun","negatif","bearish","koreksi",
               "gagal","anjlok","fall","loss","weak","decline","crash","downgrade","cemas",
               "khawatir","merosot","tekanan","tertekan","susut","lesu","resesi","inflasi"}

_news_cache: dict = {}   # ticker -> (headline, sentiment, timestamp)
NEWS_CACHE_TTL = 300     # 5 menit


def _sentiment_score(text: str) -> float:
    words = text.lower().split()
    pos   = sum(1 for w in words if any(k in w for k in POSITIVE_KW))
    neg   = sum(1 for w in words if any(k in w for k in NEGATIVE_KW))
    total = pos + neg
    if total == 0:
        return 0.0
    return round((pos - neg) / total, 2)


async def fetch_rss_news(query: str) -> tuple[str, float]:
    url = f"https://news.google.com/rss/search?q={quote(query)}&hl=id&gl=ID&ceid=ID:id"
    try:
        async with httpx.AsyncClient(timeout=10, headers={"User-Agent": "Mozilla/5.0"}) as client:
            resp = await client.get(url)
            resp.raise_for_status()
        root  = ET.fromstring(resp.text)
        items = root.findall(".//item")
        if not items:
            return "", 0.0
        title = items[0].findtext("title", "").strip()
        if " - " in title:
            title = title.rsplit(" - ", 1)[0].strip()
        return title, _sentiment_score(title)
    except Exception as e:
        print(f"[rss] Error ({query[:20]}): {e}")
        return "", 0.0


async def fetch_news_cached(ticker: str) -> tuple[str, float]:
    """Ambil berita nyata dari Google News RSS, cache 5 menit, fallback ke stub."""
    now_ts  = time_module.time()
    cached  = _news_cache.get(ticker)
    if cached and (now_ts - cached[2]) < NEWS_CACHE_TTL:
        return cached[0], cached[1]

    query            = TICKER_QUERIES.get(ticker, ticker)
    headline, senti  = await fetch_rss_news(query)

    if headline:
        _news_cache[ticker] = (headline, senti, now_ts)
        print(f"[rss] {ticker}: {headline[:60]}")
        return headline, senti

    # Fallback ke stub jika RSS gagal
    return fetch_news(ticker)


# ─── Jam pasar IHSG ───────────────────────────────────────────────────────────
def _next_open_dt(now: datetime) -> datetime:
    """Hitung waktu buka pasar berikutnya."""
    today = now.date()
    t     = now.time()
    wd    = now.weekday()
    if wd < 5 and t < time(9, 0):
        return datetime.combine(today, time(9, 0), tzinfo=WIB)
    if wd < 5 and time(12, 0) < t < time(13, 30):
        return datetime.combine(today, time(13, 30), tzinfo=WIB)
    delta = 1
    while True:
        nxt = today + timedelta(days=delta)
        if nxt.weekday() < 5:
            return datetime.combine(nxt, time(9, 0), tzinfo=WIB)
        delta += 1


def get_market_status() -> dict:
    now = datetime.now(WIB)
    wd  = now.weekday()
    t   = now.time()

    if wd >= 5:
        reason = "Akhir pekan"
    elif t < time(9, 0):
        reason = "Belum buka (pre-market)"
    elif time(9, 0) <= t <= time(12, 0):
        return {"open": True, "session": "Sesi 1 (09:00–12:00 WIB)"}
    elif time(12, 0) < t < time(13, 30):
        reason = "Istirahat siang"
    elif time(13, 30) <= t <= time(15, 50):
        return {"open": True, "session": "Sesi 2 (13:30–15:50 WIB)"}
    else:
        reason = "Pasar sudah tutup"

    next_dt = _next_open_dt(now)
    return {
        "open":      False,
        "reason":    reason,
        "next_open": next_dt.isoformat(),
        "server_ts": now.isoformat(),
    }


# ─── State global ──────────────────────────────────────────────────────────────
class SimState:
    def __init__(self):
        self.prices        : dict[str, float]       = {t: 0.0 for t in TICKERS}
        self.price_history : dict[str, list[float]] = {t: []  for t in TICKERS}
        self.tick_count    = 0
        self.last_news     : dict[str, str]   = {t: "Menunggu berita..." for t in TICKERS}
        self.news_sentiment: dict[str, float] = {t: 0.0 for t in TICKERS}
        self.agents        = self._init_agents()
        self.clients       : list[WebSocket]  = []
        self.gemini_last_tick = 0
        self.last_trigger     = ""

    def _init_agents(self):
        return {
            name: {
                "name":        name,
                "cash":        MODAL,
                "holdings":    {t: {"positions": 0, "avg_price": 0.0} for t in TICKERS},
                "trades":      [],
                "signal":      "HOLD",
                "confidence":  0,
                "reason":      "Menunggu data...",
                "last_ticker": TICKERS[0],
            }
            for name in ("deepseek", "gemini", "qwen")
        }

    def portfolio_value(self, name: str) -> float:
        ag    = self.agents[name]
        total = ag["cash"]
        for t, h in ag["holdings"].items():
            total += h["positions"] * self.prices.get(t, 0)
        return total

    def reset(self):
        self.prices         = {t: 0.0 for t in TICKERS}
        self.price_history  = {t: []  for t in TICKERS}
        self.tick_count     = 0
        self.gemini_last_tick = 0
        self.last_trigger   = ""
        self.agents         = self._init_agents()


sim = SimState()


# ─── Utilitas teknikal ─────────────────────────────────────────────────────────
def calc_ma(prices: list[float], n: int) -> Optional[float]:
    if len(prices) < n:
        return None
    return sum(prices[-n:]) / n


def calc_rsi(prices: list[float], n: int = 14) -> float:
    if len(prices) < n + 1:
        return 50.0
    gains, losses = 0.0, 0.0
    for i in range(len(prices) - n, len(prices)):
        d = prices[i] - prices[i - 1]
        if d > 0: gains  += d
        else:     losses += abs(d)
    if losses == 0:
        return 100.0
    return 100 - (100 / (1 + gains / losses))


def calc_momentum(prices: list[float], n: int = 5) -> float:
    if len(prices) < n + 1:
        return 0.0
    return (prices[-1] - prices[-1 - n]) / prices[-1 - n] * 100


def calc_bollinger(prices: list[float], n: int = 20, k: float = 2.0):
    if len(prices) < n:
        return None, None, None
    window = prices[-n:]
    mid    = sum(window) / n
    std    = (sum((p - mid) ** 2 for p in window) / n) ** 0.5
    return mid, mid + k * std, mid - k * std


# ─── Fetch semua harga secara paralel ─────────────────────────────────────────
async def fetch_all_prices() -> dict[str, float]:
    loop = asyncio.get_event_loop()

    def get_price(ticker: str):
        try:
            price = yf.Ticker(ticker).fast_info.get("lastPrice")
            return ticker, float(price) if price else None
        except Exception as e:
            print(f"[price] Error {ticker}: {e}")
            return ticker, None

    tasks   = [loop.run_in_executor(None, get_price, t) for t in TICKERS]
    results = await asyncio.gather(*tasks)
    return {t: p for t, p in results if p is not None}


# ─── Fetch berita ──────────────────────────────────────────────────────────────
def fetch_news(ticker: str) -> tuple[str, float]:
    import random
    NEWS_POOL = {
        "BBCA.JK": [
            ("BI turunkan suku bunga 25bps, positif untuk perbankan", 0.6),
            ("BBCA catat kredit naik 12% YoY di Q1", 0.5),
            ("NPL industri perbankan naik tipis ke 2.8%", -0.4),
            ("BBCA bagikan dividen interim Rp 100/saham", 0.7),
        ],
        "TLKM.JK": [
            ("TLKM menangkan tender jaringan 5G di 10 kota", 0.8),
            ("Churn rate IndiHome meningkat di Q2", -0.5),
            ("TLKM rilis layanan enterprise AI untuk korporasi", 0.6),
            ("Persaingan dari Indosat semakin ketat", -0.3),
        ],
        "ASII.JK": [
            ("Penjualan mobil nasional naik 6% di Mei", 0.5),
            ("ASII umumkan kemitraan dengan BYD untuk EV", 0.7),
            ("Kenaikan harga nikel tekan margin ASII", -0.4),
            ("Laba ASII Q1 tumbuh 9% melampaui ekspektasi", 0.6),
        ],
        "BBRI.JK": [
            ("BBRI perluas KUR ke 500 ribu UMKM baru", 0.6),
            ("NPL BBRI naik ke 3.1%, pasar cemas", -0.6),
            ("Kredit mikro BBRI tumbuh 22% YoY", 0.5),
            ("Laba BBRI Q1 sedikit di bawah konsensus", -0.3),
        ],
        "GOTO.JK": [
            ("GOTO raih profitabilitas EBITDA adjusted pertama", 0.9),
            ("Persaingan ShopeeFood menekan margin Gofood", -0.5),
            ("GOTO umumkan program buyback saham", 0.6),
            ("Investor asing nett sell GOTO 3 hari berturut", -0.7),
        ],
    }
    pool = NEWS_POOL.get(ticker, [("Tidak ada berita tersedia", 0.0)])
    return random.choice(pool)


# ─── Rule-based Layer 1 ────────────────────────────────────────────────────────
def rule_deepseek(agent_name: str) -> dict:
    """RSI + Momentum: scan semua ticker, pilih sinyal terkuat."""
    ag         = sim.agents[agent_name]
    best       = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 40, "reason": "RSI semua saham netral"}
    best_score = 0

    for t in TICKERS:
        ph    = sim.price_history[t]
        price = sim.prices.get(t, 0)
        if not price or len(ph) < 2:
            continue
        rsi = calc_rsi(ph)
        mom = calc_momentum(ph)
        h   = ag["holdings"][t]

        if rsi < 35 and mom > 0:
            score = (35 - rsi) * 2 + mom
            if score > best_score:
                best_score = score
                best = {
                    "ticker": t, "action": "BUY",
                    "confidence": int(min(88, (35 - rsi) * 2.5 + 50)),
                    "reason": f"{t.replace('.JK','')} RSI oversold {rsi:.0f}, mom {mom:+.1f}%",
                }
        elif rsi > 65 and h["positions"] > 0:
            score = (rsi - 65) * 2
            if score > best_score:
                best_score = score
                best = {
                    "ticker": t, "action": "SELL",
                    "confidence": int(min(88, (rsi - 65) * 2.5 + 50)),
                    "reason": f"{t.replace('.JK','')} RSI overbought {rsi:.0f}",
                }
    return best


def rule_qwen(agent_name: str) -> dict:
    """MA Crossover: scan semua ticker, pilih crossover terkuat."""
    ag         = sim.agents[agent_name]
    best       = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "MA belum konfirmasi tren"}
    best_score = 0

    for t in TICKERS:
        ph    = sim.price_history[t]
        price = sim.prices.get(t, 0)
        if not price or len(ph) < 20:
            continue
        ma7  = calc_ma(ph, 7)
        ma20 = calc_ma(ph, 20)
        if ma7 is None or ma20 is None:
            continue
        h        = ag["holdings"][t]
        diff_pct = abs((ma7 - ma20) / ma20 * 100)

        if ma7 > ma20 and price > ma7:
            if diff_pct > best_score:
                best_score = diff_pct
                best = {
                    "ticker": t, "action": "BUY",
                    "confidence": int(min(88, 55 + diff_pct * 8)),
                    "reason": f"{t.replace('.JK','')} golden cross MA7>MA20",
                }
        elif ma7 < ma20 and price < ma7 and h["positions"] > 0:
            if diff_pct > best_score:
                best_score = diff_pct
                best = {
                    "ticker": t, "action": "SELL",
                    "confidence": int(min(88, 55 + diff_pct * 8)),
                    "reason": f"{t.replace('.JK','')} death cross MA7<MA20",
                }
    return best


# ─── Gemini API ────────────────────────────────────────────────────────────────
def build_prompt(agent_name: str, trigger: str = "") -> str:
    ag    = sim.agents[agent_name]
    lines = []
    for t in TICKERS:
        ph    = sim.price_history[t]
        price = sim.prices.get(t, 0)
        if not price or len(ph) < 2:
            continue
        rsi  = calc_rsi(ph)
        ma7  = calc_ma(ph, 7)
        ma20 = calc_ma(ph, 20)
        mom  = calc_momentum(ph)
        h    = ag["holdings"][t]
        pos  = f"{h['positions']//LOT_SIZE}lot" if h["positions"] > 0 else "0lot"
        ma7_s  = f"{ma7:.0f}"  if ma7  else "-"
        ma20_s = f"{ma20:.0f}" if ma20 else "-"
        news_s = f"{sim.news_sentiment[t]:+.1f}"
        lines.append(
            f"{t}|{price:.0f}|RSI:{rsi:.0f}|MA7:{ma7_s}|MA20:{ma20_s}"
            f"|mom:{mom:+.1f}%|news:{news_s}|pos:{pos}"
        )
    pf      = sim.portfolio_value(agent_name)
    pnl_pct = (pf - MODAL) / MODAL * 100
    kas_m   = f"{ag['cash']/1_000_000:.1f}M"
    summary = "\n".join(lines) + f"\nkas:{kas_m}|pnl:{pnl_pct:+.1f}%"
    if trigger:
        summary += f"\ntrigger:{trigger}"
    return summary


async def call_gemini(prompt: str) -> dict:
    if not GEMINI_API_KEY:
        return {"ticker": TICKERS[0], "action": "HOLD", "confidence": 0, "reason": "API key Gemini belum diset"}
    async with httpx.AsyncClient(timeout=25) as client:
        resp = await client.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "system_instruction": {"parts": [{"text": GEMINI_SYSTEM}]},
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": 80,
                    "temperature": 0.7,
                    "responseMimeType": "application/json",
                },
            },
        )
        resp.raise_for_status()
        raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(raw)


# ─── Eksekusi trade ────────────────────────────────────────────────────────────
async def execute_trade(agent_name: str, ticker: str, action: str, price: float):
    ag = sim.agents[agent_name]
    h  = ag["holdings"][ticker]

    if action == "BUY" and ag["cash"] > price * LOT_SIZE:
        lots = int(ag["cash"] * BUY_FRACTION / (price * LOT_SIZE))
        if lots < 1:
            return
        cost           = lots * LOT_SIZE * price
        ag["cash"]    -= cost
        total          = h["positions"] + lots * LOT_SIZE
        h["avg_price"] = (h["positions"] * h["avg_price"] + lots * LOT_SIZE * price) / total
        h["positions"] = total
        ag["trades"].append({
            "type": "BUY", "ticker": ticker, "price": price,
            "lots": lots, "time": datetime.now().isoformat(),
        })
        await db.save_transaction(agent_name, ticker, "BUY", price, lots)

    elif action == "SELL" and h["positions"] > 0:
        lots_sold   = h["positions"] // LOT_SIZE
        gain        = (price - h["avg_price"]) * h["positions"]
        ag["cash"] += h["positions"] * price
        ag["trades"].append({
            "type": "SELL", "ticker": ticker, "price": price,
            "lots": lots_sold, "gain": gain, "time": datetime.now().isoformat(),
        })
        await db.save_transaction(agent_name, ticker, "SELL", price, lots_sold, gain)
        h["positions"] = 0
        h["avg_price"] = 0.0


# ─── Deteksi trigger Gemini ───────────────────────────────────────────────────
def detect_trigger() -> list[str]:
    for t in TICKERS:
        ph    = sim.price_history[t]
        price = sim.prices.get(t, 0)
        if not price or len(ph) < 2:
            continue
        rsi          = calc_rsi(ph)
        ma20         = calc_ma(ph, 20)
        mom          = calc_momentum(ph)
        _, bbu, bbl  = calc_bollinger(ph)
        prev         = ph[-2]
        short        = t.replace(".JK", "")

        if rsi < 30:
            return [f"{short} RSI oversold {rsi:.0f}"]
        if rsi > 70:
            return [f"{short} RSI overbought {rsi:.0f}"]
        if ma20 and prev < ma20 <= price:
            return [f"{short} tembus MA20 ke atas"]
        if ma20 and prev > ma20 >= price:
            return [f"{short} tembus MA20 ke bawah"]
        if bbu and price > bbu:
            return [f"{short} Bollinger breakout atas"]
        if bbl and price < bbl:
            return [f"{short} Bollinger breakout bawah"]
        if abs(mom) > 2.0:
            return [f"{short} momentum ekstrem {mom:+.1f}%"]
        if abs(sim.news_sentiment[t]) > 0.6:
            return [f"{short} berita kuat {sim.news_sentiment[t]:+.1f}"]
    return []


# ─── Broadcast ────────────────────────────────────────────────────────────────
async def broadcast(payload: dict):
    dead = []
    for ws in sim.clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        sim.clients.remove(ws)


# ─── Loop utama simulasi ──────────────────────────────────────────────────────
async def trading_loop():
    print("[loop] Trading loop dimulai — mode multi-saham")
    while True:
        market = get_market_status()

        if not market["open"]:
            await broadcast({
                "type":      "market_closed",
                "reason":    market["reason"],
                "next_open": market["next_open"],
                "server_ts": market["server_ts"],
            })
            await asyncio.sleep(30)
            continue

        new_prices = await fetch_all_prices()
        if not new_prices:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        sim.prices.update(new_prices)
        for t, p in new_prices.items():
            sim.price_history[t].append(p)
        sim.tick_count += 1

        # Update berita berkala (RSS nyata, cache 5 menit)
        if sim.tick_count % NEWS_EVERY == 0:
            for t in TICKERS:
                sim.last_news[t], sim.news_sentiment[t] = await fetch_news_cached(t)

        # ── Layer 1: Rule-based (setiap LAYER1_EVERY tick) ──
        if sim.tick_count % LAYER1_EVERY == 0:
            for agent_name, rule_fn in [("deepseek", rule_deepseek), ("qwen", rule_qwen)]:
                result = rule_fn(agent_name)
                sim.agents[agent_name]["signal"]      = result["action"]
                sim.agents[agent_name]["confidence"]  = result["confidence"]
                sim.agents[agent_name]["reason"]      = result["reason"]
                sim.agents[agent_name]["last_ticker"] = result["ticker"]
                if result["action"] != "HOLD":
                    price = sim.prices.get(result["ticker"], 0)
                    if price:
                        await execute_trade(agent_name, result["ticker"], result["action"], price)

        # ── Layer 2: Gemini saat trigger ──
        triggers    = detect_trigger()
        ticks_since = sim.tick_count - sim.gemini_last_tick
        if triggers and ticks_since >= GEMINI_COOLDOWN:
            sim.gemini_last_tick = sim.tick_count
            sim.last_trigger     = " & ".join(triggers[:2])
            print(f"[trigger] tick={sim.tick_count} → {sim.last_trigger}")
            try:
                result = await call_gemini(build_prompt("gemini", sim.last_trigger))
                ticker = result.get("ticker", TICKERS[0])
                action = result.get("action", "HOLD")
                if ticker not in TICKERS:
                    ticker = TICKERS[0]
                sim.agents["gemini"]["signal"]      = action
                sim.agents["gemini"]["confidence"]  = result.get("confidence", 0)
                sim.agents["gemini"]["reason"]      = result.get("reason", "-")
                sim.agents["gemini"]["last_ticker"] = ticker
                if action != "HOLD" and sim.prices.get(ticker):
                    await execute_trade("gemini", ticker, action, sim.prices[ticker])
            except Exception as e:
                print(f"[AI:gemini] Error: {e}")
        elif not triggers:
            sim.last_trigger = ""

        # ── Susun payload ──
        payload = {
            "type":    "tick",
            "session": market.get("session", ""),
            "tick":    sim.tick_count,
            "prices":  {t: round(p, 2) for t, p in sim.prices.items() if p},
            "trigger": sim.last_trigger,
            "news":    {t: {"text": sim.last_news[t], "sentiment": round(sim.news_sentiment[t], 2)} for t in TICKERS},
            "agents": {
                name: {
                    "signal":      ag["signal"],
                    "confidence":  ag["confidence"],
                    "reason":      ag["reason"],
                    "last_ticker": ag["last_ticker"],
                    "cash":        round(ag["cash"]),
                    "holdings": {
                        t: {
                            "positions": h["positions"] // LOT_SIZE,
                            "avg_price": round(h["avg_price"], 2),
                            "value":     round(h["positions"] * sim.prices.get(t, 0)),
                        }
                        for t, h in ag["holdings"].items() if h["positions"] > 0
                    },
                    "portfolio": round(sim.portfolio_value(name)),
                    "pnl":       round(sim.portfolio_value(name) - MODAL),
                    "pnl_pct":   round((sim.portfolio_value(name) - MODAL) / MODAL * 100, 2),
                    "trades":    len(ag["trades"]),
                }
                for name, ag in sim.agents.items()
            },
        }

        await broadcast(payload)

        # ── Simpan ke DB secara berkala ──
        if sim.tick_count % PRICE_SAVE_EVERY == 0:
            await db.save_prices({t: round(p, 2) for t, p in sim.prices.items() if p})

        if sim.tick_count % SNAPSHOT_EVERY == 0:
            snapshots = [
                {
                    "name":      name,
                    "portfolio": round(sim.portfolio_value(name)),
                    "cash":      round(ag["cash"]),
                    "pnl":       round(sim.portfolio_value(name) - MODAL),
                    "pnl_pct":   round((sim.portfolio_value(name) - MODAL) / MODAL * 100, 2),
                }
                for name, ag in sim.agents.items()
            ]
            await db.save_portfolio_snapshots(snapshots)

        if sim.tick_count % RESULT_EVERY == 0:
            results = [
                {
                    "name":      name,
                    "portfolio": round(sim.portfolio_value(name)),
                    "pnl":       round(sim.portfolio_value(name) - MODAL),
                    "trades":    len(ag["trades"]),
                }
                for name, ag in sim.agents.items()
            ]
            await db.save_competition_results(results)

        await asyncio.sleep(POLL_INTERVAL)



# ════════════════════════════════════════════════════════════════════════════
# XAU/USD SIMULATOR
# ════════════════════════════════════════════════════════════════════════════
XAU_TICKER  = "GC=F"         # Gold Futures di yfinance
XAU_MODAL   = 10_000.0       # $10,000 modal awal per AI
XAU_BUY_PCT = 0.20           # beli 20% kas per BUY
XAU_MIN_OZ  = 0.1            # minimum 0.1 oz per transaksi

XAU_GEMINI_SYSTEM = (
    "Kamu adalah AI trader XAU/USD (emas). Analisis data teknikal dan berita global. "
    "Respond HANYA dengan JSON: "
    '{"action":"BUY"|"SELL"|"HOLD","confidence":0-100,"reason":"maks 20 kata"}'
)

XAU_NEWS_POOL = [
    ("Fed sinyal pemangkasan suku bunga, emas menguat", 0.8),
    ("Inflasi AS naik, dolar menguat, emas tertekan", -0.6),
    ("Ketegangan geopolitik dorong permintaan safe haven", 0.7),
    ("Data NFP AS kuat, dolar menguat, emas turun", -0.5),
    ("Bank sentral global tambah cadangan emas", 0.6),
    ("Yield obligasi AS naik, emas tertekan", -0.4),
    ("Ketidakpastian pasar dorong aksi beli emas", 0.5),
    ("Pelemahan dolar AS angkat harga emas spot", 0.6),
    ("Kekhawatiran resesi global dorong safe haven", 0.7),
    ("Data ekonomi AS solid, minat emas berkurang", -0.3),
]


class XauState:
    def __init__(self):
        self.price_history : list[float] = []
        self.price          = 0.0
        self.tick_count     = 0
        self.last_news      = "Menunggu berita..."
        self.news_sentiment = 0.0
        self.agents         = self._init_agents()
        self.clients        : list[WebSocket] = []
        self.gemini_last_tick = 0
        self.last_trigger   = ""

    def _init_agents(self):
        return {
            name: {
                "name":      name,
                "cash":      XAU_MODAL,
                "positions": 0.0,
                "avg_price": 0.0,
                "trades":    [],
                "signal":    "HOLD",
                "confidence":0,
                "reason":    "Menunggu data...",
            }
            for name in ("deepseek", "gemini", "qwen")
        }

    def portfolio_value(self, name: str) -> float:
        ag = self.agents[name]
        return ag["cash"] + ag["positions"] * self.price

    def reset(self):
        self.price_history  = []
        self.price          = 0.0
        self.tick_count     = 0
        self.gemini_last_tick = 0
        self.last_trigger   = ""
        self.agents         = self._init_agents()


xau = XauState()


def is_xau_market_open() -> bool:
    """XAU/USD tutup Sabtu 05:00 - Senin 05:00 WIB (approx weekend close)."""
    now = datetime.now(WIB)
    wd  = now.weekday()
    t   = now.time()
    if wd == 5: return False                           # Sabtu tutup
    if wd == 6 and t < time(5, 0): return False        # Minggu dini hari tutup
    if wd == 4 and t >= time(23, 0): return False      # Jumat malam tutup
    return True


def fetch_xau_price() -> Optional[float]:
    try:
        price = yf.Ticker(XAU_TICKER).fast_info.get("lastPrice")
        return round(float(price), 2) if price else None
    except Exception as e:
        print(f"[xau] Error fetch: {e}")
        return None


def fetch_xau_news() -> tuple[str, float]:
    import random
    return random.choice(XAU_NEWS_POOL)


def xau_rule_deepseek() -> dict:
    ph  = xau.price_history
    rsi = calc_rsi(ph)
    mom = calc_momentum(ph)
    ag  = xau.agents["deepseek"]
    if rsi < 35 and mom > 0:
        return {"action": "BUY",  "confidence": int(min(88, (35-rsi)*2.5+50)),
                "reason": f"XAU RSI oversold {rsi:.0f}, momentum positif {mom:+.1f}%"}
    if rsi > 65 and ag["positions"] > 0:
        return {"action": "SELL", "confidence": int(min(88, (rsi-65)*2.5+50)),
                "reason": f"XAU RSI overbought {rsi:.0f}, potensi koreksi"}
    return {"action": "HOLD", "confidence": 45, "reason": f"XAU RSI netral {rsi:.0f}"}


def xau_rule_qwen() -> dict:
    ph    = xau.price_history
    price = xau.price
    ma7   = calc_ma(ph, 7)
    ma20  = calc_ma(ph, 20)
    ag    = xau.agents["qwen"]
    if ma7 is None or ma20 is None:
        return {"action": "HOLD", "confidence": 30, "reason": "Data MA belum cukup"}
    diff  = abs((ma7 - ma20) / ma20 * 100)
    if ma7 > ma20 and price > ma7:
        return {"action": "BUY",  "confidence": int(min(88, 55+diff*8)),
                "reason": "XAU golden cross MA7>MA20, tren naik"}
    if ma7 < ma20 and price < ma7 and ag["positions"] > 0:
        return {"action": "SELL", "confidence": int(min(88, 55+diff*8)),
                "reason": "XAU death cross MA7<MA20, tren turun"}
    return {"action": "HOLD", "confidence": 40, "reason": "XAU MA belum konfirmasi tren"}


def build_xau_prompt(trigger: str = "") -> str:
    ag   = xau.agents["gemini"]
    ph   = xau.price_history
    rsi  = calc_rsi(ph)
    ma7  = calc_ma(ph, 7)
    ma20 = calc_ma(ph, 20)
    mom  = calc_momentum(ph)
    pos  = f"{ag['positions']:.1f}oz" if ag["positions"] > 0 else "0oz"
    pf   = xau.portfolio_value("gemini")
    pnl  = (pf - XAU_MODAL) / XAU_MODAL * 100
    line = (f"XAU/USD|{xau.price:.2f}|RSI:{rsi:.0f}"
            f"|MA7:{ma7:.1f if ma7 else '-'}|MA20:{ma20:.1f if ma20 else '-'}"
            f"|mom:{mom:+.1f}%|news:{xau.news_sentiment:+.1f}"
            f"|\"{xau.last_news[:40]}\"|pos:{pos}|cash:${ag['cash']:.0f}|pnl:{pnl:+.1f}%")
    return line + (f"|trigger:{trigger}" if trigger else "")


async def call_gemini_xau(prompt: str) -> dict:
    if not GEMINI_API_KEY:
        return {"action": "HOLD", "confidence": 0, "reason": "API key Gemini belum diset"}
    async with httpx.AsyncClient(timeout=25) as client:
        resp = await client.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "system_instruction": {"parts": [{"text": XAU_GEMINI_SYSTEM}]},
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 80, "temperature": 0.7,
                                     "responseMimeType": "application/json"},
            },
        )
        resp.raise_for_status()
        raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(raw)


def execute_xau_trade(agent_name: str, action: str, price: float):
    ag = xau.agents[agent_name]
    if action == "BUY" and ag["cash"] > price * XAU_MIN_OZ:
        oz   = round(ag["cash"] * XAU_BUY_PCT / price, 2)
        if oz < XAU_MIN_OZ:
            return
        cost          = oz * price
        ag["cash"]   -= cost
        total         = round(ag["positions"] + oz, 2)
        ag["avg_price"] = (ag["positions"] * ag["avg_price"] + oz * price) / total
        ag["positions"] = total
        ag["trades"].append({"type": "BUY", "price": price, "oz": oz,
                             "time": datetime.now().isoformat()})
    elif action == "SELL" and ag["positions"] > 0:
        gain          = (price - ag["avg_price"]) * ag["positions"]
        ag["cash"]   += ag["positions"] * price
        ag["trades"].append({"type": "SELL", "price": price, "oz": ag["positions"],
                             "gain": gain, "time": datetime.now().isoformat()})
        ag["positions"] = 0.0
        ag["avg_price"] = 0.0


async def broadcast_xau(payload: dict):
    dead = []
    for ws in xau.clients:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        xau.clients.remove(ws)


async def xau_trading_loop():
    print("[xau] XAU/USD trading loop dimulai")
    while True:
        if not is_xau_market_open():
            # Hitung next open
            now = datetime.now(WIB)
            wd  = now.weekday()
            if wd == 4:  # Jumat
                nxt = datetime.combine(now.date() + timedelta(days=3), time(5, 0), tzinfo=WIB)
            elif wd == 5:  # Sabtu
                nxt = datetime.combine(now.date() + timedelta(days=2), time(5, 0), tzinfo=WIB)
            else:
                nxt = datetime.combine(now.date() + timedelta(days=1), time(5, 0), tzinfo=WIB)
            await broadcast_xau({"type": "market_closed", "reason": "Akhir pekan (forex tutup)",
                                  "next_open": nxt.isoformat(), "server_ts": now.isoformat()})
            await asyncio.sleep(60)
            continue

        price = await asyncio.get_event_loop().run_in_executor(None, fetch_xau_price)
        if price is None:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        xau.price = price
        xau.price_history.append(price)
        xau.tick_count += 1

        if xau.tick_count % NEWS_EVERY == 0:
            xau.last_news, xau.news_sentiment = await fetch_news_cached("XAU")

        # Layer 1: Rule-based
        if xau.tick_count % LAYER1_EVERY == 0:
            for agent_name, rule_fn in [("deepseek", xau_rule_deepseek), ("qwen", xau_rule_qwen)]:
                result = rule_fn()
                xau.agents[agent_name]["signal"]     = result["action"]
                xau.agents[agent_name]["confidence"] = result["confidence"]
                xau.agents[agent_name]["reason"]     = result["reason"]
                if result["action"] != "HOLD":
                    execute_xau_trade(agent_name, result["action"], price)

        # Layer 2: Gemini on trigger
        triggers    = detect_trigger()  # reuse indikator teknikal
        ticks_since = xau.tick_count - xau.gemini_last_tick
        if xau.price_history and len(xau.price_history) >= 2:
            ph   = xau.price_history
            rsi  = calc_rsi(ph)
            mom  = calc_momentum(ph)
            trigs = []
            if rsi < 40: trigs.append(f"RSI oversold {rsi:.0f}")
            elif rsi > 60: trigs.append(f"RSI overbought {rsi:.0f}")
            if abs(mom) > 0.5: trigs.append(f"momentum {mom:+.1f}%")
            if abs(xau.news_sentiment) > 0.4: trigs.append(f"berita {xau.news_sentiment:+.1f}")

            if trigs and ticks_since >= GEMINI_COOLDOWN:
                xau.gemini_last_tick = xau.tick_count
                xau.last_trigger = " & ".join(trigs[:2])
                try:
                    result = await call_gemini_xau(build_xau_prompt(xau.last_trigger))
                    xau.agents["gemini"]["signal"]     = result.get("action", "HOLD")
                    xau.agents["gemini"]["confidence"] = result.get("confidence", 0)
                    xau.agents["gemini"]["reason"]     = result.get("reason", "-")
                    if result.get("action") != "HOLD":
                        execute_xau_trade("gemini", result.get("action", "HOLD"), price)
                except Exception as e:
                    print(f"[xau:gemini] Error: {e}")
            elif not trigs:
                xau.last_trigger = ""

        open_price = xau.price_history[0] if xau.price_history else price
        payload = {
            "type":      "tick",
            "tick":      xau.tick_count,
            "price":     round(price, 2),
            "open":      round(open_price, 2),
            "change":    round(price - open_price, 2),
            "change_pct":round((price - open_price) / open_price * 100, 2),
            "rsi":       round(calc_rsi(xau.price_history), 1),
            "ma7":       round(calc_ma(xau.price_history, 7) or 0, 2),
            "ma20":      round(calc_ma(xau.price_history, 20) or 0, 2),
            "news":      xau.last_news,
            "news_sentiment": round(xau.news_sentiment, 2),
            "trigger":   xau.last_trigger,
            "agents": {
                name: {
                    "signal":     ag["signal"],
                    "confidence": ag["confidence"],
                    "reason":     ag["reason"],
                    "cash":       round(ag["cash"], 2),
                    "positions":  round(ag["positions"], 2),
                    "avg_price":  round(ag["avg_price"], 2),
                    "portfolio":  round(xau.portfolio_value(name), 2),
                    "pnl":        round(xau.portfolio_value(name) - XAU_MODAL, 2),
                    "pnl_pct":    round((xau.portfolio_value(name) - XAU_MODAL) / XAU_MODAL * 100, 2),
                    "trades":     len(ag["trades"]),
                }
                for name, ag in xau.agents.items()
            },
        }
        await broadcast_xau(payload)
        await asyncio.sleep(POLL_INTERVAL)
# ─── FastAPI app ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    task1 = asyncio.create_task(trading_loop())
    task2 = asyncio.create_task(xau_trading_loop())
    yield
    task1.cancel()
    task2.cancel()


app = FastAPI(title="Z Trader", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return FileResponse("index.html")


@app.get("/status")
def status():
    return {"status": "ok", "ticks": sim.tick_count, "tickers": TICKERS}


@app.post("/reset")
def reset():
    sim.reset()
    return {"ok": True}


@app.get("/history/transactions")
async def history_transactions(agent: str = None, limit: int = 100):
    return await db.get_transactions(agent, limit)


@app.get("/history/portfolio")
async def history_portfolio(agent: str = None, limit: int = 200):
    return await db.get_portfolio_history(agent, limit)


@app.get("/history/prices/{ticker}")
async def history_prices(ticker: str, limit: int = 200):
    return await db.get_price_history(ticker, limit)


@app.get("/history/results")
async def history_results(limit: int = 50):
    return await db.get_competition_results(limit)


@app.get("/xauusd")
def xauusd_page():
    return FileResponse("xauusd.html")


@app.post("/xau/reset")
def xau_reset():
    xau.reset()
    return {"ok": True}


@app.websocket("/ws/xau")
async def xau_websocket(ws: WebSocket):
    await ws.accept()
    xau.clients.append(ws)
    print(f"[xau-ws] Client terhubung. Total: {len(xau.clients)}")
    try:
        while True:
            data = await ws.receive_text()
            msg  = json.loads(data)
            if msg.get("action") == "reset":
                xau.reset()
    except WebSocketDisconnect:
        xau.clients.remove(ws)
        print(f"[xau-ws] Client terputus. Total: {len(xau.clients)}")


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    sim.clients.append(ws)
    print(f"[ws] Client terhubung. Total: {len(sim.clients)}")
    try:
        while True:
            data = await ws.receive_text()
            msg  = json.loads(data)
            if msg.get("action") == "reset":
                sim.reset()
    except WebSocketDisconnect:
        sim.clients.remove(ws)
        print(f"[ws] Client terputus. Total: {len(sim.clients)}")