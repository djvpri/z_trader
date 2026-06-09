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

# ─── 45 Saham LQ45 IDX ──────────────────────────────────────────────────────
TICKERS = [
    "AALI.JK", "ADRO.JK", "AKRA.JK", "AMRT.JK", "ANTM.JK",
    "ASII.JK", "BBCA.JK", "BBNI.JK", "BBRI.JK", "BBTN.JK",
    "BMRI.JK", "BRIS.JK", "BUKA.JK", "CPIN.JK", "CTRA.JK",
    "EXCL.JK", "GGRM.JK", "GOTO.JK", "ICBP.JK", "INCO.JK",
    "INDF.JK", "INKP.JK", "INTP.JK", "ITMG.JK", "KLBF.JK",
    "MAPI.JK", "MBMA.JK", "MDKA.JK", "MEDC.JK", "MIKA.JK",
    "MNCN.JK", "PGAS.JK", "PGEO.JK", "PTBA.JK", "SMGR.JK",
    "TLKM.JK", "TOWR.JK", "TPIA.JK", "UNTR.JK", "UNVR.JK",
    "ACES.JK", "BRPT.JK", "EMTK.JK", "HRUM.JK", "BJTM.JK",
]
WIB = timezone(timedelta(hours=7))

# ─── Konfigurasi RSS ──────────────────────────────────────────────────────────
TICKER_QUERIES = {
    "IDX_GENERAL": "saham IDX IHSG bursa efek Indonesia terbaru",
    "XAU":         "harga emas XAU gold price",
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


async def fetch_news_cached(ticker: str = "IDX_GENERAL") -> tuple[str, float]:
    """Ambil berita IDX umum dari Google News RSS, cache 5 menit, fallback ke stub."""
    cache_key = "XAU" if ticker == "XAU" else "IDX_GENERAL"
    now_ts    = time_module.time()
    cached    = _news_cache.get(cache_key)
    if cached and (now_ts - cached[2]) < NEWS_CACHE_TTL:
        return cached[0], cached[1]

    query           = TICKER_QUERIES.get(cache_key, "saham IDX IHSG")
    headline, senti = await fetch_rss_news(query)

    if headline:
        _news_cache[cache_key] = (headline, senti, now_ts)
        print(f"[rss] {cache_key}: {headline[:60]}")
        return headline, senti

    return fetch_news("BBCA.JK")  # fallback ke stub


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
            for name in ("rsi_bot", "gemini", "ma_bot",
                             "bollinger", "macd", "mean_rev",
                             "breakout", "stochastic", "triple_ma", "roc")
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


def calc_ema(prices: list[float], n: int) -> Optional[float]:
    if len(prices) < n:
        return None
    k   = 2 / (n + 1)
    ema = sum(prices[:n]) / n
    for p in prices[n:]:
        ema = p * k + ema * (1 - k)
    return ema


def calc_stochastic(prices: list[float], n: int = 14) -> Optional[float]:
    if len(prices) < n:
        return None
    window = prices[-n:]
    lo, hi = min(window), max(window)
    return 50.0 if hi == lo else (prices[-1] - lo) / (hi - lo) * 100


def calc_roc(prices: list[float], n: int = 10) -> float:
    if len(prices) < n + 1:
        return 0.0
    return (prices[-1] - prices[-1 - n]) / prices[-1 - n] * 100


# ─── Fetch semua harga secara paralel ─────────────────────────────────────────
async def fetch_all_prices() -> dict[str, float]:
    """Fetch harga semua LQ45 dalam batch 15 agar tidak kena rate limit."""
    loop       = asyncio.get_event_loop()
    batch_size = 15
    results    = {}

    def get_price(ticker: str):
        try:
            price = yf.Ticker(ticker).fast_info.get("lastPrice")
            return ticker, float(price) if price else None
        except Exception as e:
            print(f"[price] Error {ticker}: {e}")
            return ticker, None

    for i in range(0, len(TICKERS), batch_size):
        batch        = TICKERS[i:i + batch_size]
        tasks        = [loop.run_in_executor(None, get_price, t) for t in batch]
        batch_result = await asyncio.gather(*tasks)
        results.update({t: p for t, p in batch_result if p is not None})
        if i + batch_size < len(TICKERS):
            await asyncio.sleep(0.3)

    return results


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


def rule_bollinger(agent_name: str) -> dict:
    """Bollinger Band: beli di lower band, jual di upper band."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "Bollinger netral"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 20: continue
        _, bbu, bbl = calc_bollinger(ph)
        if bbu is None: continue
        h = ag["holdings"][t]
        if price < bbl:
            score = (bbl - price) / bbl * 100
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "BUY", "confidence": int(min(88, 55 + score * 8)),
                        "reason": f"{t.replace('.JK','')} sentuh Bollinger lower band"}
        elif price > bbu and h["positions"] > 0:
            score = (price - bbu) / bbu * 100
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "SELL", "confidence": int(min(88, 55 + score * 8)),
                        "reason": f"{t.replace('.JK','')} sentuh Bollinger upper band"}
    return best


def rule_macd(agent_name: str) -> dict:
    """MACD: beli saat EMA12 > EMA26, jual saat EMA12 < EMA26 dengan posisi."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "MACD netral"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 26: continue
        e12 = calc_ema(ph, 12); e26 = calc_ema(ph, 26)
        if e12 is None or e26 is None: continue
        macd = e12 - e26
        h    = ag["holdings"][t]
        diff = abs(macd)
        if macd > 0 and h["positions"] == 0:
            if diff > best_score:
                best_score = diff
                best = {"ticker": t, "action": "BUY", "confidence": int(min(85, 55 + diff / price * 5000)),
                        "reason": f"{t.replace('.JK','')} MACD positif EMA12>EMA26"}
        elif macd < 0 and h["positions"] > 0:
            if diff > best_score:
                best_score = diff
                best = {"ticker": t, "action": "SELL", "confidence": int(min(85, 55 + diff / price * 5000)),
                        "reason": f"{t.replace('.JK','')} MACD negatif EMA12<EMA26"}
    return best


def rule_mean_rev(agent_name: str) -> dict:
    """Mean Reversion: beli saat harga jauh di bawah MA20, jual saat kembali ke MA20."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "Harga dekat MA20"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 20: continue
        ma20 = calc_ma(ph, 20)
        if ma20 is None: continue
        dev  = (price - ma20) / ma20 * 100
        h    = ag["holdings"][t]
        if dev < -2.5:
            score = abs(dev)
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "BUY", "confidence": int(min(88, 55 + score * 3)),
                        "reason": f"{t.replace('.JK','')} {abs(dev):.1f}% di bawah MA20, revert peluang"}
        elif dev > 1.0 and h["positions"] > 0:
            score = abs(dev)
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "SELL", "confidence": int(min(85, 50 + score * 3)),
                        "reason": f"{t.replace('.JK','')} harga kembali ke MA20"}
    return best


def rule_breakout(agent_name: str) -> dict:
    """Breakout: beli saat harga tembus high 20 periode, jual saat tembus low."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "Belum breakout"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 22: continue
        window  = ph[-21:-1]
        hi20    = max(window); lo20 = min(window)
        h       = ag["holdings"][t]
        if price > hi20:
            score = (price - hi20) / hi20 * 100
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "BUY", "confidence": int(min(88, 60 + score * 5)),
                        "reason": f"{t.replace('.JK','')} breakout atas high 20 periode"}
        elif price < lo20 and h["positions"] > 0:
            score = (lo20 - price) / lo20 * 100
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "SELL", "confidence": int(min(88, 60 + score * 5)),
                        "reason": f"{t.replace('.JK','')} breakdown bawah low 20 periode"}
    return best


def rule_stochastic(agent_name: str) -> dict:
    """Stochastic: beli saat %K < 20 (oversold), jual saat %K > 80 (overbought)."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "Stochastic netral"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 14: continue
        k = calc_stochastic(ph)
        if k is None: continue
        h = ag["holdings"][t]
        if k < 20:
            score = 20 - k
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "BUY", "confidence": int(min(88, 55 + score * 1.5)),
                        "reason": f"{t.replace('.JK','')} Stochastic oversold %K={k:.0f}"}
        elif k > 80 and h["positions"] > 0:
            score = k - 80
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "SELL", "confidence": int(min(88, 55 + score * 1.5)),
                        "reason": f"{t.replace('.JK','')} Stochastic overbought %K={k:.0f}"}
    return best


def rule_triple_ma(agent_name: str) -> dict:
    """Triple MA: beli saat MA5>MA10>MA20 (bullish), jual saat MA5<MA10<MA20 (bearish)."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "MA belum alignment"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 20: continue
        ma5  = calc_ma(ph, 5); ma10 = calc_ma(ph, 10); ma20 = calc_ma(ph, 20)
        if None in (ma5, ma10, ma20): continue
        h = ag["holdings"][t]
        if ma5 > ma10 > ma20:
            score = (ma5 - ma20) / ma20 * 100
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "BUY", "confidence": int(min(88, 55 + score * 5)),
                        "reason": f"{t.replace('.JK','')} MA5>MA10>MA20 bullish alignment"}
        elif ma5 < ma10 < ma20 and h["positions"] > 0:
            score = (ma20 - ma5) / ma20 * 100
            if score > best_score:
                best_score = score
                best = {"ticker": t, "action": "SELL", "confidence": int(min(88, 55 + score * 5)),
                        "reason": f"{t.replace('.JK','')} MA5<MA10<MA20 bearish alignment"}
    return best


def rule_roc(agent_name: str) -> dict:
    """Rate of Change: beli saat ROC positif kuat, jual saat ROC negatif dengan posisi."""
    ag = sim.agents[agent_name]
    best, best_score = {"ticker": TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "ROC lemah"}, 0
    for t in TICKERS:
        ph = sim.price_history[t]; price = sim.prices.get(t, 0)
        if not price or len(ph) < 11: continue
        roc = calc_roc(ph, 10)
        h   = ag["holdings"][t]
        if roc > 1.5:
            if roc > best_score:
                best_score = roc
                best = {"ticker": t, "action": "BUY", "confidence": int(min(88, 55 + roc * 5)),
                        "reason": f"{t.replace('.JK','')} ROC+{roc:.1f}% momentum kuat"}
        elif roc < -1.5 and h["positions"] > 0:
            if abs(roc) > best_score:
                best_score = abs(roc)
                best = {"ticker": t, "action": "SELL", "confidence": int(min(88, 55 + abs(roc) * 5)),
                        "reason": f"{t.replace('.JK','')} ROC{roc:.1f}% momentum negatif"}
    return best


RULE_BOTS = {
    "rsi_bot":   rule_deepseek,
    "ma_bot":       rule_qwen,
    "bollinger":  rule_bollinger,
    "macd":       rule_macd,
    "mean_rev":   rule_mean_rev,
    "breakout":   rule_breakout,
    "stochastic": rule_stochastic,
    "triple_ma":  rule_triple_ma,
    "roc":        rule_roc,
}


# ─── Gemini API ────────────────────────────────────────────────────────────────
def build_prompt(agent_name: str, trigger: str = "") -> str:
    """Kirim hanya top-10 sinyal terkuat + saham yg dipegang ke Gemini."""
    ag = sim.agents[agent_name]

    # Skor tiap ticker berdasarkan kekuatan sinyal
    scored = []
    for t in TICKERS:
        ph    = sim.price_history[t]
        price = sim.prices.get(t, 0)
        if not price or len(ph) < 2:
            continue
        rsi   = calc_rsi(ph)
        mom   = calc_momentum(ph)
        held  = ag["holdings"][t]["positions"] > 0
        score = abs(rsi - 50) + abs(mom) * 5 + (20 if held else 0)
        scored.append((score, t))

    # Top-10 + saham yang dipegang (agar tidak tiba-tiba di-ignore)
    top = {t for _, t in sorted(scored, reverse=True)[:10]}
    top |= {t for t in TICKERS if ag["holdings"][t]["positions"] > 0}

    lines = []
    for t in sorted(top):
        ph     = sim.price_history[t]
        price  = sim.prices.get(t, 0)
        if not price or len(ph) < 2:
            continue
        rsi    = calc_rsi(ph)
        ma7    = calc_ma(ph, 7)
        ma20   = calc_ma(ph, 20)
        mom    = calc_momentum(ph)
        h      = ag["holdings"][t]
        pos    = f"{h['positions']//LOT_SIZE}lot" if h["positions"] > 0 else "0lot"
        lines.append(
            f"{t}|{price:.0f}|RSI:{rsi:.0f}"
            f"|MA7:{f'{ma7:.0f}' if ma7 else '-'}"
            f"|MA20:{f'{ma20:.0f}' if ma20 else '-'}"
            f"|mom:{mom:+.1f}%|pos:{pos}"
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

        # Update berita berkala — 1 query umum IDX untuk semua saham
        if sim.tick_count % NEWS_EVERY == 0:
            headline, senti = await fetch_news_cached("IDX_GENERAL")
            for t in TICKERS:
                sim.last_news[t]      = headline
                sim.news_sentiment[t] = senti

        # ── Layer 1: Rule-based (setiap LAYER1_EVERY tick) ──
        if sim.tick_count % LAYER1_EVERY == 0:
            for agent_name, rule_fn in RULE_BOTS.items():
                result = rule_fn(agent_name)
                sim.agents[agent_name]["signal"]      = result["action"]
                sim.agents[agent_name]["confidence"]  = result["confidence"]
                sim.agents[agent_name]["reason"]      = result["reason"]
                sim.agents[agent_name]["last_ticker"] = result["ticker"]
                if result["action"] != "HOLD":
                    price = sim.prices.get(result["ticker"], 0)
                    if price:
                        await execute_trade(agent_name, result["ticker"], result["action"], price)

        # ── Layer 2: Gemini — trigger event ATAU review berkala setiap ~90 detik ──
        triggers    = detect_trigger()
        ticks_since = sim.tick_count - sim.gemini_last_tick
        force_call  = ticks_since >= 18   # review berkala setiap 18 tick = ~90 detik
        if (triggers or force_call) and ticks_since >= GEMINI_COOLDOWN:
            sim.gemini_last_tick = sim.tick_count
            sim.last_trigger     = " & ".join(triggers[:2]) if triggers else ""
            label = sim.last_trigger if sim.last_trigger else "analisis berkala"
            print(f"[gemini-idx] tick={sim.tick_count} → {label}")
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
            # Simpan holdings detail untuk restore saat restart
            holdings_data = [
                {
                    "name":     name,
                    "cash":     float(ag["cash"]),
                    "holdings": {
                        t: {"positions": float(h["positions"]), "avg_price": float(h["avg_price"])}
                        for t, h in ag["holdings"].items() if h["positions"] > 0
                    }
                }
                for name, ag in sim.agents.items()
            ]
            await db.save_holdings_snapshot("idx", holdings_data)

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
# GLOBAL MARKETS SIMULATOR (Multi-Pair: Commodities, Crypto, Indices, Forex)
# ════════════════════════════════════════════════════════════════════════════
GLOBAL_PAIRS = {
    "GC=F":     {"name": "Gold",      "short": "XAU"},
    "SI=F":     {"name": "Silver",    "short": "XAG"},
    "CL=F":     {"name": "Crude Oil", "short": "OIL"},
    "NG=F":     {"name": "Nat Gas",   "short": "GAS"},
    "BTC-USD":  {"name": "Bitcoin",   "short": "BTC"},
    "ES=F":     {"name": "S&P 500",   "short": "SPX"},
    "NQ=F":     {"name": "Nasdaq",    "short": "NDX"},
    "EURUSD=X": {"name": "EUR/USD",   "short": "EUR"},
    "GBPUSD=X": {"name": "GBP/USD",   "short": "GBP"},
    "USDJPY=X": {"name": "USD/JPY",   "short": "JPY"},
    "AUDUSD=X": {"name": "AUD/USD",   "short": "AUD"},
}
GLOBAL_TICKERS = list(GLOBAL_PAIRS.keys())
GLOBAL_MODAL   = 10_000.0
GLOBAL_BUY_PCT = 0.20
FOREX_TICKERS  = [t for t in GLOBAL_TICKERS if "=X" in t]

GLOBAL_GEMINI_SYSTEM = (
    "Kamu adalah AI trader global markets (komoditas, crypto, indeks, forex). "
    "Pilih pair terbaik berdasarkan data teknikal. "
    "Gunakan kode pendek untuk field ticker: XAU=Gold, XAG=Silver, OIL=CrudeOil, GAS=NatGas, "
    "BTC=Bitcoin, SPX=SP500, NDX=Nasdaq, EUR=EURUSD, GBP=GBPUSD, JPY=USDJPY, AUD=AUDUSD. "
    "Respond HANYA dengan JSON: "
    '{"ticker":"KODE_PENDEK","action":"BUY"|"SELL"|"HOLD","confidence":0-100,"reason":"maks 20 kata"}'
)

GLOBAL_NEWS_POOL = [
    ("Fed pertahankan suku bunga, dolar melemah, aset berisiko naik", 0.6),
    ("Inflasi AS melebihi ekspektasi, pasar global tertekan", -0.6),
    ("Data NFP kuat dorong dolar AS menguat, emas turun", -0.4),
    ("Ketegangan geopolitik picu aksi beli safe haven", 0.7),
    ("Bank sentral Eropa naikkan suku bunga, EUR menguat", 0.5),
    ("China rilis data PMI lemah, komoditas tertekan", -0.5),
    ("Bitcoin tembus level resistensi kunci, sentimen bullish", 0.7),
    ("Kekhawatiran resesi dorong rotasi ke obligasi dari saham", -0.3),
    ("OPEC+ pangkas produksi, harga minyak melambung", 0.8),
    ("Data PDB AS lebih baik dari perkiraan, risk-on rally", 0.6),
]


class GlobalState:
    def __init__(self):
        self.prices         : dict[str, float]       = {t: 0.0 for t in GLOBAL_TICKERS}
        self.price_history  : dict[str, list[float]] = {t: []  for t in GLOBAL_TICKERS}
        self.tick_count     = 0
        self.last_news      = "Menunggu berita..."
        self.news_sentiment = 0.0
        self.agents         = self._init_agents()
        self.clients        : list[WebSocket] = []
        self.gemini_last_tick = 0
        self.last_trigger     = ""

    def _init_agents(self):
        return {
            name: {
                "name":        name,
                "cash":        GLOBAL_MODAL,
                "holdings":    {t: {"positions": 0.0, "avg_price": 0.0} for t in GLOBAL_TICKERS},
                "trades":      [],
                "signal":      "HOLD",
                "confidence":  0,
                "reason":      "Menunggu data...",
                "last_ticker": GLOBAL_TICKERS[0],
            }
            for name in ("rsi_bot", "gemini", "ma_bot", "bollinger", "macd",
                        "mean_rev", "breakout", "stochastic", "triple_ma", "roc")
        }

    def portfolio_value(self, name: str) -> float:
        ag = self.agents[name]
        return ag["cash"] + sum(
            h["positions"] * self.prices.get(t, 0)
            for t, h in ag["holdings"].items()
        )

    def reset(self):
        self.prices        = {t: 0.0 for t in GLOBAL_TICKERS}
        self.price_history = {t: []  for t in GLOBAL_TICKERS}
        self.tick_count    = 0
        self.gemini_last_tick = 0
        self.last_trigger  = ""
        self.agents        = self._init_agents()


glob = GlobalState()


def is_global_market_open() -> bool:
    """Tutup Sabtu seharian dan Minggu sebelum 22:00 WIB."""
    now = datetime.now(WIB)
    wd  = now.weekday()
    t   = now.time()
    if wd == 5: return False
    if wd == 6 and t < time(22, 0): return False
    return True


async def fetch_global_prices() -> dict[str, float]:
    """Fetch semua pair: yfinance dulu, Frankfurter fallback untuk forex."""
    loop    = asyncio.get_event_loop()
    results = {}

    def _get(ticker: str):
        try:
            p = yf.Ticker(ticker).fast_info.get("lastPrice")
            return ticker, float(p) if p else None
        except:
            return ticker, None

    tasks = [loop.run_in_executor(None, _get, t) for t in GLOBAL_TICKERS]
    for t, p in await asyncio.gather(*tasks):
        if p:
            results[t] = p

    # Frankfurter fallback untuk forex yang gagal
    failed_forex = [t for t in FOREX_TICKERS if t not in results]
    if failed_forex:
        try:
            currencies = set()
            for pair in failed_forex:
                currencies.add(pair[:3])
                currencies.add(pair[3:6])
            currencies.discard("USD")
            url  = f"https://api.frankfurter.app/latest?from=USD&to={','.join(currencies)}"
            async with httpx.AsyncClient(timeout=8) as client:
                r  = await client.get(url)
                fx = r.json().get("rates", {})
                fx["USD"] = 1.0
            for pair in failed_forex:
                base, quote = pair[:3], pair[3:6]
                if base in fx and quote in fx:
                    results[pair] = round(fx[quote] / fx[base], 6)
        except Exception as e:
            print(f"[global] Frankfurter fallback error: {e}")

    return results


def fetch_global_news() -> tuple[str, float]:
    import random
    return random.choice(GLOBAL_NEWS_POOL)


# ─── Rule-based Layer 1 (scan semua GLOBAL_TICKERS) ──────────────────────────
def _global_rule_template(agent_name: str, strategy_fn) -> dict:
    """Helper: scan semua pair, kembalikan sinyal terkuat."""
    ag         = glob.agents[agent_name]
    best       = {"ticker": GLOBAL_TICKERS[0], "action": "HOLD", "confidence": 35, "reason": "Belum ada sinyal"}
    best_score = 0
    for t in GLOBAL_TICKERS:
        ph    = glob.price_history[t]
        price = glob.prices.get(t, 0)
        if not price or len(ph) < 2:
            continue
        result, score = strategy_fn(t, ph, price, ag["holdings"][t])
        if score > best_score:
            best_score = score
            best = result

    # Fallback: jika tidak ada sinyal DAN agent belum punya posisi sama sekali,
    # beli pair dengan ROC positif terbaik setelah 15 tick pertama
    if best["action"] == "HOLD" and glob.tick_count >= 15:
        no_positions = all(h["positions"] == 0 for h in ag["holdings"].values())
        if no_positions:
            best_roc, best_t = 0.0, None
            for t in GLOBAL_TICKERS:
                ph = glob.price_history[t]
                if len(ph) < 6: continue
                roc = calc_roc(ph, 5)
                if roc > best_roc:
                    best_roc = roc
                    best_t   = t
            if best_t and best_roc > 0:
                short = GLOBAL_PAIRS[best_t]["short"]
                best  = {"ticker": best_t, "action": "BUY", "confidence": 52,
                         "reason": f"Posisi awal: {short} ROC+{best_roc:.2f}%"}

    return best


def glob_rule_deepseek(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        rsi = calc_rsi(ph); mom = calc_momentum(ph)
        short = GLOBAL_PAIRS[t]["short"]
        if rsi < 42 and mom >= 0:
            s = (42-rsi)*2 + abs(mom) + 0.1
            return {"ticker":t,"action":"BUY","confidence":int(min(88,(42-rsi)*2.5+50)),"reason":f"{short} RSI oversold {rsi:.0f} mom{mom:+.1f}%"}, s
        if rsi > 58 and h["positions"] > 0:
            s = (rsi-58)*2
            return {"ticker":t,"action":"SELL","confidence":int(min(88,(rsi-58)*2.5+50)),"reason":f"{short} RSI overbought {rsi:.0f}"}, s
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} RSI netral {rsi:.0f}"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_qwen(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        ma7 = calc_ma(ph,7); ma20 = calc_ma(ph,20)
        short = GLOBAL_PAIRS[t]["short"]
        if ma7 is None or ma20 is None: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"MA belum siap"}, 0
        diff = abs((ma7-ma20)/ma20*100)
        if ma7>ma20 and price>ma7: return {"ticker":t,"action":"BUY","confidence":int(min(88,55+diff*8)),"reason":f"{short} golden cross MA7>MA20"}, diff
        if ma7<ma20 and price<ma7 and h["positions"]>0: return {"ticker":t,"action":"SELL","confidence":int(min(88,55+diff*8)),"reason":f"{short} death cross MA7<MA20"}, diff
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} MA netral"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_bollinger(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<20: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data belum cukup"}, 0
        _, bbu, bbl = calc_bollinger(ph)
        short = GLOBAL_PAIRS[t]["short"]
        if bbu is None: return {"ticker":t,"action":"HOLD","confidence":35,"reason":"Bollinger belum siap"}, 0
        if price<bbl: s=(bbl-price)/bbl*100; return {"ticker":t,"action":"BUY","confidence":int(min(88,55+s*8)),"reason":f"{short} sentuh lower band"}, s
        if price>bbu and h["positions"]>0: s=(price-bbu)/bbu*100; return {"ticker":t,"action":"SELL","confidence":int(min(88,55+s*8)),"reason":f"{short} sentuh upper band"}, s
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} dalam band"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_macd(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<26: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data MACD belum cukup"}, 0
        e12=calc_ema(ph,12); e26=calc_ema(ph,26)
        short = GLOBAL_PAIRS[t]["short"]
        if e12 is None or e26 is None: return {"ticker":t,"action":"HOLD","confidence":35,"reason":"MACD belum siap"}, 0
        macd=e12-e26; diff=abs(macd)
        if macd>0 and h["positions"]==0: return {"ticker":t,"action":"BUY","confidence":int(min(85,55+diff/price*5000)),"reason":f"{short} MACD positif EMA12>EMA26"}, diff
        if macd<0 and h["positions"]>0: return {"ticker":t,"action":"SELL","confidence":int(min(85,55+diff/price*5000)),"reason":f"{short} MACD negatif EMA12<EMA26"}, diff
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} MACD netral"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_mean_rev(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<20: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data belum cukup"}, 0
        ma20=calc_ma(ph,20); short=GLOBAL_PAIRS[t]["short"]
        if ma20 is None: return {"ticker":t,"action":"HOLD","confidence":35,"reason":"MA20 belum siap"}, 0
        dev=(price-ma20)/ma20*100
        if dev<-2.0: return {"ticker":t,"action":"BUY","confidence":int(min(88,55+abs(dev)*3)),"reason":f"{short} {abs(dev):.1f}% di bawah MA20"}, abs(dev)
        if dev>1.5 and h["positions"]>0: return {"ticker":t,"action":"SELL","confidence":int(min(85,50+abs(dev)*3)),"reason":f"{short} mean reversion +{dev:.1f}%"}, abs(dev)
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} dekat MA20"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_breakout(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<22: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data belum cukup"}, 0
        window=ph[-21:-1]; hi20=max(window); lo20=min(window); short=GLOBAL_PAIRS[t]["short"]
        if price>hi20: s=(price-hi20)/hi20*100; return {"ticker":t,"action":"BUY","confidence":int(min(88,60+s*5)),"reason":f"{short} breakout high 20 periode"}, s
        if price<lo20 and h["positions"]>0: s=(lo20-price)/lo20*100; return {"ticker":t,"action":"SELL","confidence":int(min(88,60+s*5)),"reason":f"{short} breakdown low 20 periode"}, s
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} belum breakout"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_stochastic(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<14: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data belum cukup"}, 0
        k=calc_stochastic(ph); short=GLOBAL_PAIRS[t]["short"]
        if k is None: return {"ticker":t,"action":"HOLD","confidence":35,"reason":"Stoch belum siap"}, 0
        if k<20: return {"ticker":t,"action":"BUY","confidence":int(min(88,55+(20-k)*1.5)),"reason":f"{short} Stoch oversold %K={k:.0f}"}, 20-k
        if k>80 and h["positions"]>0: return {"ticker":t,"action":"SELL","confidence":int(min(88,55+(k-80)*1.5)),"reason":f"{short} Stoch overbought %K={k:.0f}"}, k-80
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} Stoch netral %K={k:.0f}"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_triple_ma(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<20: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data belum cukup"}, 0
        ma5=calc_ma(ph,5); ma10=calc_ma(ph,10); ma20=calc_ma(ph,20); short=GLOBAL_PAIRS[t]["short"]
        if None in (ma5,ma10,ma20): return {"ticker":t,"action":"HOLD","confidence":35,"reason":"Triple MA belum siap"}, 0
        if ma5>ma10>ma20: s=(ma5-ma20)/ma20*100; return {"ticker":t,"action":"BUY","confidence":int(min(88,55+s*5)),"reason":f"{short} MA5>MA10>MA20 bullish"}, s
        if ma5<ma10<ma20 and h["positions"]>0: s=(ma20-ma5)/ma20*100; return {"ticker":t,"action":"SELL","confidence":int(min(88,55+s*5)),"reason":f"{short} MA5<MA10<MA20 bearish"}, s
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} MA belum alignment"}, 0
    return _global_rule_template(agent_name, fn)


def glob_rule_roc(agent_name: str) -> dict:
    def fn(t, ph, price, h):
        if len(ph)<11: return {"ticker":t,"action":"HOLD","confidence":30,"reason":"Data belum cukup"}, 0
        roc=calc_roc(ph,10); short=GLOBAL_PAIRS[t]["short"]
        if roc>0.4: return {"ticker":t,"action":"BUY","confidence":int(min(88,55+roc*5)),"reason":f"{short} ROC+{roc:.1f}% momentum kuat"}, roc
        if roc<-1.5 and h["positions"]>0: return {"ticker":t,"action":"SELL","confidence":int(min(88,55+abs(roc)*5)),"reason":f"{short} ROC{roc:.1f}% momentum turun"}, abs(roc)
        return {"ticker":t,"action":"HOLD","confidence":35,"reason":f"{short} ROC lemah {roc:+.1f}%"}, 0
    return _global_rule_template(agent_name, fn)


GLOBAL_RULE_BOTS = {
    "rsi_bot":   glob_rule_deepseek,
    "ma_bot":       glob_rule_qwen,
    "bollinger":  glob_rule_bollinger,
    "macd":       glob_rule_macd,
    "mean_rev":   glob_rule_mean_rev,
    "breakout":   glob_rule_breakout,
    "stochastic": glob_rule_stochastic,
    "triple_ma":  glob_rule_triple_ma,
    "roc":        glob_rule_roc,
}


def build_global_prompt(trigger: str = "") -> str:
    """Kirim top-10 sinyal terkuat ke Gemini."""
    ag     = glob.agents["gemini"]
    scored = []
    for t in GLOBAL_TICKERS:
        ph    = glob.price_history[t]
        price = glob.prices.get(t, 0)
        if not price or len(ph) < 2: continue
        rsi   = calc_rsi(ph)
        mom   = calc_momentum(ph)
        held  = ag["holdings"][t]["positions"] > 0
        score = abs(rsi-50) + abs(mom)*5 + (20 if held else 0)
        scored.append((score, t))

    top   = {t for _, t in sorted(scored, reverse=True)[:10]}
    top  |= {t for t in GLOBAL_TICKERS if ag["holdings"][t]["positions"] > 0}
    lines = []
    for t in sorted(top):
        ph    = glob.price_history[t]
        price = glob.prices.get(t, 0)
        if not price or len(ph) < 2: continue
        rsi   = calc_rsi(ph); ma7=calc_ma(ph,7); ma20=calc_ma(ph,20); mom=calc_momentum(ph)
        h     = ag["holdings"][t]
        pos   = f"{h['positions']:.4g}" if h["positions"] > 0 else "0"
        short = GLOBAL_PAIRS[t]["short"]
        lines.append(f"{short}({t})|{price:.4g}|RSI:{rsi:.0f}|MA7:{f'{ma7:.4g}' if ma7 else '-'}|MA20:{f'{ma20:.4g}' if ma20 else '-'}|mom:{mom:+.1f}%|pos:{pos}")

    pf      = glob.portfolio_value("gemini")
    pnl_pct = (pf - GLOBAL_MODAL) / GLOBAL_MODAL * 100
    kas_m   = f"${ag['cash']:.0f}"
    summary = "\n".join(lines) + f"\n{kas_m}|pnl:{pnl_pct:+.1f}%"
    if trigger: summary += f"\ntrigger:{trigger}"
    return summary


async def call_gemini_global(prompt: str) -> dict:
    if not GEMINI_API_KEY:
        return {"ticker": GLOBAL_TICKERS[0], "action": "HOLD", "confidence": 0, "reason": "API key Gemini belum diset"}
    async with httpx.AsyncClient(timeout=25) as client:
        resp = await client.post(
            f"{GEMINI_URL}?key={GEMINI_API_KEY}",
            headers={"Content-Type": "application/json"},
            json={
                "system_instruction": {"parts": [{"text": GLOBAL_GEMINI_SYSTEM}]},
                "contents": [{"parts": [{"text": prompt}]}],
                "generationConfig": {"maxOutputTokens": 80, "temperature": 0.7,
                                     "responseMimeType": "application/json"},
            },
        )
        resp.raise_for_status()
        raw = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
        return json.loads(raw)


def execute_global_trade(agent_name: str, ticker: str, action: str, price: float):
    ag = glob.agents[agent_name]
    h  = ag["holdings"][ticker]
    if action == "BUY" and ag["cash"] > price * 0.001:
        spend       = ag["cash"] * GLOBAL_BUY_PCT
        units       = round(spend / price, 6)
        if units <= 0: return
        ag["cash"] -= spend
        total        = round(h["positions"] + units, 6)
        h["avg_price"] = (h["positions"]*h["avg_price"] + units*price) / total
        h["positions"] = total
        ag["trades"].append({"type":"BUY","ticker":ticker,"price":price,"units":units,"time":datetime.now().isoformat()})
        asyncio.create_task(db.save_transaction(agent_name, ticker, "BUY", price, units, 0.0, "global"))
    elif action == "SELL" and h["positions"] > 0:
        gain        = (price - h["avg_price"]) * h["positions"]
        ag["cash"] += h["positions"] * price
        ag["trades"].append({"type":"SELL","ticker":ticker,"price":price,"units":h["positions"],"gain":gain,"time":datetime.now().isoformat()})
        asyncio.create_task(db.save_transaction(agent_name, ticker, "SELL", price, h["positions"], gain, "global"))
        h["positions"] = 0.0
        h["avg_price"] = 0.0


def detect_global_trigger() -> list[str]:
    for t in GLOBAL_TICKERS:
        ph    = glob.price_history[t]
        price = glob.prices.get(t, 0)
        if not price or len(ph) < 2: continue
        rsi  = calc_rsi(ph); mom = calc_momentum(ph)
        _, bbu, bbl = calc_bollinger(ph)
        prev = ph[-2]; short = GLOBAL_PAIRS[t]["short"]
        ma20 = calc_ma(ph, 20)
        if rsi < 32: return [f"{short} RSI oversold {rsi:.0f}"]
        if rsi > 68: return [f"{short} RSI overbought {rsi:.0f}"]
        if abs(mom) > 1.0: return [f"{short} momentum {mom:+.1f}%"]
        if bbu and price > bbu: return [f"{short} Bollinger breakout atas"]
        if bbl and price < bbl: return [f"{short} Bollinger breakout bawah"]
        if ma20 and prev < ma20 <= price: return [f"{short} tembus MA20 ke atas"]
        if ma20 and prev > ma20 >= price: return [f"{short} tembus MA20 ke bawah"]
        if abs(glob.news_sentiment) > 0.5: return [f"{short} berita kuat {glob.news_sentiment:+.1f}"]
    return []


async def broadcast_global(payload: dict):
    dead = []
    for ws in glob.clients:
        try: await ws.send_json(payload)
        except: dead.append(ws)
    for ws in dead: glob.clients.remove(ws)


async def global_trading_loop():
    print("[global] Global Markets trading loop dimulai")
    while True:
        if not is_global_market_open():
            now = datetime.now(WIB)
            wd  = now.weekday()
            nxt = datetime.combine(
                now.date() + timedelta(days=(6-wd) % 7 + 1),
                time(22, 0), tzinfo=WIB
            ) if wd == 6 else datetime.combine(
                now.date() + timedelta(days=2 if wd==5 else 1),
                time(22, 0), tzinfo=WIB
            )
            await broadcast_global({"type":"market_closed","reason":"Akhir pekan (global tutup)",
                                     "next_open":nxt.isoformat(),"server_ts":now.isoformat()})
            await asyncio.sleep(60)
            continue

        new_prices = await fetch_global_prices()
        if not new_prices:
            await asyncio.sleep(POLL_INTERVAL)
            continue

        glob.prices.update(new_prices)
        for t, p in new_prices.items():
            glob.price_history[t].append(p)
        glob.tick_count += 1

        if glob.tick_count % NEWS_EVERY == 0:
            glob.last_news, glob.news_sentiment = fetch_global_news()

        # Layer 1
        if glob.tick_count % LAYER1_EVERY == 0:
            for agent_name, rule_fn in GLOBAL_RULE_BOTS.items():
                result = rule_fn(agent_name)
                glob.agents[agent_name]["signal"]      = result["action"]
                glob.agents[agent_name]["confidence"]  = result["confidence"]
                glob.agents[agent_name]["reason"]      = result["reason"]
                glob.agents[agent_name]["last_ticker"] = result["ticker"]
                if result["action"] != "HOLD":
                    price = glob.prices.get(result["ticker"], 0)
                    if price: execute_global_trade(agent_name, result["ticker"], result["action"], price)

        # Layer 2: Gemini — trigger event ATAU review berkala setiap ~90 detik
        triggers    = detect_global_trigger()
        ticks_since = glob.tick_count - glob.gemini_last_tick
        force_call  = ticks_since >= 18
        if (triggers or force_call) and ticks_since >= GEMINI_COOLDOWN:
            glob.gemini_last_tick = glob.tick_count
            glob.last_trigger     = " & ".join(triggers[:2]) if triggers else ""
            label = glob.last_trigger if glob.last_trigger else "analisis berkala"
            print(f"[gemini-global] tick={glob.tick_count} → {label}")
            try:
                result  = await call_gemini_global(build_global_prompt(glob.last_trigger))
                ticker  = result.get("ticker", GLOBAL_TICKERS[0])
                action  = result.get("action", "HOLD")
                if ticker not in GLOBAL_TICKERS:
                    ticker = next((t for t, v in GLOBAL_PAIRS.items() if v["short"] == ticker), GLOBAL_TICKERS[0])
                glob.agents["gemini"]["signal"]      = action
                glob.agents["gemini"]["confidence"]  = result.get("confidence", 0)
                glob.agents["gemini"]["reason"]      = result.get("reason", "-")
                glob.agents["gemini"]["last_ticker"] = ticker
                if action != "HOLD" and glob.prices.get(ticker):
                    execute_global_trade("gemini", ticker, action, glob.prices[ticker])
            except Exception as e:
                print(f"[global:gemini] Error: {e}")
        elif not triggers:
            glob.last_trigger = ""

        # Payload
        payload = {
            "type":    "tick",
            "tick":    glob.tick_count,
            "prices":  {t: round(p, 6) for t, p in glob.prices.items() if p},
            "pairs":   GLOBAL_PAIRS,
            "trigger": glob.last_trigger,
            "news":    glob.last_news,
            "news_sentiment": round(glob.news_sentiment, 2),
            "agents": {
                name: {
                    "signal":      ag["signal"],
                    "confidence":  ag["confidence"],
                    "reason":      ag["reason"],
                    "last_ticker": ag["last_ticker"],
                    "cash":        round(ag["cash"], 2),
                    "holdings": {
                        t: {"positions": round(h["positions"],6), "avg_price": round(h["avg_price"],6),
                            "value": round(h["positions"]*glob.prices.get(t,0),2)}
                        for t, h in ag["holdings"].items() if h["positions"] > 0
                    },
                    "portfolio": round(glob.portfolio_value(name), 2),
                    "pnl":       round(glob.portfolio_value(name) - GLOBAL_MODAL, 2),
                    "pnl_pct":   round((glob.portfolio_value(name) - GLOBAL_MODAL) / GLOBAL_MODAL * 100, 2),
                    "trades":    len(ag["trades"]),
                }
                for name, ag in glob.agents.items()
            },
        }
        await broadcast_global(payload)

        # Simpan holdings Global setiap SNAPSHOT_EVERY tick
        if glob.tick_count % SNAPSHOT_EVERY == 0:
            holdings_data = [
                {
                    "name":     name,
                    "cash":     float(ag["cash"]),
                    "holdings": {
                        t: {"positions": float(h["positions"]), "avg_price": float(h["avg_price"])}
                        for t, h in ag["holdings"].items() if h["positions"] > 0
                    }
                }
                for name, ag in glob.agents.items()
            ]
            await db.save_holdings_snapshot("global", holdings_data)

        await asyncio.sleep(POLL_INTERVAL)


# ─── Restore portfolio dari DB ───────────────────────────────────────────────
def _apply_holdings(agents_dict: dict, data: dict, label: str):
    """Helper: terapkan data holdings ke agents."""
    restored = 0
    for name, state in data.items():
        if name not in agents_dict:
            continue
        agents_dict[name]["cash"] = float(state["cash"])
        for ticker, h in state["holdings"].items():
            if ticker in agents_dict[name]["holdings"]:
                agents_dict[name]["holdings"][ticker] = {
                    "positions": float(h.get("positions", 0)),
                    "avg_price": float(h.get("avg_price", 0)),
                }
        restored += 1
    print(f"[restore] {label}: {restored} agent dipulihkan dari snapshot DB")
    return restored


async def _reconstruct_from_transactions(market: str, agents_dict: dict,
                                         tickers_list: list, modal: float,
                                         lot_size: int = 1, label: str = ""):
    """Rekonstruksi posisi dengan memutar ulang semua transaksi dari DB."""
    txs = await db.get_all_transactions_asc(market)
    if not txs:
        print(f"[restore] {label}: tidak ada transaksi di DB, mulai dari awal")
        return

    # Kelompokkan per agent
    from collections import defaultdict
    agent_txs: dict = defaultdict(list)
    for tx in txs:
        agent_txs[tx["agent_name"]].append(tx)

    reconstructed = 0
    for name, agent_tx_list in agent_txs.items():
        if name not in agents_dict:
            continue
        cash     = float(modal)
        holdings = {t: {"positions": 0.0, "avg_price": 0.0} for t in tickers_list}

        for tx in sorted(agent_tx_list, key=lambda x: x["ts"]):
            ticker = tx["ticker"]
            if ticker not in holdings:
                continue
            price = float(tx["price"])
            units = float(tx["lots"])   # lots = units (FLOAT sekarang)

            if tx["type"] == "BUY":
                cost  = units * (lot_size if lot_size > 1 else price) if lot_size > 1 else units * price
                cost  = units * lot_size * price if lot_size > 1 else units * price
                cash -= cost
                total = holdings[ticker]["positions"] + (units * lot_size if lot_size > 1 else units)
                if total > 0:
                    prev_pos = holdings[ticker]["positions"]
                    new_pos  = units * lot_size if lot_size > 1 else units
                    holdings[ticker]["avg_price"] = (prev_pos * holdings[ticker]["avg_price"] + new_pos * price) / total
                holdings[ticker]["positions"] = total
            elif tx["type"] == "SELL":
                cash += holdings[ticker]["positions"] * price
                holdings[ticker]["positions"] = 0.0
                holdings[ticker]["avg_price"] = 0.0

        agents_dict[name]["cash"] = max(0.0, cash)
        for t, h in holdings.items():
            agents_dict[name]["holdings"][t] = h
        reconstructed += 1

    print(f"[restore] {label}: {reconstructed} agent direkonstruksi dari {len(txs)} transaksi DB")


async def restore_idx_from_db():
    # Prioritas 1: snapshot holdings
    data = await db.get_latest_holdings("idx")
    if data:
        _apply_holdings(sim.agents, data, "IDX")
        return
    # Fallback: rekonstruksi dari transaksi
    print("[restore] IDX: tidak ada snapshot, rekonstruksi dari transaksi...")
    await _reconstruct_from_transactions("idx", sim.agents, TICKERS, MODAL, LOT_SIZE, "IDX")


async def restore_global_from_db():
    # Prioritas 1: snapshot holdings
    data = await db.get_latest_holdings("global")
    if data:
        _apply_holdings(glob.agents, data, "Global")
        return
    # Fallback: rekonstruksi dari transaksi
    print("[restore] Global: tidak ada snapshot, rekonstruksi dari transaksi...")
    await _reconstruct_from_transactions("global", glob.agents, GLOBAL_TICKERS, GLOBAL_MODAL, 1, "Global")


# ─── FastAPI app ───────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    await db.save_session_start()
    await restore_idx_from_db()
    await restore_global_from_db()
    task1 = asyncio.create_task(trading_loop())
    task2 = asyncio.create_task(global_trading_loop())
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


@app.get("/signal")
def get_all_signals():
    """Sinyal terkini semua bot IDX."""
    now = datetime.now(WIB).isoformat()
    return {
        "market": "IDX",
        "timestamp": now,
        "signals": {
            name: {
                "signal":     ag["signal"],
                "confidence": ag["confidence"],
                "reason":     ag["reason"],
                "ticker":     ag["last_ticker"],
                "portfolio":  round(sim.portfolio_value(name)),
                "pnl_pct":    round((sim.portfolio_value(name) - MODAL) / MODAL * 100, 2),
            }
            for name, ag in sim.agents.items()
        }
    }


@app.get("/signal/{agent_name}")
def get_agent_signal(agent_name: str):
    """Sinyal terkini satu bot IDX tertentu. Contoh: /signal/gemini"""
    if agent_name not in sim.agents:
        return {"error": f"Agent '{agent_name}' tidak ditemukan. Tersedia: {list(sim.agents.keys())}"}
    ag     = sim.agents[agent_name]
    pf     = sim.portfolio_value(agent_name)
    ticker = ag["last_ticker"]
    return {
        "market":     "IDX",
        "agent":      agent_name,
        "signal":     ag["signal"],
        "confidence": ag["confidence"],
        "reason":     ag["reason"],
        "ticker":     ticker,
        "price":      round(sim.prices.get(ticker, 0), 2),
        "prices":     {t: round(p, 2) for t, p in sim.prices.items() if p},
        "holdings":   {t: {"positions": h["positions"]//LOT_SIZE, "avg_price": round(h["avg_price"],2),
                            "value": round(h["positions"]*sim.prices.get(t,0))}
                       for t, h in ag["holdings"].items() if h["positions"] > 0},
        "portfolio":  round(pf),
        "pnl":        round(pf - MODAL),
        "pnl_pct":    round((pf - MODAL) / MODAL * 100, 2),
        "trades":     len(ag["trades"]),
        "timestamp":  datetime.now(WIB).isoformat(),
    }


@app.get("/signal/global/all")
def get_all_global_signals():
    """Sinyal terkini semua bot Global Markets."""
    now = datetime.now(WIB).isoformat()
    return {
        "market": "Global",
        "timestamp": now,
        "signals": {
            name: {
                "signal":     ag["signal"],
                "confidence": ag["confidence"],
                "reason":     ag["reason"],
                "ticker":     ag["last_ticker"],
                "portfolio":  round(glob.portfolio_value(name), 2),
                "pnl_pct":    round((glob.portfolio_value(name) - GLOBAL_MODAL) / GLOBAL_MODAL * 100, 2),
            }
            for name, ag in glob.agents.items()
        }
    }


@app.get("/signal/global/{agent_name}")
def get_global_agent_signal(agent_name: str):
    """Sinyal terkini satu bot Global Markets. Contoh: /signal/global/gemini"""
    if agent_name not in glob.agents:
        return {"error": f"Agent '{agent_name}' tidak ditemukan. Tersedia: {list(glob.agents.keys())}"}
    ag     = glob.agents[agent_name]
    pf     = glob.portfolio_value(agent_name)
    ticker = ag["last_ticker"]
    return {
        "market":     "Global",
        "agent":      agent_name,
        "signal":     ag["signal"],
        "confidence": ag["confidence"],
        "reason":     ag["reason"],
        "ticker":     ticker,
        "price":      round(glob.prices.get(ticker, 0), 4),
        "prices":     {t: round(p, 4) for t, p in glob.prices.items() if p},
        "holdings":   {t: {"positions": round(h["positions"],4), "avg_price": round(h["avg_price"],4),
                            "value": round(h["positions"]*glob.prices.get(t,0),2)}
                       for t, h in ag["holdings"].items() if h["positions"] > 0},
        "portfolio":  round(pf, 2),
        "pnl":        round(pf - GLOBAL_MODAL, 2),
        "pnl_pct":    round((pf - GLOBAL_MODAL) / GLOBAL_MODAL * 100, 2),
        "trades":     len(ag["trades"]),
        "timestamp":  datetime.now(WIB).isoformat(),
    }


@app.post("/reset")
def reset():
    sim.reset()
    return {"ok": True}


@app.get("/history")
def history_page():
    return FileResponse("history.html")


@app.get("/bot")
def bot_page():
    return FileResponse("bot.html")


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


@app.get("/history/sessions")
async def history_sessions(limit: int = 50):
    return await db.get_sessions(limit)


# ─── Global Markets endpoints ─────────────────────────────────────────────────

@app.get("/global")
def global_page():
    return FileResponse("global.html")


@app.get("/xauusd")
def xauusd_redirect():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/global")


@app.post("/global/reset")
def global_reset():
    glob.reset()
    return {"ok": True}


@app.websocket("/ws/global")
async def global_websocket(ws: WebSocket):
    await ws.accept()
    glob.clients.append(ws)
    print(f"[global-ws] Client terhubung. Total: {len(glob.clients)}")
    try:
        while True:
            data = await ws.receive_text()
            msg  = json.loads(data)
            if msg.get("action") == "reset":
                glob.reset()
    except WebSocketDisconnect:
        glob.clients.remove(ws)
        print(f"[global-ws] Client terputus. Total: {len(glob.clients)}")

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