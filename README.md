# AI Trading Battle — Backend

Simulator trading 3 AI (DeepSeek · Claude · Qwen) dengan data harga IDX real-time
via yfinance dan analisa berita fundamental.

## Struktur file

```
ai_trading_backend/
├── main.py           ← FastAPI backend (price fetch + AI dispatcher + WebSocket)
├── index.html        ← Frontend (Chart.js + WebSocket client)
├── requirements.txt  ← Python dependencies
├── .env.example      ← Template API key
└── README.md
```

## Cara setup

### 1. Install dependencies

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Isi API key

```bash
cp .env.example .env
# Edit .env, isi DEEPSEEK_API_KEY, ANTHROPIC_API_KEY, QWEN_API_KEY
```

### 3. Jalankan backend

```bash
uvicorn main:app --reload --port 8000
```

Backend akan berjalan di http://localhost:8000

### 4. Buka frontend

Buka `index.html` langsung di browser (double-click), atau serve via:

```bash
python -m http.server 3000
# lalu buka http://localhost:3000
```

## Cara mendapatkan API key

| Provider   | URL                                          | Model         |
|------------|----------------------------------------------|---------------|
| DeepSeek   | https://platform.deepseek.com/api-keys       | deepseek-chat |
| Anthropic  | https://console.anthropic.com/settings/keys  | claude-sonnet-4|
| Qwen/Alibaba | https://dashscope.aliyuncs.com             | qwen-plus     |

## Catatan data harga

- `yfinance` menggunakan data Yahoo Finance dengan delay ~15 menit (gratis)
- Untuk data real-time IDX tanpa delay, ganti fungsi `fetch_price()` di `main.py`
  dengan Stockbit API atau IDX Datafeed berbayar
- Ticker IDX di Yahoo Finance menggunakan suffix `.JK` (contoh: `BBCA.JK`)

## API endpoints

| Method | Path              | Keterangan                        |
|--------|-------------------|-----------------------------------|
| GET    | /                 | Status server                     |
| GET    | /tickers          | Daftar ticker yang didukung       |
| POST   | /ticker/{ticker}  | Ganti ticker aktif (reset state)  |
| POST   | /reset            | Reset semua state & portofolio    |
| WS     | /ws               | WebSocket stream tick data        |

## Format pesan WebSocket

```json
{
  "type": "tick",
  "tick": 42,
  "ticker": "BBCA.JK",
  "price": 9525.0,
  "change": 25.0,
  "change_pct": 0.26,
  "rsi": 54.3,
  "ma7": 9510.0,
  "ma20": 9480.0,
  "news": "BBCA catat kredit naik 12% YoY",
  "news_sentiment": 0.5,
  "agents": {
    "deepseek": {
      "signal": "BUY",
      "confidence": 75,
      "reason": "RSI netral, sentimen positif, MA mendukung tren naik",
      "cash": 72000000,
      "positions": 30,
      "avg_price": 9480.0,
      "portfolio": 103000000,
      "pnl": 3000000,
      "pnl_pct": 3.0,
      "trades": 2
    },
    "claude":   { ... },
    "qwen":     { ... }
  }
}
```
