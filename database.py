"""
database.py — Koneksi dan operasi PostgreSQL untuk Z Trader
Nonaktif otomatis jika DATABASE_URL tidak diset (mode lokal tanpa DB).
"""

import os

try:
    import asyncpg
    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False
    print("[db] asyncpg tidak terinstall — database dinonaktifkan")

pool = None
# Prioritas: private URL (gratis) > public URL (kena egress fee)
DATABASE_URL = (
    os.getenv("DATABASE_PRIVATE_URL", "") or
    os.getenv("DATABASE_URL", "")
)


async def init_db():
    global pool
    if not ASYNCPG_AVAILABLE or not DATABASE_URL:
        print("[db] DATABASE_URL tidak diset — database dinonaktifkan")
        return
    # Railway kadang pakai prefix 'postgres://', asyncpg butuh 'postgresql://'
    url = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    try:
        pool = await asyncpg.create_pool(url, min_size=1, max_size=5)
        await _create_tables()
        print("[db] Database terhubung dan tabel siap")
    except Exception as e:
        print(f"[db] Gagal koneksi: {e}")
        pool = None


async def _create_tables():
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id          SERIAL PRIMARY KEY,
                agent_name  VARCHAR(20)  NOT NULL,
                ticker      VARCHAR(20)  NOT NULL,
                type        VARCHAR(10)  NOT NULL,
                price       FLOAT        NOT NULL,
                lots        FLOAT        NOT NULL DEFAULT 0,
                gain        FLOAT        DEFAULT 0,
                market      VARCHAR(10)  DEFAULT 'idx',
                ts          TIMESTAMPTZ  DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_tx_agent  ON transactions(agent_name);
            CREATE INDEX IF NOT EXISTS idx_tx_ts     ON transactions(ts);
            CREATE INDEX IF NOT EXISTS idx_tx_market ON transactions(market);

            CREATE TABLE IF NOT EXISTS portfolio_snapshots (
                id          SERIAL PRIMARY KEY,
                agent_name  VARCHAR(20)  NOT NULL,
                portfolio   BIGINT       NOT NULL,
                cash        BIGINT       NOT NULL,
                pnl         BIGINT       NOT NULL,
                pnl_pct     FLOAT        NOT NULL,
                ts          TIMESTAMPTZ  DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_snap_agent ON portfolio_snapshots(agent_name);
            CREATE INDEX IF NOT EXISTS idx_snap_ts    ON portfolio_snapshots(ts);

            CREATE TABLE IF NOT EXISTS price_history (
                id      SERIAL PRIMARY KEY,
                ticker  VARCHAR(20) NOT NULL,
                price   FLOAT       NOT NULL,
                ts      TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_price_ticker ON price_history(ticker);
            CREATE INDEX IF NOT EXISTS idx_price_ts     ON price_history(ts);

            CREATE TABLE IF NOT EXISTS competition_results (
                id          SERIAL PRIMARY KEY,
                agent_name  VARCHAR(20) NOT NULL,
                portfolio   BIGINT      NOT NULL,
                pnl         BIGINT      NOT NULL,
                trades      INTEGER     NOT NULL,
                rank        INTEGER     NOT NULL,
                ts          TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_result_ts ON competition_results(ts);

            CREATE TABLE IF NOT EXISTS sessions (
                id         SERIAL PRIMARY KEY,
                started_at TIMESTAMPTZ DEFAULT NOW(),
                note       TEXT
            );

            CREATE TABLE IF NOT EXISTS holdings_snapshot (
                id         SERIAL PRIMARY KEY,
                market     VARCHAR(10)  NOT NULL,
                agent_name VARCHAR(20)  NOT NULL,
                cash       FLOAT        NOT NULL,
                holdings   JSONB        NOT NULL,
                ts         TIMESTAMPTZ  DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_hold_market_agent ON holdings_snapshot(market, agent_name, ts DESC);
        """)


# ─── Write ────────────────────────────────────────────────────────────────────

async def save_transaction(agent_name: str, ticker: str, type_: str,
                           price: float, lots: float, gain: float = 0.0,
                           market: str = "idx"):
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO transactions(agent_name,ticker,type,price,lots,gain,market) "
                "VALUES($1,$2,$3,$4,$5,$6,$7)",
                agent_name, ticker, type_, float(price), float(lots), float(gain), market
            )
    except Exception as e:
        print(f"[db] save_transaction error: {e}")


async def save_portfolio_snapshots(agents_data: list[dict]):
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO portfolio_snapshots(agent_name,portfolio,cash,pnl,pnl_pct) "
                "VALUES($1,$2,$3,$4,$5)",
                [(a["name"], a["portfolio"], a["cash"], a["pnl"], a["pnl_pct"])
                 for a in agents_data]
            )
    except Exception as e:
        print(f"[db] save_portfolio_snapshots error: {e}")


async def save_prices(prices: dict[str, float]):
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO price_history(ticker,price) VALUES($1,$2)",
                list(prices.items())
            )
    except Exception as e:
        print(f"[db] save_prices error: {e}")


async def save_competition_results(agents_data: list[dict]):
    if not pool:
        return
    try:
        sorted_agents = sorted(agents_data, key=lambda x: x["portfolio"], reverse=True)
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO competition_results(agent_name,portfolio,pnl,trades,rank) "
                "VALUES($1,$2,$3,$4,$5)",
                [(a["name"], a["portfolio"], a["pnl"], a["trades"], i + 1)
                 for i, a in enumerate(sorted_agents)]
            )
    except Exception as e:
        print(f"[db] save_competition_results error: {e}")


async def save_holdings_snapshot(market: str, agents_data: list[dict]):
    if not pool:
        return
    try:
        import json
        async with pool.acquire() as conn:
            await conn.executemany(
                "INSERT INTO holdings_snapshot(market, agent_name, cash, holdings) VALUES($1,$2,$3,$4::jsonb)",
                [(market, a["name"], float(a["cash"]), json.dumps(a["holdings"])) for a in agents_data]
            )
    except Exception as e:
        print(f"[db] save_holdings error: {e}")


async def get_all_transactions_asc(market_filter: str = None) -> list:
    """Ambil semua transaksi urut dari terlama — untuk rekonstruksi posisi."""
    if not pool:
        return []
    async with pool.acquire() as conn:
        if market_filter:
            rows = await conn.fetch(
                "SELECT * FROM transactions WHERE market=$1 ORDER BY ts ASC", market_filter
            )
        else:
            rows = await conn.fetch("SELECT * FROM transactions ORDER BY ts ASC")
    return [dict(r) for r in rows]


async def get_latest_holdings(market: str) -> dict:
    """Ambil snapshot holdings terakhir per agent untuk market tertentu."""
    if not pool:
        return {}
    import json
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT DISTINCT ON (agent_name) agent_name, cash, holdings
            FROM holdings_snapshot
            WHERE market = $1
            ORDER BY agent_name, ts DESC
        """, market)
    return {
        r["agent_name"]: {
            "cash":     r["cash"],
            "holdings": json.loads(r["holdings"]) if isinstance(r["holdings"], str) else dict(r["holdings"])
        }
        for r in rows
    }


async def save_session_start():
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO sessions(note) VALUES($1)", "Server restart")
        print("[db] Session baru dicatat")
    except Exception as e:
        print(f"[db] save_session error: {e}")


# ─── Read ─────────────────────────────────────────────────────────────────────

async def get_transactions(agent: str = None, limit: int = 100) -> list:
    if not pool:
        return []
    async with pool.acquire() as conn:
        if agent:
            rows = await conn.fetch(
                "SELECT * FROM transactions WHERE agent_name=$1 ORDER BY ts DESC LIMIT $2",
                agent, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM transactions ORDER BY ts DESC LIMIT $1", limit
            )
    return [dict(r) for r in rows]


async def get_portfolio_history(agent: str = None, limit: int = 200) -> list:
    if not pool:
        return []
    async with pool.acquire() as conn:
        if agent:
            rows = await conn.fetch(
                "SELECT * FROM portfolio_snapshots WHERE agent_name=$1 ORDER BY ts DESC LIMIT $2",
                agent, limit
            )
        else:
            rows = await conn.fetch(
                "SELECT * FROM portfolio_snapshots ORDER BY ts DESC LIMIT $1", limit
            )
    return [dict(r) for r in rows]


async def get_price_history(ticker: str, limit: int = 200) -> list:
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM price_history WHERE ticker=$1 ORDER BY ts DESC LIMIT $2",
            ticker, limit
        )
    return [dict(r) for r in rows]


async def get_competition_results(limit: int = 50) -> list:
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM competition_results ORDER BY ts DESC LIMIT $1", limit
        )
    return [dict(r) for r in rows]


async def get_sessions(limit: int = 50) -> list:
    if not pool:
        return []
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM sessions ORDER BY started_at DESC LIMIT $1", limit
        )
    return [dict(r) for r in rows]