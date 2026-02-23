/**
 * Postgres schema and helpers for Hyperliquid pair + candle data.
 * Uses DATABASE_URL (Vercel/Neon inject this).
 */

const { Pool } = require('pg');

function getPool() {
  const url = process.env.DATABASE_URL || process.env.POSTGRES_URL;
  if (!url) throw new Error('DATABASE_URL or POSTGRES_URL is required');
  return new Pool({ connectionString: url, ssl: { rejectUnauthorized: false } });
}

const INIT_SQL = `
-- Pairs from Hyperliquid meta (id = symbol)
CREATE TABLE IF NOT EXISTS pairs (
  symbol TEXT PRIMARY KEY,
  name TEXT,
  sz_decimals INT,
  max_leverage INT,
  only_isolated BOOLEAN,
  is_delisted BOOLEAN,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- OHLCV candles: one row per (symbol, interval, ts_ms)
CREATE TABLE IF NOT EXISTS candles (
  symbol TEXT NOT NULL,
  interval TEXT NOT NULL,
  ts_ms BIGINT NOT NULL,
  open NUMERIC NOT NULL,
  high NUMERIC NOT NULL,
  low NUMERIC NOT NULL,
  close NUMERIC NOT NULL,
  volume NUMERIC NOT NULL,
  PRIMARY KEY (symbol, interval, ts_ms)
);

CREATE INDEX IF NOT EXISTS ix_candles_symbol_interval ON candles(symbol, interval);
CREATE INDEX IF NOT EXISTS ix_candles_ts ON candles(ts_ms);

-- Sync state: which batch we're on (for cron batching)
CREATE TABLE IF NOT EXISTS sync_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT NOW()
);
`;

async function initDb(pool) {
  await pool.query(INIT_SQL);
}

async function upsertPairs(pool, symbols) {
  const client = await pool.connect();
  try {
    for (const symbol of symbols) {
      await client.query(
        `INSERT INTO pairs (symbol, name, updated_at) VALUES ($1, $2, NOW())
         ON CONFLICT (symbol) DO UPDATE SET name = $2, updated_at = NOW()`,
        [symbol, symbol]
      );
    }
  } finally {
    client.release();
  }
}

const BATCH_SIZE = 100;

async function upsertCandles(pool, rows) {
  if (!rows.length) return;
  const client = await pool.connect();
  try {
    for (let i = 0; i < rows.length; i += BATCH_SIZE) {
      const batch = rows.slice(i, i + BATCH_SIZE);
      const values = batch.flatMap((r, j) => {
        const o = i + j;
        return [r.symbol, r.interval, r.ts_ms, r.open, r.high, r.low, r.close, r.volume];
      });
      const placeholders = batch.map((_, j) => {
        const o = j * 8;
        return `($${o + 1}, $${o + 2}, $${o + 3}, $${o + 4}, $${o + 5}, $${o + 6}, $${o + 7}, $${o + 8})`;
      }).join(', ');
      await client.query(
        `INSERT INTO candles (symbol, interval, ts_ms, open, high, low, close, volume)
         VALUES ${placeholders}
         ON CONFLICT (symbol, interval, ts_ms) DO UPDATE SET
         open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
         close = EXCLUDED.close, volume = EXCLUDED.volume`,
        values
      );
    }
  } finally {
    client.release();
  }
}

async function getSyncState(pool, key) {
  const r = await pool.query('SELECT value FROM sync_state WHERE key = $1', [key]);
  return r.rows[0]?.value ?? null;
}

async function setSyncState(pool, key, value) {
  await pool.query(
    `INSERT INTO sync_state (key, value, updated_at) VALUES ($1, $2, NOW())
     ON CONFLICT (key) DO UPDATE SET value = $2, updated_at = NOW()`,
    [key, String(value)]
  );
}

async function getCandles(pool, symbol, interval, startMs, endMs) {
  const r = await pool.query(
    `SELECT ts_ms, open, high, low, close, volume
     FROM candles WHERE symbol = $1 AND interval = $2 AND ts_ms >= $3 AND ts_ms <= $4
     ORDER BY ts_ms`,
    [symbol, interval, startMs, endMs]
  );
  return r.rows;
}

async function getAllPairs(pool) {
  const r = await pool.query('SELECT symbol, name FROM pairs ORDER BY symbol');
  return r.rows;
}

module.exports = {
  getPool,
  initDb,
  upsertPairs,
  upsertCandles,
  getSyncState,
  setSyncState,
  getCandles,
  getAllPairs,
};
