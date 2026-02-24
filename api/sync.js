/**
 * Sync job: fetch Hyperliquid pairs + candles and store in Postgres.
 * Designed for Vercel cron (runs in batches to stay under ~5 min timeout).
 * Batch size and intervals are tuned so each run does a small slice of work.
 */

const { getMeta, getCandleSnapshot } = require('../lib/hyperliquid');
const {
  getPool,
  initDb,
  upsertPairs,
  upsertCandles,
  getSyncState,
  setSyncState,
  getAllPairs,
} = require('../lib/db');

// How many pairs to sync per run. ~1.2s per HL call; 24 pairs × 9 intervals = 216 calls ≈ 4.3 min (under 5 min limit).
// With ~250 pairs, 250/24 ≈ 11 runs = one full cycle. Run every 2h → 12 runs/day = every pair updated daily.
const PAIRS_PER_RUN = 24;
// Intervals to store (HL supports: 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 8h, 12h, 1d, 3d, 1w, 1M)
const INTERVALS = ['1m', '5m', '15m', '30m', '1h', '2h', '4h', '1d', '3d'];
// For each interval, fetch this many ms of history (HL returns max 5000 per request)
const MS_PER_INTERVAL = {
  '1m': 24 * 60 * 60 * 1000,
  '5m': 7 * 24 * 60 * 60 * 1000,
  '15m': 14 * 24 * 60 * 60 * 1000,
  '30m': 30 * 24 * 60 * 60 * 1000,
  '1h': 30 * 24 * 60 * 60 * 1000,
  '2h': 90 * 24 * 60 * 60 * 1000,
  '4h': 90 * 24 * 60 * 60 * 1000,
  '1d': 365 * 24 * 60 * 60 * 1000,
  '3d': 365 * 24 * 60 * 60 * 1000,
};

module.exports = async (req, res) => {
  // Optional: require cron secret so random users can't trigger
  const cronSecret = process.env.CRON_SECRET;
  if (cronSecret && req.headers.authorization !== `Bearer ${cronSecret}`) {
    res.status(401).json({ error: 'Unauthorized' });
    return;
  }

  res.setHeader('Content-Type', 'application/json');
  const out = { ok: true, pairsSynced: 0, candlesInserted: 0, error: null };

  let pool;
  try {
    pool = getPool();
    await initDb(pool);

    let symbols = await getAllPairs(pool).then((rows) => rows.map((r) => r.symbol));
    if (symbols.length === 0) {
      symbols = await getMeta();
      await upsertPairs(pool, symbols);
      out.pairsFetched = symbols.length;
    }

    const cursor = parseInt(await getSyncState(pool, 'sync_cursor'), 10) || 0;
    const batch = symbols.slice(cursor, cursor + PAIRS_PER_RUN);
    const nextCursor = cursor + batch.length >= symbols.length ? 0 : cursor + batch.length;
    await setSyncState(pool, 'sync_cursor', nextCursor);

    const endMs = Date.now();
    const allRows = [];

    for (const symbol of batch) {
      for (const interval of INTERVALS) {
        try {
          const startMs = endMs - MS_PER_INTERVAL[interval];
          const raw = await getCandleSnapshot(symbol, interval, startMs, endMs);
          for (const c of raw) {
            const t = c.T ?? c.t;
            if (t == null) continue;
            allRows.push({
              symbol,
              interval,
              ts_ms: t,
              open: Number(c.o) || 0,
              high: Number(c.h) || 0,
              low: Number(c.l) || 0,
              close: Number(c.c) || 0,
              volume: Number(c.v) || 0,
            });
          }
        } catch (e) {
          if (e.message === 'RATE_LIMIT') {
            await new Promise((r) => setTimeout(r, 15000));
            // retry once
            const startMs = endMs - MS_PER_INTERVAL[interval];
            const raw = await getCandleSnapshot(symbol, interval, startMs, endMs);
            for (const c of raw) {
              const t = c.T ?? c.t;
              if (t == null) continue;
              allRows.push({
                symbol,
                interval,
                ts_ms: t,
                open: Number(c.o) || 0,
                high: Number(c.h) || 0,
                low: Number(c.l) || 0,
                close: Number(c.c) || 0,
                volume: Number(c.v) || 0,
              });
            }
          }
          // else skip this symbol/interval
        }
      }
    }

    if (allRows.length) {
      await upsertCandles(pool, allRows);
      out.candlesInserted = allRows.length;
    }
    out.pairsSynced = batch.length;
    out.nextCursor = nextCursor;
    res.status(200).json(out);
  } catch (e) {
    out.ok = false;
    out.error = e.message || String(e);
    res.status(500).json(out);
  } finally {
    if (pool) pool.end().catch(() => {});
  }
};
