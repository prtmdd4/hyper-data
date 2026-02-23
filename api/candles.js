/**
 * GET /api/candles?symbol=BTC&interval=1h&start=startMs&end=endMs
 * Returns JSON array of { ts_ms, open, high, low, close, volume } for Harmonics V2 to consume.
 */

const { getPool, initDb, getCandles } = require('../lib/db');

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  const symbol = req.query?.symbol?.trim();
  const interval = req.query?.interval?.trim() || '1h';
  const startMs = parseInt(req.query?.start, 10);
  const endMs = parseInt(req.query?.end, 10);

  if (!symbol) {
    res.status(400).json({ error: 'Missing query: symbol' });
    return;
  }
  if (!Number.isFinite(startMs) || !Number.isFinite(endMs)) {
    res.status(400).json({ error: 'Missing or invalid query: start, end (milliseconds)' });
    return;
  }

  res.setHeader('Content-Type', 'application/json');
  let pool;
  try {
    pool = getPool();
    await initDb(pool);
    const rows = await getCandles(pool, symbol, interval, startMs, endMs);
    res.status(200).json(rows);
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  } finally {
    if (pool) pool.end().catch(() => {});
  }
};
