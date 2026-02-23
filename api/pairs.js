/**
 * GET /api/pairs - list all pairs (symbols) stored in the DB.
 */

const { getPool, initDb, getAllPairs } = require('../lib/db');

module.exports = async (req, res) => {
  if (req.method !== 'GET') {
    res.status(405).json({ error: 'Method not allowed' });
    return;
  }

  res.setHeader('Content-Type', 'application/json');
  let pool;
  try {
    pool = getPool();
    await initDb(pool);
    const rows = await getAllPairs(pool);
    res.status(200).json(rows.map((r) => ({ symbol: r.symbol, name: r.name })));
  } catch (e) {
    res.status(500).json({ error: e.message || String(e) });
  } finally {
    if (pool) pool.end().catch(() => {});
  }
};
