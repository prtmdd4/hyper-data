/**
 * Hyperliquid REST client: meta (all pairs) and candle snapshot.
 * Uses mainnet by default; set HYPERLIQUID_TESTNET=1 for testnet.
 */

const HL_BASE = process.env.HYPERLIQUID_TESTNET === '1'
  ? 'https://api.hyperliquid-testnet.xyz'
  : 'https://api.hyperliquid.xyz';

const REST_MIN_MS = 1200;
let lastCall = 0;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function rateLimit() {
  const now = Date.now();
  const elapsed = now - lastCall;
  if (elapsed < REST_MIN_MS) await sleep(REST_MIN_MS - elapsed);
  lastCall = Date.now();
}

/**
 * Fetch all perp symbols from meta.
 * @returns {Promise<string[]>} e.g. ['BTC', 'ETH', ...]
 */
async function getMeta() {
  await rateLimit();
  const res = await fetch(`${HL_BASE}/info`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ type: 'meta' }),
  });
  if (!res.ok) throw new Error(`Hyperliquid meta failed: ${res.status}`);
  const data = await res.json();
  const universe = Array.isArray(data) ? data : data?.universe ?? [];
  return universe.map((u) => (typeof u === 'string' ? u : u?.name)).filter(Boolean);
}

/**
 * Fetch OHLCV candles for one symbol.
 * @param {string} coin - e.g. 'BTC'
 * @param {string} interval - '1m' | '5m' | '15m' | '30m' | '1h' | '2h' | '4h' | '8h' | '12h' | '1d' | '3d' | '1w' | '1M'
 * @param {number} startTime - ms
 * @param {number} endTime - ms
 * @returns {Promise<Array<{t:number,T:number,o:number,h:number,l:number,c:number,v:number}>>}
 */
async function getCandleSnapshot(coin, interval, startTime, endTime) {
  await rateLimit();
  const res = await fetch(`${HL_BASE}/info`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({
      type: 'candleSnapshot',
      req: { coin, interval, startTime, endTime },
    }),
  });
  if (!res.ok) {
    if (res.status === 429) throw new Error('RATE_LIMIT');
    throw new Error(`Hyperliquid candles failed: ${res.status}`);
  }
  const data = await res.json();
  return Array.isArray(data) ? data : [];
}

module.exports = { getMeta, getCandleSnapshot, HL_BASE };
