/**
 * Run sync once locally (e.g. for testing or manual backfill).
 * Requires DATABASE_URL in .env.
 */

require('dotenv').config({ path: require('path').join(__dirname, '..', '.env') });

const handler = require('../api/sync');

const req = {
  method: 'POST',
  headers: process.env.CRON_SECRET ? { authorization: `Bearer ${process.env.CRON_SECRET}` } : {},
  query: {},
};
const res = {
  _status: null,
  _body: null,
  setHeader() {},
  status(code) {
    this._status = code;
    return this;
  },
  json(obj) {
    this._body = obj;
    console.log(JSON.stringify(obj, null, 2));
    process.exit(this._status >= 400 ? 1 : 0);
  },
};

handler(req, res).catch((err) => {
  console.error(err);
  process.exit(1);
});
