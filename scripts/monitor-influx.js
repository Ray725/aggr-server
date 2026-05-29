/**
 * InfluxDB health + data verification monitor for aggr-server.
 *
 * Zero dependencies: uses only Node built-ins so it runs on the host even when
 * node_modules / the app environment is not installed. Reads config.json (and
 * env vars) to target the same DB / host / retention policies as the server.
 *
 * Usage:
 *   node scripts/monitor-influx.js                 # one-shot report, exits 0/1/2
 *   node scripts/monitor-influx.js --watch         # repeat every 60s
 *   node scripts/monitor-influx.js --watch=30      # repeat every 30s
 *   node scripts/monitor-influx.js --json          # machine-readable single check
 *   node scripts/monitor-influx.js --stale=180     # mark data stale after 180s
 *   node scripts/monitor-influx.js --host=1.2.3.4 --port=8086 --db=significant_trades
 *
 * Exit codes (one-shot): 0 = healthy, 1 = degraded (WARN), 2 = down/critical (FAIL).
 */

const http = require('http')
const fs = require('fs')
const path = require('path')

function parseArgs(argv) {
  const args = {}
  for (const raw of argv.slice(2)) {
    const m = /^--([^=]+)(?:=(.*))?$/.exec(raw)
    if (!m) continue
    args[m[1]] = m[2] === undefined ? true : m[2]
  }
  return args
}

const args = parseArgs(process.argv)

// Load config.json if present (only overrides a few keys; rest are server defaults).
let fileConfig = {}
try {
  const configPath = path.resolve(__dirname, '../config.json')
  fileConfig = JSON.parse(fs.readFileSync(configPath, 'utf8'))
} catch (err) {
  // fall back to defaults below
}

// Server defaults (must mirror src/config.js defaults).
const DEFAULTS = {
  influxHost: 'localhost',
  influxPort: 8086,
  influxDatabase: 'significant_trades',
  influxMeasurement: 'trades',
  influxTimeframe: 10000,
  influxRetentionPrefix: 'aggr_'
}

function cfg(key) {
  return fileConfig[key] !== undefined ? fileConfig[key] : DEFAULTS[key]
}

/** Convert a clean ms timeframe to the literal src/helper.getHms produces (e.g. 10000 -> "10s"). */
function timeframeLiteral(ms) {
  if (ms % 86400000 === 0) return ms / 86400000 + 'd'
  if (ms % 3600000 === 0) return ms / 3600000 + 'h'
  if (ms % 60000 === 0) return ms / 60000 + 'm'
  if (ms % 1000 === 0) return ms / 1000 + 's'
  return ms + 'ms'
}

/** Human-readable duration from seconds. */
function humanAge(totalSec) {
  if (totalSec < 60) return `${totalSec}s`
  const d = Math.floor(totalSec / 86400)
  const h = Math.floor((totalSec % 86400) / 3600)
  const m = Math.floor((totalSec % 3600) / 60)
  const s = totalSec % 60
  return [d && `${d}d`, h && `${h}h`, m && `${m}m`, s && `${s}s`]
    .filter(Boolean)
    .join(' ')
}

const HOST = args.host || process.env.MONITOR_INFLUX_HOST || cfg('influxHost')
const PORT = parseInt(args.port || process.env.MONITOR_INFLUX_PORT || cfg('influxPort'), 10)
const DB = args.db || cfg('influxDatabase')
const BASE_TIMEFRAME = timeframeLiteral(cfg('influxTimeframe')) // e.g. "10s"
const BASE_RP = cfg('influxRetentionPrefix') + BASE_TIMEFRAME // e.g. "aggr_10s"
const BASE_MEASUREMENT = cfg('influxMeasurement') + '_' + BASE_TIMEFRAME // e.g. "trades_10s"
const FQ_MEASUREMENT = `"${BASE_RP}"."${BASE_MEASUREMENT}"`

const STALE_SECONDS = parseInt(args.stale || args['stale-seconds'] || 120, 10)
const ACTIVE_WINDOW = args.window || '5m'
const EXPECTED_MARKETS = Array.isArray(fileConfig.pairs) ? fileConfig.pairs.length : 0
const HTTP_TIMEOUT = 8000

const PASS = 'PASS'
const WARN = 'WARN'
const FAIL = 'FAIL'

const COLORS = {
  [PASS]: '\x1b[32m',
  [WARN]: '\x1b[33m',
  [FAIL]: '\x1b[31m',
  reset: '\x1b[0m',
  dim: '\x1b[90m'
}
const useColor = process.stdout.isTTY && !args['no-color']
function color(status, text) {
  return useColor ? `${COLORS[status] || ''}${text}${COLORS.reset}` : text
}

/** Normalize errors (incl. AggregateError, which has an empty message) to a readable string. */
function errMsg(err) {
  if (err && Array.isArray(err.errors) && err.errors.length) {
    return err.errors.map(e => e.message || String(e)).join('; ')
  }
  return err && err.message ? err.message : String(err)
}

/** Minimal HTTP GET returning { status, headers, body }. Rejects on network error/timeout. */
function httpGet(pathname, params) {
  return new Promise((resolve, reject) => {
    const qs = params
      ? '?' + Object.entries(params).map(([k, v]) => `${k}=${encodeURIComponent(v)}`).join('&')
      : ''
    const req = http.get(
      { host: HOST, port: PORT, path: pathname + qs, timeout: HTTP_TIMEOUT },
      res => {
        let body = ''
        res.on('data', c => (body += c))
        res.on('end', () => resolve({ status: res.statusCode, headers: res.headers, body }))
      }
    )
    req.on('timeout', () => req.destroy(new Error(`timeout after ${HTTP_TIMEOUT}ms`)))
    req.on('error', err => reject(new Error(errMsg(err))))
  })
}

/** Run an InfluxQL query through the HTTP API and return the raw series. */
async function query(q, db = DB) {
  const res = await httpGet('/query', { db, q, epoch: 'ms' })
  if (res.status !== 200) {
    throw new Error(`HTTP ${res.status}: ${res.body.slice(0, 200)}`)
  }
  const data = JSON.parse(res.body)
  const results = (data && data.results) || []
  if (results[0] && results[0].error) throw new Error(results[0].error)
  return (results[0] && results[0].series) || []
}

/** Flatten Influx series into array of row objects keyed by column name. */
function rows(series) {
  const out = []
  for (const s of series || []) {
    for (const values of s.values || []) {
      const row = { _tags: s.tags || {} }
      s.columns.forEach((col, i) => (row[col] = values[i]))
      out.push(row)
    }
  }
  return out
}

async function runChecks() {
  const checks = []
  const add = (name, status, detail) => checks.push({ name, status, detail })
  const facts = {}

  // 1. Connectivity + version via /ping
  try {
    const res = await httpGet('/ping', { verbose: 'true' })
    if (res.status !== 200 && res.status !== 204) {
      throw new Error(`unexpected status ${res.status}`)
    }
    const version =
      res.headers['x-influxdb-version'] ||
      (res.body && JSON.parse(res.body || '{}').version) ||
      'unknown'
    facts.version = version
    add('reachable', PASS, `${HOST}:${PORT} (InfluxDB ${version})`)
  } catch (err) {
    add('reachable', FAIL, `cannot reach ${HOST}:${PORT}: ${err.message}`)
    return { checks, facts } // nothing else works without a connection
  }

  // 2. /health endpoint
  try {
    const res = await httpGet('/health')
    const status = JSON.parse(res.body || '{}').status
    add('health', status === 'pass' ? PASS : WARN, `status=${status || res.status}`)
  } catch (err) {
    add('health', WARN, `/health unavailable: ${err.message}`)
  }

  // 3. Target database exists
  try {
    const dbs = rows(await query('SHOW DATABASES', '')).map(r => r.name)
    facts.databases = dbs
    if (dbs.includes(DB)) {
      add('database', PASS, `"${DB}" present`)
    } else {
      add('database', FAIL, `"${DB}" missing (found: ${dbs.join(', ') || 'none'})`)
      return { checks, facts }
    }
  } catch (err) {
    add('database', FAIL, `SHOW DATABASES failed: ${err.message}`)
    return { checks, facts }
  }

  // 4. Retention policies
  try {
    const rps = rows(await query(`SHOW RETENTION POLICIES ON "${DB}"`))
    facts.retentionPolicies = rps.map(r => r.name)
    const hasBase = rps.some(r => r.name === BASE_RP)
    add(
      'retention',
      hasBase ? PASS : WARN,
      `${rps.length} policies, base "${BASE_RP}" ${hasBase ? 'present' : 'MISSING'}`
    )
  } catch (err) {
    add('retention', WARN, `could not read policies: ${err.message}`)
  }

  // 5. Base measurement exists
  try {
    const measurements = rows(await query(`SHOW MEASUREMENTS ON "${DB}"`)).map(r => r.name)
    facts.measurements = measurements
    add(
      'measurement',
      measurements.includes(BASE_MEASUREMENT) ? PASS : WARN,
      measurements.includes(BASE_MEASUREMENT)
        ? `"${BASE_MEASUREMENT}" present (${measurements.length} total)`
        : `"${BASE_MEASUREMENT}" not found (${measurements.length} measurements)`
    )
  } catch (err) {
    add('measurement', WARN, `SHOW MEASUREMENTS failed: ${err.message}`)
  }

  // 6. Data freshness — newest point in the base measurement
  try {
    const latest = rows(
      await query(`SELECT * FROM ${FQ_MEASUREMENT} ORDER BY time DESC LIMIT 1`)
    )[0]
    if (!latest) {
      add('freshness', FAIL, `no data in ${FQ_MEASUREMENT}`)
    } else {
      const ageSec = Math.round((Date.now() - latest.time) / 1000)
      facts.latestTime = new Date(latest.time).toISOString()
      facts.latestAgeSeconds = ageSec
      add(
        'freshness',
        ageSec <= STALE_SECONDS ? PASS : WARN,
        `newest point ${humanAge(ageSec)} ago (${facts.latestTime})`
      )
    }
  } catch (err) {
    add('freshness', FAIL, `freshness query failed: ${err.message}`)
  }

  // 7. Active markets + ingest rate in the recent window
  try {
    const perMarket = rows(
      await query(
        `SELECT count(close) AS c FROM ${FQ_MEASUREMENT} WHERE time > now() - ${ACTIVE_WINDOW} GROUP BY market`
      )
    )
    const activeMarkets = perMarket.filter(r => r.c > 0).length
    const totalPoints = perMarket.reduce((sum, r) => sum + (r.c || 0), 0)
    facts.activeMarkets = activeMarkets
    facts.pointsInWindow = totalPoints
    facts.expectedMarkets = EXPECTED_MARKETS

    let status = PASS
    if (activeMarkets === 0) status = FAIL
    else if (EXPECTED_MARKETS && activeMarkets < EXPECTED_MARKETS * 0.5) status = WARN

    const pct = EXPECTED_MARKETS
      ? ` of ${EXPECTED_MARKETS} configured (${Math.round((activeMarkets / EXPECTED_MARKETS) * 100)}%)`
      : ''
    add(
      'ingest',
      status,
      `${activeMarkets} markets active${pct}, ${totalPoints} bars in last ${ACTIVE_WINDOW}`
    )
  } catch (err) {
    add('ingest', WARN, `ingest query failed: ${err.message}`)
  }

  // 8. Sample bars so the data can be eyeballed
  try {
    const sample = rows(
      await query(
        `SELECT close, vbuy, vsell FROM ${FQ_MEASUREMENT} WHERE time > now() - ${ACTIVE_WINDOW} GROUP BY market ORDER BY time DESC LIMIT 1`
      )
    )
      .sort((a, b) => (b.vbuy + b.vsell || 0) - (a.vbuy + a.vsell || 0))
      .slice(0, 5)
      .map(r => ({
        market: r._tags.market,
        close: r.close,
        vbuy: Math.round(r.vbuy || 0),
        vsell: Math.round(r.vsell || 0),
        time: new Date(r.time).toISOString()
      }))
    facts.sample = sample
  } catch (err) {
    facts.sampleError = err.message
  }

  return { checks, facts }
}

function overall(checks) {
  if (checks.some(c => c.status === FAIL)) return FAIL
  if (checks.some(c => c.status === WARN)) return WARN
  return PASS
}

function printReport({ checks, facts }) {
  const status = overall(checks)
  console.log(
    `\n${color(status, `[${status}]`)} InfluxDB monitor @ ${new Date().toISOString()}  ${HOST}:${PORT}/${DB}`
  )
  for (const c of checks) {
    console.log(`  ${color(c.status, c.status.padEnd(4))}  ${c.name.padEnd(12)} ${c.detail}`)
  }
  if (facts.sample && facts.sample.length) {
    console.log(`        top markets by volume (last ${ACTIVE_WINDOW}):`)
    for (const s of facts.sample) {
      console.log(
        `         ${String(s.market).padEnd(24)} close=${s.close}  vbuy=${s.vbuy} vsell=${s.vsell}`
      )
    }
  }
}

async function once() {
  const result = await runChecks()
  if (args.json) {
    console.log(
      JSON.stringify(
        {
          timestamp: new Date().toISOString(),
          target: `${HOST}:${PORT}/${DB}`,
          overall: overall(result.checks),
          checks: result.checks,
          facts: result.facts
        },
        null,
        2
      )
    )
  } else {
    printReport(result)
  }
  return overall(result.checks)
}

async function main() {
  if (args.watch !== undefined) {
    const interval = (parseInt(args.watch, 10) || 60) * 1000
    console.log(`[monitor] watching ${HOST}:${PORT}/${DB} every ${interval / 1000}s (Ctrl+C to stop)`)
    // eslint-disable-next-line no-constant-condition
    while (true) {
      try {
        await once()
      } catch (err) {
        console.error(`[monitor] unexpected error: ${err.message}`)
      }
      await new Promise(r => setTimeout(r, interval))
    }
  } else {
    const status = await once()
    process.exit(status === FAIL ? 2 : status === WARN ? 1 : 0)
  }
}

main().catch(err => {
  console.error(`[monitor] fatal: ${err.message}`)
  process.exit(2)
})
