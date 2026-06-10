"""InfluxDB liveness + write-freshness check for aggr-server.

One-shot check: pings InfluxDB /ping, queries the prefixed retention policy
(e.g. aggr_10s.trades_10s) for the newest write, and reports to healthchecks.io
with /start + (success|fail) pings.

Configuration precedence (high to low):
  1. CLI flags
  2. Env vars (AGGR_INFLUX_HOST, AGGR_INFLUX_PORT, AGGR_INFLUX_DATABASE,
     STALE_SECONDS, FAIL_AFTER, HEALTHCHECK_URL, HEALTHCHECK_UUID)
  3. config.json in the repo (influxHost, influxPort, influxDatabase,
     influxRetentionPrefix)
  4. Defaults that mirror src/config.js

The AGGR_ prefix on env vars is intentional: aggr-server's own INFLUX_HOST
env var resolves to "aggr-influx" (the docker hostname) inside the container
network, which is wrong when running this check from the host.

Staleness is debounced: a stale/no-data/error result only reports /fail to
healthchecks.io after --fail-after consecutive occurrences (default 2). Earlier
occurrences send a success ping whose body is marked "suppressed", so transient
stalls show up in the event history without firing a DOWN alert. An unreachable
InfluxDB always fails immediately. Debouncing needs loop mode; --once has no
state between runs and never suppresses.

Exit codes: 0 healthy, 1 stale / no data, 2 InfluxDB unreachable.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any

import httpx

# Defaults mirror src/config.js
DEFAULTS = {
    "host": "localhost",
    "port": 8086,
    "database": "significant_trades",
    "rp_prefix": "aggr_",
    "timeframe": "10s",
    "stale_seconds": 600,
    "fail_after": 2,
}

HC_DEFAULT_BASE = "https://hc-ping.com"

INFLUX_TIMEOUT = 20.0
HC_TIMEOUT = 5.0


@dataclass
class Config:
    host: str
    port: int
    database: str
    rp: str            # e.g. "aggr_10s"
    measurement: str   # e.g. "trades_10s"
    stale_seconds: int
    fail_after: int    # consecutive stale results before reporting /fail
    hc_url: str | None
    no_ping: bool

    @property
    def fq_measurement(self) -> str:
        """Quoted, fully qualified `"rp"."measurement"` for InfluxQL FROM clauses."""
        return f'"{self.rp}"."{self.measurement}"'

    @property
    def influx_base(self) -> str:
        return f"http://{self.host}:{self.port}"


def find_config_json() -> Path | None:
    """Walk up from cwd and from this module's location looking for config.json."""
    candidates: list[Path] = [Path.cwd() / "config.json"]
    candidates += [p / "config.json" for p in Path.cwd().resolve().parents]
    here = Path(__file__).resolve()
    candidates += [p / "config.json" for p in here.parents]
    seen: set[Path] = set()
    for c in candidates:
        try:
            c = c.resolve()
        except OSError:
            continue
        if c in seen:
            continue
        seen.add(c)
        if c.is_file():
            return c
    return None


def load_config_json() -> dict[str, Any]:
    p = find_config_json()
    if not p:
        return {}
    try:
        return json.loads(p.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def resolve_hc_url(cli_url: str | None) -> str | None:
    if cli_url:
        return cli_url.rstrip("/")
    env_url = os.environ.get("HEALTHCHECK_URL")
    if env_url:
        return env_url.rstrip("/")
    uuid = os.environ.get("HEALTHCHECK_UUID")
    if uuid:
        return f"{HC_DEFAULT_BASE}/{uuid}"
    return None


def resolve_config(args: argparse.Namespace) -> Config:
    file_cfg = load_config_json()

    def pick(cli_val: Any, env_name: str | None, json_key: str | None, default: Any) -> Any:
        if cli_val is not None:
            return cli_val
        if env_name:
            val = os.environ.get(env_name)
            if val:
                return val
        if json_key and json_key in file_cfg:
            return file_cfg[json_key]
        return default

    host = pick(args.host, "AGGR_INFLUX_HOST", "influxHost", DEFAULTS["host"])
    port = int(pick(args.port, "AGGR_INFLUX_PORT", "influxPort", DEFAULTS["port"]))
    database = pick(args.db, "AGGR_INFLUX_DATABASE", "influxDatabase", DEFAULTS["database"])
    rp_prefix = pick(None, None, "influxRetentionPrefix", DEFAULTS["rp_prefix"])
    timeframe = DEFAULTS["timeframe"]
    stale = int(pick(args.stale, "STALE_SECONDS", None, DEFAULTS["stale_seconds"]))
    fail_after = max(1, int(pick(args.fail_after, "FAIL_AFTER", None, DEFAULTS["fail_after"])))

    return Config(
        host=str(host),
        port=port,
        database=str(database),
        rp=f"{rp_prefix}{timeframe}",
        measurement=f"trades_{timeframe}",
        stale_seconds=stale,
        fail_after=fail_after,
        hc_url=resolve_hc_url(args.hc_url),
        no_ping=args.no_ping,
    )


def hc_ping(cfg: Config, suffix: str, body: str) -> None:
    """Best-effort ping; logs but never raises so the check exits cleanly."""
    if cfg.no_ping or not cfg.hc_url:
        return
    url = cfg.hc_url + suffix
    try:
        httpx.post(url, content=body.encode("utf-8")[:100_000], timeout=HC_TIMEOUT)
    except httpx.RequestError as e:
        sys.stderr.write(f"[hc] warning: ping to {url} failed: {e}\n")


def check_ping(client: httpx.Client, cfg: Config) -> tuple[bool, str]:
    try:
        r = client.get(f"{cfg.influx_base}/ping", params={"verbose": "true"}, timeout=INFLUX_TIMEOUT)
    except httpx.RequestError as e:
        return False, f"unreachable: {e}"
    if r.status_code not in (200, 204):
        return False, f"unexpected HTTP {r.status_code}"
    version = r.headers.get("X-Influxdb-Version", "unknown")
    return True, f"InfluxDB {version}"


def check_freshness(client: httpx.Client, cfg: Config) -> tuple[str, int | None, str]:
    """Returns (status, age_seconds, detail). status in {FRESH, STALE, NO_DATA, ERROR}."""
    # Bound the scan window so InfluxDB doesn't sweep every shard in the
    # measurement. Generous multiple of the stale threshold keeps the age
    # detail informative when writes are lagging but not yet dead.
    lookback_s = max(cfg.stale_seconds * 4, 1800)
    params = {
        "db": cfg.database,
        "epoch": "s",
        "q": (
            f"SELECT * FROM {cfg.fq_measurement} "
            f"WHERE time > now() - {lookback_s}s "
            f"ORDER BY time DESC LIMIT 1"
        ),
    }
    try:
        r = client.get(f"{cfg.influx_base}/query", params=params, timeout=INFLUX_TIMEOUT)
    except httpx.RequestError as e:
        return "ERROR", None, f"query failed: {e}"
    if r.status_code != 200:
        return "ERROR", None, f"HTTP {r.status_code}: {r.text[:200]}"
    try:
        data = r.json()
    except ValueError as e:
        return "ERROR", None, f"bad JSON: {e}"
    results = data.get("results", [])
    if results and "error" in results[0]:
        return "ERROR", None, f"influx error: {results[0]['error']}"
    series = (results[0] if results else {}).get("series", [])
    if not series or not series[0].get("values"):
        return "NO_DATA", None, f"no rows in {cfg.fq_measurement}"
    last_ts = int(series[0]["values"][0][0])
    age = int(time.time()) - last_ts
    last_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(last_ts))
    detail = f"last write {age}s ago ({last_iso})"
    if age <= cfg.stale_seconds:
        return "FRESH", age, detail
    return "STALE", age, f"{detail} > {cfg.stale_seconds}s threshold"


def make_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="check-influx",
        description="InfluxDB liveness + write-freshness check for aggr-server, with healthchecks.io ping.",
    )
    p.add_argument("--host", help="InfluxDB host (default: $AGGR_INFLUX_HOST / config.json / localhost)")
    p.add_argument("--port", type=int, help="InfluxDB port (default: 8086)")
    p.add_argument("--db", help="InfluxDB database (default: significant_trades)")
    p.add_argument("--stale", type=int, help="Max seconds since last write before STALE (default 600)")
    p.add_argument(
        "--fail-after",
        type=int,
        help="Consecutive stale/no-data/error results before reporting /fail (default 2, or "
        "$FAIL_AFTER). Earlier results ping success with a 'suppressed' body. Loop mode only; "
        "--once always reports immediately. Unreachable InfluxDB always fails immediately.",
    )
    p.add_argument(
        "--hc-url",
        help="Full healthchecks.io ping URL (e.g. https://hc-ping.com/<uuid>). "
        "Also reads $HEALTHCHECK_URL or $HEALTHCHECK_UUID.",
    )
    p.add_argument("--no-ping", action="store_true", help="Skip all healthchecks.io pings (local testing)")
    p.add_argument(
        "--interval",
        type=int,
        help="Seconds between checks when running as a loop (default 600, or $INTERVAL_SECONDS)",
    )
    p.add_argument("--once", action="store_true", help="Run a single check and exit (default: loop forever)")
    return p


def run_once(client: httpx.Client, cfg: Config, prior_failures: int = 0) -> int:
    """One check cycle. `prior_failures` is the number of consecutive non-FRESH
    results immediately before this run; a non-FRESH result here only reports
    /fail once the streak reaches cfg.fail_after, otherwise it sends a success
    ping with a "suppressed" body so the stall is logged but doesn't alert."""
    ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    header = (
        f"== InfluxDB check @ {ts}: {cfg.influx_base}/{cfg.database}  "
        f"RP={cfg.rp}  stale>{cfg.stale_seconds}s =="
    )
    lines: list[str] = [header]

    # Signal start to healthchecks.io (best-effort).
    hc_ping(cfg, "/start", body=header + "\n")

    up, ping_detail = check_ping(client, cfg)
    lines.append(f"  ping        {'UP  ' if up else 'DOWN'}  {ping_detail}")
    if not up:
        report = "\n".join(lines) + "\noverall: FAIL (unreachable)"
        print(report, flush=True)
        hc_ping(cfg, "/fail", body=report)
        return 2

    status, _age, fresh_detail = check_freshness(client, cfg)
    lines.append(f"  freshness   {status:<5}  {fresh_detail}")

    if status == "FRESH":
        report = "\n".join(lines) + "\noverall: OK"
        print(report, flush=True)
        hc_ping(cfg, "", body=report)
        return 0

    failures = prior_failures + 1
    if failures < cfg.fail_after:
        report = "\n".join(lines) + (
            f"\noverall: {status} (suppressed {failures}/{cfg.fail_after}, "
            f"pinging success — will FAIL after {cfg.fail_after} consecutive)"
        )
        print(report, flush=True)
        hc_ping(cfg, "", body=report)
        return 1

    streak = f", {failures} consecutive" if failures > 1 else ""
    report = "\n".join(lines) + f"\noverall: FAIL ({status.lower()}{streak})"
    print(report, flush=True)
    hc_ping(cfg, "/fail", body=report)
    return 1


def run_loop(cfg: Config, interval: int) -> int:
    """Self-scheduled loop. Fixed cadence: next iteration scheduled `interval`
    seconds after the previous one started, so pings arrive on a regular beat
    even when a check takes a moment. Continues across transient errors so the
    healthchecks.io schedule stays intact."""
    print(f"[loop] running check every {interval}s (Ctrl-C to stop)", flush=True)
    next_run = time.monotonic()
    consecutive_failures = 0
    try:
        with httpx.Client() as client:
            while True:
                try:
                    rc = run_once(client, cfg, consecutive_failures)
                    consecutive_failures = 0 if rc == 0 else consecutive_failures + 1
                except Exception as e:  # noqa: BLE001 — keep the daemon alive
                    sys.stderr.write(f"[loop] iteration error: {e!r}\n")
                next_run += interval
                sleep_for = next_run - time.monotonic()
                if sleep_for < 0:
                    next_run = time.monotonic()  # drifted past schedule; reset
                    continue
                time.sleep(sleep_for)
    except KeyboardInterrupt:
        print("\n[loop] stopped by signal", flush=True)
        return 0


def main(argv: list[str] | None = None) -> int:
    args = make_parser().parse_args(argv)
    cfg = resolve_config(args)

    if args.once:
        # No state between --once runs, so debouncing can't apply.
        cfg = replace(cfg, fail_after=1)
        with httpx.Client() as client:
            return run_once(client, cfg)

    interval = args.interval
    if interval is None:
        interval = int(os.environ.get("INTERVAL_SECONDS", 600))
    return run_loop(cfg, interval)


if __name__ == "__main__":
    sys.exit(main())
