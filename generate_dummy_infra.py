"""
generate_dummy_infra.py — Synthetic AWS Infrastructure Data Generator
======================================================================

Simulates a realistic AWS production environment with:
  - 2× EC2 instances  (web servers)
  - 1× RDS instance   (PostgreSQL database)
  - 1× Lambda function
  - 1× ALB            (Application Load Balancer)

Writes synthetic metrics into the same SQLite DB that anomaly_detection.py reads.

Usage:
  # Seed 2 hours of historical data then run live (default)
  python generate_dummy_infra.py

  # Seed only — fill DB then exit
  python generate_dummy_infra.py --mode seed

  # Inject anomalies every N seconds for testing
  python generate_dummy_infra.py --inject-anomalies

  # Wipe DB and start fresh
  python generate_dummy_infra.py --reset

  # Show what resources & metrics will be generated
  python generate_dummy_infra.py --list

Anomaly scenarios injected (with --inject-anomalies or automatically after 3 min):
  1. EC2 CPU spike       → cpu_utilization_percent jumps to 94%
  2. RDS write latency   → write_latency_seconds jumps to 1.8s   (real incident)
  3. Lambda timeout      → duration_max_ms jumps to 29_500ms      (near 30s limit)
  4. ALB 5xx burst       → http_5xx_count set to 12               (always-bad)
  5. RDS freeable memory → freeable_memory_bytes drops to 60 MB   (hard limit)
  6. DB connections      → database_connections jumps to 85        (hard limit)
"""
from __future__ import annotations

import argparse
import math
import os
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import yaml

# ── Config path — same as anomaly_detection.py reads ────────────────────────
CONFIG_PATH = os.getenv("CONFIG_PATH", "config/cloud_observability.yaml")
DB_PATH     = "observability_data/metrics.db"

# ── Resolve DB path from config if possible ──────────────────────────────────
try:
    with open(CONFIG_PATH, encoding="utf-8") as _f:
        _cfg = yaml.safe_load(_f) or {}
    DB_PATH = _cfg.get("storage", {}).get("sqlite", {}).get("path", DB_PATH)
except Exception:
    pass

os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

# ── DDL — must match MetricsReader exactly ────────────────────────────────────
_DDL = """
CREATE TABLE IF NOT EXISTS metrics (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    collected_at  TEXT    NOT NULL,
    cloud         TEXT    NOT NULL DEFAULT 'aws',
    region        TEXT    NOT NULL,
    resource_type TEXT    NOT NULL,
    resource_id   TEXT    NOT NULL,
    resource_name TEXT    NOT NULL,
    metric_name   TEXT    NOT NULL,
    metric_unit   TEXT    NOT NULL,
    metric_value  REAL    NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_m_res_metric ON metrics(resource_id, metric_name, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_m_collected  ON metrics(collected_at DESC);
"""

# ═══════════════════════════════════════════════════════════════════════════════
# INFRASTRUCTURE DEFINITION
# Each resource has a list of metrics with realistic baseline distributions.
# Format: (metric_name, unit, mean, std, min_val, max_val)
# ═══════════════════════════════════════════════════════════════════════════════
RESOURCES = [
    # ── EC2: web-server-1 ──────────────────────────────────────────────────
    {
        "resource_type": "ec2",
        "resource_id":   "i-0abc123def456001",
        "resource_name": "web-server-1",
        "region":        "eu-north-1",
        "metrics": [
            # metric_name,                    unit,         mean,   std,    min,   max
            ("cpu_utilization_percent",        "percent",    32.0,   8.0,    2.0,   85.0),
            ("network_in_bytes",               "bytes",      850_000, 120_000, 50_000, 5_000_000),
            ("network_out_bytes",              "bytes",      620_000, 90_000,  30_000, 3_000_000),
            ("network_packets_in",             "count",      1_800,  300,    100,   8_000),
            ("network_packets_out",            "count",      1_400,  250,    80,    7_000),
            ("disk_queue_depth",               "count",      0.12,   0.05,   0.0,   0.5),
            ("read_iops",                      "count",      45.0,   12.0,   5.0,   120.0),
            ("write_iops",                     "count",      38.0,   10.0,   4.0,   100.0),
        ],
    },
    # ── EC2: web-server-2 ──────────────────────────────────────────────────
    {
        "resource_type": "ec2",
        "resource_id":   "i-0abc123def456002",
        "resource_name": "web-server-2",
        "region":        "eu-north-1",
        "metrics": [
            ("cpu_utilization_percent",        "percent",    28.0,   7.0,    2.0,   82.0),
            ("network_in_bytes",               "bytes",      790_000, 110_000, 40_000, 4_500_000),
            ("network_out_bytes",              "bytes",      580_000, 85_000,  25_000, 2_800_000),
            ("network_packets_in",             "count",      1_600,  280,    90,    7_500),
            ("network_packets_out",            "count",      1_300,  220,    70,    6_500),
            ("disk_queue_depth",               "count",      0.10,   0.04,   0.0,   0.4),
            ("read_iops",                      "count",      40.0,   11.0,   4.0,   110.0),
            ("write_iops",                     "count",      35.0,   9.0,    3.0,   95.0),
        ],
    },
    # ── RDS: database-1 ───────────────────────────────────────────────────
    {
        "resource_type": "rds",
        "resource_id":   "db-ABCDEF1234567890A",
        "resource_name": "database-1",
        "region":        "eu-north-1",
        "metrics": [
            # Latency — realistic healthy values (milliseconds scale, stored as seconds)
            ("read_latency_seconds",           "seconds",    0.0017, 0.0004, 0.0005, 0.015),
            ("write_latency_seconds",          "seconds",    0.0019, 0.0005, 0.0005, 0.018),
            # IOPS
            ("read_iops",                      "count",      2.9,    0.6,    0.5,    15.0),
            ("write_iops",                     "count",      1.3,    0.4,    0.2,    8.0),
            # Memory — healthy: 130–160 MB free
            ("freeable_memory_bytes",          "bytes",      145_000_000, 8_000_000, 105_000_000, 180_000_000),
            # Storage — 50 GB free
            ("free_storage_bytes",             "bytes",      50_000_000_000, 500_000_000, 5_000_000_000, 60_000_000_000),
            # Connections
            ("database_connections",           "count",      18.0,   4.0,    2.0,    75.0),
            # Network
            ("network_transmit_bytes_per_sec", "bytes",      16_000, 1_500,  2_000,  80_000),
            ("network_receive_bytes_per_sec",  "bytes",      1_400,  200,    200,    8_000),
            # Disk queue
            ("disk_queue_depth",               "count",      0.003,  0.001,  0.0,    0.05),
            # Replica lag (healthy: near 0)
            ("replica_lag_seconds",            "seconds",    0.5,    0.2,    0.0,    5.0),
        ],
    },
    # ── Lambda: api-processor ─────────────────────────────────────────────
    {
        "resource_type": "lambda",
        "resource_id":   "arn:aws:lambda:eu-north-1:123456789:function:api-processor",
        "resource_name": "api-processor",
        "region":        "eu-north-1",
        "metrics": [
            ("duration_avg_ms",                "ms",         120.0,  25.0,   20.0,   800.0),
            ("duration_max_ms",                "ms",         350.0,  80.0,   50.0,   2_000.0),
            ("invocations_total",              "count",      85.0,   20.0,   5.0,    500.0),
            ("concurrent_executions",          "count",      6.0,    2.0,    0.0,    40.0),
            ("unreserved_concurrent_executions","count",     6.0,    2.0,    0.0,    40.0),
            # errors_total starts at 0 — non-zero = always-bad
            ("errors_total",                   "count",      0.0,    0.0,    0.0,    0.0),
            # throttles_total starts at 0 — non-zero = always-bad
            ("throttles_total",                "count",      0.0,    0.0,    0.0,    0.0),
        ],
    },
    # ── ALB: main-load-balancer ───────────────────────────────────────────
    {
        "resource_type": "alb",
        "resource_id":   "app/main-load-balancer/abc123def456",
        "resource_name": "main-load-balancer",
        "region":        "eu-north-1",
        "metrics": [
            ("request_count",                  "count",      320.0,  60.0,   10.0,   2_000.0),
            ("target_response_time_s",         "seconds",    0.045,  0.012,  0.005,  0.5),
            ("healthy_host_count",             "count",      2.0,    0.0,    2.0,    2.0),  # always 2
            # http_5xx_count: always 0 in healthy state
            ("http_5xx_count",                 "count",      0.0,    0.0,    0.0,    0.0),
        ],
    },
]

# ═══════════════════════════════════════════════════════════════════════════════
# ANOMALY SCENARIOS — injected on demand or automatically
# ═══════════════════════════════════════════════════════════════════════════════
ANOMALY_SCENARIOS = [
    {
        "name":        "EC2 CPU spike",
        "resource_id": "i-0abc123def456001",
        "metric_name": "cpu_utilization_percent",
        "value":       94.0,     # triggers hard limit (>=90)
        "duration_s":  90,
    },
    {
        "name":        "RDS write latency spike",
        "resource_id": "db-ABCDEF1234567890A",
        "metric_name": "write_latency_seconds",
        "value":       1.85,     # triggers hard limit (>=1.0)
        "duration_s":  120,
    },
    {
        "name":        "Lambda near-timeout",
        "resource_id": "arn:aws:lambda:eu-north-1:123456789:function:api-processor",
        "metric_name": "duration_max_ms",
        "value":       29_500.0, # triggers hard limit (>=28000)
        "duration_s":  60,
    },
    {
        "name":        "ALB 5xx burst",
        "resource_id": "app/main-load-balancer/abc123def456",
        "metric_name": "http_5xx_count",
        "value":       12.0,     # triggers always-bad (non-zero)
        "duration_s":  60,
    },
    {
        "name":        "RDS memory critical",
        "resource_id": "db-ABCDEF1234567890A",
        "metric_name": "freeable_memory_bytes",
        "value":       60_000_000.0,  # triggers hard limit (<100 MB)
        "duration_s":  120,
    },
    {
        "name":        "RDS connection pool exhaustion",
        "resource_id": "db-ABCDEF1234567890A",
        "metric_name": "database_connections",
        "value":       85.0,          # triggers hard limit (>=80)
        "duration_s":  90,
    },
]


# ═══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════════════
def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_DDL)
    conn.commit()
    return conn


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts_iso(dt: datetime) -> str:
    return dt.isoformat()


def _clamp(val: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, val))


def _generate_value(
    mean: float, std: float, min_val: float, max_val: float,
    t_offset_hours: float = 0.0,
) -> float:
    """
    Generates a realistic metric value with:
    - Gaussian noise around mean
    - Mild time-of-day variation (±15% amplitude sinusoid, peak at 2pm)
    - Hard clamp to [min_val, max_val]
    """
    if std == 0.0:
        return mean  # constant metric (e.g. healthy_host_count, errors when healthy)

    # Time-of-day bias: resources are busier in business hours
    hour_fraction = (t_offset_hours % 24) / 24
    tod_factor    = 1.0 + 0.15 * math.sin(2 * math.pi * (hour_fraction - 14/24))
    biased_mean   = mean * tod_factor

    noise = random.gauss(biased_mean, std)
    return _clamp(noise, min_val, max_val)


def _insert_batch(conn: sqlite3.Connection, rows: List[tuple]) -> None:
    conn.executemany(
        "INSERT INTO metrics "
        "(collected_at, cloud, region, resource_type, resource_id, resource_name, "
        "metric_name, metric_unit, metric_value) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()


def _resource_map() -> Dict[str, dict]:
    """Returns {resource_id: resource_dict} for quick lookup."""
    return {r["resource_id"]: r for r in RESOURCES}


# ═══════════════════════════════════════════════════════════════════════════════
# SEED: fill historical data
# ═══════════════════════════════════════════════════════════════════════════════
def seed_history(conn: sqlite3.Connection, hours: int = 3, interval_seconds: int = 22) -> None:
    """
    Backfills `hours` of historical data at `interval_seconds` resolution.
    This gives anomaly_detection.py enough history to pass warmup checks
    (MIN_DATAPOINTS=30, WARMUP_MINUTES=20) immediately.
    """
    print(f"\n[Seed] Writing {hours}h of history at {interval_seconds}s intervals...")
    now    = datetime.now(timezone.utc)
    start  = now - timedelta(hours=hours)
    steps  = int((hours * 3600) / interval_seconds)

    batch: List[tuple] = []
    for step in range(steps):
        ts           = start + timedelta(seconds=step * interval_seconds)
        ts_str       = _ts_iso(ts)
        t_offset_h   = (ts - start).total_seconds() / 3600

        for res in RESOURCES:
            for (metric_name, unit, mean, std, mn, mx) in res["metrics"]:
                val = _generate_value(mean, std, mn, mx, t_offset_hours=t_offset_h)
                batch.append((
                    ts_str, "aws", res["region"], res["resource_type"],
                    res["resource_id"], res["resource_name"],
                    metric_name, unit, round(val, 6),
                ))

        if len(batch) >= 5000:
            _insert_batch(conn, batch)
            batch.clear()

    if batch:
        _insert_batch(conn, batch)

    total_resources = len(RESOURCES)
    total_metrics   = sum(len(r["metrics"]) for r in RESOURCES)
    print(
        f"[Seed] Done. {steps} cycles × {total_metrics} metrics "
        f"across {total_resources} resources = {steps * total_metrics:,} rows."
    )
    print(f"[Seed] DB: {DB_PATH}")
    print()


# ═══════════════════════════════════════════════════════════════════════════════
# LIVE: continuous metric emission
# ═══════════════════════════════════════════════════════════════════════════════
class AnomalyInjector:
    """Tracks active anomaly overrides. Thread-safe via simple dict."""

    def __init__(self) -> None:
        self._active: Dict[Tuple[str, str], Tuple[float, float]] = {}
        # {(resource_id, metric_name): (override_value, expires_at_unix)}

    def inject(self, resource_id: str, metric_name: str, value: float, duration_s: int) -> None:
        expires = time.time() + duration_s
        self._active[(resource_id, metric_name)] = (value, expires)
        print(f"\n  🔴 INJECTING anomaly: {metric_name}={value} on {resource_id} for {duration_s}s")

    def get_override(self, resource_id: str, metric_name: str) -> Optional[float]:
        key = (resource_id, metric_name)
        if key not in self._active:
            return None
        val, expires = self._active[key]
        if time.time() > expires:
            del self._active[key]
            return None
        return val

    def clear_expired(self) -> None:
        now = time.time()
        expired = [k for k, (_, exp) in self._active.items() if now > exp]
        for k in expired:
            del self._active[k]
            print(f"  ✅ Anomaly cleared: {k[1]} on {k[0]}")


def emit_live(
    conn: sqlite3.Connection,
    interval_s: int = 22,
    inject_anomalies: bool = False,
    auto_inject_after_s: int = 180,
) -> None:
    """
    Emits one row per metric per resource every `interval_s` seconds.
    If inject_anomalies=True, cycles through ANOMALY_SCENARIOS automatically.
    """
    injector       = AnomalyInjector()
    scenario_index = 0
    start_time     = time.time()
    last_inject_t  = start_time

    total_metrics = sum(len(r["metrics"]) for r in RESOURCES)
    print(f"[Live] Emitting {total_metrics} metrics every {interval_s}s across {len(RESOURCES)} resources.")
    if inject_anomalies:
        print(f"[Live] Anomaly injection ON — first scenario in {auto_inject_after_s}s.")
    print("[Live] Ctrl+C to stop.\n")

    try:
        cycle = 0
        while True:
            cycle += 1
            now       = datetime.now(timezone.utc)
            now_str   = _ts_iso(now)
            t_off_h   = (time.time() - start_time) / 3600
            batch: List[tuple] = []

            injector.clear_expired()

            # Auto-inject next scenario
            if inject_anomalies and (time.time() - last_inject_t) >= auto_inject_after_s:
                scenario     = ANOMALY_SCENARIOS[scenario_index % len(ANOMALY_SCENARIOS)]
                scenario_index += 1
                last_inject_t = time.time()
                injector.inject(
                    scenario["resource_id"],
                    scenario["metric_name"],
                    scenario["value"],
                    scenario["duration_s"],
                )
                print(f"  📌 Scenario: {scenario['name']}")

            for res in RESOURCES:
                for (metric_name, unit, mean, std, mn, mx) in res["metrics"]:
                    override = injector.get_override(res["resource_id"], metric_name)
                    val = override if override is not None else _generate_value(mean, std, mn, mx, t_off_h)
                    batch.append((
                        now_str, "aws", res["region"], res["resource_type"],
                        res["resource_id"], res["resource_name"],
                        metric_name, unit, round(val, 6),
                    ))

            _insert_batch(conn, batch)
            print(
                f"[Live] Cycle #{cycle:4d} | {now_str[:19]}Z | "
                f"{len(batch)} rows written | "
                f"Active overrides: {sum(1 for _ in injector._active)}"
            )
            time.sleep(interval_s)

    except KeyboardInterrupt:
        print("\n[Live] Stopped.")


# ═══════════════════════════════════════════════════════════════════════════════
# CLI
# ═══════════════════════════════════════════════════════════════════════════════
def _list_resources() -> None:
    print("\nDummy AWS Infrastructure:")
    print("─" * 70)
    for res in RESOURCES:
        print(f"  [{res['resource_type'].upper():6}] {res['resource_name']} ({res['resource_id']})")
        print(f"           region: {res['region']}")
        for m in res["metrics"]:
            print(f"           • {m[0]:42s} mean={m[2]}, std={m[3]}, unit={m[1]}")
        print()
    print("Anomaly Scenarios:")
    print("─" * 70)
    for i, s in enumerate(ANOMALY_SCENARIOS, 1):
        print(f"  {i}. {s['name']:35s} value={s['value']}  ({s['duration_s']}s)")
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Dummy AWS infrastructure data generator for anomaly_detection.py"
    )
    parser.add_argument(
        "--mode", choices=["seed", "live", "both"], default="both",
        help="seed=fill history only, live=emit live only, both=seed then emit live (default: both)",
    )
    parser.add_argument(
        "--seed-hours", type=int, default=3,
        help="Hours of historical data to seed (default: 3)",
    )
    parser.add_argument(
        "--interval", type=int, default=22,
        help="Seconds between live metric emissions (default: 22, matches detector)",
    )
    parser.add_argument(
        "--inject-anomalies", action="store_true",
        help="Automatically cycle through anomaly scenarios every 3 minutes",
    )
    parser.add_argument(
        "--inject-interval", type=int, default=180,
        help="Seconds between anomaly injections (default: 180)",
    )
    parser.add_argument(
        "--reset", action="store_true",
        help="Delete the DB and start completely fresh",
    )
    parser.add_argument(
        "--list", action="store_true",
        help="Print resource and metric definitions, then exit",
    )
    args = parser.parse_args()

    if args.list:
        _list_resources()
        return

    if args.reset and os.path.exists(DB_PATH):
        os.remove(DB_PATH)
        print(f"[Reset] Deleted {DB_PATH}")

    print("=" * 70)
    print("  Dummy AWS Infrastructure Generator")
    print(f"  DB      : {DB_PATH}")
    print(f"  Config  : {CONFIG_PATH}")
    print(f"  Mode    : {args.mode}")
    print(f"  Anomalies: {'auto-injecting every ' + str(args.inject_interval) + 's' if args.inject_anomalies else 'off'}")
    print("=" * 70)

    conn = _connect(DB_PATH)

    if args.mode in ("seed", "both"):
        seed_history(conn, hours=args.seed_hours, interval_seconds=args.interval)

    if args.mode in ("live", "both"):
        emit_live(
            conn,
            interval_s=args.interval,
            inject_anomalies=args.inject_anomalies,
            auto_inject_after_s=args.inject_interval,
        )


if __name__ == "__main__":
    main()