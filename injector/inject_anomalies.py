#!/usr/bin/env python3
"""
inject_anomalies.py
===================
Injects controlled anomalies into the testing infrastructure so you can
validate that anomaly_detection.py correctly catches them (and doesn't
fire on normal traffic — false-positive check).

Two injection strategies
────────────────────────
1. DB Injection  — writes synthetic metric rows directly into SQLite.
   Fast, deterministic, no network needed.
   Use this to unit-test every detector layer.

2. HTTP/AWS Injection — generates real traffic to the ALB / Lambda
   endpoint that actually stresses the resource.
   Use this for end-to-end validation.

Usage examples:
    # See all scenarios
    python3 injector/inject_anomalies.py --list

    # DB injection: spike CPU on one EC2
    python3 injector/inject_anomalies.py --scenario cpu_spike --resource-id i-0abc123

    # DB injection: drain RDS free storage
    python3 injector/inject_anomalies.py --scenario low_storage --resource-id mydb-identifier

    # DB injection: Lambda error burst
    python3 injector/inject_anomalies.py --scenario lambda_errors --resource-id my-function

    # HTTP injection: hit the /slow endpoint on ALB (real latency anomaly)
    python3 injector/inject_anomalies.py --scenario alb_latency --alb-url http://your-alb.amazonaws.com

    # Restore normal baseline after a scenario
    python3 injector/inject_anomalies.py --scenario restore --resource-id i-0abc123 --metric cpu_utilization_percent

Requirements: pip install requests pyyaml
"""
from __future__ import annotations

import argparse
import logging
import os
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import yaml

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [injector] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/injector.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("injector")

# ── Scenario definitions ──────────────────────────────────────────────────────
# Each scenario: name → list of (metric_name, unit, value_fn, n_points, interval_secs)
# value_fn(i) returns the value for the i-th injected point

def _normal_cpu():      return random.gauss(25, 5)      # typical CPU
def _spike_cpu():       return random.gauss(94, 2)      # anomaly: > 90% hard limit
def _normal_mem_bytes():return random.gauss(512_000_000, 50_000_000)  # ~512 MB free
def _low_mem_bytes():   return random.gauss(60_000_000,  5_000_000)   # < 100 MB → hard limit
def _normal_storage():  return random.gauss(15_000_000_000, 500_000_000)
def _low_storage():     return random.gauss(800_000_000,   50_000_000) # < 1 GB → hard limit
def _normal_conn():     return random.gauss(10, 3)
def _high_conn():       return random.gauss(85, 3)       # > 80 → hard limit
def _normal_lat():      return random.gauss(0.005, 0.001)
def _high_lat():        return random.gauss(1.5, 0.2)    # > 1.0s → hard limit
def _zero_errors():     return 0.0
def _errors():          return random.gauss(5, 1)        # always_bad metric
def _normal_duration(): return random.gauss(50, 10)      # ms
def _slow_duration():   return random.gauss(26_000, 500) # > 28s → hard limit
def _normal_resp():     return random.gauss(0.05, 0.01)
def _slow_resp():       return random.gauss(7.0, 0.5)    # > 5s → hard limit

SCENARIOS: Dict[str, Dict[str, Any]] = {
    # ── EC2 scenarios ──────────────────────────────────────────────────────────
    "cpu_spike": {
        "description": "CPU spikes to ~94% — triggers hard_limit (ceiling 90%)",
        "resource_type": "ec2",
        "metrics": [
            ("cpu_utilization_percent", "percent", _spike_cpu, 10, 60),
        ],
    },
    "cpu_gradual": {
        "description": "CPU rises gradually — tests z-score / isoforest (no hard limit until 90%)",
        "resource_type": "ec2",
        "metrics": [
            ("cpu_utilization_percent", "percent",
             lambda: 25 + (time.time() % 600) / 6,  # ramps 25→125 over 10 min
             20, 30),
        ],
    },
    "memory_low": {
        "description": "Freeable memory drops to 60 MB — triggers hard_limit (floor 100 MB)",
        "resource_type": "ec2",
        "metrics": [
            ("freeable_memory_bytes", "bytes", _low_mem_bytes, 10, 60),
        ],
    },
    "normal_ec2": {
        "description": "Inject 60 min of normal EC2 baseline (use BEFORE anomaly scenarios for warm-up)",
        "resource_type": "ec2",
        "metrics": [
            ("cpu_utilization_percent",  "percent", _normal_cpu,        60, 60),
            ("network_in_bytes",         "bytes",   lambda: random.gauss(1_000_000, 200_000), 60, 60),
            ("network_out_bytes",        "bytes",   lambda: random.gauss(500_000,   100_000), 60, 60),
            ("status_check_failed",      "count",   _zero_errors,        60, 60),
        ],
    },

    # ── RDS scenarios ──────────────────────────────────────────────────────────
    "low_storage": {
        "description": "Free storage drops to 800 MB — triggers hard_limit (floor 1 GB)",
        "resource_type": "rds",
        "metrics": [
            ("free_storage_bytes", "bytes", _low_storage, 10, 60),
        ],
    },
    "db_connections_high": {
        "description": "DB connections rise to ~85 — triggers hard_limit (ceiling 80)",
        "resource_type": "rds",
        "metrics": [
            ("database_connections", "count", _high_conn, 10, 60),
        ],
    },
    "db_latency_high": {
        "description": "Read/write latency at 1.5s — triggers hard_limit (ceiling 1.0s)",
        "resource_type": "rds",
        "metrics": [
            ("read_latency_seconds",  "seconds", _high_lat, 10, 60),
            ("write_latency_seconds", "seconds", _high_lat, 10, 60),
        ],
    },
    "normal_rds": {
        "description": "60 min of normal RDS baseline (warm-up)",
        "resource_type": "rds",
        "metrics": [
            ("cpu_utilization_percent",  "percent", _normal_cpu,          60, 60),
            ("database_connections",     "count",   _normal_conn,         60, 60),
            ("freeable_memory_bytes",    "bytes",   _normal_mem_bytes,    60, 60),
            ("free_storage_bytes",       "bytes",   _normal_storage,      60, 60),
            ("read_latency_seconds",     "seconds", _normal_lat,          60, 60),
            ("write_latency_seconds",    "seconds", _normal_lat,          60, 60),
        ],
    },

    # ── Lambda scenarios ───────────────────────────────────────────────────────
    "lambda_errors": {
        "description": "Lambda errors spike — triggers always_bad",
        "resource_type": "lambda",
        "metrics": [
            ("errors_total",  "count", _errors,     10, 60),
            ("duration_avg_ms","ms",   _normal_duration, 10, 60),
        ],
    },
    "lambda_throttles": {
        "description": "Lambda throttles — triggers always_bad",
        "resource_type": "lambda",
        "metrics": [
            ("throttles_total", "count", lambda: random.gauss(15, 3), 10, 60),
        ],
    },
    "lambda_timeout": {
        "description": "Lambda duration near 28s — triggers hard_limit (ceiling 28000ms)",
        "resource_type": "lambda",
        "metrics": [
            ("duration_max_ms", "ms", _slow_duration, 10, 60),
        ],
    },
    "normal_lambda": {
        "description": "60 min of normal Lambda baseline (warm-up)",
        "resource_type": "lambda",
        "metrics": [
            ("invocations_total",  "count", lambda: random.gauss(60, 10),  60, 60),
            ("errors_total",       "count", _zero_errors,                  60, 60),
            ("throttles_total",    "count", _zero_errors,                  60, 60),
            ("duration_avg_ms",    "ms",    _normal_duration,              60, 60),
            ("concurrent_executions","count",lambda: random.gauss(5, 2),   60, 60),
        ],
    },

    # ── ALB scenarios ──────────────────────────────────────────────────────────
    "alb_5xx": {
        "description": "ALB target 5xx count goes non-zero — triggers always_bad",
        "resource_type": "alb",
        "metrics": [
            ("http_5xx_count",       "count", lambda: random.gauss(20, 5), 10, 60),
            ("target_response_time_s","seconds", _normal_resp,             10, 60),
        ],
    },
    "alb_latency": {
        "description": "ALB target response time at 7s — triggers hard_limit (ceiling 5s)",
        "resource_type": "alb",
        "metrics": [
            ("target_response_time_s", "seconds", _slow_resp, 10, 60),
        ],
    },
    "alb_unhealthy": {
        "description": "Unhealthy host count drops healthy hosts — triggers always_bad",
        "resource_type": "alb",
        "metrics": [
            ("unhealthy_host_count", "count", lambda: 1.0, 10, 60),
            ("healthy_host_count",   "count", lambda: 1.0, 10, 60),  # drops to 1
        ],
    },
    "normal_alb": {
        "description": "60 min of normal ALB baseline (warm-up)",
        "resource_type": "alb",
        "metrics": [
            ("request_count",         "count",   lambda: random.gauss(100, 20),   60, 60),
            ("target_response_time_s","seconds", _normal_resp,                    60, 60),
            ("http_5xx_count",        "count",   _zero_errors,                    60, 60),
            ("healthy_host_count",    "count",   lambda: 2.0,                     60, 60),
            ("unhealthy_host_count",  "count",   _zero_errors,                    60, 60),
        ],
    },

    # ── Multi-metric correlated incident ──────────────────────────────────────
    "correlated_incident": {
        "description": "CPU + DB connections + latency spike at once — tests correlation grouping",
        "resource_type": "rds",
        "metrics": [
            ("cpu_utilization_percent", "percent", _spike_cpu,  5, 60),
            ("database_connections",    "count",   _high_conn,  5, 60),
            ("read_latency_seconds",    "seconds", _high_lat,   5, 60),
        ],
    },

    # ── False-positive probes (should NOT fire) ────────────────────────────────
    "fp_probe_ec2": {
        "description": "Normal EC2 metrics — anomaly detector should stay SILENT",
        "resource_type": "ec2",
        "metrics": [
            ("cpu_utilization_percent", "percent", _normal_cpu,  30, 60),
            ("status_check_failed",     "count",   _zero_errors, 30, 60),
        ],
    },
    "fp_probe_rds": {
        "description": "Normal RDS metrics — anomaly detector should stay SILENT",
        "resource_type": "rds",
        "metrics": [
            ("cpu_utilization_percent", "percent", _normal_cpu,       30, 60),
            ("database_connections",    "count",   _normal_conn,      30, 60),
            ("read_latency_seconds",    "seconds", _normal_lat,       30, 60),
            ("free_storage_bytes",      "bytes",   _normal_storage,   30, 60),
        ],
    },
}


# ── DB injection ──────────────────────────────────────────────────────────────

def _lookup_resource(conn: sqlite3.Connection, resource_id: str) -> Optional[Dict]:
    """Try to find resource metadata from existing metrics table."""
    row = conn.execute(
        "SELECT cloud, region, resource_type, resource_id, resource_name "
        "FROM metrics WHERE resource_id=? LIMIT 1",
        (resource_id,),
    ).fetchone()
    if row:
        return {
            "cloud": row[0], "region": row[1],
            "resource_type": row[2], "resource_id": row[3], "resource_name": row[4],
        }
    return None


def inject_db(
    conn: sqlite3.Connection,
    resource: Dict[str, Any],
    scenario: Dict[str, Any],
    backfill_minutes: int = 0,
) -> None:
    """
    Insert synthetic metric rows into the metrics table.

    backfill_minutes > 0 → write rows back in time (for warm-up injection).
    backfill_minutes = 0 → write rows at current time (anomaly injection).
    """
    now  = datetime.now(timezone.utc)
    rows = []

    for metric_name, unit, value_fn, n_points, interval_s in scenario["metrics"]:
        for i in range(n_points):
            if backfill_minutes > 0:
                # Spread across the past backfill_minutes minutes
                offset_s = (backfill_minutes * 60) - i * interval_s
                ts = (now - timedelta(seconds=offset_s)).isoformat()
            else:
                # Cluster around now
                ts = (now + timedelta(seconds=i * interval_s)).isoformat()

            value = value_fn()
            rows.append((
                resource.get("cloud",         "aws"),
                resource.get("region",        "ap-south-1"),
                resource.get("resource_type", scenario.get("resource_type", "unknown")),
                resource["resource_id"],
                resource.get("resource_name", resource["resource_id"]),
                metric_name, unit,
                max(0.0, value),
                ts,
            ))

    conn.executemany(
        "INSERT INTO metrics "
        "(cloud,region,resource_type,resource_id,resource_name,"
        "metric_name,metric_unit,metric_value,collected_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    log.info(f"Injected {len(rows)} rows for scenario '{scenario.get('_name', '?')}' "
             f"into resource '{resource['resource_id']}'")


def restore_normal(
    conn: sqlite3.Connection,
    resource_id: str,
    metric_name: str,
    n_points: int = 10,
) -> None:
    """
    Append a run of normal-looking values to push the anomalous ones
    out of the detector's lookback window.
    """
    normal_values = {
        "cpu_utilization_percent":   (_normal_cpu,        "percent"),
        "freeable_memory_bytes":     (_normal_mem_bytes,  "bytes"),
        "free_storage_bytes":        (_normal_storage,    "bytes"),
        "database_connections":      (_normal_conn,       "count"),
        "read_latency_seconds":      (_normal_lat,        "seconds"),
        "write_latency_seconds":     (_normal_lat,        "seconds"),
        "errors_total":              (_zero_errors,       "count"),
        "throttles_total":           (_zero_errors,       "count"),
        "duration_avg_ms":           (_normal_duration,   "ms"),
        "target_response_time_s":    (_normal_resp,       "seconds"),
    }
    if metric_name not in normal_values:
        log.warning(f"No normal baseline defined for {metric_name!r}")
        return

    fn, unit = normal_values[metric_name]
    meta = _lookup_resource(conn, resource_id)
    if not meta:
        log.error(f"Resource {resource_id!r} not found in DB")
        return

    now  = datetime.now(timezone.utc)
    rows = []
    for i in range(n_points):
        ts = (now + timedelta(seconds=i * 60)).isoformat()
        rows.append((
            meta["cloud"], meta["region"], meta["resource_type"],
            resource_id, meta["resource_name"],
            metric_name, unit, max(0.0, fn()), ts,
        ))
    conn.executemany(
        "INSERT INTO metrics "
        "(cloud,region,resource_type,resource_id,resource_name,"
        "metric_name,metric_unit,metric_value,collected_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        rows,
    )
    conn.commit()
    log.info(f"Restored {n_points} normal points for {resource_id}/{metric_name}")


# ── HTTP / AWS injection ──────────────────────────────────────────────────────

def inject_http_latency(alb_url: str, n_requests: int = 20, concurrency: int = 5) -> None:
    """Hit /slow on the ALB to generate real latency anomalies in CloudWatch."""
    try:
        import threading
        import requests
    except ImportError:
        log.error("pip install requests")
        return

    url = alb_url.rstrip("/") + "/slow"
    log.info(f"Sending {n_requests} slow requests to {url}...")

    def do_request():
        try:
            r = requests.get(url, timeout=30)
            log.info(f"  /slow → {r.status_code} ({r.elapsed.total_seconds():.2f}s)")
        except Exception as e:
            log.warning(f"  request failed: {e}")

    threads = []
    for _ in range(n_requests):
        t = threading.Thread(target=do_request)
        t.start()
        threads.append(t)
        if len(threads) >= concurrency:
            for t in threads:
                t.join()
            threads = []
    for t in threads:
        t.join()

    log.info("HTTP latency injection complete. "
             "Wait ~2 minutes for CloudWatch to reflect in DB.")


def inject_http_errors(alb_url: str, n_requests: int = 30) -> None:
    """Hit /error on the ALB to generate real 5xx metrics in CloudWatch."""
    try:
        import requests
    except ImportError:
        log.error("pip install requests")
        return

    url = alb_url.rstrip("/") + "/error"
    log.info(f"Sending {n_requests} error requests to {url}...")
    for i in range(n_requests):
        try:
            r = requests.get(url, timeout=10)
            log.info(f"  [{i+1}/{n_requests}] /error → {r.status_code}")
        except Exception as e:
            log.warning(f"  request failed: {e}")
        time.sleep(0.5)


def inject_lambda_slow(function_name: str, region: str, n: int = 5) -> None:
    """Invoke Lambda with mode=slow to generate real duration anomalies."""
    try:
        import boto3, json
    except ImportError:
        log.error("pip install boto3")
        return

    client = boto3.client("lambda", region_name=region)
    log.info(f"Invoking {function_name} with mode=slow ({n}x)...")
    for i in range(n):
        try:
            resp = client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps({"mode": "slow"}),
            )
            log.info(f"  [{i+1}/{n}] status={resp['StatusCode']}")
        except Exception as e:
            log.warning(f"  invoke failed: {e}")
        time.sleep(2)


def inject_lambda_errors(function_name: str, region: str, n: int = 10) -> None:
    """Invoke Lambda with mode=error to spike error count in CloudWatch."""
    try:
        import boto3, json
    except ImportError:
        log.error("pip install boto3")
        return

    client = boto3.client("lambda", region_name=region)
    log.info(f"Invoking {function_name} with mode=error ({n}x)...")
    for i in range(n):
        try:
            resp = client.invoke(
                FunctionName=function_name,
                InvocationType="RequestResponse",
                Payload=json.dumps({"mode": "error"}),
            )
            log.info(f"  [{i+1}/{n}] status={resp['StatusCode']}")
        except Exception as e:
            log.warning(f"  invoke failed: {e}")
        time.sleep(1)


# ── CLI ───────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inject anomalies into AIOps test infra",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--config",      default="config/cloud_observability.yaml")
    parser.add_argument("--scenario",    help="Scenario name to inject")
    parser.add_argument("--resource-id", help="resource_id to inject into (for DB injection)")
    parser.add_argument("--resource-name", help="Human-readable name (optional)")
    parser.add_argument("--metric",      help="Metric name (for --scenario restore)")
    parser.add_argument("--backfill",    type=int, default=0,
                        help="Backfill N minutes of data before now (for warm-up)")
    parser.add_argument("--alb-url",     help="ALB DNS for HTTP injection")
    parser.add_argument("--function-name", help="Lambda function name for AWS injection")
    parser.add_argument("--region",      default="ap-south-1")
    parser.add_argument("--list",        action="store_true", help="List all scenarios")
    args = parser.parse_args()

    if args.list:
        print("\nAvailable scenarios:")
        print(f"{'Name':<30} {'Resource':<10}  Description")
        print("─" * 80)
        for name, sc in sorted(SCENARIOS.items()):
            print(f"  {name:<28} {sc['resource_type']:<10}  {sc['description']}")
        print()
        return

    if not args.scenario:
        parser.print_help()
        sys.exit(1)

    # Load config for DB path
    try:
        with open(args.config, encoding="utf-8") as f:
            config = yaml.safe_load(f) or {}
    except FileNotFoundError:
        config = {}

    db_path = (config.get("storage", {})
               .get("sqlite", {})
               .get("path", "observability_data/metrics.db"))
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # ── restore ────────────────────────────────────────────────────────────────
    if args.scenario == "restore":
        if not args.resource_id or not args.metric:
            log.error("--scenario restore requires --resource-id and --metric")
            sys.exit(1)
        restore_normal(conn, args.resource_id, args.metric)
        return

    # ── HTTP scenarios ─────────────────────────────────────────────────────────
    if args.scenario == "http_latency":
        if not args.alb_url:
            log.error("--scenario http_latency requires --alb-url")
            sys.exit(1)
        inject_http_latency(args.alb_url)
        return

    if args.scenario == "http_errors":
        if not args.alb_url:
            log.error("--scenario http_errors requires --alb-url")
            sys.exit(1)
        inject_http_errors(args.alb_url)
        return

    if args.scenario == "lambda_slow_http":
        if not args.function_name:
            log.error("--scenario lambda_slow_http requires --function-name")
            sys.exit(1)
        inject_lambda_slow(args.function_name, args.region)
        return

    if args.scenario == "lambda_errors_http":
        if not args.function_name:
            log.error("--scenario lambda_errors_http requires --function-name")
            sys.exit(1)
        inject_lambda_errors(args.function_name, args.region)
        return

    # ── DB injection ───────────────────────────────────────────────────────────
    sc = SCENARIOS.get(args.scenario)
    if sc is None:
        log.error(f"Unknown scenario {args.scenario!r}. Run --list to see options.")
        sys.exit(1)

    sc["_name"] = args.scenario

    # Build resource dict
    if args.resource_id:
        meta = _lookup_resource(conn, args.resource_id) or {}
        resource = {
            "resource_id":   args.resource_id,
            "resource_name": args.resource_name or meta.get("resource_name", args.resource_id),
            "resource_type": meta.get("resource_type", sc["resource_type"]),
            "cloud":         meta.get("cloud",  "aws"),
            "region":        meta.get("region", args.region),
        }
    else:
        # Try to pick matching resource from config
        resources = [
            r for r in config.get("resources", [])
            if r.get("resource_type", "").lower() == sc["resource_type"].lower()
        ]
        if not resources:
            log.error(
                f"No --resource-id provided and no {sc['resource_type']} "
                f"resources found in config."
            )
            sys.exit(1)
        resource = resources[0]
        log.info(f"Auto-selected resource: {resource['resource_id']}")

    log.info(f"\n{'='*60}")
    log.info(f"  Scenario  : {args.scenario}")
    log.info(f"  Resource  : {resource['resource_id']} ({resource.get('resource_name')})")
    log.info(f"  DB        : {db_path}")
    log.info(f"  Backfill  : {args.backfill} min")
    log.info(f"  Description: {sc['description']}")
    log.info(f"{'='*60}\n")

    inject_db(conn, resource, sc, backfill_minutes=args.backfill)

    log.info("\nDone! Now run anomaly_detection.py and check for alerts.")
    log.info("To verify no false positives, run the matching fp_probe_* scenario next.")


if __name__ == "__main__":
    main()
