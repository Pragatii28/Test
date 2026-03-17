#!/usr/bin/env python3
"""
verify_rds.py
─────────────
Compares what the collector stored in SQLite vs what CloudWatch
actually returns right now (using 60s period which matches AWS Console).
Run:  python verify_rds.py
"""
import boto3
import yaml
import sqlite3
import os
from datetime import datetime, timedelta, timezone

# ── Load config ───────────────────────────────────────────────────────────────
with open("config/cloud_observability.yaml") as f:
    cfg = yaml.safe_load(f)

aws_cfg = cfg["clouds"]["aws"]
session = boto3.Session(
    aws_access_key_id     = aws_cfg.get("access_key_id"),
    aws_secret_access_key = aws_cfg.get("secret_access_key"),
)
region = aws_cfg.get("regions", ["us-east-1"])[0]
DB_PATH = "observability_data/metrics.db"

# ── Exact correct params from debug output ────────────────────────────────────
# Period=60s gives the most recent datapoint (17:14 UTC vs 17:10 for 300s)
CORRECT_PARAMS = {
    "cpu_utilization_percent":       ("CPUUtilization",             "Average", 60),
    "database_connections":          ("DatabaseConnections",         "Maximum", 60),
    "free_storage_bytes":            ("FreeStorageSpace",            "Average", 60),
    "read_iops":                     ("ReadIOPS",                    "Average", 60),
    "write_iops":                    ("WriteIOPS",                   "Average", 60),
    "read_latency_seconds":          ("ReadLatency",                 "Average", 60),
    "write_latency_seconds":         ("WriteLatency",                "Average", 60),
    "freeable_memory_bytes":         ("FreeableMemory",              "Average", 60),
    "network_receive_bytes_per_sec": ("NetworkReceiveThroughput",    "Average", 60),
    "network_transmit_bytes_per_sec":("NetworkTransmitThroughput",   "Average", 60),
    "swap_usage_bytes":              ("SwapUsage",                   "Average", 60),
    "disk_queue_depth":              ("DiskQueueDepth",              "Average", 60),
    "burst_balance_percent":         ("BurstBalance",                "Average", 60),
}

NOW = datetime.now(timezone.utc)

# ── Fetch correct values from CloudWatch ──────────────────────────────────────
print("\nFetching correct values from CloudWatch (60s period, 10m window)...")
cw = session.client("cloudwatch", region_name=region)
rds_client = session.client("rds", region_name=region)

# Get DB instance ID
db_id = None
for page in rds_client.get_paginator("describe_db_instances").paginate():
    for db in page["DBInstances"]:
        db_id = db["DBInstanceIdentifier"]
        break
    if db_id:
        break

if not db_id:
    print("No RDS instance found.")
    exit(1)

print(f"DB Instance: {db_id}\n")

correct_values = {}
for metric_name, (cw_name, stat, period) in CORRECT_PARAMS.items():
    resp = cw.get_metric_statistics(
        Namespace  = "AWS/RDS",
        MetricName = cw_name,
        Dimensions = [{"Name": "DBInstanceIdentifier", "Value": db_id}],
        StartTime  = NOW - timedelta(minutes=10),
        EndTime    = NOW,
        Period     = period,
        Statistics = [stat],
    )
    dps = sorted(resp["Datapoints"], key=lambda x: x["Timestamp"], reverse=True)
    correct_values[metric_name] = round(dps[0][stat], 6) if dps else None

# ── Fetch what collector stored in DB ─────────────────────────────────────────
db_values = {}
if os.path.exists(DB_PATH):
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT metric_name, metric_value, collected_at
        FROM   metrics
        WHERE  resource_type = 'rds'
          AND  resource_id   = ?
        ORDER  BY collected_at DESC
    """, (db_id,)).fetchall()
    conn.close()

    seen = set()
    for metric_name, val, ts in rows:
        if metric_name not in seen:
            db_values[metric_name] = (round(val, 6), ts)
            seen.add(metric_name)
else:
    print(f"WARNING: Database not found at {DB_PATH}")

# ── Compare ───────────────────────────────────────────────────────────────────
print(f"{'Metric':<40} {'CloudWatch':>18} {'In DB':>18} {'DB Timestamp':<28} {'Match?'}")
print(f"{'─'*40} {'─'*18} {'─'*18} {'─'*28} {'─'*8}")

all_match = True
for metric_name, cw_val in correct_values.items():
    db_entry = db_values.get(metric_name)
    db_val   = db_entry[0] if db_entry else None
    db_ts    = db_entry[1] if db_entry else "NOT IN DB"

    if cw_val is None:
        match = "N/A (no CW data)"
    elif db_val is None:
        match = "❌ MISSING"
        all_match = False
    elif abs(cw_val - db_val) / max(abs(cw_val), 1) < 0.05:  # within 5%
        match = "✓ OK"
    else:
        match = f"❌ DIFF {round(abs(cw_val-db_val)/max(abs(cw_val),1)*100,1)}%"
        all_match = False

    cw_str = f"{cw_val:.4f}" if cw_val is not None else "--"
    db_str = f"{db_val:.4f}" if db_val is not None else "--"
    print(f"{metric_name:<40} {cw_str:>18} {db_str:>18} {str(db_ts):<28} {match}")

print()
if all_match:
    print("✓ All values match — collector is working correctly!")
else:
    print("✗ Some values differ — run the fix below and restart main.py")

# ── Show what the dashboard displays via Prometheus ───────────────────────────
print("\n── Checking Prometheus endpoint ──────────────────────────────────────────")
try:
    import requests
    prom_text = requests.get("http://localhost:8000/metrics", timeout=3).text
    rds_lines = [l for l in prom_text.splitlines()
                 if "rds" in l.lower() and db_id[:8].lower() in l.lower() and not l.startswith("#")]
    if rds_lines:
        print(f"Found {len(rds_lines)} RDS metric lines in Prometheus:")
        for l in rds_lines[:20]:
            print(f"  {l}")
    else:
        print("No RDS metrics found in Prometheus yet — collector may not have run a cycle.")
except Exception as e:
    print(f"Could not reach Prometheus: {e}")
    print("Make sure main.py is running.")