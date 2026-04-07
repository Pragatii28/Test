"""
inject_test_anomaly.py  —  Dummy anomaly + spike metric injector
================================================
Run this to verify:
  1. The anomalies table exists and UI can read it      (--anomaly)
  2. The metrics table is being written to              (--metric)
  3. The detector picks up a CPU spike on next cycle    (--spike)
  4. Full end-to-end check                              (--all)

Usage:
  python inject_test_anomaly.py --all
  python inject_test_anomaly.py --anomaly          # just write a fake anomaly row
  python inject_test_anomaly.py --metric           # just write a normal metric row
  python inject_test_anomaly.py --spike            # write a 100% CPU spike metric row
  python inject_test_anomaly.py --check            # show last 10 anomalies in DB
  python inject_test_anomaly.py --db path/to.db   # override DB path (default auto-detect)
"""

import argparse
import glob
import os
import sqlite3
import sys
from datetime import datetime, timezone

# ── Auto-detect DB path ───────────────────────────────────────────────────────
DEFAULT_DB_CANDIDATES = [
    "observability_data/metrics.db",
    "metrics.db",
    "data/metrics.db",
]

def find_db(override: str = "") -> str:
    if override:
        return override
    for path in DEFAULT_DB_CANDIDATES:
        if os.path.exists(path):
            return path
    # Search recursively up to 2 levels
    for pattern in ["**/*.db", "*.db"]:
        matches = glob.glob(pattern, recursive=True)
        for m in matches:
            if "metrics" in m.lower():
                return m
    return DEFAULT_DB_CANDIDATES[0]  # fallback, will be created


def get_conn(db_path: str) -> sqlite3.Connection:
    os.makedirs(os.path.dirname(db_path) if os.path.dirname(db_path) else ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


# ── Ensure tables exist ───────────────────────────────────────────────────────
def ensure_anomalies_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT NOT NULL,
        cloud TEXT NOT NULL,
        region TEXT NOT NULL,
        resource_type TEXT NOT NULL,
        resource_id TEXT NOT NULL,
        resource_name TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        metric_unit TEXT NOT NULL,
        current_value REAL NOT NULL,
        avg_value REAL NOT NULL,
        std_value REAL NOT NULL,
        upper_bound REAL NOT NULL,
        lower_bound REAL NOT NULL,
        severity TEXT NOT NULL,
        reason TEXT NOT NULL,
        data_points INTEGER NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0,
        algorithm TEXT NOT NULL DEFAULT 'zscore',
        correlation_id TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX IF NOT EXISTS idx_an_time ON anomalies(detected_at DESC);
    """)
    conn.commit()


def ensure_metrics_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        cloud TEXT NOT NULL DEFAULT 'aws',
        region TEXT NOT NULL DEFAULT 'us-east-1',
        resource_type TEXT NOT NULL DEFAULT 'EC2',
        resource_id TEXT NOT NULL,
        resource_name TEXT NOT NULL,
        metric_name TEXT NOT NULL,
        metric_unit TEXT NOT NULL DEFAULT 'Percent',
        metric_value REAL NOT NULL,
        collected_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_m_rid_mn_ts
        ON metrics(resource_id, metric_name, collected_at DESC);
    """)
    conn.commit()


# ── Inject a dummy anomaly row directly ──────────────────────────────────────
def inject_anomaly(conn: sqlite3.Connection, severity: str = "critical") -> int:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO anomalies
        (detected_at, cloud, region, resource_type, resource_id, resource_name,
         metric_name, metric_unit, current_value, avg_value, std_value,
         upper_bound, lower_bound, severity, reason, data_points, algorithm, correlation_id)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        now,
        "aws",
        "eu-north-1",
        "EC2",
        "i-TEST000000000000",
        "test",                         # matches the instance shown in UI
        "cpu_utilization_percent",
        "Percent",
        100.6,                          # current (matches what UI shows)
        34.1,                           # avg
        12.5,                           # std
        90.0,                           # upper_bound (hard limit)
        0.0,                            # lower_bound
        severity,
        f"[TEST-INJECT] cpu_utilization_percent=100.60 — DUMMY anomaly injected at {now}",
        48,
        "hard_limit",
        "",
    ))
    conn.commit()
    row_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    return row_id


# ── Inject a normal-range metric row ─────────────────────────────────────────
def inject_metric(conn: sqlite3.Connection, value: float, metric_name: str = "cpu_utilization_percent") -> None:
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""
        INSERT INTO metrics
        (cloud, region, resource_type, resource_id, resource_name,
         metric_name, metric_unit, metric_value, collected_at)
        VALUES (?,?,?,?,?,?,?,?,?)
    """, (
        "aws", "eu-north-1", "EC2",
        "i-TEST000000000000",
        "test",
        metric_name,
        "Percent",
        value,
        now,
    ))
    conn.commit()


# ── Show last N anomalies ─────────────────────────────────────────────────────
def show_recent_anomalies(conn: sqlite3.Connection, n: int = 10) -> None:
    rows = conn.execute(
        "SELECT id, detected_at, resource_name, metric_name, current_value, "
        "severity, algorithm, reason FROM anomalies "
        "ORDER BY detected_at DESC LIMIT ?", (n,)
    ).fetchall()

    if not rows:
        print("\n  ❌  No anomalies in DB yet.\n")
        return

    print(f"\n  ✅  Last {len(rows)} anomaly row(s) in DB:\n")
    print(f"  {'ID':>4}  {'Detected At':24}  {'Resource':15}  {'Metric':30}  {'Value':8}  {'Sev':8}  {'Algo':16}")
    print("  " + "-" * 115)
    for r in rows:
        print(
            f"  {r['id']:>4}  {r['detected_at'][:23]:24}  "
            f"{r['resource_name'][:15]:15}  {r['metric_name'][:30]:30}  "
            f"{r['current_value']:8.2f}  {r['severity']:8}  {r['algorithm'][:16]:16}"
        )
    print()


# ── Show last N metric rows ───────────────────────────────────────────────────
def show_recent_metrics(conn: sqlite3.Connection, n: int = 10) -> None:
    rows = conn.execute(
        "SELECT id, collected_at, resource_name, metric_name, metric_value "
        "FROM metrics ORDER BY collected_at DESC LIMIT ?", (n,)
    ).fetchall()

    if not rows:
        print("\n  ❌  No metric rows in DB yet. Is the collector running?\n")
        return

    print(f"\n  ✅  Last {len(rows)} metric row(s) in DB:\n")
    print(f"  {'ID':>6}  {'Collected At':24}  {'Resource':15}  {'Metric':35}  {'Value':12}")
    print("  " + "-" * 100)
    for r in rows:
        print(
            f"  {r['id']:>6}  {r['collected_at'][:23]:24}  "
            f"{r['resource_name'][:15]:15}  {r['metric_name'][:35]:35}  "
            f"{r['metric_value']:12.4f}"
        )
    print()


# ── Main ──────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Dummy anomaly / metric injector for CloudOps")
    parser.add_argument("--db",      default="", help="Path to metrics.db (auto-detected if omitted)")
    parser.add_argument("--anomaly", action="store_true", help="Inject a dummy CRITICAL anomaly row")
    parser.add_argument("--metric",  action="store_true", help="Inject a normal CPU metric row (34%)")
    parser.add_argument("--spike",   action="store_true", help="Inject a CPU=100.6%% spike metric row (detector should catch it)")
    parser.add_argument("--check",   action="store_true", help="Show last 10 anomalies and metrics")
    parser.add_argument("--all",     action="store_true", help="Run all: inject anomaly + spike + check")
    args = parser.parse_args()

    if not any([args.anomaly, args.metric, args.spike, args.check, args.all]):
        parser.print_help()
        sys.exit(0)

    db_path = find_db(args.db)
    print(f"\n  📂  Using DB: {os.path.abspath(db_path)}")
    conn = get_conn(db_path)

    ensure_anomalies_table(conn)
    ensure_metrics_table(conn)

    if args.all:
        args.anomaly = args.spike = args.check = True

    # ── Inject dummy anomaly directly into anomalies table ────────────────
    if args.anomaly:
        row_id = inject_anomaly(conn, severity="critical")
        print(f"\n  ✅  Dummy CRITICAL anomaly inserted → anomalies.id = {row_id}")
        print("      → Refresh the Anomalies tab in the UI. You should see 1 anomaly.")
        print("      → If UI still shows 0, the UI is not reading from this DB file.")

    # ── Inject normal CPU baseline rows (so warmup passes) ───────────────
    if args.metric:
        print("\n  ⏳  Injecting 20 baseline CPU metric rows (value=34%) for warmup ...")
        for i in range(20):
            # Spread timestamps across last 15 minutes
            from datetime import timedelta
            ts = (datetime.now(timezone.utc) - timedelta(minutes=15 - i * 0.7)).isoformat()
            conn.execute("""
                INSERT INTO metrics
                (cloud, region, resource_type, resource_id, resource_name,
                 metric_name, metric_unit, metric_value, collected_at)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, ("aws", "eu-north-1", "EC2", "i-TEST000000000000", "test",
                  "cpu_utilization_percent", "Percent", 34.0 + i * 0.1, ts))
        conn.commit()
        print("  ✅  20 baseline rows written → detector warmup should now pass.")

    # ── Inject a CPU spike so detector fires on next cycle ────────────────
    if args.spike:
        inject_metric(conn, 100.6, "cpu_utilization_percent")
        print("\n  ✅  CPU spike row (100.6%) written to metrics table.")
        print("      → On the next detector cycle the hard-limit (≥90%) should fire.")
        print("      → Watch logs/anomaly_detector.log for:")
        print("         ANOMALY [CRITICAL][hard_limit] EC2/test — cpu_utilization_percent: 100.6000")
        print()
        print("      Also writing as 'CPUUtilization' (AWS raw name) to test aliasing:")
        inject_metric(conn, 100.6, "CPUUtilization")
        print("  ✅  CPU spike row (100.6%) written as 'CPUUtilization' (alias test).")

    # ── Show current DB state ─────────────────────────────────────────────
    if args.check:
        print("\n  ── Anomalies table ──────────────────────────────────────────")
        show_recent_anomalies(conn, n=10)
        print("  ── Metrics table (last 10 rows) ─────────────────────────────")
        show_recent_metrics(conn, n=10)

        # Extra diagnostics
        total_metrics = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
        total_anomalies = conn.execute("SELECT COUNT(*) FROM anomalies").fetchone()[0]
        distinct_names = conn.execute(
            "SELECT DISTINCT metric_name FROM metrics ORDER BY metric_name"
        ).fetchall()
        print(f"  📊  Total rows — metrics: {total_metrics}  |  anomalies: {total_anomalies}")
        print(f"\n  📋  Distinct metric_name values in DB ({len(distinct_names)} total):")
        for row in distinct_names:
            print(f"       • {row[0]}")
        print()

    conn.close()
    print("  Done. ✔\n")


if __name__ == "__main__":
    main()