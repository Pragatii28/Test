#!/usr/bin/env python3
"""
bridge/mongo_bridge.py
──────────────────────
Real-time bridge between the Python collector's SQLite database
and MongoDB Atlas used by the Node.js dashboard.

Flow:
  main.py (collector)   → writes metrics    → observability_data/metrics.db (SQLite)
  anomaly_detection.py  → writes anomalies  → observability_data/metrics.db (SQLite)
  THIS SCRIPT           → reads both tables → pushes to MongoDB Atlas (real-time)
  Node.js backend       → reads MongoDB     → serves React dashboard

Run this alongside main.py and anomaly_detection.py:
  python bridge/mongo_bridge.py

Environment variables (same .env as the Node.js backend, or set manually):
  MONGODB_URI   = your Atlas connection string
  SQLITE_PATH   = path to metrics.db  (default: observability_data/metrics.db)
  BRIDGE_INTERVAL = seconds between sync cycles (default: 20)
  USER_ID       = MongoDB user _id to attach data to (from Atlas users collection)
"""

import os
import sys
import sqlite3
import time
import logging
from datetime import datetime, timezone

# ── Optional: load .env from the backend folder ───────────────────────────────
try:
    from dotenv import load_dotenv
    # Try loading from backend/.env relative to this script
    _base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    load_dotenv(os.path.join(_base, "backend", ".env"))
    load_dotenv(os.path.join(_base, ".env"))  # also try root
except ImportError:
    pass  # pip install python-dotenv  if you want .env support

try:
    from pymongo import MongoClient, ASCENDING, DESCENDING
    from pymongo.errors import DuplicateKeyError
except ImportError:
    print("❌  pymongo not installed. Run:  pip install pymongo")
    sys.exit(1)

# ── Config ────────────────────────────────────────────────────────────────────
MONGODB_URI     = os.getenv("MONGODB_URI", "")
SQLITE_PATH     = os.getenv("SQLITE_PATH", "observability_data/metrics.db")
BRIDGE_INTERVAL = int(os.getenv("BRIDGE_INTERVAL", "20"))
USER_ID         = os.getenv("DASHBOARD_USER_ID", "")   # optional — set after first login

if not MONGODB_URI:
    print("❌  MONGODB_URI not set.")
    print("    Either set it as environment variable or create backend/.env")
    sys.exit(1)

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [bridge] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("logs/mongo_bridge.log", encoding="utf-8"),
    ],
)
log = logging.getLogger("bridge")
os.makedirs("logs", exist_ok=True)

# ── MongoDB collections ───────────────────────────────────────────────────────
mongo   = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=10_000)
db      = mongo["selfheal"]

col_metrics   = db["realmetrics"]    # raw metric points
col_resources = db["realresources"]  # discovered resources
col_incidents = db["incidents"]      # anomalies → become incidents in dashboard
col_state     = db["bridge_state"]   # cursor state (last synced row id)

# Indexes
col_metrics.create_index([("resource_id", ASCENDING), ("metric_name", ASCENDING), ("collected_at", DESCENDING)])
col_resources.create_index("resource_id", unique=True)
col_incidents.create_index("sqlite_anomaly_id", unique=True, sparse=True)
log.info("✅  MongoDB connected → %s", MONGODB_URI.split("@")[-1])

# ── SQLite helper ──────────────────────────────────────────────────────────────
def get_sqlite_conn():
    if not os.path.exists(SQLITE_PATH):
        return None
    conn = sqlite3.connect(SQLITE_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

# ── State helpers (persist last-synced row id in MongoDB) ────────────────────
def get_cursor(key: str) -> int:
    doc = col_state.find_one({"_id": key})
    return doc["last_id"] if doc else 0

def set_cursor(key: str, last_id: int):
    col_state.update_one({"_id": key}, {"$set": {"last_id": last_id}}, upsert=True)

# ── Get first user id from MongoDB (to attach real data to) ──────────────────
def get_user_id():
    if USER_ID:
        return USER_ID
    user = db["users"].find_one({}, {"_id": 1})
    return str(user["_id"]) if user else None

# ── Sync metrics from SQLite → MongoDB ───────────────────────────────────────
def sync_metrics(conn, user_id: str) -> int:
    last_id   = get_cursor("metrics")
    rows      = conn.execute(
        "SELECT * FROM metrics WHERE id > ? ORDER BY id ASC LIMIT 2000",
        (last_id,)
    ).fetchall()

    if not rows:
        return 0

    docs       = []
    new_last   = last_id
    resources  = {}

    for r in rows:
        doc = {
            "sqlite_id":    r["id"],
            "collected_at": r["collected_at"],
            "cloud":        r["cloud"],
            "region":       r["region"],
            "resource_type":r["resource_type"],
            "resource_id":  r["resource_id"],
            "resource_name":r["resource_name"],
            "metric_name":  r["metric_name"],
            "metric_value": r["metric_value"],
            "metric_unit":  r["metric_unit"],
            "labels":       r["labels"] if "labels" in r.keys() else "{}",
            "synced_at":    datetime.now(timezone.utc).isoformat(),
        }
        docs.append(doc)
        new_last = max(new_last, r["id"])

        # Track unique resources
        rkey = f"{r['cloud']}:{r['resource_type']}:{r['resource_id']}"
        resources[rkey] = {
            "resource_id":   r["resource_id"],
            "resource_name": r["resource_name"],
            "resource_type": r["resource_type"],
            "cloud":         r["cloud"],
            "region":        r["region"],
            "last_seen":     r["collected_at"],
        }

    # Bulk insert metrics (skip duplicates)
    inserted = 0
    for doc in docs:
        try:
            col_metrics.insert_one(doc)
            inserted += 1
        except DuplicateKeyError:
            pass

    # Upsert resources
    for rkey, rdoc in resources.items():
        col_resources.update_one(
            {"resource_id": rdoc["resource_id"]},
            {"$set": rdoc},
            upsert=True
        )

    set_cursor("metrics", new_last)
    return inserted

# ── Sync anomalies from SQLite → MongoDB incidents ────────────────────────────
def sync_anomalies(conn, user_id: str) -> int:
    # Check if anomalies table exists
    tables = [r[0] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()]
    if "anomalies" not in tables:
        return 0

    last_id = get_cursor("anomalies")
    rows    = conn.execute(
        "SELECT * FROM anomalies WHERE id > ? ORDER BY id ASC LIMIT 500",
        (last_id,)
    ).fetchall()

    if not rows:
        return 0

    inserted  = 0
    new_last  = last_id
    cols      = [d[0] for d in conn.execute("PRAGMA table_info(anomalies)").fetchall()]

    for r in rows:
        row_dict = dict(zip(cols, tuple(r)))
        new_last = max(new_last, row_dict["id"])

        severity = row_dict.get("severity", "warning").lower()
        # Map to status
        status   = "open"

        # Build incident document matching the Node.js Incident model schema
        incident = {
            "sqlite_anomaly_id": row_dict["id"],
            "userId":            user_id,           # string — converted below
            "incidentId":        _make_inc_id(row_dict["detected_at"], row_dict["id"]),
            "severity":          severity if severity in ["critical","warning","info"] else "warning",
            "status":            status,
            "cloud":             row_dict.get("cloud", "aws"),
            "region":            row_dict.get("region", "us-east-1"),
            "resourceType":      row_dict.get("resource_type", ""),
            "resourceId":        row_dict.get("resource_id", ""),
            "resourceName":      row_dict.get("resource_name", ""),
            "metricName":        row_dict.get("metric_name", ""),
            "currentValue":      float(row_dict.get("current_value", 0)),
            "threshold":         float(row_dict.get("upper_bound", 0)),
            "algorithm":         row_dict.get("algorithm", "zscore"),
            "reason":            row_dict.get("reason", ""),
            "autoRemediated":    False,
            "detectedAt":        _parse_dt(row_dict.get("detected_at")),
            "correlationId":     row_dict.get("correlation_id", ""),
        }

        # Convert userId string to ObjectId if possible
        try:
            from bson import ObjectId
            incident["userId"] = ObjectId(user_id)
        except Exception:
            pass

        try:
            col_incidents.insert_one(incident)
            inserted += 1
            log.warning(
                "  NEW INCIDENT [%s] %s/%s — %s: %.4f",
                incident["severity"].upper(),
                incident["resourceType"],
                incident["resourceName"],
                incident["metricName"],
                incident["currentValue"],
            )
        except DuplicateKeyError:
            pass

    set_cursor("anomalies", new_last)
    return inserted

# ── Helpers ───────────────────────────────────────────────────────────────────
def _make_inc_id(detected_at: str, row_id: int) -> str:
    try:
        date_part = detected_at[:10].replace("-", "")
    except Exception:
        date_part = datetime.now().strftime("%Y%m%d")
    return f"INC-{date_part}-{str(row_id).zfill(4)}"

def _parse_dt(s: str):
    if not s:
        return datetime.now(timezone.utc)
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return datetime.now(timezone.utc)

# ── Print resource summary ────────────────────────────────────────────────────
def print_resource_summary():
    total = col_resources.count_documents({})
    if total == 0:
        return
    log.info("  Resources in MongoDB (%d):", total)
    for r in col_resources.find({}, {"resource_name":1,"resource_type":1,"cloud":1,"_id":0}):
        log.info("    %-12s %-30s (%s)", r["resource_type"], r["resource_name"], r["cloud"])

# ── Main loop ─────────────────────────────────────────────────────────────────
def main():
    log.info("=" * 68)
    log.info("  MongoDB Bridge — Real-Time SQLite → Atlas Sync")
    log.info("  SQLite   : %s", SQLITE_PATH)
    log.info("  MongoDB  : %s", MONGODB_URI.split("@")[-1])
    log.info("  Interval : %ds", BRIDGE_INTERVAL)
    log.info("=" * 68)

    user_id = get_user_id()
    if not user_id:
        log.warning("  No users found in MongoDB yet.")
        log.warning("  Log in to the dashboard first, then restart the bridge.")
        log.warning("  Waiting for a user to appear...")

    cycle = 0
    while True:
        cycle += 1

        # Re-fetch user_id if not yet available
        if not user_id:
            user_id = get_user_id()
            if user_id:
                log.info("  User found: %s — starting sync", user_id)

        conn = get_sqlite_conn()
        if not conn:
            log.warning("  SQLite not found at %s — is main.py running?", SQLITE_PATH)
            time.sleep(BRIDGE_INTERVAL)
            continue

        if user_id:
            try:
                m = sync_metrics(conn, user_id)
                a = sync_anomalies(conn, user_id)

                if m > 0 or a > 0:
                    log.info("Cycle #%d — pushed %d metrics, %d new anomaly/incidents", cycle, m, a)
                else:
                    log.info("Cycle #%d — no new data", cycle)

                if cycle % 10 == 1:
                    print_resource_summary()

            except Exception as e:
                log.error("Cycle #%d error: %s", cycle, e, exc_info=True)
        else:
            log.info("Cycle #%d — waiting for dashboard user...", cycle)

        conn.close()
        time.sleep(BRIDGE_INTERVAL)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        log.info("Bridge stopped.")