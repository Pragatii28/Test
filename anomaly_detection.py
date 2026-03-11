"""
anomaly_detection.py  —  AIOps-grade Multi-Algorithm Detector
"""
from __future__ import annotations

import json
import logging
import os
import sqlite3
import statistics
import time
import warnings
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yaml
from sklearn.ensemble import IsolationForest

warnings.filterwarnings("ignore")
os.environ.setdefault("CMDSTAN_QUIET", "1")

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [anomaly] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/anomaly_detector.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("anomaly")
logging.getLogger("prophet").setLevel(logging.ERROR)
logging.getLogger("cmdstanpy").setLevel(logging.ERROR)

# ── Settings ──────────────────────────────────────────────────────────────────
SLACK_WEBHOOK  = os.getenv("SLACK_WEBHOOK_URL", "")
INTERVAL       = int(os.getenv("DETECTOR_INTERVAL",  "22"))
MODE           = os.getenv("DETECTOR_MODE",           "continuous")
CONFIG_PATH    = os.getenv("CONFIG_PATH",             "config/cloud_observability.yaml")
SENSITIVITY    = float(os.getenv("SENSITIVITY",       "2.0"))
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS",      "2"))
MIN_DATAPOINTS = int(os.getenv("MIN_DATA_POINTS",     "15"))
WARMUP_MINUTES = int(os.getenv("WARMUP_MINUTES",      "10"))

PROPHET_MIN_HOURS        = 2
ISOFOREST_MIN_MINS       = 30
ISOFOREST_CONTAMINATION  = 0.05
STD_FLOOR_PCT            = 0.10

ALWAYS_BAD_METRICS = {
    "status_check_failed", "status_check_failed_instance",
    "status_check_failed_system", "throttles_total", "errors_total",
    "system_errors", "user_errors", "throttled_requests",
}

IGNORE_METRICS = {
    "network_in_bytes", "network_out_bytes", "network_packets_in",
    "network_packets_out", "disk_read_bytes", "disk_write_bytes",
    "disk_read_ops", "disk_write_ops", "processed_bytes",
}

METRIC_SENSITIVITY = {
    "cpu_utilization_percent": 2.5, "database_connections": 2.0,
    "read_latency_seconds": 2.0, "write_latency_seconds": 2.0,
    "duration_avg_ms": 2.5, "target_response_time_s": 2.0,
    "free_storage_bytes": 3.0, "freeable_memory_bytes": 2.5,
    "request_count": 2.0, "invocations_total": 2.5,
    "network_transmit_bytes_per_sec": 2.5, "network_receive_bytes_per_sec": 2.5,
    "disk_queue_depth": 2.5, "read_iops": 2.5, "write_iops": 2.5,
}


# ── Data class ────────────────────────────────────────────────────────────────
@dataclass
class Anomaly:
    detected_at: str
    cloud: str
    region: str
    resource_type: str
    resource_id: str
    resource_name: str
    metric_name: str
    metric_unit: str
    current_value: float
    avg_value: float
    std_value: float
    upper_bound: float
    lower_bound: float
    severity: str
    reason: str
    data_points: int
    algorithm: str = "zscore"
    correlation_id: str = ""

    @property
    def deviation(self):
        return 0.0 if self.std_value == 0 else abs(self.current_value - self.avg_value) / self.std_value

    def slack_message(self):
        icon = "🔴" if self.severity == "critical" else "🟡"
        algo_label = {"prophet": "📈 Prophet", "isolation_forest": "🌲 Isolation Forest",
                      "zscore": "📊 Z-score", "always_bad": "🚨 Always-bad"}.get(self.algorithm, self.algorithm)
        direction = "▲ ABOVE" if self.current_value > self.upper_bound else "▼ BELOW"
        return {
            "text": f"{icon} Anomaly — {self.resource_name} / {self.metric_name}",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"{icon} {self.resource_name}"}},
                {"type": "section", "fields": [
                    {"type": "mrkdwn", "text": f"*Resource:*\n{self.resource_name}"},
                    {"type": "mrkdwn", "text": f"*Metric:*\n{self.metric_name}"},
                    {"type": "mrkdwn", "text": f"*Severity:*\n{self.severity.upper()}"},
                    {"type": "mrkdwn", "text": f"*Current:*\n{self.current_value:.4f} {direction}"},
                    {"type": "mrkdwn", "text": f"*Range:*\n{self.lower_bound:.4f}–{self.upper_bound:.4f}"},
                    {"type": "mrkdwn", "text": f"*Algorithm:*\n{algo_label}"},
                ]},
                {"type": "context", "elements": [{"type": "mrkdwn",
                    "text": f"{self.reason} | {self.data_points} pts | {self.detected_at}" +
                            (f" | corr:{self.correlation_id}" if self.correlation_id else "")}]}
            ]
        }


# ── Database ──────────────────────────────────────────────────────────────────
class MetricsReader:
    # Base table — old schema, always safe to CREATE IF NOT EXISTS
    _DDL_CREATE = """
    CREATE TABLE IF NOT EXISTS anomalies (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        detected_at TEXT NOT NULL, cloud TEXT NOT NULL, region TEXT NOT NULL,
        resource_type TEXT NOT NULL, resource_id TEXT NOT NULL, resource_name TEXT NOT NULL,
        metric_name TEXT NOT NULL, metric_unit TEXT NOT NULL,
        current_value REAL NOT NULL, avg_value REAL NOT NULL, std_value REAL NOT NULL,
        upper_bound REAL NOT NULL, lower_bound REAL NOT NULL,
        severity TEXT NOT NULL, reason TEXT NOT NULL, data_points INTEGER NOT NULL,
        acknowledged INTEGER NOT NULL DEFAULT 0
    );
    CREATE INDEX IF NOT EXISTS idx_an_time ON anomalies(detected_at DESC);
    """

    # New columns added in v2 — migrated via ALTER TABLE on existing DBs
    _DDL_MIGRATE = [
        ("algorithm",      "TEXT NOT NULL DEFAULT 'zscore'"),
        ("correlation_id", "TEXT NOT NULL DEFAULT ''"),
    ]

    def __init__(self, config_path):
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        storage = cfg.get("storage", {})
        backend = storage.get("backend", "sqlite").lower()

        if backend == "sqlite":
            db_path = storage.get("sqlite", {}).get("path", "observability_data/metrics.db")
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            # Step 1: create base table if missing (old schema, always succeeds)
            self._conn.executescript(self._DDL_CREATE)
            # Step 2: add new columns if they don't exist yet (safe migration)
            existing_cols = {row[1] for row in self._conn.execute("PRAGMA table_info(anomalies)")}
            for col, defn in self._DDL_MIGRATE:
                if col not in existing_cols:
                    self._conn.execute(f"ALTER TABLE anomalies ADD COLUMN {col} {defn}")
                    log.info(f"Migrated anomalies table: added column '{col}'")
            # Step 3: correlation index (safe now that column exists)
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_an_corr ON anomalies(correlation_id)"
            )
            self._conn.commit()
            self._backend = "sqlite"
            log.info(f"Connected to SQLite -> {db_path}")
        else:
            try:
                import psycopg2
                import psycopg2.extras
                self._psycopg2 = psycopg2
                self._extras = psycopg2.extras
            except ImportError:
                raise ImportError("pip install psycopg2-binary")
            pg = storage.get("postgres", {})
            dsn = (pg.get("dsn") or os.getenv("DATABASE_URL") or
                   f"postgresql://{pg.get('user','postgres')}:{pg.get('password','')}@"
                   f"{pg.get('host','localhost')}:{pg.get('port',5432)}/{pg.get('dbname','observability')}")
            self._conn = psycopg2.connect(dsn)
            self._backend = "postgres"
            with self._conn.cursor() as cur:
                cur.execute(self._DDL.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
                                     .replace("INTEGER NOT NULL DEFAULT 0", "SMALLINT NOT NULL DEFAULT 0"))
            self._conn.commit()
            log.info("Connected to PostgreSQL")

    def get_history(self, resource_id, metric_name, hours=LOOKBACK_HOURS):
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        if self._backend == "sqlite":
            rows = self._conn.execute(
                "SELECT collected_at, metric_value FROM metrics "
                "WHERE resource_id=? AND metric_name=? AND collected_at>=? ORDER BY collected_at ASC",
                (resource_id, metric_name, cutoff)).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT collected_at, metric_value FROM metrics "
                            "WHERE resource_id=%s AND metric_name=%s AND collected_at>=%s ORDER BY collected_at ASC",
                            (resource_id, metric_name, cutoff))
                rows = cur.fetchall()
        return [(r[0], float(r[1])) for r in rows]

    def get_all_active_metrics(self):
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        if self._backend == "sqlite":
            rows = self._conn.execute(
                "SELECT cloud, region, resource_type, resource_id, resource_name, "
                "metric_name, metric_unit, metric_value, MAX(collected_at) AS latest_at "
                "FROM metrics WHERE collected_at>=? GROUP BY resource_id, metric_name "
                "ORDER BY resource_type, resource_name, metric_name", (cutoff,)).fetchall()
        else:
            with self._conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
                cur.execute("SELECT DISTINCT ON (resource_id, metric_name) "
                            "cloud, region, resource_type, resource_id, resource_name, "
                            "metric_name, metric_unit, metric_value, collected_at AS latest_at "
                            "FROM metrics WHERE collected_at>=%s "
                            "ORDER BY resource_id, metric_name, collected_at DESC", (cutoff,))
                rows = cur.fetchall()
        return [dict(r) for r in rows]

    def save_anomaly(self, a):
        vals = (a.detected_at, a.cloud, a.region, a.resource_type, a.resource_id,
                a.resource_name, a.metric_name, a.metric_unit, a.current_value,
                a.avg_value, a.std_value, a.upper_bound, a.lower_bound,
                a.severity, a.reason, a.data_points, a.algorithm, a.correlation_id)
        if self._backend == "sqlite":
            self._conn.execute(
                "INSERT INTO anomalies (detected_at,cloud,region,resource_type,resource_id,"
                "resource_name,metric_name,metric_unit,current_value,avg_value,std_value,"
                "upper_bound,lower_bound,severity,reason,data_points,algorithm,correlation_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", vals)
            self._conn.commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute("INSERT INTO anomalies (detected_at,cloud,region,resource_type,resource_id,"
                            "resource_name,metric_name,metric_unit,current_value,avg_value,std_value,"
                            "upper_bound,lower_bound,severity,reason,data_points,algorithm,correlation_id) "
                            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", vals)
            self._conn.commit()

    def recent_anomaly_count(self, resource_id, metric_name, minutes=30):
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        if self._backend == "sqlite":
            row = self._conn.execute(
                "SELECT COUNT(*) FROM anomalies WHERE resource_id=? AND metric_name=? AND detected_at>=?",
                (resource_id, metric_name, cutoff)).fetchone()
        else:
            with self._conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM anomalies WHERE resource_id=%s AND metric_name=%s AND detected_at>=%s",
                            (resource_id, metric_name, cutoff))
                row = cur.fetchone()
        return int(row[0]) if row else 0


# ── Helpers ───────────────────────────────────────────────────────────────────
def _parse_ts(ts):
    ts = ts.replace("Z", "+00:00")
    if "+" not in ts[10:] and len(ts) == 19:
        ts += "+00:00"
    return datetime.fromisoformat(ts)

def _to_df(history_rows):
    records = []
    for ts, val in history_rows:
        try:
            records.append({"ds": _parse_ts(ts).replace(tzinfo=None), "y": val})
        except Exception:
            pass
    return pd.DataFrame(records)

def _warmup_ok(history_rows):
    if len(history_rows) < MIN_DATAPOINTS:
        return False, f"only {len(history_rows)}/{MIN_DATAPOINTS} pts"
    now = datetime.now(timezone.utc)
    try:
        oldest = _parse_ts(history_rows[0][0])
        newest = _parse_ts(history_rows[-1][0])
    except Exception:
        return False, "bad timestamps"
    age_min    = (now - oldest).total_seconds() / 60
    spread_min = (newest - oldest).total_seconds() / 60
    if age_min < WARMUP_MINUTES:
        return False, f"oldest {age_min:.1f}m (need {WARMUP_MINUTES}m)"
    if spread_min < WARMUP_MINUTES * 0.5:
        return False, f"spread {spread_min:.1f}m too narrow"
    return True, ""

def _span_hours(history_rows):
    if len(history_rows) < 2:
        return 0.0
    try:
        return (_parse_ts(history_rows[-1][0]) - _parse_ts(history_rows[0][0])).total_seconds() / 3600
    except Exception:
        return 0.0


# ── Layer 1: Prophet ──────────────────────────────────────────────────────────
def _detect_prophet(row, history_rows, sens, now_str):
    if not PROPHET_AVAILABLE:
        return None
    df = _to_df(history_rows)
    if len(df) < 20:
        return None
    try:
        m = Prophet(
            interval_width=min(0.99, 1 - 1 / (sens * 3)),
            daily_seasonality=len(df) >= 60,
            weekly_seasonality=len(df) >= 2016,
            yearly_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            m.fit(df)
        forecast   = m.predict(pd.DataFrame({"ds": [datetime.utcnow()]}))
        yhat       = float(forecast["yhat"].iloc[0])
        yhat_lower = max(0.0, float(forecast["yhat_lower"].iloc[0]))
        yhat_upper = float(forecast["yhat_upper"].iloc[0])
        current    = float(row["metric_value"])
        avg        = float(df["y"].mean())
        std        = float(df["y"].std()) if len(df) > 1 else 0.0
        if current > yhat_upper or current < yhat_lower:
            direction = "ABOVE" if current > yhat_upper else "BELOW"
            pct_off   = abs(current - yhat) / max(abs(yhat), 1e-9) * 100
            return Anomaly(
                detected_at=now_str, cloud=row["cloud"], region=row["region"],
                resource_type=row["resource_type"], resource_id=row["resource_id"],
                resource_name=row["resource_name"], metric_name=row["metric_name"],
                metric_unit=row["metric_unit"], current_value=current,
                avg_value=round(avg, 6), std_value=round(std, 6),
                upper_bound=round(yhat_upper, 6), lower_bound=round(yhat_lower, 6),
                severity="critical" if pct_off > 50 else "warning",
                reason=(f"[Prophet] {row['metric_name']} is {current:.4f} — "
                        f"{pct_off:.1f}% {direction} seasonality-adjusted forecast {yhat:.4f} "
                        f"(expected {yhat_lower:.4f}–{yhat_upper:.4f})"),
                data_points=len(df), algorithm="prophet",
            )
    except Exception as e:
        log.debug(f"Prophet failed {row['resource_name']}/{row['metric_name']}: {e}")
    return None


# ── Layer 2: Isolation Forest ─────────────────────────────────────────────────
def _detect_isoforest(row, history_rows, sens, now_str):
    df = _to_df(history_rows)
    if len(df) < 10:
        return None
    try:
        values  = df["y"].values
        hours   = pd.to_datetime(df["ds"]).dt.hour.values
        deltas  = np.diff(values, prepend=values[0])
        X       = np.column_stack([values, deltas, hours])
        current = float(row["metric_value"])
        cur_d   = current - values[-1]
        X_cur   = np.array([[current, cur_d, datetime.utcnow().hour]])
        clf     = IsolationForest(contamination=min(0.5, max(0.01, ISOFOREST_CONTAMINATION)),
                                  n_estimators=100, random_state=42, n_jobs=-1)
        clf.fit(X)
        pred    = clf.predict(X_cur)[0]
        score   = clf.score_samples(X_cur)[0]
        if pred == -1:
            avg       = float(np.mean(values))
            std       = float(np.std(values))
            std_floor = max(std, abs(avg) * STD_FLOOR_PCT, 1e-9)
            pct_off   = abs(current - avg) / max(abs(avg), 1e-9) * 100
            return Anomaly(
                detected_at=now_str, cloud=row["cloud"], region=row["region"],
                resource_type=row["resource_type"], resource_id=row["resource_id"],
                resource_name=row["resource_name"], metric_name=row["metric_name"],
                metric_unit=row["metric_unit"], current_value=current,
                avg_value=round(avg, 6), std_value=round(std, 6),
                upper_bound=round(avg + sens * std_floor, 6),
                lower_bound=round(max(0.0, avg - sens * std_floor), 6),
                severity="critical" if pct_off > 50 else "warning",
                reason=(f"[IsolationForest] {row['metric_name']} is {current:.4f} — "
                        f"ML flagged as outlier (score:{score:.3f}, {pct_off:.1f}% from mean {avg:.4f}, "
                        f"delta:{cur_d:+.4f})"),
                data_points=len(df), algorithm="isolation_forest",
            )
    except Exception as e:
        log.debug(f"IsoForest failed {row['resource_name']}/{row['metric_name']}: {e}")
    return None


# ── Layer 3: Z-score fallback ─────────────────────────────────────────────────
def _detect_zscore(row, history_rows, sens, now_str):
    values  = [v for _, v in history_rows]
    current = float(row["metric_value"])
    if not values:
        return None
    avg = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    if avg < 1e-9 and std < 1e-9:
        if current > 1e-9:
            return Anomaly(
                detected_at=now_str, cloud=row["cloud"], region=row["region"],
                resource_type=row["resource_type"], resource_id=row["resource_id"],
                resource_name=row["resource_name"], metric_name=row["metric_name"],
                metric_unit=row["metric_unit"], current_value=current,
                avg_value=0.0, std_value=0.0, upper_bound=0.0, lower_bound=0.0,
                severity="warning",
                reason=f"[Z-score] {row['metric_name']} was always 0 but is now {current:.4f}",
                data_points=len(values), algorithm="zscore",
            )
        return None
    std_floor   = max(std, abs(avg) * STD_FLOOR_PCT, 1e-9)
    upper_bound = avg + sens * std_floor
    lower_bound = max(0.0, avg - sens * std_floor)
    if current > upper_bound or current < lower_bound:
        deviation = abs(current - avg) / std_floor
        direction = "ABOVE" if current > upper_bound else "BELOW"
        return Anomaly(
            detected_at=now_str, cloud=row["cloud"], region=row["region"],
            resource_type=row["resource_type"], resource_id=row["resource_id"],
            resource_name=row["resource_name"], metric_name=row["metric_name"],
            metric_unit=row["metric_unit"], current_value=current,
            avg_value=round(avg, 6), std_value=round(std, 6),
            upper_bound=round(upper_bound, 6), lower_bound=round(lower_bound, 6),
            severity="critical" if deviation > sens * 1.5 else "warning",
            reason=(f"[Z-score] {row['metric_name']} is {current:.4f} — "
                    f"{deviation:.1f}x std devs {direction} {LOOKBACK_HOURS}h avg of {avg:.4f}"),
            data_points=len(values), algorithm="zscore",
        )
    return None


# ── Detection router ──────────────────────────────────────────────────────────
def detect(row, history_rows):
    metric_name   = row["metric_name"]
    current_value = float(row["metric_value"])
    now_str       = datetime.now(timezone.utc).isoformat()
    sens          = METRIC_SENSITIVITY.get(metric_name, SENSITIVITY)

    # Always-bad
    if metric_name in ALWAYS_BAD_METRICS and current_value > 0:
        return Anomaly(
            detected_at=now_str, cloud=row["cloud"], region=row["region"],
            resource_type=row["resource_type"], resource_id=row["resource_id"],
            resource_name=row["resource_name"], metric_name=metric_name,
            metric_unit=row["metric_unit"], current_value=current_value,
            avg_value=0.0, std_value=0.0, upper_bound=0.0, lower_bound=0.0,
            severity="critical",
            reason=f"[Always-bad] {metric_name} = {current_value} — any non-zero value is an incident",
            data_points=len(history_rows), algorithm="always_bad",
        )

    # Warm-up gate
    ok, skip_reason = _warmup_ok(history_rows)
    if not ok:
        log.debug(f"  WARMUP {row['resource_name']}/{metric_name}: {skip_reason}")
        return None

    span_hours = _span_hours(history_rows)
    span_mins  = span_hours * 60

    # Prophet (2h+ data)
    if PROPHET_AVAILABLE and span_hours >= PROPHET_MIN_HOURS:
        return _detect_prophet(row, history_rows, sens, now_str)

    # Isolation Forest (30min+ data)
    if span_mins >= ISOFOREST_MIN_MINS:
        return _detect_isoforest(row, history_rows, sens, now_str)

    # Z-score fallback
    return _detect_zscore(row, history_rows, sens, now_str)


# ── Correlation grouping ──────────────────────────────────────────────────────
def assign_correlation_ids(anomalies):
    import hashlib
    from collections import defaultdict
    groups = defaultdict(list)
    for a in anomalies:
        groups[a.resource_id].append(a)
    for resource_id, group in groups.items():
        if len(group) > 1:
            cid = hashlib.sha1(f"{resource_id}:{group[0].detected_at[:16]}".encode()).hexdigest()[:8]
            for a in group:
                a.correlation_id = cid
            log.warning(
                f"  CORRELATED [{cid}] {group[0].resource_name}: "
                f"{len(group)} metrics anomalous simultaneously — likely single incident "
                f"({', '.join(a.metric_name for a in group)})"
            )


# ── Slack ─────────────────────────────────────────────────────────────────────
def send_slack(anomaly):
    if not SLACK_WEBHOOK:
        return
    try:
        import urllib.request
        req = urllib.request.Request(
            SLACK_WEBHOOK, data=json.dumps(anomaly.slack_message()).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(f"Slack {resp.status}")
    except Exception as e:
        log.warning(f"Slack failed: {e}")


# ── Main loop ─────────────────────────────────────────────────────────────────
def run_detection(reader):
    all_metrics = reader.get_all_active_metrics()
    if not all_metrics:
        log.warning("No metrics found. Is main.py running?")
        return []

    log.info(f"Checking {len(all_metrics)} metric/resource combinations...")
    found = []
    normal = warming = 0
    algo_counts = {}

    for row in all_metrics:
        if row["metric_name"] in IGNORE_METRICS:
            continue
        history_rows = reader.get_history(row["resource_id"], row["metric_name"])
        anomaly      = detect(row, history_rows)
        if anomaly is None:
            ok, _ = _warmup_ok(history_rows)
            if not ok and row["metric_name"] not in ALWAYS_BAD_METRICS:
                warming += 1
            else:
                normal += 1
            continue
        if reader.recent_anomaly_count(row["resource_id"], row["metric_name"], minutes=30) > 0:
            log.debug(f"  Suppressed repeat: {row['resource_name']}/{row['metric_name']}")
            continue
        algo_counts[anomaly.algorithm] = algo_counts.get(anomaly.algorithm, 0) + 1
        found.append(anomaly)

    assign_correlation_ids(found)

    for a in found:
        log.warning(
            f"  ANOMALY [{a.severity.upper()}][{a.algorithm}] "
            f"{a.resource_type}/{a.resource_name} — "
            f"{a.metric_name}: {a.current_value:.4f} "
            f"(normal: {a.lower_bound:.4f}–{a.upper_bound:.4f})"
            + (f" [corr:{a.correlation_id}]" if a.correlation_id else "")
        )
        reader.save_anomaly(a)
        send_slack(a)

    algo_str = ", ".join(f"{k}:{v}" for k, v in algo_counts.items()) or "none"
    log.info(f"Done. Found {len(found)} [{algo_str}]. Normal:{normal}. Warming:{warming}.")
    return found


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  AIOps Anomaly Detector")
    log.info(f"  Config      : {CONFIG_PATH}")
    log.info(f"  Sensitivity : {SENSITIVITY} std devs")
    log.info(f"  Lookback    : {LOOKBACK_HOURS}h  |  Interval: {INTERVAL}s")
    log.info(f"  Warmup gate : {WARMUP_MINUTES}m + {MIN_DATAPOINTS} pts")
    log.info(f"  Prophet     : {'enabled' if PROPHET_AVAILABLE else 'NOT installed — pip install prophet'}")
    log.info(f"  IsoForest   : enabled (scikit-learn)")
    log.info(f"  Slack       : {'configured' if SLACK_WEBHOOK else 'NOT SET'}")
    log.info("=" * 70)

    if not PROPHET_AVAILABLE:
        log.warning("Prophet not available — using IsolationForest + Z-score only.")

    reader = MetricsReader(CONFIG_PATH)

    if MODE == "once":
        run_detection(reader)
        log.info("Single run complete.")
    else:
        cycle = 0
        log.info(f"Running every {INTERVAL}s. Ctrl+C to stop.")
        try:
            while True:
                cycle += 1
                log.info(f"\n{'─'*70}")
                log.info(f"Cycle #{cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                log.info(f"{'─'*70}")
                t0 = time.time()
                try:
                    run_detection(reader)
                except Exception as exc:
                    log.error(f"Cycle #{cycle} failed: {exc}", exc_info=True)
                elapsed = time.time() - t0
                log.info(f"Cycle took {elapsed:.1f}s — next in {max(0, INTERVAL-elapsed):.0f}s")
                time.sleep(max(0, INTERVAL - elapsed))
        except KeyboardInterrupt:
            log.info("Stopped.")