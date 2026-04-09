"""
anomaly_detection.py  —  AIOps-grade Multi-Algorithm Detector
                          Single-file-per-algorithm continuous training
                          UPDATED: All patches applied (v2)
"""
from __future__ import annotations
from rca.engine import RCAEngine
from rca.pdf_report import generate_pdf
import copy
import json
import logging
import os
import sqlite3
import statistics
import threading
import time
import uuid
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import joblib
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

try:
    from river import anomaly as river_anomaly
    RIVER_AVAILABLE = True
except ImportError:
    RIVER_AVAILABLE = False

os.makedirs("logs",   exist_ok=True)
os.makedirs("models", exist_ok=True)

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
SLACK_WEBHOOK           = os.getenv("SLACK_WEBHOOK_URL",           "")
INTERVAL                = int(os.getenv("DETECTOR_INTERVAL",       "10"))
MODE                    = os.getenv("DETECTOR_MODE",                "continuous")
CONFIG_PATH             = os.getenv("CONFIG_PATH",                  "config/cloud_observability.yaml")
SENSITIVITY             = float(os.getenv("SENSITIVITY",            "2.0"))
LOOKBACK_HOURS          = int(os.getenv("LOOKBACK_HOURS",           "2"))
MIN_DATAPOINTS          = int(os.getenv("MIN_DATA_POINTS",          "8"))
WARMUP_MINUTES          = int(os.getenv("WARMUP_MINUTES",           "3"))
MODEL_DIR               = os.getenv("MODEL_DIR",                    "models")
STALE_THRESHOLD_MINUTES = int(os.getenv("STALE_THRESHOLD_MINUTES",  "15"))
DEDUP_WINDOW_MINUTES    = int(os.getenv("DEDUP_WINDOW_MINUTES",     "5"))
DETECTION_WORKERS       = int(os.getenv("DETECTION_WORKERS",        "8"))
CPU_SAFETY_NET_THRESHOLD = float(os.getenv("CPU_SAFETY_NET_THRESHOLD", "95.0"))

# ── PATCH #1: RCA Configuration ───────────────────────────────────────────────
RCA_ENABLED = os.getenv("RCA_ENABLED", "true").lower() == "true"
RCA_REPORT_DIR = os.getenv("RCA_REPORT_DIR", "rca_reports/")
RCA_TIMEOUT_SECONDS = int(os.getenv("RCA_TIMEOUT_SECONDS", "30"))
RCA_MAX_ANOMALIES_PER_CYCLE = int(os.getenv("RCA_MAX_ANOMALIES_PER_CYCLE", "5"))

# IsoForest
ISOFOREST_RETRAIN_THRESHOLD   = float(os.getenv("ISOFOREST_RETRAIN_THRESHOLD",   "0.10"))
ISOFOREST_WINDOW_HOURS        = int(os.getenv("ISOFOREST_WINDOW_HOURS",          "24"))
ISOFOREST_MAX_AGE_HOURS       = int(os.getenv("ISOFOREST_MAX_AGE_HOURS",         "6"))
ISOFOREST_MIN_PCT_DEVIATION   = float(os.getenv("ISOFOREST_MIN_PCT_DEVIATION",   "25.0"))
ISOFOREST_MIN_SCORE_THRESHOLD = float(os.getenv("ISOFOREST_MIN_SCORE_THRESHOLD", "-0.70"))
ISOFOREST_MIN_ABS_DEVIATION   = float(os.getenv("ISOFOREST_MIN_ABS_DEVIATION",   "0.0"))

# False-positive suppression
MIN_STD_FLOOR                 = float(os.getenv("MIN_STD_FLOOR", "0.001"))
TRANSIENT_SUPPRESSION_PCT     = float(os.getenv("TRANSIENT_SUPPRESSION_PCT", "7.5"))
TRANSIENT_SUPPRESSION_ABS     = float(os.getenv("TRANSIENT_SUPPRESSION_ABS", "0.05"))

# Prophet
PROPHET_RETRAIN_THRESHOLD      = float(os.getenv("PROPHET_RETRAIN_THRESHOLD",      "0.20"))
PROPHET_WINDOW_HOURS           = int(os.getenv("PROPHET_WINDOW_HOURS",             "168"))
PROPHET_MAX_AGE_HOURS          = int(os.getenv("PROPHET_MAX_AGE_HOURS",            "6"))
PROPHET_IMPLAUSIBLE_FACTOR     = float(os.getenv("PROPHET_IMPLAUSIBLE_FACTOR",     "3.0"))
PROPHET_IMPLAUSIBLE_ABS_FACTOR = float(os.getenv("PROPHET_IMPLAUSIBLE_ABS_FACTOR", "5.0"))

_PROPHET_MIN_PCT_DEVIATION_DEFAULT: Dict[str, float] = {
    "network_transmit_bytes_per_sec":  15.0,
    "network_receive_bytes_per_sec":   15.0,
    "network_in_bytes":                15.0,
    "network_out_bytes":               15.0,
    "network_packets_in":              15.0,
    "network_packets_out":             15.0,
    "cpu_utilization_percent":         10.0,
    "database_connections":            10.0,
    "disk_queue_depth":                20.0,
}
try:
    _overrides = json.loads(os.getenv("PROPHET_MIN_PCT_DEVIATION_JSON", "{}"))
    PROPHET_MIN_PCT_DEVIATION: Dict[str, float] = {
        **_PROPHET_MIN_PCT_DEVIATION_DEFAULT, **_overrides
    }
except Exception:
    PROPHET_MIN_PCT_DEVIATION = dict(_PROPHET_MIN_PCT_DEVIATION_DEFAULT)

RIVER_SCORE_THRESHOLD = float(os.getenv("RIVER_SCORE_THRESHOLD", "0.75"))

# ── PATCH #7: Unified Deduplication Logic ─────────────────────────────────────
_DEDUP_WINDOW_BY_ALGORITHM: Dict[str, int] = {
    "always_bad": 30,          # Non-zero is incident; avoid storm
    "hard_limit": 30,          # Hard thresholds rarely fluctuate
    "cpu_safety_net": 5,       # Safety net fires rarely; quick recovery
    "prophet": 5,              # Time-series forecast is fine-grained
    "isolation_forest": 5,     # Outlier detection is frequent
    "river": 5,                # Online learning adapts quickly
    "zscore": 5,               # Basic stats are snapshot-based
}

def _get_dedup_minutes(algorithm: str) -> int:
    """Return deduplication window for anomaly algorithm."""
    return _DEDUP_WINDOW_BY_ALGORITHM.get(algorithm, DEDUP_WINDOW_MINUTES)

PROPHET_INTERNAL_MIN_ROWS = 20
ISOFOREST_MIN_MINS        = 30
ISOFOREST_CONTAMINATION   = 0.01
STD_FLOOR_PCT             = 0.10

_NO_SUPPRESS_ALGOS: Set[str] = {"hard_limit", "always_bad", "cpu_safety_net"}

# ── Metric name normalisation ─────────────────────────────────────────────────
_METRIC_NAME_ALIASES: Dict[str, str] = {
    # CPU
    "CPUUtilization":                      "cpu_utilization_percent",
    "cpu_usage_percent":                   "cpu_utilization_percent",
    "cpu_percent":                         "cpu_utilization_percent",
    "cpu_usage_idle":                      "cpu_utilization_percent",
    "node_cpu_seconds_total":              "cpu_utilization_percent",
    "cpu_utilization":                     "cpu_utilization_percent",
    "system_cpu_usage":                    "cpu_utilization_percent",
    "process_cpu_seconds_total":           "cpu_utilization_percent",

    # Memory / freeable
    "FreeableMemory":                      "freeable_memory_bytes",
    "freeable_memory":                     "freeable_memory_bytes",
    "MemoryUtilization":                   "freeable_memory_bytes",
    "node_memory_MemFree_bytes":           "freeable_memory_bytes",
    "mem_free":                            "freeable_memory_bytes",
    "available_memory_bytes":              "freeable_memory_bytes",

    # Free storage
    "FreeStorageSpace":                    "free_storage_bytes",
    "free_storage":                        "free_storage_bytes",
    "disk_free":                           "free_storage_bytes",
    "node_filesystem_free_bytes":          "free_storage_bytes",
    "node_filesystem_avail_bytes":         "free_storage_bytes",

    # Burst balance
    "BurstBalance":                        "burst_balance_percent",
    "burst_balance":                       "burst_balance_percent",

    # DB connections
    "DatabaseConnections":                 "database_connections",
    "db_connections":                      "database_connections",
    "active_connections":                  "database_connections",
    "pg_stat_activity_count":             "database_connections",

    # IOPS
    "ReadIOPS":                            "read_iops",
    "WriteIOPS":                           "write_iops",
    "read_ops":                            "read_iops",
    "write_ops":                           "write_iops",
    "disk_read_iops":                      "read_iops",
    "disk_write_iops":                     "write_iops",

    # Latency
    "ReadLatency":                         "read_latency_seconds",
    "WriteLatency":                        "write_latency_seconds",
    "read_latency":                        "read_latency_seconds",
    "write_latency":                       "write_latency_seconds",

    # Disk queue
    "DiskQueueDepth":                      "disk_queue_depth",
    "disk_queue":                          "disk_queue_depth",
    "io_queue_depth":                      "disk_queue_depth",

    # Network (bytes/sec variants)
    "NetworkIn":                           "network_in_bytes",
    "NetworkOut":                          "network_out_bytes",
    "network_in":                          "network_in_bytes",
    "network_out":                         "network_out_bytes",
    "bytes_in":                            "network_in_bytes",
    "bytes_out":                           "network_out_bytes",
    "node_network_receive_bytes_total":    "network_receive_bytes_per_sec",
    "node_network_transmit_bytes_total":   "network_transmit_bytes_per_sec",
    "NetworkPacketsIn":                    "network_packets_in",
    "NetworkPacketsOut":                   "network_packets_out",

    # Replica lag
    "ReplicaLag":                          "replica_lag_seconds",
    "replica_lag":                         "replica_lag_seconds",
    "AuroraReplicaLag":                    "replica_lag_aurora_seconds",
    "aurora_replica_lag":                  "replica_lag_aurora_seconds",

    # Healthy hosts
    "HealthyHostCount":                    "healthy_host_count",
    "healthy_hosts":                       "healthy_host_count",
    "UnHealthyHostCount":                  "unhealthy_host_count",
    "unhealthy_hosts":                     "unhealthy_host_count",

    # Swap
    "SwapUsage":                           "swap_usage_bytes",
    "swap_usage":                          "swap_usage_bytes",
    "node_memory_SwapFree_bytes":          "swap_usage_bytes",

    # Binlog
    "BinLogDiskUsage":                     "binlog_disk_usage_bytes",
    "binlog_disk_usage":                   "binlog_disk_usage_bytes",

    # Lambda / concurrency
    "ConcurrentExecutions":                "concurrent_executions",
    "UnreservedConcurrentExecutions":      "unreserved_concurrent_executions",
    "Invocations":                         "invocations_total",
    "invocations":                         "invocations_total",
    "Throttles":                           "throttles_total",
    "throttles":                           "throttles_total",
    "Errors":                              "errors_total",
    "Duration":                            "duration_avg_ms",

    # ALB / response time
    "TargetResponseTime":                  "target_response_time_s",
    "RequestCount":                        "request_count",
    "request_count":                       "request_count",
    "HTTPCode_Target_5XX_Count":           "http_5xx_count",
    "http_5xx":                            "http_5xx_count",

    # Status checks
    "StatusCheckFailed":                   "status_check_failed",
    "StatusCheckFailed_Instance":          "status_check_failed_instance",
    "StatusCheckFailed_System":            "status_check_failed_system",
}


def _normalize_metric_name(raw: str) -> str:
    """Return the canonical metric name for a raw collector name."""
    if raw in _METRIC_NAME_ALIASES:
        return _METRIC_NAME_ALIASES[raw]
    raw_lower = raw.lower()
    for alias, canonical in _METRIC_NAME_ALIASES.items():
        if alias.lower() == raw_lower:
            return canonical
    return raw_lower.replace(" ", "_").replace("-", "_")


def _normalize_row(row: dict) -> dict:
    """Return a shallow copy of row with metric_name normalised."""
    raw_name = row.get("metric_name", "")
    canonical = _normalize_metric_name(raw_name)

    new_row = dict(row)
    new_row["metric_name"]     = canonical
    new_row["_raw_metric_name"] = raw_name

    if raw_name.lower() in ("cpu_usage_idle", "cpu_idle_percent"):
        try:
            new_row["metric_value"] = 100.0 - float(row["metric_value"])
        except Exception:
            pass

    return new_row


# ── Per-metric physically non-negative ───────────────────────────────────────
_NON_NEGATIVE_METRICS: Set[str] = {
    "cpu_utilization_percent", "freeable_memory_bytes", "free_storage_bytes",
    "burst_balance_percent", "database_connections", "read_iops", "write_iops",
    "read_latency_seconds", "write_latency_seconds", "disk_queue_depth",
    "network_in_bytes", "network_out_bytes", "network_packets_in", "network_packets_out",
    "network_receive_bytes_per_sec", "network_transmit_bytes_per_sec",
    "replica_lag_seconds", "replica_lag_aurora_seconds", "healthy_host_count",
    "concurrent_executions", "unreserved_concurrent_executions",
    "request_count", "invocations_total", "target_response_time_s",
    "duration_avg_ms", "duration_max_ms", "swap_usage_bytes",
    "binlog_disk_usage_bytes", "active_connections", "unhealthy_host_count",
    "http_5xx_count", "throttles_total", "errors_total",
}


def _clamp_forecast(
    metric_name: str, training_mean: float,
    yhat: float, yhat_lower: float, yhat_upper: float,
) -> Tuple[float, float, float]:
    """Clamp Prophet forecast to >= 0 for non-negative metrics."""
    if (metric_name in _NON_NEGATIVE_METRICS) or (training_mean >= 0):
        yhat       = max(0.0, yhat)
        yhat_lower = max(0.0, yhat_lower)
        yhat_upper = max(0.0, yhat_upper)
    return yhat, yhat_lower, yhat_upper


# ── PATCH #3: Safe Percentage Deviation ───────────────────────────────────────
def _safe_pct_off(
    current: float, yhat: float, training_mean: float,
    abs_threshold: float = 1e-3
) -> float:
    """
    Compute percentage deviation safely.
    
    Handles three cases:
    1. Normal: yhat is meaningful → use as denominator
    2. Clamped: yhat = 0 but training_mean > threshold → use training_mean
    3. Edge case: both near-zero → return 0 (no meaningful deviation)
    
    Args:
        current: Current metric value
        yhat: Forecast value (may have been clamped to 0)
        training_mean: Historical average
        abs_threshold: Minimum absolute value to consider "meaningful"
    
    Returns:
        Percentage deviation (0-100+), or 0 if both ref values are near-zero
    """
    # Try forecast first, fall back to training mean
    ref = yhat if abs(yhat) >= abs_threshold else training_mean
    
    # If reference is still near-zero, both are too small to compute meaningful %
    if abs(ref) < abs_threshold:
        # Return large % if current is significant, else 0
        return (
            (abs(current) / abs_threshold * 100)
            if abs(current) >= abs_threshold * 10
            else 0.0
        )
    
    return abs(current - yhat) / abs(ref) * 100


# ── Per-metric directional + severity policy ──────────────────────────────────
from dataclasses import dataclass as _dc

@_dc
class _MetricPolicy:
    alert_high:          bool  = True
    alert_low:           bool  = True
    high_severity:       str   = "critical"
    low_severity:        str   = "critical"
    low_near_zero_only:  bool  = False
    low_pct_threshold:   float = 0.0


_METRIC_POLICY: Dict[str, _MetricPolicy] = {
    "network_packets_in":               _MetricPolicy(True,  True,  "critical", "critical", low_near_zero_only=True),
    "network_packets_out":              _MetricPolicy(True,  True,  "critical", "critical", low_near_zero_only=True),
    "network_in_bytes":                 _MetricPolicy(True,  False, "critical", "warning"),
    "network_out_bytes":                _MetricPolicy(True,  False, "critical", "warning"),
    "network_receive_bytes_per_sec":    _MetricPolicy(True,  True,  "critical", "critical", low_near_zero_only=True),
    "network_transmit_bytes_per_sec":   _MetricPolicy(True,  True,  "critical", "critical", low_near_zero_only=True),
    "cpu_utilization_percent":          _MetricPolicy(True,  True,  "critical", "warning",  low_pct_threshold=70.0),
    "read_latency_seconds":             _MetricPolicy(True,  False, "critical", "warning"),
    "write_latency_seconds":            _MetricPolicy(True,  False, "critical", "warning"),
    "disk_queue_depth":                 _MetricPolicy(True,  False, "critical", "warning"),
    "duration_avg_ms":                  _MetricPolicy(True,  False, "critical", "warning"),
    "duration_max_ms":                  _MetricPolicy(True,  False, "critical", "warning"),
    "target_response_time_s":           _MetricPolicy(True,  False, "critical", "warning"),
    "replica_lag_seconds":              _MetricPolicy(True,  False, "critical", "warning"),
    "replica_lag_aurora_seconds":       _MetricPolicy(True,  False, "critical", "warning"),
    "free_storage_bytes":               _MetricPolicy(False, True,  "warning",  "critical"),
    "freeable_memory_bytes":            _MetricPolicy(False, True,  "warning",  "critical"),
    "burst_balance_percent":            _MetricPolicy(False, True,  "warning",  "critical"),
    "healthy_host_count":               _MetricPolicy(False, True,  "warning",  "critical"),
    "database_connections":             _MetricPolicy(True,  True,  "critical", "warning",  low_near_zero_only=True),
    "read_iops":                        _MetricPolicy(True,  True,  "warning",  "warning",  low_near_zero_only=True),
    "write_iops":                       _MetricPolicy(True,  True,  "warning",  "warning",  low_near_zero_only=True),
    "concurrent_executions":            _MetricPolicy(True,  False, "critical", "warning"),
    "unreserved_concurrent_executions": _MetricPolicy(True,  False, "critical", "warning"),
    "active_connections":               _MetricPolicy(True,  True,  "warning",  "warning",  low_near_zero_only=True),
    "request_count":                    _MetricPolicy(True,  False, "warning",  "warning"),
    "binlog_disk_usage_bytes":          _MetricPolicy(True,  False, "warning",  "warning"),
    "swap_usage_bytes":                 _MetricPolicy(True,  False, "warning",  "warning"),
}


def _get_policy(metric_name: str) -> _MetricPolicy:
    return _METRIC_POLICY.get(metric_name, _MetricPolicy())


def _direction_severity(metric_name: str, current: float, ref: float) -> Optional[str]:
    policy   = _get_policy(metric_name)
    going_up = current > ref
    if going_up:
        return policy.high_severity if policy.alert_high else None
    if not policy.alert_low:
        return None
    if policy.low_near_zero_only:
        if current > max(abs(ref) * 0.05, 1e-3):
            return None
    if policy.low_pct_threshold > 0.0 and ref > 1e-9:
        if (ref - current) / ref * 100 < policy.low_pct_threshold:
            return None
    return policy.low_severity


def _alert_high_ok(metric_name: str) -> bool:
    return _get_policy(metric_name).alert_high


_ONLY_HIGH_IS_BAD: Set[str] = {m for m, p in _METRIC_POLICY.items() if p.alert_high and not p.alert_low}
_ONLY_LOW_IS_BAD:  Set[str] = {m for m, p in _METRIC_POLICY.items() if p.alert_low  and not p.alert_high}

# ── Detection config sets ─────────────────────────────────────────────────────
_DEFAULT_ALWAYS_BAD: Set[str] = {
    "status_check_failed", "status_check_failed_instance",
    "status_check_failed_system", "throttles_total", "errors_total",
    "system_errors", "user_errors", "throttled_requests", "unhealthy_host_count",
    "http_5xx_count", "replica_lag_aurora_seconds",
}
_DEFAULT_IGNORE: Set[str] = {
    "disk_read_bytes", "disk_write_bytes", "disk_read_ops",
    "disk_write_ops", "processed_bytes",
}
_DEFAULT_METRIC_SENSITIVITY: Dict[str, float] = {
    "cpu_utilization_percent": 2.5,  "database_connections": 2.0,
    "read_latency_seconds": 2.0,     "write_latency_seconds": 2.0,
    "duration_avg_ms": 2.5,          "target_response_time_s": 2.0,
    "free_storage_bytes": 3.0,       "freeable_memory_bytes": 2.5,
    "request_count": 2.0,            "invocations_total": 2.5,
    "network_transmit_bytes_per_sec": 2.5,
    "network_receive_bytes_per_sec":  2.5,
    "disk_queue_depth": 2.5,
    "read_iops": 2.5,                "write_iops": 2.5,
    "network_in_bytes": 3.5,         "network_out_bytes": 3.5,
    "network_packets_in": 3.5,       "network_packets_out": 3.5,
    "concurrent_executions": 2.0,    "unreserved_concurrent_executions": 2.0,
}
_DEFAULT_HARD_LIMITS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    "burst_balance_percent":            (5.0,           None),
    "healthy_host_count":               (1.0,            None),
    "concurrent_executions":            (None,           800.0),
    "unreserved_concurrent_executions": (None,           800.0),
    "cpu_utilization_percent":          (None,           90.0),
    "freeable_memory_bytes":            (50_000_000,    None),
    "free_storage_bytes":               (1_073_741_824,  None),
    "replica_lag_seconds":              (None,           30.0),
    "read_latency_seconds":             (None,           1.0),
    "write_latency_seconds":            (None,           1.0),
    "disk_queue_depth":                 (None,           10.0),
    "duration_max_ms":                  (None,           28_000.0),
    "target_response_time_s":           (None,           5.0),
    "http_5xx_count":                   (None,           0.5),
    "database_connections":             (None,           80.0),
}

ALWAYS_BAD_METRICS: Set[str] = set(_DEFAULT_ALWAYS_BAD)
IGNORE_METRICS:     Set[str] = set(_DEFAULT_IGNORE)
METRIC_SENSITIVITY: Dict[str, float] = dict(_DEFAULT_METRIC_SENSITIVITY)
HARD_LIMITS: Dict[str, Tuple[Optional[float], Optional[float]]] = dict(_DEFAULT_HARD_LIMITS)


def _load_detection_config(config_path: str) -> None:
    global ALWAYS_BAD_METRICS, IGNORE_METRICS, METRIC_SENSITIVITY, HARD_LIMITS
    try:
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f) or {}
        det = cfg.get("anomaly_detection", {})
        if "always_bad_metrics" in det:
            ALWAYS_BAD_METRICS = set(_DEFAULT_ALWAYS_BAD) | set(det["always_bad_metrics"])
        if "ignore_metrics" in det:
            IGNORE_METRICS = set(_DEFAULT_IGNORE) | set(det["ignore_metrics"])
        if "metric_sensitivity" in det:
            METRIC_SENSITIVITY.update(det["metric_sensitivity"])
        if "hard_limits" in det:
            for metric, bounds in det["hard_limits"].items():
                HARD_LIMITS[metric] = (bounds.get("floor"), bounds.get("ceiling"))
    except FileNotFoundError:
        log.warning(f"Config not found at {config_path!r} — using defaults.")
    except Exception as e:
        log.warning(f"Could not parse config: {e} — using defaults.")


# ── Helpers ───────────────────────────────────────────────────────────────────
def _parse_ts(ts: str) -> datetime:
    ts = ts.replace("Z", "+00:00")
    if "+" not in ts[10:] and len(ts) == 19:
        ts += "+00:00"
    return datetime.fromisoformat(ts)


def _to_df(history_rows: List[Tuple[str, float]]) -> pd.DataFrame:
    records = []
    for ts, val in history_rows:
        try:
            records.append({"ds": _parse_ts(ts).replace(tzinfo=None), "y": val})
        except Exception:
            pass
    return pd.DataFrame(records)


# ── PATCH #6: Metric-specific minimum datapoints ──────────────────────────────
def _metric_min_datapoints(metric_name: str) -> int:
    """
    Return minimum datapoints required for warmup, per metric type.
    
    Network metrics are naturally sparse (not every interval has packets).
    Status checks are binary (0 or >0), so need fewer points.
    Other metrics collect frequently and need more history.
    """
    return {
        # Network: sparse by nature
        "network_packets_in": 2,
        "network_packets_out": 2,
        "network_in_bytes": 2,
        "network_out_bytes": 2,
        # Status checks: binary, need fewer
        "status_check_failed": 1,
        "status_check_failed_instance": 1,
        "status_check_failed_system": 1,
        # Always-bad: non-zero is failure
        "throttles_total": 1,
        "errors_total": 1,
        # Default: require more history
    }.get(metric_name, MIN_DATAPOINTS)


def _warmup_ok(
    history_rows: List[Tuple[str, float]], metric_name: str = None
) -> Tuple[bool, str]:
    """
    Check if metric has enough historical data for statistical analysis.
    
    Different metrics have different requirements:
    - Network packets: 2 points (sparse collector)
    - Status checks: 1 point (binary, state is immediately informative)
    - Others: 8 points (default, for robust std dev estimation)
    """
    min_pts = _metric_min_datapoints(metric_name) if metric_name else MIN_DATAPOINTS
    
    if len(history_rows) < min_pts:
        return False, f"only {len(history_rows)}/{min_pts} pts"
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


def _span_hours(history_rows: List[Tuple[str, float]]) -> float:
    if len(history_rows) < 2:
        return 0.0
    try:
        return (
            _parse_ts(history_rows[-1][0]) - _parse_ts(history_rows[0][0])
        ).total_seconds() / 3600
    except Exception:
        return 0.0


def _is_stale(latest_at: str) -> bool:
    try:
        age_s = (datetime.now(timezone.utc) - _parse_ts(latest_at)).total_seconds()
        return age_s > STALE_THRESHOLD_MINUTES * 60
    except Exception:
        return True


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL REGISTRY (PATCH #5: Thread-safe cache)
# ═══════════════════════════════════════════════════════════════════════════════
class ModelRegistry:
    def __init__(self, model_dir: str = MODEL_DIR) -> None:
        self._dir = Path(model_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Any] = {}
        self._prophet_lock    = threading.Lock()
        self._iso_lock        = threading.Lock()
        self._river_lock_save = threading.Lock()
        log.info(f"ModelRegistry: {self._dir.resolve()}")

    @property
    def _iso_path(self)     -> Path: return self._dir / "isoforest.joblib"
    @property
    def _prophet_path(self) -> Path: return self._dir / "prophet.joblib"
    @property
    def _river_path(self)   -> Path: return self._dir / "river.joblib"
    @property
    def _meta_path(self)    -> Path: return self._dir / "models.meta.json"

    def _atomic_save(self, path: Path, obj: Any) -> None:
        tmp = path.with_name(f"{path.stem}_{uuid.uuid4().hex}.tmp")
        try:
            joblib.dump(obj, tmp, compress=3)
            tmp.replace(path)
        except Exception as e:
            tmp.unlink(missing_ok=True)
            raise e

    def _load_meta(self) -> Dict[str, Any]:
        try:
            return json.loads(self._meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_meta(self, meta: Dict[str, Any]) -> None:
        tmp = self._meta_path.with_name(f"{self._meta_path.stem}_{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        tmp.replace(self._meta_path)

    def _update_meta(self, algo: str, data_size: int, last_ts: str) -> None:
        meta = self._load_meta()
        meta[algo] = {
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "data_size":  data_size,
            "last_ts":    last_ts,
        }
        self._save_meta(meta)

    # ── IsolationForest (PATCH #5: Thread-safe) ───────────────────────────────
    def save_isoforest(
        self, model: IsolationForest, metric_stats: Dict[str, Dict[str, float]],
        metric_id_map: Dict[str, float], data_size: int, last_ts: str,
    ) -> None:
        payload = {"model": model, "metric_stats": metric_stats, "metric_id_map": metric_id_map}
        with self._iso_lock:
            self._atomic_save(self._iso_path, payload)
            self._update_meta("isoforest", data_size, last_ts)
            self._cache["isoforest"] = payload
        log.info(f"[Registry] isoforest saved ({data_size} pts / {len(metric_stats)} metrics)")

    def load_isoforest(self) -> Tuple[Optional[IsolationForest], Dict, Dict]:
        """Load IsoForest model with thread-safe cache."""
        with self._iso_lock:
            if "isoforest" in self._cache:
                p = self._cache["isoforest"]
                return p["model"], p["metric_stats"], p.get("metric_id_map", {})
            
            if not self._iso_path.exists():
                return None, {}, {}
            
            try:
                p = joblib.load(self._iso_path)
                self._cache["isoforest"] = p
                return p["model"], p["metric_stats"], p.get("metric_id_map", {})
            except Exception as e:
                log.warning(f"[Registry] Corrupt isoforest: {e}")
                return None, {}, {}

    # ── Prophet (PATCH #5: Thread-safe) ──────────────────────────────────────
    def evict_prophet_series(self, resource_id: str, metric_name: str) -> None:
        key = f"{resource_id}::{metric_name}"
        with self._prophet_lock:
            d = self.load_prophet_all()
            if key in d:
                del d[key]
                self._atomic_save(self._prophet_path, d)
                self._cache["prophet"] = d
                log.info(f"[Registry] Evicted drifted Prophet: {key}")

    def save_prophet_series(
        self, resource_id: str, metric_name: str,
        model: Any, data_size: int, last_ts: str,
    ) -> None:
        with self._prophet_lock:
            d   = self.load_prophet_all()
            key = f"{resource_id}::{metric_name}"
            d[key] = {
                "model": model, "trained_at": datetime.now(timezone.utc).isoformat(),
                "data_size": data_size, "last_ts": last_ts,
            }
            self._atomic_save(self._prophet_path, d)
            self._cache["prophet"] = d
        log.info(f"[Registry] prophet saved — {key} ({len(d)} series)")

    def load_prophet_all(self) -> Dict[str, Any]:
        """Load all Prophet models with thread-safe cache."""
        with self._prophet_lock:
            if "prophet" in self._cache:
                return copy.deepcopy(self._cache["prophet"])
            
            if not self._prophet_path.exists():
                return {}
            
            try:
                d = joblib.load(self._prophet_path)
                self._cache["prophet"] = d
                return copy.deepcopy(d)
            except Exception as e:
                log.warning(f"[Registry] Corrupt prophet: {e}")
                return {}

    def load_prophet_series(
        self, resource_id: str, metric_name: str
    ) -> Tuple[Optional[Any], Optional[Dict]]:
        entry = self.load_prophet_all().get(f"{resource_id}::{metric_name}")
        return (entry["model"], entry) if entry else (None, None)

    # ── River (PATCH #5: Thread-safe) ───────────────────────────────────────
    def save_river(self, model: Any, data_size: int, last_ts: str) -> None:
        with self._river_lock_save:
            self._atomic_save(self._river_path, model)
            self._update_meta("river", data_size, last_ts)
            self._cache["river"] = model

    def load_river(self) -> Optional[Any]:
        """Load River model with thread-safe cache."""
        with self._river_lock_save:
            if "river" in self._cache:
                return self._cache["river"]
            
            if not self._river_path.exists():
                return None
            
            try:
                m = joblib.load(self._river_path)
                self._cache["river"] = m
                return m
            except Exception as e:
                log.warning(f"[Registry] Corrupt river: {e}")
                return None

    def needs_retrain(
        self, algo: str, current_data_size: int,
        retrain_threshold: float, max_age_hours: float,
    ) -> Tuple[bool, str]:
        meta = self._load_meta().get(algo)
        if meta is None:
            return True, "cold start"
        try:
            age_h = (
                datetime.now(timezone.utc) - datetime.fromisoformat(meta["trained_at"])
            ).total_seconds() / 3600
        except Exception:
            return True, "unreadable trained_at"
        if age_h >= max_age_hours:
            return True, f"age {age_h:.1f}h >= {max_age_hours}h"
        last_size = meta.get("data_size", 0)
        if last_size > 0:
            growth = (current_data_size - last_size) / last_size
            if growth >= retrain_threshold:
                return True, f"+{growth*100:.1f}% new data"
        return False, f"fresh ({age_h:.1f}h)"


# ═══════════════════════════════════════════════════════════════════════════════
# CONTINUOUS TRAINER
# ═══════════════════════════════════════════════════════════════════════════════
class ContinuousTrainer:

    def __init__(self, registry: ModelRegistry) -> None:
        self.registry = registry
        self._river_model: Optional[Any] = None
        self._river_dirty: bool = False
        self._river_lock  = threading.Lock()

    def maybe_retrain_isoforest(
        self, all_training_data: Dict[str, List[Tuple[str, float]]],
    ) -> None:
        total_rows = sum(len(v) for v in all_training_data.values())
        should, reason = self.registry.needs_retrain(
            "isoforest", total_rows, ISOFOREST_RETRAIN_THRESHOLD, ISOFOREST_MAX_AGE_HOURS,
        )
        if not should:
            log.debug(f"[IsoForest] Skip retrain: {reason}")
            return

        log.info(f"[IsoForest] Retrain: {reason} ({total_rows} pts, {len(all_training_data)} series)")

        metric_values: Dict[str, List[float]] = {}
        for key, rows in all_training_data.items():
            mn = key.split("::", 1)[1]
            metric_values.setdefault(mn, []).extend(v for _, v in rows)

        metric_stats: Dict[str, Dict[str, float]] = {
            m: {"mean": float(np.mean(vals)), "std": float(np.std(vals)) or 1.0}
            for m, vals in metric_values.items()
        }
        metric_id_map: Dict[str, float] = {
            m: i / max(len(metric_stats), 1)
            for i, m in enumerate(sorted(metric_stats))
        }

        rows_list: List[List[float]] = []
        for key, history_rows in all_training_data.items():
            mn  = key.split("::", 1)[1]
            st  = metric_stats[mn]
            mid = metric_id_map.get(mn, 0.0)
            vals   = np.array([v for _, v in history_rows])
            deltas = np.diff(vals, prepend=vals[0])
            try:
                hours = np.array([_parse_ts(ts).hour for ts, _ in history_rows], dtype=float)
            except Exception:
                hours = np.zeros(len(vals))
            norm_v = (vals   - st["mean"]) / st["std"]
            norm_d =  deltas / max(st["std"], 1e-9)
            for i in range(len(vals)):
                rows_list.append([
                    norm_v[i], norm_d[i],
                    np.sin(2 * np.pi * hours[i] / 24),
                    np.cos(2 * np.pi * hours[i] / 24),
                    mid,
                ])

        if len(rows_list) < 10:
            log.warning("[IsoForest] Not enough data for retrain")
            return
        try:
            clf = IsolationForest(
                contamination=ISOFOREST_CONTAMINATION, n_estimators=100,
                random_state=42, n_jobs=-1,
            )
            clf.fit(np.array(rows_list))
            last_ts = max(
                (rows[-1][0] for rows in all_training_data.values() if rows),
                default=datetime.now(timezone.utc).isoformat(),
            )
            self.registry.save_isoforest(clf, metric_stats, metric_id_map, total_rows, last_ts)
        except Exception as e:
            log.error(f"[IsoForest] Retrain failed: {e}")

    def score_isoforest(
        self, metric_name: str, current_value: float,
        history_rows: List[Tuple[str, float]],
    ) -> Tuple[int, float]:
        model, metric_stats, metric_id_map = self.registry.load_isoforest()
        if model is None or metric_name not in metric_stats:
            return 1, 0.0
        st     = metric_stats[metric_name]
        prev   = history_rows[-1][1] if history_rows else current_value
        delta  = current_value - prev
        hour   = datetime.now(timezone.utc).hour
        norm_v = (current_value - st["mean"]) / st["std"]
        norm_d =  delta / max(st["std"], 1e-9)
        mid    = metric_id_map.get(metric_name, 0.0)
        X = np.array([[
            norm_v, norm_d,
            np.sin(2 * np.pi * hour / 24),
            np.cos(2 * np.pi * hour / 24),
            mid,
        ]])
        try:
            return int(model.predict(X)[0]), float(model.score_samples(X)[0])
        except Exception as e:
            log.debug(f"[IsoForest] score failed {metric_name}: {e}")
            return 1, 0.0

    def maybe_retrain_prophet(
        self, resource_id: str, metric_name: str,
        history_rows: List[Tuple[str, float]], sens: float,
    ) -> Optional[Any]:
        if not PROPHET_AVAILABLE:
            return None
        df = _to_df(history_rows)
        if len(df) < PROPHET_INTERNAL_MIN_ROWS:
            return None

        existing_model, existing_meta = self.registry.load_prophet_series(resource_id, metric_name)
        last_size      = (existing_meta or {}).get("data_size", 0)
        trained_at_str = (existing_meta or {}).get("trained_at")
        age_h          = float("inf")
        if trained_at_str:
            try:
                age_h = (
                    datetime.now(timezone.utc) - datetime.fromisoformat(trained_at_str)
                ).total_seconds() / 3600
            except Exception:
                pass

        # Dual eviction gate for drift detection
        if existing_model is not None:
            try:
                training_mean = float(df["y"].mean())
                training_std  = float(df["y"].std()) if len(df) > 1 else 0.0
                now_naive     = datetime.now(timezone.utc).replace(tzinfo=None)
                fc_check      = existing_model.predict(pd.DataFrame({"ds": [now_naive]}))
                yhat_check    = float(fc_check["yhat"].iloc[0])

                evict        = False
                evict_reason = ""
                if training_mean > 1e-6 and abs(yhat_check) > PROPHET_IMPLAUSIBLE_FACTOR * training_mean:
                    evict = True
                    evict_reason = (
                        f"yhat={yhat_check:.2f} > {PROPHET_IMPLAUSIBLE_FACTOR}× "
                        f"mean={training_mean:.2f}"
                    )
                elif (
                    PROPHET_IMPLAUSIBLE_ABS_FACTOR > 0
                    and training_std > 1e-9
                    and abs(yhat_check - training_mean) > PROPHET_IMPLAUSIBLE_ABS_FACTOR * training_std
                ):
                    evict = True
                    evict_reason = (
                        f"|yhat-mean|={abs(yhat_check-training_mean):.2f} > "
                        f"{PROPHET_IMPLAUSIBLE_ABS_FACTOR}× std={training_std:.2f}"
                    )

                if evict:
                    log.warning(
                        f"[Prophet] Drift eviction {resource_id}::{metric_name}: {evict_reason}"
                    )
                    self.registry.evict_prophet_series(resource_id, metric_name)
                    existing_model = None
                    existing_meta  = None
            except Exception:
                pass

        growth = (len(df) - last_size) / max(last_size, 1) if last_size else 1.0
        needs  = (
            existing_model is None
            or age_h >= PROPHET_MAX_AGE_HOURS
            or growth >= PROPHET_RETRAIN_THRESHOLD
        )
        if not needs:
            return existing_model

        changepoints = None
        if existing_model is not None:
            try:
                changepoints = list(existing_model.changepoints)
            except Exception:
                pass

        reason = (
            "cold start" if existing_model is None
            else f"age {age_h:.1f}h" if age_h >= PROPHET_MAX_AGE_HOURS
            else f"+{growth*100:.1f}% new data"
        )
        span_h = _span_hours(history_rows)
        log.info(
            f"[Prophet] Retraining {resource_id}::{metric_name}: {reason}"
            + (" [warm-start]" if changepoints else "")
        )
        try:
            kwargs: Dict[str, Any] = dict(
                interval_width=min(0.99, 1 - 1 / (sens * 3)),
                daily_seasonality=span_h >= 24,
                weekly_seasonality=span_h >= 168,
                yearly_seasonality=False,
                changepoint_prior_scale=0.05,
            )
            if changepoints:
                kwargs["changepoints"] = changepoints
            m = Prophet(**kwargs)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                m.fit(df)
            self.registry.save_prophet_series(
                resource_id, metric_name, m, len(df), history_rows[-1][0]
            )
            return m
        except Exception as e:
            log.warning(f"[Prophet] Retrain failed {resource_id}::{metric_name}: {e}")
            return existing_model

    def get_or_create_river(self) -> Optional[Any]:
        if not RIVER_AVAILABLE:
            return None
        with self._river_lock:
            if self._river_model is None:
                m = self.registry.load_river()
                if m is None:
                    log.info("[River] Creating new HalfSpaceTrees model")
                    m = river_anomaly.HalfSpaceTrees(
                        n_trees=25, height=8, window_size=250, seed=42
                    )
                self._river_model = m
        return self._river_model

    def learn_and_score_river(
        self, model: Any, x: Dict[str, float], data_size: int, last_ts: str,
    ) -> float:
        if not RIVER_AVAILABLE or model is None:
            return 0.0
        try:
            with self._river_lock:
                score = model.score_one(x)
                model.learn_one(x)
                self._river_dirty = True
            return float(score)
        except Exception as e:
            log.debug(f"[River] score failed: {e}")
            return 0.0

    def flush_river(self, data_size: int, last_ts: str) -> None:
        if not RIVER_AVAILABLE or self._river_model is None:
            return
        with self._river_lock:
            if not self._river_dirty:
                return
            self.registry.save_river(self._river_model, data_size, last_ts)
            self._river_dirty = False


# ── Data class ────────────────────────────────────────────────────────────────
@dataclass
class Anomaly:
    detected_at:    str
    cloud:          str
    region:         str
    resource_type:  str
    resource_id:    str
    resource_name:  str
    metric_name:    str
    metric_unit:    str
    current_value:  float
    avg_value:      float
    std_value:      float
    upper_bound:    float
    lower_bound:    float
    severity:       str
    reason:         str
    data_points:    int
    algorithm:      str = "zscore"
    correlation_id: str = ""

    @property
    def deviation(self) -> float:
        if self.algorithm in ("hard_limit", "always_bad", "cpu_safety_net"):
            return abs(self.current_value - self.avg_value) / max(abs(self.avg_value), 1e-9)
        return (
            0.0 if self.std_value == 0
            else abs(self.current_value - self.avg_value) / self.std_value
        )

    def slack_message(self) -> dict:
        icon = "🔴" if self.severity == "critical" else "🟡"
        algo_label = {
            "prophet":          "📈 Prophet",
            "isolation_forest": "🌲 Isolation Forest",
            "river":            "🌊 River (online)",
            "zscore":           "📊 Z-score",
            "always_bad":       "🚨 Always-bad",
            "hard_limit":       "🔒 Hard Limit",
            "cpu_safety_net":   "🔥 CPU Safety Net",
        }.get(self.algorithm, self.algorithm)
        direction = "▲ ABOVE" if self.current_value > self.upper_bound else "▼ BELOW"
        return {
            "text": f"{icon} Anomaly — {self.resource_name} / {self.metric_name}",
            "blocks": [
                {"type": "header", "text": {"type": "plain_text", "text": f"{icon} {self.resource_name}"}},
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Resource:*\n{self.resource_name}"},
                        {"type": "mrkdwn", "text": f"*Metric:*\n{self.metric_name}"},
                        {"type": "mrkdwn", "text": f"*Severity:*\n{self.severity.upper()}"},
                        {"type": "mrkdwn", "text": f"*Current:*\n{self.current_value:.4f} {direction}"},
                        {"type": "mrkdwn", "text": f"*Range:*\n{self.lower_bound:.4f}–{self.upper_bound:.4f}"},
                        {"type": "mrkdwn", "text": f"*Algorithm:*\n{algo_label}"},
                    ],
                },
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn", "text": (
                        f"{self.reason} | {self.data_points} pts | {self.detected_at}"
                        + (f" | corr:{self.correlation_id}" if self.correlation_id else "")
                    )}],
                },
            ],
        }


# ── Database ──────────────────────────────────────────────────────────────────
class MetricsReader:
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
    _DDL_MIGRATE = [
        ("algorithm",      "TEXT NOT NULL DEFAULT 'zscore'"),
        ("correlation_id", "TEXT NOT NULL DEFAULT ''"),
    ]

    def __init__(self, config_path: str) -> None:
        with open(config_path, encoding="utf-8") as f:
            cfg = yaml.safe_load(f)
        storage = cfg.get("storage", {})
        backend = storage.get("backend", "sqlite").lower()
        self._write_lock = threading.Lock()
        self._prev_metric_count: int = 0

        if backend == "sqlite":
            db_path = storage.get("sqlite", {}).get("path", "observability_data/metrics.db")
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
            self._conn = sqlite3.connect(db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.executescript(self._DDL_CREATE)
            existing_cols = {row[1] for row in self._conn.execute("PRAGMA table_info(anomalies)")}
            for col, defn in self._DDL_MIGRATE:
                if col not in existing_cols:
                    self._conn.execute(f"ALTER TABLE anomalies ADD COLUMN {col} {defn}")
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_an_corr ON anomalies(correlation_id)")
            self._conn.commit()
            self._backend = "sqlite"
            log.info(f"Connected SQLite -> {db_path}")
        else:
            try:
                import psycopg2, psycopg2.extras
                self._psycopg2 = psycopg2
                self._extras   = psycopg2.extras
            except ImportError:
                raise ImportError("pip install psycopg2-binary")
            pg  = storage.get("postgres", {})
            dsn = (
                pg.get("dsn") or os.getenv("DATABASE_URL")
                or (
                    f"postgresql://{pg.get('user','postgres')}:{pg.get('password','')}@"
                    f"{pg.get('host','localhost')}:{pg.get('port',5432)}"
                    f"/{pg.get('dbname','observability')}"
                )
            )
            self._conn    = psycopg2.connect(dsn)
            self._backend = "postgres"
            with self._conn.cursor() as cur:
                ddl_pg = (
                    self._DDL_CREATE
                    .replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replace("INTEGER NOT NULL DEFAULT 0",        "SMALLINT NOT NULL DEFAULT 0")
                )
                cur.execute(ddl_pg)
                cur.execute(
                    "SELECT column_name FROM information_schema.columns WHERE table_name='anomalies'"
                )
                existing_pg = {r[0] for r in cur.fetchall()}
                for col, defn in self._DDL_MIGRATE:
                    if col not in existing_pg:
                        cur.execute(f"ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS {col} {defn}")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_an_corr ON anomalies(correlation_id)")
            self._conn.commit()
            log.info("Connected PostgreSQL")

    def get_all_active_metrics(self) -> List[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        if self._backend == "sqlite":
            rows = self._conn.execute(
                """
                SELECT m.cloud, m.region, m.resource_type, m.resource_id,
                       m.resource_name, m.metric_name, m.metric_unit,
                       m.metric_value, m.collected_at AS latest_at
                FROM metrics m
                INNER JOIN (
                    SELECT resource_id, metric_name, MAX(collected_at) AS max_ts
                    FROM metrics WHERE collected_at >= ?
                    GROUP BY resource_id, metric_name
                ) latest
                  ON  m.resource_id  = latest.resource_id
                  AND m.metric_name  = latest.metric_name
                  AND m.collected_at = latest.max_ts
                ORDER BY m.resource_type, m.resource_name, m.metric_name
                """,
                (cutoff,),
            ).fetchall()
        else:
            with self._conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT DISTINCT ON (resource_id, metric_name) "
                    "cloud, region, resource_type, resource_id, resource_name, "
                    "metric_name, metric_unit, metric_value, collected_at AS latest_at "
                    "FROM metrics WHERE collected_at >= %s "
                    "ORDER BY resource_id, metric_name, collected_at DESC",
                    (cutoff,),
                )
                rows = cur.fetchall()

        result = [dict(r) for r in rows]

        active = []
        stale_names: List[str] = []
        for r in result:
            if _is_stale(r["latest_at"]):
                stale_names.append(f"{r['resource_name']}/{r['metric_name']}")
            else:
                active.append(r)

        if stale_names:
            log.warning(
                f"Filtered {len(stale_names)} stale metric(s) "
                f"(threshold={STALE_THRESHOLD_MINUTES}m). "
                f"First few: {stale_names[:5]}"
            )

        current_count = len(active)
        if self._prev_metric_count > 0:
            drop_pct = (self._prev_metric_count - current_count) / self._prev_metric_count
            if drop_pct > 0.5:
                log.warning(
                    f"Metric count dropped {drop_pct*100:.0f}%: "
                    f"{self._prev_metric_count} → {current_count}."
                )
        self._prev_metric_count = current_count
        return active

    def get_all_history_bulk(self, hours: int) -> Dict[str, List[Tuple[str, float]]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        if self._backend == "sqlite":
            rows = self._conn.execute(
                "SELECT resource_id, metric_name, collected_at, metric_value "
                "FROM metrics WHERE collected_at >= ? "
                "ORDER BY resource_id, metric_name, collected_at ASC",
                (cutoff,),
            ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT resource_id, metric_name, collected_at, metric_value "
                    "FROM metrics WHERE collected_at >= %s "
                    "ORDER BY resource_id, metric_name, collected_at ASC",
                    (cutoff,),
                )
                rows = cur.fetchall()
        result: Dict[str, List[Tuple[str, float]]] = {}
        for r in rows:
            canonical = _normalize_metric_name(r[1])
            result.setdefault(f"{r[0]}::{canonical}", []).append((r[2], float(r[3])))
        return result

    def get_history(
        self, resource_id: str, metric_name: str, hours: int = LOOKBACK_HOURS
    ) -> List[Tuple[str, float]]:
        """
        Fetch metric history, normalizing metric_name aliases.
        IMPORTANT: Matches both raw metric names and their canonical forms
        to handle collector variant names (e.g. CPUUtilization → cpu_utilization_percent).
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        canonical = _normalize_metric_name(metric_name)
        if self._backend == "sqlite":
            rows = self._conn.execute(
                "SELECT collected_at, metric_value FROM metrics "
                "WHERE resource_id = ? AND metric_name IN (?, ?) AND collected_at >= ? "
                "ORDER BY collected_at ASC",
                (resource_id, metric_name, canonical, cutoff),
            ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT collected_at, metric_value FROM metrics "
                    "WHERE resource_id = %s AND metric_name IN (%s, %s) AND collected_at >= %s "
                    "ORDER BY collected_at ASC",
                    (resource_id, metric_name, canonical, cutoff),
                )
                rows = cur.fetchall()
        return [(r[0], float(r[1])) for r in rows]

    def get_all_training_data(self, hours: int) -> Dict[str, List[Tuple[str, float]]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        if self._backend == "sqlite":
            ph = ",".join("?" * len(IGNORE_METRICS)) if IGNORE_METRICS else "'__none__'"
            rows = self._conn.execute(
                f"SELECT resource_id, metric_name, collected_at, metric_value "
                f"FROM metrics WHERE collected_at >= ? "
                f"{'AND metric_name NOT IN (' + ph + ')' if IGNORE_METRICS else ''} "
                f"ORDER BY resource_id, metric_name, collected_at ASC",
                (cutoff, *IGNORE_METRICS) if IGNORE_METRICS else (cutoff,),
            ).fetchall()
        else:
            with self._conn.cursor() as cur:
                if IGNORE_METRICS:
                    ph = ",".join(["%s"] * len(IGNORE_METRICS))
                    cur.execute(
                        f"SELECT resource_id, metric_name, collected_at, metric_value "
                        f"FROM metrics WHERE collected_at >= %s "
                        f"AND metric_name NOT IN ({ph}) "
                        f"ORDER BY resource_id, metric_name, collected_at ASC",
                        (cutoff, *IGNORE_METRICS),
                    )
                else:
                    cur.execute(
                        "SELECT resource_id, metric_name, collected_at, metric_value "
                        "FROM metrics WHERE collected_at >= %s "
                        "ORDER BY resource_id, metric_name, collected_at ASC",
                        (cutoff,),
                    )
                rows = cur.fetchall()
        result: Dict[str, List[Tuple[str, float]]] = {}
        for r in rows:
            canonical = _normalize_metric_name(r[1])
            result.setdefault(f"{r[0]}::{canonical}", []).append((r[2], float(r[3])))
        return result

    def recent_anomaly_count(
        self, resource_id: str, metric_name: str, minutes: int = DEDUP_WINDOW_MINUTES
    ) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        if self._backend == "sqlite":
            row = self._conn.execute(
                "SELECT COUNT(*) FROM anomalies "
                "WHERE resource_id = ? AND metric_name = ? AND detected_at >= ?",
                (resource_id, metric_name, cutoff),
            ).fetchone()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM anomalies "
                    "WHERE resource_id = %s AND metric_name = %s AND detected_at >= %s",
                    (resource_id, metric_name, cutoff),
                )
                row = cur.fetchone()
        return int(row[0]) if row else 0

    def save_anomaly(self, a: Anomaly) -> None:
        vals = (
            a.detected_at, a.cloud, a.region, a.resource_type, a.resource_id,
            a.resource_name, a.metric_name, a.metric_unit, a.current_value,
            a.avg_value, a.std_value, a.upper_bound, a.lower_bound,
            a.severity, a.reason, a.data_points, a.algorithm, a.correlation_id,
        )
        with self._write_lock:
            if self._backend == "sqlite":
                self._conn.execute(
                    "INSERT INTO anomalies "
                    "(detected_at,cloud,region,resource_type,resource_id,resource_name,"
                    "metric_name,metric_unit,current_value,avg_value,std_value,"
                    "upper_bound,lower_bound,severity,reason,data_points,algorithm,correlation_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    vals,
                )
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO anomalies "
                        "(detected_at,cloud,region,resource_type,resource_id,resource_name,"
                        "metric_name,metric_unit,current_value,avg_value,std_value,"
                        "upper_bound,lower_bound,severity,reason,data_points,algorithm,correlation_id) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        vals,
                    )
                self._conn.commit()


# ── PATCH #8: Data Quality Assessment ─────────────────────────────────────────
def _assess_data_quality(
    all_metrics: List[dict],
    history_2h: Dict[str, List[Tuple[str, float]]],
) -> Dict[str, int]:
    """
    Assess data quality across all metrics.
    
    Returns counts of:
    - zero_values: Metrics with value=0 (may indicate dead sensor)
    - single_point: Metrics with only 1 historical datapoint
    - missing_history: Metrics with no history in 2h window
    - nan_inf: Metrics with NaN/Inf values
    """
    issues = {
        "zero_values": 0,
        "single_point": 0,
        "missing_history": 0,
        "nan_inf": 0,
        "negative_non_negative": 0,
    }
    
    for r in all_metrics:
        key = f"{r['resource_id']}::{r['metric_name']}"
        hist = history_2h.get(key, [])
        try:
            val = float(r["metric_value"])
        except (ValueError, TypeError):
            issues["nan_inf"] += 1
            continue
        
        # Detect bad values
        if not (-1e15 < val < 1e15):
            issues["nan_inf"] += 1
        elif r["metric_name"] in _NON_NEGATIVE_METRICS and val < 0:
            issues["negative_non_negative"] += 1
        elif val == 0.0 and "count" not in r["metric_name"].lower():
            issues["zero_values"] += 1
        
        # Detect missing history
        if not hist:
            issues["missing_history"] += 1
        elif len(hist) == 1:
            issues["single_point"] += 1
    
    return issues


# ── Detection layers ──────────────────────────────────────────────────────────

def _detect_high_cpu_safety_net(
    row: dict, history_rows: List[Tuple[str, float]], now_str: str,
) -> Optional[Anomaly]:
    """
    Layer 0 safety net: If any metric whose name contains "cpu" has a value
    >= CPU_SAFETY_NET_THRESHOLD, fire CRITICAL unconditionally.
    """
    raw_name = row.get("_raw_metric_name", row.get("metric_name", ""))
    canonical = row["metric_name"]

    is_cpu = (
        "cpu" in canonical.lower()
        or "cpu" in raw_name.lower()
    )
    if not is_cpu:
        return None

    current = float(row["metric_value"])
    if current < CPU_SAFETY_NET_THRESHOLD:
        return None

    values = [v for _, v in history_rows]
    avg    = statistics.mean(values) if values else current
    std    = statistics.stdev(values) if len(values) > 1 else 0.0

    return Anomaly(
        detected_at=now_str, cloud=row["cloud"], region=row["region"],
        resource_type=row["resource_type"], resource_id=row["resource_id"],
        resource_name=row["resource_name"], metric_name=canonical,
        metric_unit=row["metric_unit"], current_value=current,
        avg_value=round(avg, 6), std_value=round(std, 6),
        upper_bound=round(CPU_SAFETY_NET_THRESHOLD, 6),
        lower_bound=0.0,
        severity="critical",
        reason=(
            f"[CPU-SafetyNet] {canonical} (raw: {raw_name}) = {current:.2f}% "
            f">= {CPU_SAFETY_NET_THRESHOLD}% threshold"
        ),
        data_points=len(history_rows), algorithm="cpu_safety_net",
    )


def _detect_river(
    row: dict, history_rows: List[Tuple[str, float]],
    sens: float, now_str: str, trainer: ContinuousTrainer,
) -> Optional[Anomaly]:
    if not RIVER_AVAILABLE:
        return None
    model = trainer.get_or_create_river()
    if model is None:
        return None

    current = float(row["metric_value"])
    values  = [v for _, v in history_rows]
    avg     = statistics.mean(values) if values else 0.0
    std     = statistics.stdev(values) if len(values) > 1 else MIN_STD_FLOOR

    sev = _direction_severity(row["metric_name"], current, avg)
    if sev is None:
        return None

    prev  = history_rows[-2][1] if len(history_rows) >= 2 else current
    delta = current - prev
    score = trainer.learn_and_score_river(
        model, {"value": current, "delta": delta},
        data_size=len(history_rows),
        last_ts=history_rows[-1][0] if history_rows else now_str,
    )
    if score >= RIVER_SCORE_THRESHOLD:
        std_floor = max(std, abs(avg) * STD_FLOOR_PCT, MIN_STD_FLOOR)
        lower_bound = max(0.0, avg - sens * std_floor)
        upper_bound = avg + sens * std_floor
        if lower_bound <= current <= upper_bound:
            log.debug(
                f"[River/HST] Suppressed because {row['metric_name']} is inside z-score bounds"
            )
            return None
        if _suppress_transient_spike(history_rows, current, avg, lower_bound, upper_bound, row["metric_name"]):
            log.debug(f"[River/HST] Suppressed transient spike for {row['metric_name']}")
            return None
        pct_off   = abs(current - avg) / max(abs(avg), MIN_STD_FLOOR) * 100
        final_sev = "critical" if score > 0.9 else sev
        return Anomaly(
            detected_at=now_str, cloud=row["cloud"], region=row["region"],
            resource_type=row["resource_type"], resource_id=row["resource_id"],
            resource_name=row["resource_name"], metric_name=row["metric_name"],
            metric_unit=row["metric_unit"], current_value=current,
            avg_value=round(avg, 6), std_value=round(std, 6),
            upper_bound=round(avg + sens * std_floor, 6),
            lower_bound=round(max(0.0, avg - sens * std_floor), 6),
            severity=final_sev,
            reason=(
                f"[River/HST] {row['metric_name']} score {score:.3f} "
                f">= {RIVER_SCORE_THRESHOLD} ({pct_off:.1f}% from mean {avg:.4f})"
            ),
            data_points=len(history_rows), algorithm="river",
        )
    return None


def _detect_prophet(
    row: dict, history_rows: List[Tuple[str, float]],
    training_rows: List[Tuple[str, float]], sens: float,
    now_str: str, trainer: ContinuousTrainer,
) -> Optional[Anomaly]:
    """PATCH #3: Uses safe percentage deviation calculation."""
    m = trainer.maybe_retrain_prophet(row["resource_id"], row["metric_name"], training_rows, sens)
    if m is None:
        return None
    try:
        metric_name = row["metric_name"]
        now_naive   = datetime.now(timezone.utc).replace(tzinfo=None)
        forecast    = m.predict(pd.DataFrame({"ds": [now_naive]}))

        df_train     = _to_df(training_rows)
        training_avg = float(df_train["y"].mean()) if not df_train.empty else 0.0
        training_std = float(df_train["y"].std())  if len(df_train) > 1  else 0.0

        raw_yhat       = float(forecast["yhat"].iloc[0])
        raw_yhat_lower = float(forecast["yhat_lower"].iloc[0])
        raw_yhat_upper = float(forecast["yhat_upper"].iloc[0])

        yhat, yhat_lower, yhat_upper = _clamp_forecast(
            metric_name, training_avg, raw_yhat, raw_yhat_lower, raw_yhat_upper
        )

        current     = float(row["metric_value"])
        above_upper = current > yhat_upper
        below_lower = current < yhat_lower

        if not (above_upper or below_lower):
            return None

        # PATCH #3: Safe pct_off using training_avg as fallback denominator
        pct_off = _safe_pct_off(current, yhat, training_avg)

        min_pct = PROPHET_MIN_PCT_DEVIATION.get(metric_name, 0.0)
        if pct_off < min_pct:
            log.debug(
                f"[Prophet] Suppressed min-pct {metric_name}: {pct_off:.1f}% < {min_pct}%"
            )
            return None

        sev = _direction_severity(metric_name, current, yhat)
        if sev is None:
            log.debug(
                f"[Prophet] Suppressed by policy {metric_name}: "
                f"{'ABOVE' if above_upper else 'BELOW'} yhat={yhat:.4f}"
            )
            return None

        direction = "ABOVE" if above_upper else "BELOW"
        if pct_off > 50 and sev == "warning":
            sev = "critical"

        return Anomaly(
            detected_at=now_str, cloud=row["cloud"], region=row["region"],
            resource_type=row["resource_type"], resource_id=row["resource_id"],
            resource_name=row["resource_name"], metric_name=metric_name,
            metric_unit=row["metric_unit"], current_value=current,
            avg_value=round(training_avg, 6), std_value=round(training_std, 6),
            upper_bound=round(yhat_upper, 6),
            lower_bound=round(yhat_lower, 6),
            severity=sev,
            reason=(
                f"[Prophet] {metric_name} {current:.4f} — "
                f"{pct_off:.1f}% {direction} forecast {yhat:.4f} "
                f"(168h avg {training_avg:.4f})"
            ),
            data_points=len(df_train), algorithm="prophet",
        )
    except Exception as e:
        log.debug(f"[Prophet] predict failed {row['resource_name']}/{row['metric_name']}: {e}")
    return None


def _detect_isoforest(
    row: dict, history_rows: List[Tuple[str, float]],
    sens: float, now_str: str, trainer: ContinuousTrainer,
) -> Optional[Anomaly]:
    current     = float(row["metric_value"])
    pred, score = trainer.score_isoforest(row["metric_name"], current, history_rows)
    if pred != -1:
        return None

    values    = [v for _, v in history_rows]
    avg       = statistics.mean(values) if values else 0.0
    std       = statistics.stdev(values) if len(values) > 1 else 0.0
    std_floor = max(std, abs(avg) * STD_FLOOR_PCT, MIN_STD_FLOOR)
    pct_off   = abs(current - avg) / max(abs(avg), MIN_STD_FLOOR) * 100
    abs_off   = abs(current - avg)
    min_abs   = max(ISOFOREST_MIN_ABS_DEVIATION, _get_min_detection_delta(row["metric_name"], avg))

    if pct_off < ISOFOREST_MIN_PCT_DEVIATION:
        log.debug(f"[IsoForest] Suppressed pct {row['metric_name']}: {pct_off:.1f}%")
        return None
    if score > ISOFOREST_MIN_SCORE_THRESHOLD:
        log.debug(f"[IsoForest] Suppressed score {row['metric_name']}: {score:.3f}")
        return None
    if abs_off < min_abs:
        log.debug(f"[IsoForest] Suppressed abs-too-small {row['metric_name']}: {abs_off:.6f} < {min_abs:.6f}")
        return None
    if _suppress_transient_spike(history_rows, current, avg, max(0.0, avg - sens * std_floor), avg + sens * std_floor, row["metric_name"]):
        log.debug(f"[IsoForest] Suppressed transient spike for {row['metric_name']}")
        return None

    sev = _direction_severity(row["metric_name"], current, avg)
    if sev is None:
        return None
    if pct_off > 50:
        sev = "critical"

    return Anomaly(
        detected_at=now_str, cloud=row["cloud"], region=row["region"],
        resource_type=row["resource_type"], resource_id=row["resource_id"],
        resource_name=row["resource_name"], metric_name=row["metric_name"],
        metric_unit=row["metric_unit"], current_value=current,
        avg_value=round(avg, 6), std_value=round(std, 6),
        upper_bound=round(avg + sens * std_floor, 6),
        lower_bound=round(max(0.0, avg - sens * std_floor), 6),
        severity=sev,
        reason=(
            f"[IsoForest] {row['metric_name']} {current:.4f} — "
            f"outlier (score:{score:.3f}, {pct_off:.1f}% from mean {avg:.4f})"
        ),
        data_points=len(history_rows), algorithm="isolation_forest",
    )

_METRIC_MIN_ABS_DEVIATION: Dict[str, float] = {
    "write_latency_seconds":   0.005,
    "read_latency_seconds":    0.005,
    "disk_queue_depth":        0.05,
    "read_iops":               5.0,
    "write_iops":              5.0,
    "cpu_utilization_percent": 15.0,
    "network_in_bytes":        50_000,
    "network_out_bytes":       50_000,
}

def _get_min_detection_delta(metric_name: str, avg: float) -> float:
    return max(
        _METRIC_MIN_ABS_DEVIATION.get(metric_name, 0.0),
        abs(avg) * 0.005,
        MIN_STD_FLOOR,
    )


def _suppress_transient_spike(
    history_rows: List[Tuple[str, float]],
    current: float,
    avg: float,
    lower_bound: float,
    upper_bound: float,
    metric_name: str,
) -> bool:
    if len(history_rows) < 2:
        return False
    prev = history_rows[-1][1]
    if not (lower_bound <= prev <= upper_bound):
        return False
    delta = abs(current - prev)
    threshold = max(
        abs(avg) * TRANSIENT_SUPPRESSION_PCT / 100,
        TRANSIENT_SUPPRESSION_ABS,
        _get_min_detection_delta(metric_name, avg),
    )
    return delta <= threshold

def _detect_zscore(
    row: dict, history_rows: List[Tuple[str, float]],
    sens: float, now_str: str,
) -> Optional[Anomaly]:
    values  = [v for _, v in history_rows]
    current = float(row["metric_value"])
    if not values:
        return None
    avg = statistics.mean(values)
    std = statistics.stdev(values) if len(values) > 1 else 0.0
    if avg < 1e-9 and std < 1e-9:
        if current > 1e-9:
            if not _alert_high_ok(row["metric_name"]):
                return None
            sev = _get_policy(row["metric_name"]).high_severity
            return Anomaly(
                detected_at=now_str, cloud=row["cloud"], region=row["region"],
                resource_type=row["resource_type"], resource_id=row["resource_id"],
                resource_name=row["resource_name"], metric_name=row["metric_name"],
                metric_unit=row["metric_unit"], current_value=current,
                avg_value=0.0, std_value=0.0, upper_bound=0.0, lower_bound=0.0,
                severity=sev,
                reason=f"[Z-score] {row['metric_name']} was 0 but is now {current:.4f}",
                data_points=len(values), algorithm="zscore",
            )
        return None
    std_floor   = max(std, abs(avg) * STD_FLOOR_PCT, MIN_STD_FLOOR)
    upper_bound = avg + sens * std_floor
    lower_bound = max(0.0, avg - sens * std_floor)
    if _suppress_transient_spike(history_rows, current, avg, lower_bound, upper_bound, row["metric_name"]):
        log.debug(f"[Z-score] Suppressed transient spike for {row['metric_name']}")
        return None
    if current > upper_bound or current < lower_bound:
        sev = _direction_severity(row["metric_name"], current, avg)
        if sev is None:
            return None
        deviation = abs(current - avg) / std_floor
        min_abs = _get_min_detection_delta(row["metric_name"], avg)
        if abs(current - avg) < min_abs:
            log.debug(f"[Z-score] Suppressed abs-too-small {row['metric_name']}: "
                    f"|{current:.4f} - {avg:.4f}| < {min_abs:.6f}")
            return None
        if deviation > sens * 1.5:
            sev = "critical"
        direction = "ABOVE" if current > upper_bound else "BELOW"
        return Anomaly(
            detected_at=now_str, cloud=row["cloud"], region=row["region"],
            resource_type=row["resource_type"], resource_id=row["resource_id"],
            resource_name=row["resource_name"], metric_name=row["metric_name"],
            metric_unit=row["metric_unit"], current_value=current,
            avg_value=round(avg, 6), std_value=round(std, 6),
            upper_bound=round(upper_bound, 6), lower_bound=round(lower_bound, 6),
            severity=sev,
            reason=(
                f"[Z-score] {row['metric_name']} {current:.4f} — "
                f"{deviation:.1f}sigma {direction} {LOOKBACK_HOURS}h avg {avg:.4f}"
            ),
            data_points=len(values), algorithm="zscore",
        )
    return None


# ── Detection router ──────────────────────────────────────────────────────────
def detect(
    row: dict,
    history_rows: List[Tuple[str, float]],
    training_rows: List[Tuple[str, float]],
    trainer: ContinuousTrainer,
) -> Optional[Anomaly]:
    metric_name   = row["metric_name"]
    current_value = float(row["metric_value"])
    now_str       = datetime.now(timezone.utc).isoformat()
    sens          = METRIC_SENSITIVITY.get(metric_name, SENSITIVITY)

    # Layer 0: CPU safety net
    result = _detect_high_cpu_safety_net(row, history_rows, now_str)
    if result is not None:
        log.debug(
            f"[CPU-SafetyNet] {row['resource_name']}/{metric_name}: "
            f"{current_value:.2f}% >= {CPU_SAFETY_NET_THRESHOLD}%"
        )
        return result

    # Layer 1: Hard limits
    if metric_name in HARD_LIMITS:
        lo, hi = HARD_LIMITS[metric_name]
        if lo is not None and current_value < lo:
            return Anomaly(
                detected_at=now_str, cloud=row["cloud"], region=row["region"],
                resource_type=row["resource_type"], resource_id=row["resource_id"],
                resource_name=row["resource_name"], metric_name=metric_name,
                metric_unit=row["metric_unit"], current_value=current_value,
                avg_value=lo, std_value=0.0, upper_bound=lo, lower_bound=0.0,
                severity="critical",
                reason=f"[HardLimit] {metric_name}={current_value:.2f} BELOW floor {lo}",
                data_points=len(history_rows), algorithm="hard_limit",
            )
        if hi is not None and current_value >= hi:
            return Anomaly(
                detected_at=now_str, cloud=row["cloud"], region=row["region"],
                resource_type=row["resource_type"], resource_id=row["resource_id"],
                resource_name=row["resource_name"], metric_name=metric_name,
                metric_unit=row["metric_unit"], current_value=current_value,
                avg_value=hi, std_value=0.0, upper_bound=hi, lower_bound=0.0,
                severity="critical",
                reason=f"[HardLimit] {metric_name}={current_value:.2f} AT/ABOVE ceiling {hi}",
                data_points=len(history_rows), algorithm="hard_limit",
            )

    # Layer 2: Always-bad
    if metric_name in ALWAYS_BAD_METRICS and current_value > 0:
        return Anomaly(
            detected_at=now_str, cloud=row["cloud"], region=row["region"],
            resource_type=row["resource_type"], resource_id=row["resource_id"],
            resource_name=row["resource_name"], metric_name=metric_name,
            metric_unit=row["metric_unit"], current_value=current_value,
            avg_value=0.0, std_value=0.0, upper_bound=0.0, lower_bound=0.0,
            severity="critical",
            reason=f"[Always-bad] {metric_name}={current_value} — non-zero is an incident",
            data_points=len(history_rows), algorithm="always_bad",
        )

    # Layers 3-6 require warmup
    ok, skip_reason = _warmup_ok(history_rows, metric_name=metric_name)
    if not ok:
        log.debug(f"  WARMUP {row['resource_name']}/{metric_name}: {skip_reason}")
        return None

    span_hours = _span_hours(history_rows)

    # Layer 3: River
    result = _detect_river(row, history_rows, sens, now_str, trainer)
    if result is not None:
        return result

    # Layer 4: Prophet
    if PROPHET_AVAILABLE and span_hours >= 2:
        result = _detect_prophet(row, history_rows, training_rows, sens, now_str, trainer)
        if result is not None:
            return result

    # Layer 5: IsolationForest
    if span_hours * 60 >= ISOFOREST_MIN_MINS:
        result = _detect_isoforest(row, history_rows, sens, now_str, trainer)
        if result is not None:
            return result

    # Layer 6: Z-score
    return _detect_zscore(row, history_rows, sens, now_str)


# ── Correlation grouping ──────────────────────────────────────────────────────
def assign_correlation_ids(anomalies: List[Anomaly]) -> None:
    import hashlib
    from collections import defaultdict
    groups: Dict[str, List[Anomaly]] = defaultdict(list)
    for a in anomalies:
        groups[a.resource_id].append(a)
    for resource_id, group in groups.items():
        if len(group) > 1:
            cid = hashlib.sha1(
                f"{resource_id}:{group[0].detected_at[:16]}".encode()
            ).hexdigest()[:8]
            for a in group:
                a.correlation_id = cid
            log.warning(
                f"  CORRELATED [{cid}] {group[0].resource_name}: "
                f"{len(group)} metrics ({', '.join(a.metric_name for a in group)})"
            )


# ── Slack ─────────────────────────────────────────────────────────────────────
def _send_slack_blocking(anomaly: Anomaly) -> None:
    if not SLACK_WEBHOOK:
        return
    try:
        import urllib.request
        req = urllib.request.Request(
            SLACK_WEBHOOK,
            data=json.dumps(anomaly.slack_message()).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                log.warning(f"Slack HTTP {resp.status}")
    except Exception as e:
        log.warning(f"Slack failed: {e}")


def send_slack(anomaly: Anomaly) -> None:
    if SLACK_WEBHOOK:
        threading.Thread(target=_send_slack_blocking, args=(anomaly,), daemon=True).start()


# ── PATCH #1: RCA Integration with Error Handling ──────────────────────────────
def _run_rca_async(anomaly: Anomaly, reader: MetricsReader) -> None:
    """Run RCA in background thread with error handling."""
    def _rca_worker():
        try:
            engine = RCAEngine("observability_data/metrics.db")
            report = engine.run_rca(
                trigger_resource_id=anomaly.resource_id,
                trigger_metric=anomaly.metric_name,
                trigger_time=anomaly.detected_at,
                window_minutes=30,
            )
            os.makedirs(RCA_REPORT_DIR, exist_ok=True)
            pdf_path = generate_pdf(report, output_dir=RCA_REPORT_DIR)
            log.info(f"[RCA] Generated {pdf_path} for {anomaly.resource_name}::{anomaly.metric_name}")
        except Exception as e:
            log.warning(
                f"[RCA] Failed for {anomaly.resource_id}::{anomaly.metric_name}: {e}"
            )
    
    t = threading.Thread(target=_rca_worker, daemon=True)
    t.start()


# ── Main loop ─────────────────────────────────────────────────────────────────
def run_detection(reader: MetricsReader, trainer: ContinuousTrainer) -> List[Anomaly]:
    all_metrics_raw = reader.get_all_active_metrics()
    if not all_metrics_raw:
        log.warning("No active metrics found. Is the collector running?")
        return []

    all_metrics = [_normalize_row(r) for r in all_metrics_raw]

    name_pairs = {
        (r.get("_raw_metric_name", ""), r["metric_name"])
        for r in all_metrics
        if r.get("_raw_metric_name", "") != r["metric_name"]
    }
    if name_pairs:
        sample = list(name_pairs)[:8]
        log.info(
            f"[Normalise] {len(name_pairs)} metric name(s) aliased. "
            f"Sample: {sample}"
        )

    all_training = reader.get_all_training_data(hours=ISOFOREST_WINDOW_HOURS)
    trainer.maybe_retrain_isoforest(all_training)

    log.info("Fetching bulk history (2h + 168h windows)...")
    history_2h   = reader.get_all_history_bulk(hours=LOOKBACK_HOURS)
    history_168h = reader.get_all_history_bulk(hours=PROPHET_WINDOW_HOURS)

    # PATCH #8: Assess data quality
    qa = _assess_data_quality(all_metrics, history_2h)
    if any(qa.values()):
        log.warning(f"[DataQA] Issues found: {qa}")
        for issue, count in qa.items():
            if count > 0:
                log.warning(f"  {issue}: {count} metrics")

    log.info(f"Checking {len(all_metrics)} metric/resource combinations...")

    distinct_canonical = sorted({r["metric_name"] for r in all_metrics})
    log.info(f"[Metrics] Distinct canonical names ({len(distinct_canonical)}): "
             f"{distinct_canonical[:20]}"
             + ("…" if len(distinct_canonical) > 20 else ""))

    found:        List[Anomaly]  = []
    found_lock    = threading.Lock()
    counters_lock = threading.Lock()
    normal = warming             = 0
    algo_counts: Dict[str, int]  = {}
    _cycle_seen: Set[str]        = set()

    def _check_one(row: dict) -> Tuple[dict, Optional[Anomaly]]:
        if row["metric_name"] in IGNORE_METRICS:
            return row, None
        key = f"{row['resource_id']}::{row['metric_name']}"
        return row, detect(
            row,
            history_2h.get(key, []),
            history_168h.get(key, []),
            trainer,
        )

    with ThreadPoolExecutor(max_workers=DETECTION_WORKERS) as executor:
        futures = {executor.submit(_check_one, row): row for row in all_metrics}
        for future in as_completed(futures):
            try:
                row, anomaly = future.result()
            except Exception as exc:
                log.error(f"Detection task failed: {exc}", exc_info=True)
                continue

            if anomaly is None:
                key = f"{row['resource_id']}::{row['metric_name']}"
                ok, _ = _warmup_ok(history_2h.get(key, []), metric_name=row["metric_name"])
                with counters_lock:
                    if not ok and row["metric_name"] not in ALWAYS_BAD_METRICS:
                        warming += 1
                    else:
                        normal += 1
                continue

            # PATCH #7: Unified deduplication logic
            dedup_min = _get_dedup_minutes(anomaly.algorithm)
            if reader.recent_anomaly_count(row["resource_id"], row["metric_name"],
                                            minutes=dedup_min) > 0:
                log.debug(f"  Dedup [{dedup_min}m]: {row['resource_id']}::{row['metric_name']}")
                continue

            dedup_key = f"{row['resource_id']}::{row['metric_name']}"
            with found_lock:
                if dedup_key in _cycle_seen:
                    log.debug(f"  Cycle-dedup: {dedup_key}")
                    continue
                _cycle_seen.add(dedup_key)
                algo_counts[anomaly.algorithm] = algo_counts.get(anomaly.algorithm, 0) + 1
                found.append(anomaly)

    assign_correlation_ids(found)

    # PATCH #1: RCA with error handling
    for i, a in enumerate(found):
        log.warning(
            f"  ANOMALY [{a.severity.upper()}][{a.algorithm}] "
            f"{a.resource_type}/{a.resource_name} — "
            f"{a.metric_name}: {a.current_value:.4f} "
            f"(normal: {a.lower_bound:.4f}–{a.upper_bound:.4f})"
            + (f" [corr:{a.correlation_id}]" if a.correlation_id else "")
        )
        reader.save_anomaly(a)
        send_slack(a)
        
        if RCA_ENABLED and i < RCA_MAX_ANOMALIES_PER_CYCLE:
            _run_rca_async(a, reader)

    now_ts = datetime.now(timezone.utc).isoformat()
    trainer.flush_river(data_size=len(all_metrics), last_ts=now_ts)

    algo_str = ", ".join(f"{k}:{v}" for k, v in algo_counts.items()) or "none"
    log.info(f"Done. {len(found)} anomalies [{algo_str}]. Normal:{normal}. Warming:{warming}.")
    return found


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info("=" * 70)
    log.info("  AIOps Anomaly Detector — v2 (all patches applied)")
    log.info(f"  Config          : {CONFIG_PATH}")
    log.info(f"  Model dir       : {MODEL_DIR}/")
    log.info(f"  Interval        : {INTERVAL}s")
    log.info(f"  Warmup          : {WARMUP_MINUTES}m / {MIN_DATAPOINTS} pts (metric-specific)")
    log.info(f"  Stale skip      : {STALE_THRESHOLD_MINUTES}m")
    log.info(f"  Dedup window    : metric-specific (5–30m per algorithm)")
    log.info(f"  Workers         : {DETECTION_WORKERS}")
    log.info(f"  CPU safety-net  : >= {CPU_SAFETY_NET_THRESHOLD}% → always CRITICAL")
    log.info(f"  RCA             : {'enabled' if RCA_ENABLED else 'disabled'}")
    log.info(f"  IsoForest       : min_pct={ISOFOREST_MIN_PCT_DEVIATION}%, "
             f"min_abs={ISOFOREST_MIN_ABS_DEVIATION}, "
             f"score<={ISOFOREST_MIN_SCORE_THRESHOLD}, "
             f"window={ISOFOREST_WINDOW_HOURS}h")
    log.info(f"  Prophet         : {'on' if PROPHET_AVAILABLE else 'NOT installed'}, "
             f"window={PROPHET_WINDOW_HOURS}h, max_age={PROPHET_MAX_AGE_HOURS}h")
    log.info(f"  River           : {'on' if RIVER_AVAILABLE else 'NOT installed'}, "
             f"threshold={RIVER_SCORE_THRESHOLD}")
    log.info(f"  Slack           : {'configured' if SLACK_WEBHOOK else 'NOT SET'}")
    log.info(f"  Hard limits     : {len(HARD_LIMITS)} metrics")
    log.info(f"  Always-bad      : {len(ALWAYS_BAD_METRICS)} metrics")
    log.info(f"  Non-negative    : {len(_NON_NEGATIVE_METRICS)} metrics (forecast clamped >= 0)")
    log.info(f"  Aliases         : {len(_METRIC_NAME_ALIASES)} metric name aliases")
    both    = sum(1 for p in _METRIC_POLICY.values() if p.alert_high and p.alert_low)
    hi_only = sum(1 for p in _METRIC_POLICY.values() if p.alert_high and not p.alert_low)
    lo_only = sum(1 for p in _METRIC_POLICY.values() if p.alert_low  and not p.alert_high)
    log.info(f"  Metric policies : {len(_METRIC_POLICY)} "
             f"({both} bidirectional, {hi_only} high-only, {lo_only} low-only)")
    log.info("=" * 70)

    _load_detection_config(CONFIG_PATH)

    registry = ModelRegistry(model_dir=MODEL_DIR)
    trainer  = ContinuousTrainer(registry)
    reader   = MetricsReader(CONFIG_PATH)

    if MODE == "once":
        run_detection(reader, trainer)
        log.info("Single run complete.")
    else:
        cycle = 0
        log.info(f"Running every {INTERVAL}s. Ctrl+C to stop.")
        try:
            while True:
                cycle += 1
                log.info(f"\n{'─' * 70}")
                log.info(f"Cycle #{cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                log.info(f"{'─' * 70}")
                t0 = time.time()
                try:
                    run_detection(reader, trainer)
                except Exception as exc:
                    log.error(f"Cycle #{cycle} failed: {exc}", exc_info=True)
                elapsed = time.time() - t0
                sleep_s = max(0, INTERVAL - elapsed)
                log.info(f"Cycle {elapsed:.1f}s — next in {sleep_s:.0f}s")
                time.sleep(sleep_s)
        except KeyboardInterrupt:
            log.info("Stopped.")