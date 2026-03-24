"""
anomaly_detection.py  —  AIOps-grade Multi-Algorithm Detector
                          Single-file-per-algorithm continuous training

Model file layout (exactly 4 files total):
  models/
    isoforest.joblib  ← ONE global IsolationForest trained on ALL metrics
    prophet.joblib    ← ONE dict: {"resource_id::metric_name": Prophet, ...}
    river.joblib      ← ONE global River HalfSpaceTrees
    models.meta.json  ← metadata for all three (trained_at, data_size, etc.)

Why single files?
  - No file explosion (was: 1 file per resource × metric × algo = hundreds of files)
  - Atomic replace on retrain (write to .tmp, rename → no corrupt half-writes)
  - Easy to inspect, backup, or delete

Training strategy per algorithm:
  IsolationForest  → Global model trained on ALL resources + ALL metrics combined.
                     Values are normalized per metric_name before training so
                     cpu_percent (0-100) and latency_seconds (0-0.5) live in the
                     same feature space without one dominating.
                     Retrains when >=10% new data OR model age >= 6h.
                     Old isoforest.joblib is REPLACED entirely.

  Prophet          → Per-(resource, metric) models stored in a single dict file.
                     On retrain only the affected key is updated; the rest are kept.
                     Warm-start: previous changepoints are seeded into the new fit.
                     Retrains a key when >=20% new data OR >=24h.
                     prophet.joblib is REPLACED each time (with updated dict inside).

  River            → One global HalfSpaceTrees. learn_one() runs on every point.
                     river.joblib is REPLACED after every learn_one call.

  Z-score          → Pure math. No file.
"""
from __future__ import annotations

import json
import logging
import os
import shutil
import sqlite3
import statistics
import threading
import time
import warnings
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
SLACK_WEBHOOK  = os.getenv("SLACK_WEBHOOK_URL", "")
INTERVAL       = int(os.getenv("DETECTOR_INTERVAL",  "60"))
MODE           = os.getenv("DETECTOR_MODE",           "continuous")
CONFIG_PATH    = os.getenv("CONFIG_PATH",             "config/cloud_observability.yaml")
SENSITIVITY    = float(os.getenv("SENSITIVITY",       "2.0"))
LOOKBACK_HOURS = int(os.getenv("LOOKBACK_HOURS",      "2"))
MIN_DATAPOINTS = int(os.getenv("MIN_DATA_POINTS",     "15"))
WARMUP_MINUTES = int(os.getenv("WARMUP_MINUTES",      "10"))
MODEL_DIR      = os.getenv("MODEL_DIR",               "models")

# Retrain thresholds
ISOFOREST_RETRAIN_THRESHOLD = float(os.getenv("ISOFOREST_RETRAIN_THRESHOLD", "0.10"))
ISOFOREST_WINDOW_HOURS      = int(os.getenv("ISOFOREST_WINDOW_HOURS",        "24"))
ISOFOREST_MAX_AGE_HOURS     = int(os.getenv("ISOFOREST_MAX_AGE_HOURS",       "6"))

PROPHET_RETRAIN_THRESHOLD   = float(os.getenv("PROPHET_RETRAIN_THRESHOLD",   "0.20"))
PROPHET_WINDOW_HOURS        = int(os.getenv("PROPHET_WINDOW_HOURS",          "168"))
PROPHET_MAX_AGE_HOURS       = int(os.getenv("PROPHET_MAX_AGE_HOURS",         "24"))

RIVER_SCORE_THRESHOLD       = float(os.getenv("RIVER_SCORE_THRESHOLD",       "0.7"))

PROPHET_INTERNAL_MIN_ROWS = 20
ISOFOREST_MIN_MINS        = 30
ISOFOREST_CONTAMINATION   = 0.01
STD_FLOOR_PCT             = 0.10

# ── Detection config sets ─────────────────────────────────────────────────────
_DEFAULT_ALWAYS_BAD: Set[str] = {
    "status_check_failed", "status_check_failed_instance",
    "status_check_failed_system", "throttles_total", "errors_total",
    "system_errors", "user_errors", "throttled_requests", "unhealthy_host_count",
}
_DEFAULT_IGNORE: Set[str] = {
    "disk_read_bytes", "disk_write_bytes", "disk_read_ops",
    "disk_write_ops", "processed_bytes",
}
_DEFAULT_METRIC_SENSITIVITY: Dict[str, float] = {
    "cpu_utilization_percent": 2.5, "database_connections": 2.0,
    "read_latency_seconds": 2.0,   "write_latency_seconds": 2.0,
    "duration_avg_ms": 2.5,        "target_response_time_s": 2.0,
    "free_storage_bytes": 3.0,     "freeable_memory_bytes": 2.5,
    "request_count": 2.0,          "invocations_total": 2.5,
    "network_transmit_bytes_per_sec": 2.5,
    "network_receive_bytes_per_sec": 2.5,
    "disk_queue_depth": 2.5,       "read_iops": 2.5, "write_iops": 2.5,
    "network_in_bytes": 3.5,       "network_out_bytes": 3.5,
    "network_packets_in": 3.5,     "network_packets_out": 3.5,
    "concurrent_executions": 2.0,  "unreserved_concurrent_executions": 2.0,
}
_DEFAULT_HARD_LIMITS: Dict[str, Tuple[Optional[float], Optional[float]]] = {
    "burst_balance_percent":            (10.0, None),
    "healthy_host_count":               (1.0,  None),
    "concurrent_executions":            (None, 800.0),
    "unreserved_concurrent_executions": (None, 800.0),
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
            ALWAYS_BAD_METRICS = set(det["always_bad_metrics"])
        if "ignore_metrics" in det:
            IGNORE_METRICS = set(det["ignore_metrics"])
        if "metric_sensitivity" in det:
            METRIC_SENSITIVITY.update(det["metric_sensitivity"])
        if "hard_limits" in det:
            for metric, bounds in det["hard_limits"].items():
                HARD_LIMITS[metric] = (bounds.get("floor"), bounds.get("ceiling"))
    except FileNotFoundError:
        log.warning(f"Config not found at {config_path!r} — using built-in defaults.")
    except Exception as e:
        log.warning(f"Could not parse config: {e} — using defaults.")


# ═══════════════════════════════════════════════════════════════════════════════
# MODEL REGISTRY  —  exactly 3 model files + 1 metadata file
# ═══════════════════════════════════════════════════════════════════════════════
class ModelRegistry:
    """
    Manages exactly 4 files in MODEL_DIR:
      isoforest.joblib  — global IsolationForest + per-metric normalization stats
      prophet.joblib    — dict of {series_key: {model, meta}} for all series
      river.joblib      — global River HalfSpaceTrees
      models.meta.json  — training metadata for all three algorithms

    Every save is atomic: write to .tmp first, then rename over the old file.
    This prevents corrupted reads if the process is killed mid-write.
    """

    def __init__(self, model_dir: str = MODEL_DIR) -> None:
        self._dir = Path(model_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Any] = {}
        log.info(f"ModelRegistry: {self._dir.resolve()} (4 files total)")

    # ── File paths ─────────────────────────────────────────────────────────────
    @property
    def _iso_path(self)    -> Path: return self._dir / "isoforest.joblib"
    @property
    def _prophet_path(self)-> Path: return self._dir / "prophet.joblib"
    @property
    def _river_path(self)  -> Path: return self._dir / "river.joblib"
    @property
    def _meta_path(self)   -> Path: return self._dir / "models.meta.json"

    # ── Atomic write helper ────────────────────────────────────────────────────
    def _atomic_save(self, path: Path, obj: Any) -> None:
        """Write to .tmp then rename — safe against mid-write process kills."""
        tmp = path.with_suffix(".tmp")
        try:
            joblib.dump(obj, tmp, compress=3)
            shutil.move(str(tmp), str(path))
        except Exception as e:
            tmp.unlink(missing_ok=True)
            raise e

    # ── Metadata ───────────────────────────────────────────────────────────────
    def _load_meta(self) -> Dict[str, Any]:
        try:
            return json.loads(self._meta_path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _save_meta(self, meta: Dict[str, Any]) -> None:
        tmp = self._meta_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(meta, indent=2), encoding="utf-8")
        shutil.move(str(tmp), str(self._meta_path))

    def _update_meta(self, algo: str, data_size: int, last_ts: str) -> None:
        meta = self._load_meta()
        meta[algo] = {
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "data_size":  data_size,
            "last_ts":    last_ts,
        }
        self._save_meta(meta)

    # ── IsolationForest ────────────────────────────────────────────────────────
    def save_isoforest(
        self,
        model: IsolationForest,
        metric_stats: Dict[str, Dict[str, float]],
        data_size: int,
        last_ts: str,
    ) -> None:
        """
        Replace isoforest.joblib entirely with the new global model.
        metric_stats: per-metric normalization {"cpu_util": {"mean": 45.2, "std": 12.3}}
        """
        payload = {"model": model, "metric_stats": metric_stats}
        self._atomic_save(self._iso_path, payload)
        self._update_meta("isoforest", data_size, last_ts)
        self._cache["isoforest"] = payload
        log.info(f"[Registry] isoforest.joblib REPLACED "
                 f"({data_size} pts across {len(metric_stats)} metrics)")

    def load_isoforest(self) -> Tuple[Optional[IsolationForest], Dict]:
        if "isoforest" in self._cache:
            p = self._cache["isoforest"]
            return p["model"], p["metric_stats"]
        if not self._iso_path.exists():
            return None, {}
        try:
            p = joblib.load(self._iso_path)
            self._cache["isoforest"] = p
            log.debug("[Registry] Loaded isoforest.joblib from disk")
            return p["model"], p["metric_stats"]
        except Exception as e:
            log.warning(f"[Registry] Corrupt isoforest.joblib: {e} — will retrain")
            return None, {}

    # ── Prophet ────────────────────────────────────────────────────────────────
    def save_prophet_series(
        self,
        resource_id: str,
        metric_name: str,
        model: Any,
        data_size: int,
        last_ts: str,
    ) -> None:
        """
        Upsert one series into the prophet dict, then replace prophet.joblib.
        All other series are preserved untouched.
        """
        models_dict = self.load_prophet_all()
        key = f"{resource_id}::{metric_name}"
        models_dict[key] = {
            "model":      model,
            "trained_at": datetime.now(timezone.utc).isoformat(),
            "data_size":  data_size,
            "last_ts":    last_ts,
        }
        self._atomic_save(self._prophet_path, models_dict)
        self._cache["prophet"] = models_dict
        log.info(f"[Registry] prophet.joblib REPLACED — "
                 f"updated key={key} ({len(models_dict)} series total)")

    def load_prophet_all(self) -> Dict[str, Any]:
        if "prophet" in self._cache:
            return self._cache["prophet"]
        if not self._prophet_path.exists():
            return {}
        try:
            d = joblib.load(self._prophet_path)
            self._cache["prophet"] = d
            log.debug(f"[Registry] Loaded prophet.joblib ({len(d)} series)")
            return d
        except Exception as e:
            log.warning(f"[Registry] Corrupt prophet.joblib: {e} — starting fresh")
            return {}

    def load_prophet_series(
        self, resource_id: str, metric_name: str
    ) -> Tuple[Optional[Any], Optional[Dict]]:
        key   = f"{resource_id}::{metric_name}"
        entry = self.load_prophet_all().get(key)
        if entry is None:
            return None, None
        return entry["model"], entry

    # ── River ──────────────────────────────────────────────────────────────────
    def save_river(self, model: Any, data_size: int, last_ts: str) -> None:
        """Replace river.joblib with the updated River model."""
        self._atomic_save(self._river_path, model)
        self._update_meta("river", data_size, last_ts)
        self._cache["river"] = model

    def load_river(self) -> Optional[Any]:
        if "river" in self._cache:
            return self._cache["river"]
        if not self._river_path.exists():
            return None
        try:
            m = joblib.load(self._river_path)
            self._cache["river"] = m
            log.debug("[Registry] Loaded river.joblib from disk")
            return m
        except Exception as e:
            log.warning(f"[Registry] Corrupt river.joblib: {e} — will create fresh")
            return None

    # ── Retrain decision ───────────────────────────────────────────────────────
    def needs_retrain(
        self,
        algo: str,
        current_data_size: int,
        retrain_threshold: float,
        max_age_hours: float,
    ) -> Tuple[bool, str]:
        """
        Returns (True, reason) if algo should be retrained, else (False, reason).
        Reads models.meta.json only — does NOT load the joblib.
        """
        meta = self._load_meta().get(algo)
        if meta is None:
            return True, "cold start — no saved model"

        try:
            trained_at = datetime.fromisoformat(meta["trained_at"])
            age_h = (datetime.now(timezone.utc) - trained_at).total_seconds() / 3600
        except Exception:
            return True, "unreadable trained_at"

        if age_h >= max_age_hours:
            return True, f"age {age_h:.1f}h >= max {max_age_hours}h"

        last_size = meta.get("data_size", 0)
        if last_size > 0:
            growth = (current_data_size - last_size) / last_size
            if growth >= retrain_threshold:
                return True, f"+{growth*100:.1f}% new data ({last_size} -> {current_data_size})"

        return False, f"fresh ({age_h:.1f}h old)"


# ═══════════════════════════════════════════════════════════════════════════════
# CONTINUOUS TRAINER
# ═══════════════════════════════════════════════════════════════════════════════
class ContinuousTrainer:
    """
    Orchestrates when and how each model retrains.

    IsolationForest: global model, retrained once per cycle if needed.
    Prophet:         per-series entries in one file, each retrained independently.
    River:           global model, updated on every single data point.
    """

    def __init__(self, registry: ModelRegistry) -> None:
        self.registry = registry

    # ── IsolationForest ────────────────────────────────────────────────────────
    def maybe_retrain_isoforest(
        self,
        all_training_data: Dict[str, List[Tuple[str, float]]],
    ) -> None:
        """
        Called ONCE per detection cycle at the top of run_detection().
        all_training_data: {"resource_id::metric_name": [(ts, value), ...]}

        Builds one global IsolationForest on all series combined.
        Normalizes per metric_name so different scales don't dominate.
        Replaces isoforest.joblib entirely when retrain is triggered.
        """
        total_rows = sum(len(v) for v in all_training_data.values())
        should, reason = self.registry.needs_retrain(
            "isoforest", total_rows,
            ISOFOREST_RETRAIN_THRESHOLD, ISOFOREST_MAX_AGE_HOURS,
        )
        if not should:
            log.debug(f"[IsoForest] Skip retrain: {reason}")
            return

        log.info(f"[IsoForest] Global retrain triggered: {reason} "
                 f"({total_rows} pts, {len(all_training_data)} series)")

        # Build per-metric normalization stats from all training data
        metric_values: Dict[str, List[float]] = {}
        for key, rows in all_training_data.items():
            metric_name = key.split("::", 1)[1]
            metric_values.setdefault(metric_name, []).extend(v for _, v in rows)

        metric_stats: Dict[str, Dict[str, float]] = {
            m: {"mean": float(np.mean(vals)), "std": float(np.std(vals)) or 1.0}
            for m, vals in metric_values.items()
        }

        # Build feature matrix: [norm_value, norm_delta, hour_sin, hour_cos, metric_id]
        metric_id_map = {
            m: i / max(len(metric_stats), 1)
            for i, m in enumerate(sorted(metric_stats))
        }
        rows_list: List[List[float]] = []
        for key, history_rows in all_training_data.items():
            metric_name = key.split("::", 1)[1]
            st  = metric_stats[metric_name]
            mid = metric_id_map.get(metric_name, 0.0)
            vals   = np.array([v for _, v in history_rows])
            deltas = np.diff(vals, prepend=vals[0])
            try:
                hours = np.array([_parse_ts(ts).hour for ts, _ in history_rows], dtype=float)
            except Exception:
                hours = np.zeros(len(vals))

            norm_v = (vals - st["mean"]) / st["std"]
            norm_d = deltas / max(st["std"], 1e-9)
            h_sin  = np.sin(2 * np.pi * hours / 24)
            h_cos  = np.cos(2 * np.pi * hours / 24)
            for i in range(len(vals)):
                rows_list.append([norm_v[i], norm_d[i], h_sin[i], h_cos[i], mid])

        if len(rows_list) < 10:
            log.warning("[IsoForest] Not enough data for global retrain")
            return

        try:
            clf = IsolationForest(
                contamination=ISOFOREST_CONTAMINATION,
                n_estimators=100,
                random_state=42,
                n_jobs=-1,
            )
            clf.fit(np.array(rows_list))
            last_ts = max(
                (rows[-1][0] for rows in all_training_data.values() if rows),
                default=datetime.now(timezone.utc).isoformat(),
            )
            # REPLACE isoforest.joblib
            self.registry.save_isoforest(clf, metric_stats, total_rows, last_ts)
        except Exception as e:
            log.error(f"[IsoForest] Global retrain failed: {e}")

    def score_isoforest(
        self,
        metric_name: str,
        current_value: float,
        history_rows: List[Tuple[str, float]],
    ) -> Tuple[int, float]:
        """Score one value against the global model. Returns (pred, score)."""
        model, metric_stats = self.registry.load_isoforest()
        if model is None or metric_name not in metric_stats:
            return 1, 0.0

        st     = metric_stats[metric_name]
        prev   = history_rows[-1][1] if history_rows else current_value
        delta  = current_value - prev
        hour   = datetime.now(timezone.utc).hour
        norm_v = (current_value - st["mean"]) / st["std"]
        norm_d = delta / max(st["std"], 1e-9)
        X_cur  = np.array([[
            norm_v, norm_d,
            np.sin(2 * np.pi * hour / 24),
            np.cos(2 * np.pi * hour / 24),
            0.0,
        ]])
        try:
            return int(model.predict(X_cur)[0]), float(model.score_samples(X_cur)[0])
        except Exception as e:
            log.debug(f"[IsoForest] score failed for {metric_name}: {e}")
            return 1, 0.0

    # ── Prophet ────────────────────────────────────────────────────────────────
    def maybe_retrain_prophet(
        self,
        resource_id: str,
        metric_name: str,
        history_rows: List[Tuple[str, float]],
        sens: float,
    ) -> Optional[Any]:
        """
        Return a ready Prophet for this series. Retrains if needed then
        upserts into prophet.joblib (replacing the file with updated dict).
        """
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

        growth = (len(df) - last_size) / max(last_size, 1) if last_size else 1.0
        needs  = (
            existing_model is None
            or age_h >= PROPHET_MAX_AGE_HOURS
            or growth >= PROPHET_RETRAIN_THRESHOLD
        )

        if not needs:
            log.debug(f"[Prophet] Reusing {resource_id}::{metric_name} "
                      f"(age={age_h:.1f}h, +{growth*100:.1f}%)")
            return existing_model

        # Warm-start: reuse previous changepoints
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
        log.info(f"[Prophet] Retraining {resource_id}::{metric_name}: {reason}"
                 + (" [warm-start]" if changepoints else ""))
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
            # Upsert into dict, then REPLACE prophet.joblib
            self.registry.save_prophet_series(
                resource_id, metric_name, m, len(df), history_rows[-1][0]
            )
            return m
        except Exception as e:
            log.warning(f"[Prophet] Retrain failed {resource_id}::{metric_name}: {e}")
            return existing_model

    # ── River ──────────────────────────────────────────────────────────────────
    def get_or_create_river(self) -> Optional[Any]:
        if not RIVER_AVAILABLE:
            return None
        m = self.registry.load_river()
        if m is None:
            log.info("[River] Creating new global HalfSpaceTrees model")
            m = river_anomaly.HalfSpaceTrees(n_trees=25, height=8, window_size=250, seed=42)
        return m

    def learn_and_score_river(
        self, model: Any, x: Dict[str, float], data_size: int, last_ts: str,
    ) -> float:
        """Score, then learn, then REPLACE river.joblib. Returns 0-1 score."""
        if not RIVER_AVAILABLE or model is None:
            return 0.0
        try:
            score = model.score_one(x)
            model.learn_one(x)
            self.registry.save_river(model, data_size, last_ts)
            return float(score)
        except Exception as e:
            log.debug(f"[River] learn_and_score failed: {e}")
            return 0.0


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
    def deviation(self) -> float:
        return 0.0 if self.std_value == 0 else abs(self.current_value - self.avg_value) / self.std_value

    def slack_message(self) -> dict:
        icon = "🔴" if self.severity == "critical" else "🟡"
        algo_label = {
            "prophet":          "📈 Prophet",
            "isolation_forest": "🌲 Isolation Forest",
            "river":            "🌊 River (online)",
            "zscore":           "📊 Z-score",
            "always_bad":       "🚨 Always-bad",
            "hard_limit":       "🔒 Hard Limit",
        }.get(self.algorithm, self.algorithm)
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
                    "text": (
                        f"{self.reason} | {self.data_points} pts | {self.detected_at}"
                        + (f" | corr:{self.correlation_id}" if self.correlation_id else "")
                    )}]},
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
            log.info(f"Connected to SQLite -> {db_path}")
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
                or (f"postgresql://{pg.get('user','postgres')}:{pg.get('password','')}@"
                    f"{pg.get('host','localhost')}:{pg.get('port',5432)}/{pg.get('dbname','observability')}")
            )
            self._conn    = psycopg2.connect(dsn)
            self._backend = "postgres"
            with self._conn.cursor() as cur:
                ddl_pg = (
                    self._DDL_CREATE
                    .replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
                    .replace("INTEGER NOT NULL DEFAULT 0", "SMALLINT NOT NULL DEFAULT 0")
                )
                cur.execute(ddl_pg)
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='anomalies'")
                existing_pg = {r[0] for r in cur.fetchall()}
                for col, defn in self._DDL_MIGRATE:
                    if col not in existing_pg:
                        cur.execute(f"ALTER TABLE anomalies ADD COLUMN IF NOT EXISTS {col} {defn}")
                cur.execute("CREATE INDEX IF NOT EXISTS idx_an_corr ON anomalies(correlation_id)")
            self._conn.commit()
            log.info("Connected to PostgreSQL")

    def get_history(self, resource_id: str, metric_name: str, hours: int = LOOKBACK_HOURS) -> List[Tuple[str, float]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        if self._backend == "sqlite":
            rows = self._conn.execute(
                "SELECT collected_at, metric_value FROM metrics "
                "WHERE resource_id=? AND metric_name=? AND collected_at>=? ORDER BY collected_at ASC",
                (resource_id, metric_name, cutoff),
            ).fetchall()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT collected_at, metric_value FROM metrics "
                    "WHERE resource_id=%s AND metric_name=%s AND collected_at>=%s ORDER BY collected_at ASC",
                    (resource_id, metric_name, cutoff),
                )
                rows = cur.fetchall()
        return [(r[0], float(r[1])) for r in rows]

    def get_all_training_data(self, hours: int) -> Dict[str, List[Tuple[str, float]]]:
        """
        Fetch ALL metrics for ALL resources in one DB query.
        Used for global IsoForest training.
        Returns {"resource_id::metric_name": [(ts, value), ...]}
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        ignore_placeholders = ",".join("?" * len(IGNORE_METRICS)) if IGNORE_METRICS else "'__none__'"
        if self._backend == "sqlite":
            rows = self._conn.execute(
                f"SELECT resource_id, metric_name, collected_at, metric_value "
                f"FROM metrics WHERE collected_at>=? "
                f"{'AND metric_name NOT IN (' + ignore_placeholders + ')' if IGNORE_METRICS else ''} "
                f"ORDER BY resource_id, metric_name, collected_at ASC",
                (cutoff, *IGNORE_METRICS) if IGNORE_METRICS else (cutoff,),
            ).fetchall()
        else:
            ignore_list = ", ".join(f"'{m}'" for m in IGNORE_METRICS) or "'__none__'"
            with self._conn.cursor() as cur:
                cur.execute(
                    f"SELECT resource_id, metric_name, collected_at, metric_value "
                    f"FROM metrics WHERE collected_at>=%s "
                    f"AND metric_name NOT IN ({ignore_list}) "
                    f"ORDER BY resource_id, metric_name, collected_at ASC",
                    (cutoff,),
                )
                rows = cur.fetchall()
        result: Dict[str, List[Tuple[str, float]]] = {}
        for r in rows:
            result.setdefault(f"{r[0]}::{r[1]}", []).append((r[2], float(r[3])))
        return result

    def get_all_active_metrics(self) -> List[dict]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        if self._backend == "sqlite":
            rows = self._conn.execute(
                "SELECT cloud, region, resource_type, resource_id, resource_name, "
                "metric_name, metric_unit, metric_value, MAX(collected_at) AS latest_at "
                "FROM metrics WHERE collected_at>=? GROUP BY resource_id, metric_name "
                "ORDER BY resource_type, resource_name, metric_name",
                (cutoff,),
            ).fetchall()
        else:
            with self._conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
                cur.execute(
                    "SELECT DISTINCT ON (resource_id, metric_name) "
                    "cloud, region, resource_type, resource_id, resource_name, "
                    "metric_name, metric_unit, metric_value, collected_at AS latest_at "
                    "FROM metrics WHERE collected_at>=%s "
                    "ORDER BY resource_id, metric_name, collected_at DESC",
                    (cutoff,),
                )
                rows = cur.fetchall()
        return [dict(r) for r in rows]

    def recent_anomaly_count(self, resource_id: str, metric_name: str, minutes: int = 30) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        if self._backend == "sqlite":
            row = self._conn.execute(
                "SELECT COUNT(*) FROM anomalies WHERE resource_id=? AND metric_name=? AND detected_at>=?",
                (resource_id, metric_name, cutoff),
            ).fetchone()
        else:
            with self._conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM anomalies WHERE resource_id=%s AND metric_name=%s AND detected_at>=%s",
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


def _warmup_ok(history_rows: List[Tuple[str, float]]) -> Tuple[bool, str]:
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


def _span_hours(history_rows: List[Tuple[str, float]]) -> float:
    if len(history_rows) < 2:
        return 0.0
    try:
        return (
            _parse_ts(history_rows[-1][0]) - _parse_ts(history_rows[0][0])
        ).total_seconds() / 3600
    except Exception:
        return 0.0


# ── Detection layers ──────────────────────────────────────────────────────────
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
    std     = statistics.stdev(values) if len(values) > 1 else 1e-9
    x       = {"value": current, "z": (current - avg) / max(std, 1e-9)}
    score   = trainer.learn_and_score_river(
        model, x,
        data_size=len(history_rows),
        last_ts=history_rows[-1][0] if history_rows else now_str,
    )
    if score >= RIVER_SCORE_THRESHOLD:
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
            severity="critical" if score > 0.9 else "warning",
            reason=(f"[River/HST] {row['metric_name']} score {score:.3f} "
                    f">= {RIVER_SCORE_THRESHOLD} ({pct_off:.1f}% from mean {avg:.4f})"),
            data_points=len(history_rows), algorithm="river",
        )
    return None


def _detect_prophet(
    row: dict, history_rows: List[Tuple[str, float]],
    training_rows: List[Tuple[str, float]],
    sens: float, now_str: str, trainer: ContinuousTrainer,
) -> Optional[Anomaly]:
    m = trainer.maybe_retrain_prophet(row["resource_id"], row["metric_name"], training_rows, sens)
    if m is None:
        return None
    try:
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        forecast  = m.predict(pd.DataFrame({"ds": [now_naive]}))
        yhat        = float(forecast["yhat"].iloc[0])
        yhat_lower  = max(0.0, float(forecast["yhat_lower"].iloc[0]))
        yhat_upper  = float(forecast["yhat_upper"].iloc[0])
        current     = float(row["metric_value"])
        df          = _to_df(history_rows)
        avg         = float(df["y"].mean()) if not df.empty else 0.0
        std         = float(df["y"].std())  if len(df) > 1  else 0.0
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
                reason=(f"[Prophet] {row['metric_name']} {current:.4f} — "
                        f"{pct_off:.1f}% {direction} forecast {yhat:.4f}"),
                data_points=len(df), algorithm="prophet",
            )
    except Exception as e:
        log.debug(f"[Prophet] predict failed {row['resource_name']}/{row['metric_name']}: {e}")
    return None


def _detect_isoforest(
    row: dict, history_rows: List[Tuple[str, float]],
    sens: float, now_str: str, trainer: ContinuousTrainer,
) -> Optional[Anomaly]:
    current    = float(row["metric_value"])
    pred, score = trainer.score_isoforest(row["metric_name"], current, history_rows)
    if pred != -1:
        return None
    values    = [v for _, v in history_rows]
    avg       = statistics.mean(values) if values else 0.0
    std       = statistics.stdev(values) if len(values) > 1 else 0.0
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
        reason=(f"[IsoForest] {row['metric_name']} {current:.4f} — "
                f"global model flagged outlier (score:{score:.3f}, {pct_off:.1f}% from mean {avg:.4f})"),
        data_points=len(history_rows), algorithm="isolation_forest",
    )


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
            reason=(f"[Z-score] {row['metric_name']} {current:.4f} — "
                    f"{deviation:.1f}sigma {direction} {LOOKBACK_HOURS}h avg {avg:.4f}"),
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

    ok, skip_reason = _warmup_ok(history_rows)
    if not ok:
        log.debug(f"  WARMUP {row['resource_name']}/{metric_name}: {skip_reason}")
        return None

    span_hours = _span_hours(history_rows)
    span_mins  = span_hours * 60

    river_result = _detect_river(row, history_rows, sens, now_str, trainer)
    if river_result is not None:
        return river_result

    if PROPHET_AVAILABLE and span_hours >= 2:
        result = _detect_prophet(row, history_rows, training_rows, sens, now_str, trainer)
        if result is not None:
            return result

    if span_mins >= ISOFOREST_MIN_MINS:
        result = _detect_isoforest(row, history_rows, sens, now_str, trainer)
        if result is not None:
            return result

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
                log.warning(f"Slack returned HTTP {resp.status}")
    except Exception as e:
        log.warning(f"Slack failed: {e}")


def send_slack(anomaly: Anomaly) -> None:
    if not SLACK_WEBHOOK:
        return
    threading.Thread(target=_send_slack_blocking, args=(anomaly,), daemon=True).start()


# ── Main loop ─────────────────────────────────────────────────────────────────
def run_detection(reader: MetricsReader, trainer: ContinuousTrainer) -> List[Anomaly]:
    all_metrics = reader.get_all_active_metrics()
    if not all_metrics:
        log.warning("No metrics found. Is main.py running?")
        return []

    # ── Global IsoForest retrain check — ONE decision per cycle ──────────────
    # Fetches all metrics in one DB query, decides once whether to replace
    # isoforest.joblib. All per-metric detection then reuses the loaded model.
    all_training = reader.get_all_training_data(hours=ISOFOREST_WINDOW_HOURS)
    trainer.maybe_retrain_isoforest(all_training)

    log.info(f"Checking {len(all_metrics)} metric/resource combinations...")
    found:     List[Anomaly]    = []
    normal = warming            = 0
    algo_counts: Dict[str, int] = {}

    for row in all_metrics:
        if row["metric_name"] in IGNORE_METRICS:
            continue
        history_rows  = reader.get_history(row["resource_id"], row["metric_name"])
        training_rows = reader.get_history(
            row["resource_id"], row["metric_name"], hours=PROPHET_WINDOW_HOURS
        )
        anomaly = detect(row, history_rows, training_rows, trainer)

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
    log.info("  AIOps Anomaly Detector — Single-file-per-algo Continuous Training")
    log.info(f"  Config    : {CONFIG_PATH}")
    log.info(f"  Model dir : {MODEL_DIR}/")
    log.info(f"    isoforest.joblib  — global model, REPLACED on retrain")
    log.info(f"    prophet.joblib    — series dict, REPLACED on any series retrain")
    log.info(f"    river.joblib      — global model, REPLACED every cycle")
    log.info(f"    models.meta.json  — training metadata")
    log.info(f"  IsoForest : window={ISOFOREST_WINDOW_HOURS}h, "
             f"retrain@{ISOFOREST_RETRAIN_THRESHOLD*100:.0f}%new or {ISOFOREST_MAX_AGE_HOURS}h")
    log.info(f"  Prophet   : {'on' if PROPHET_AVAILABLE else 'NOT installed'}, "
             f"window={PROPHET_WINDOW_HOURS}h, "
             f"retrain@{PROPHET_RETRAIN_THRESHOLD*100:.0f}%new or {PROPHET_MAX_AGE_HOURS}h")
    log.info(f"  River     : {'on' if RIVER_AVAILABLE else 'NOT installed (pip install river)'}, "
             f"threshold={RIVER_SCORE_THRESHOLD}")
    log.info(f"  Slack     : {'configured (async)' if SLACK_WEBHOOK else 'NOT SET'}")
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
                log.info(f"Cycle took {elapsed:.1f}s — next in {sleep_s:.0f}s")
                time.sleep(sleep_s)
        except KeyboardInterrupt:
            log.info("Stopped. Models saved to disk — training resumes on next start.")