"""
root_cause_analysis.py — Phase 3: Root Cause Analysis

Receives a batch of Anomaly objects from anomaly_detection.py and determines:
  1. Which anomalies are causally related (correlation graph)
  2. What the probable root cause is (RCA result)
  3. What category of issue it is (RCA_CATEGORY)

Architecture:
  AnomalyCorrelator  — groups anomalies by time window + dependency graph
  DependencyGraph    — models known causal relationships between resource types
  RCAEngine          — scores candidate root causes and produces RCAResult
  RCAStore           — persists RCA results to DB for use by Decision Engine

Usage:
  from root_cause_analysis import RCAEngine, RCAStore
  engine = RCAEngine(config_path="config/cloud_observability.yaml")
  results = engine.analyze(anomalies)          # List[RCAResult]
  store.save_all(results)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

import yaml

# Import Anomaly dataclass from existing anomaly_detection module
try:
    from anomaly_detection import Anomaly
except ImportError:
    # Fallback stub so this file is importable standalone for testing
    @dataclass
    class Anomaly:  # type: ignore
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

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [rca] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/rca.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("rca")


# ─────────────────────────────────────────────────────────────────────────────
# Enums & constants
# ─────────────────────────────────────────────────────────────────────────────

class RCACategory(str, Enum):
    RESOURCE_EXHAUSTION   = "resource_exhaustion"    # CPU/memory/disk hitting limits
    NETWORK_DEGRADATION   = "network_degradation"    # latency, packet loss, bandwidth
    DEPENDENCY_FAILURE    = "dependency_failure"     # upstream service down
    LOAD_SPIKE            = "load_spike"             # traffic surge
    CONFIGURATION_DRIFT   = "configuration_drift"   # settings changed / misconfigured
    MEMORY_LEAK           = "memory_leak"            # slowly growing memory over time
    STORAGE_PRESSURE      = "storage_pressure"       # disk / IOPS exhaustion
    CASCADING_FAILURE     = "cascading_failure"      # one failure causing many others
    APPLICATION_ERROR     = "application_error"      # app-level errors / exceptions
    THROTTLING            = "throttling"             # rate-limit / quota hit
    UNKNOWN               = "unknown"


# Correlation time window: anomalies within this many minutes are considered related
CORRELATION_WINDOW_MINUTES = int(os.getenv("RCA_CORRELATION_WINDOW_MINUTES", "10"))

# Minimum anomalies in a group to trigger cascading_failure classification
CASCADE_MIN_RESOURCES = int(os.getenv("RCA_CASCADE_MIN_RESOURCES", "3"))

# ─────────────────────────────────────────────────────────────────────────────
# Dependency graph
# ─────────────────────────────────────────────────────────────────────────────

# Maps resource_type → list of resource_types it depends on.
# An anomaly on a dependency is more likely to be the root cause.
_DEFAULT_DEPENDENCY_GRAPH: Dict[str, List[str]] = {
    "lambda":        ["rds", "dynamodb", "elasticache", "sqs"],
    "ec2":           ["rds", "elasticache", "ebs"],
    "ecs":           ["rds", "elasticache", "sqs", "alb"],
    "alb":           ["ec2", "ecs", "lambda"],
    "app_service":   ["sql_database", "function_app"],
    "function_app":  ["sql_database", "cosmos_db", "service_bus"],
    "cloud_run":     ["cloud_sql", "pubsub", "gcs"],
    "gke":           ["cloud_sql", "pubsub"],
    "rds":           [],
    "dynamodb":      [],
    "elasticache":   [],
    "sql_database":  [],
    "cosmos_db":     [],
}


class DependencyGraph:
    """Models which resource types depend on which others."""

    def __init__(self, extra: Optional[Dict[str, List[str]]] = None) -> None:
        self._graph: Dict[str, List[str]] = dict(_DEFAULT_DEPENDENCY_GRAPH)
        if extra:
            for k, v in extra.items():
                self._graph.setdefault(k, []).extend(v)

    def dependencies_of(self, resource_type: str) -> List[str]:
        return self._graph.get(resource_type.lower(), [])

    def is_dependency_of(self, candidate: str, dependent: str) -> bool:
        """Return True if candidate is a dependency of dependent."""
        return candidate.lower() in self.dependencies_of(dependent)

    def upstream_score(self, resource_type: str) -> float:
        """
        Score how 'upstream' a resource type is (higher = more likely root cause).
        Resources with no dependencies score highest.
        """
        deps = self.dependencies_of(resource_type)
        if not deps:
            return 1.0          # leaf node — most likely root cause
        return 0.5              # mid-layer
        # Could extend: count how many other types depend on this one


# ─────────────────────────────────────────────────────────────────────────────
# Metric → category mappings
# ─────────────────────────────────────────────────────────────────────────────

_METRIC_CATEGORY_MAP: Dict[str, RCACategory] = {
    # Resource exhaustion
    "cpu_utilization_percent":           RCACategory.RESOURCE_EXHAUSTION,
    "cpu_credit_balance":                RCACategory.RESOURCE_EXHAUSTION,
    "freeable_memory_bytes":             RCACategory.RESOURCE_EXHAUSTION,
    "memory_utilization_percent":        RCACategory.RESOURCE_EXHAUSTION,
    "concurrent_executions":             RCACategory.RESOURCE_EXHAUSTION,
    "unreserved_concurrent_executions":  RCACategory.RESOURCE_EXHAUSTION,

    # Storage pressure
    "free_storage_bytes":                RCACategory.STORAGE_PRESSURE,
    "disk_queue_depth":                  RCACategory.STORAGE_PRESSURE,
    "read_iops":                         RCACategory.STORAGE_PRESSURE,
    "write_iops":                        RCACategory.STORAGE_PRESSURE,
    "burst_balance_percent":             RCACategory.STORAGE_PRESSURE,

    # Network / latency
    "read_latency_seconds":              RCACategory.NETWORK_DEGRADATION,
    "write_latency_seconds":             RCACategory.NETWORK_DEGRADATION,
    "target_response_time_s":            RCACategory.NETWORK_DEGRADATION,
    "duration_avg_ms":                   RCACategory.NETWORK_DEGRADATION,
    "network_transmit_bytes_per_sec":    RCACategory.NETWORK_DEGRADATION,
    "network_receive_bytes_per_sec":     RCACategory.NETWORK_DEGRADATION,

    # Load spike
    "request_count":                     RCACategory.LOAD_SPIKE,
    "invocations_total":                 RCACategory.LOAD_SPIKE,
    "database_connections":              RCACategory.LOAD_SPIKE,
    "healthy_host_count":                RCACategory.DEPENDENCY_FAILURE,

    # Application errors
    "errors_total":                      RCACategory.APPLICATION_ERROR,
    "user_errors":                       RCACategory.APPLICATION_ERROR,
    "system_errors":                     RCACategory.APPLICATION_ERROR,
    "http_5xx_count":                    RCACategory.APPLICATION_ERROR,
    "http_4xx_count":                    RCACategory.APPLICATION_ERROR,

    # Throttling
    "throttles_total":                   RCACategory.THROTTLING,
    "throttled_requests":                RCACategory.THROTTLING,
    "read_throttle_events":              RCACategory.THROTTLING,
    "write_throttle_events":             RCACategory.THROTTLING,

    # Status checks = dependency failure
    "status_check_failed":               RCACategory.DEPENDENCY_FAILURE,
    "status_check_failed_instance":      RCACategory.DEPENDENCY_FAILURE,
    "status_check_failed_system":        RCACategory.DEPENDENCY_FAILURE,
    "unhealthy_host_count":              RCACategory.DEPENDENCY_FAILURE,
}


def _metric_to_category(metric_name: str) -> RCACategory:
    return _METRIC_CATEGORY_MAP.get(metric_name.lower(), RCACategory.UNKNOWN)


# ─────────────────────────────────────────────────────────────────────────────
# Memory leak heuristic helpers
# ─────────────────────────────────────────────────────────────────────────────

def _is_memory_trend_leak(anomalies: List[Anomaly]) -> bool:
    """
    Heuristic: if freeable_memory anomalies show steadily declining avg_value
    across multiple resources/time points, flag as memory leak pattern.
    """
    mem = [a for a in anomalies
           if "memory" in a.metric_name.lower() or "freeable" in a.metric_name.lower()]
    if len(mem) < 2:
        return False
    # avg_value declining = baseline itself is drifting lower over time
    avgs = [a.avg_value for a in mem]
    declining = all(avgs[i] >= avgs[i + 1] for i in range(len(avgs) - 1))
    return declining and len(mem) >= 3


# ─────────────────────────────────────────────────────────────────────────────
# RCA result dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class RCAResult:
    rca_id: str                                    # UUID for this RCA event
    analyzed_at: str                               # ISO-8601 UTC timestamp
    cloud: str
    region: str
    category: RCACategory
    root_resource_id: str                          # most likely root cause resource
    root_resource_name: str
    root_resource_type: str
    root_metric: str                               # primary metric driving the RCA
    confidence: float                              # 0.0 – 1.0
    severity: str                                  # critical | warning
    summary: str                                   # human-readable one-liner
    affected_resource_ids: List[str]               # all resources in this group
    contributing_anomaly_ids: List[str]            # correlation_ids of anomalies
    category_scores: Dict[str, float]              # raw scores per category
    evidence: List[str]                            # bullet-point evidence list
    suggested_actions: List[str]                   # plain-English remediation hints
    raw_anomaly_count: int

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["category"] = self.category.value
        d["affected_resource_ids"] = json.dumps(self.affected_resource_ids)
        d["contributing_anomaly_ids"] = json.dumps(self.contributing_anomaly_ids)
        d["category_scores"] = json.dumps(self.category_scores)
        d["evidence"] = json.dumps(self.evidence)
        d["suggested_actions"] = json.dumps(self.suggested_actions)
        return d


# ─────────────────────────────────────────────────────────────────────────────
# Anomaly correlator
# ─────────────────────────────────────────────────────────────────────────────

class AnomalyCorrelator:
    """
    Groups anomalies into correlated clusters based on:
      1. Same cloud + region
      2. Overlapping time window (CORRELATION_WINDOW_MINUTES)
      3. Causal links via DependencyGraph

    Returns List[List[Anomaly]] — each inner list is one incident cluster.
    """

    def __init__(self, dep_graph: DependencyGraph) -> None:
        self._dep = dep_graph

    def _parse_ts(self, ts: str) -> datetime:
        ts = ts.replace("Z", "+00:00")
        if "+" not in ts[10:] and len(ts) == 19:
            ts += "+00:00"
        return datetime.fromisoformat(ts)

    def correlate(self, anomalies: List[Anomaly]) -> List[List[Anomaly]]:
        if not anomalies:
            return []

        # Sort by time
        sorted_a = sorted(anomalies, key=lambda a: a.detected_at)
        window = timedelta(minutes=CORRELATION_WINDOW_MINUTES)

        clusters: List[List[Anomaly]] = []
        used: Set[int] = set()

        for i, anchor in enumerate(sorted_a):
            if i in used:
                continue
            cluster = [anchor]
            used.add(i)
            anchor_ts = self._parse_ts(anchor.detected_at)

            for j, candidate in enumerate(sorted_a):
                if j in used:
                    continue
                # Same cloud + region
                if candidate.cloud != anchor.cloud or candidate.region != anchor.region:
                    continue
                # Within time window
                if abs((self._parse_ts(candidate.detected_at) - anchor_ts).total_seconds()) > window.total_seconds():
                    continue
                cluster.append(candidate)
                used.add(j)

            clusters.append(cluster)

        log.info(f"[Correlator] {len(anomalies)} anomalies → {len(clusters)} clusters")
        return clusters


# ─────────────────────────────────────────────────────────────────────────────
# RCA Engine
# ─────────────────────────────────────────────────────────────────────────────

class RCAEngine:
    """
    Core analysis engine. For each correlated cluster of anomalies:
      1. Score each RCACategory based on which metrics are firing
      2. Identify the root cause resource using DependencyGraph + severity
      3. Detect special patterns (cascade, memory leak, load spike)
      4. Produce an RCAResult with confidence score and suggested actions
    """

    def __init__(self, config_path: str = "config/cloud_observability.yaml") -> None:
        self._dep = DependencyGraph(self._load_dep_overrides(config_path))
        self._correlator = AnomalyCorrelator(self._dep)
        log.info("[RCAEngine] Initialized")

    @staticmethod
    def _load_dep_overrides(config_path: str) -> Optional[Dict[str, List[str]]]:
        try:
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            return cfg.get("rca", {}).get("dependency_overrides")
        except Exception:
            return None

    # ── Public entry point ────────────────────────────────────────────────────

    def analyze(self, anomalies: List[Anomaly]) -> List[RCAResult]:
        """
        Analyze a batch of anomalies and return one RCAResult per incident cluster.
        """
        if not anomalies:
            return []

        clusters = self._correlator.correlate(anomalies)
        results: List[RCAResult] = []

        for cluster in clusters:
            try:
                result = self._analyze_cluster(cluster)
                results.append(result)
                log.info(
                    f"[RCAEngine] {result.rca_id[:8]} | {result.category.value} | "
                    f"conf={result.confidence:.2f} | root={result.root_resource_name}"
                )
            except Exception as e:
                log.error(f"[RCAEngine] Cluster analysis failed: {e}", exc_info=True)

        return results

    # ── Cluster analysis ──────────────────────────────────────────────────────

    def _analyze_cluster(self, cluster: List[Anomaly]) -> RCAResult:
        now = datetime.now(timezone.utc).isoformat()
        rca_id = str(uuid.uuid4())

        # 1. Score each category
        cat_scores = self._score_categories(cluster)
        top_cat = max(cat_scores, key=cat_scores.get)  # type: ignore

        # 2. Check for special patterns that override the top category
        if len({a.resource_id for a in cluster}) >= CASCADE_MIN_RESOURCES:
            top_cat = RCACategory.CASCADING_FAILURE
            cat_scores[RCACategory.CASCADING_FAILURE] = cat_scores.get(RCACategory.CASCADING_FAILURE, 0) + 2.0

        if _is_memory_trend_leak(cluster):
            top_cat = RCACategory.MEMORY_LEAK
            cat_scores[RCACategory.MEMORY_LEAK] = cat_scores.get(RCACategory.MEMORY_LEAK, 0) + 1.5

        # 3. Find root cause resource
        root = self._identify_root_resource(cluster, top_cat)

        # 4. Confidence
        total_score = sum(cat_scores.values()) or 1.0
        top_score = cat_scores.get(top_cat, 0.0)
        raw_confidence = top_score / total_score
        # Boost confidence when many anomalies agree
        confidence = min(1.0, raw_confidence + 0.05 * min(len(cluster), 5))

        # 5. Severity: critical if any anomaly is critical
        severity = "critical" if any(a.severity == "critical" for a in cluster) else "warning"

        # 6. Evidence and suggested actions
        evidence = self._build_evidence(cluster, root, top_cat)
        suggestions = self._suggest_actions(top_cat, root, cluster)
        summary = self._build_summary(top_cat, root, cluster, confidence)

        return RCAResult(
            rca_id=rca_id,
            analyzed_at=now,
            cloud=cluster[0].cloud,
            region=cluster[0].region,
            category=top_cat,
            root_resource_id=root.resource_id,
            root_resource_name=root.resource_name,
            root_resource_type=root.resource_type,
            root_metric=root.metric_name,
            confidence=round(confidence, 3),
            severity=severity,
            summary=summary,
            affected_resource_ids=list({a.resource_id for a in cluster}),
            contributing_anomaly_ids=[a.correlation_id for a in cluster if a.correlation_id],
            category_scores={k.value: round(v, 3) for k, v in cat_scores.items()},
            evidence=evidence,
            suggested_actions=suggestions,
            raw_anomaly_count=len(cluster),
        )

    # ── Category scoring ──────────────────────────────────────────────────────

    def _score_categories(self, cluster: List[Anomaly]) -> Dict[RCACategory, float]:
        scores: Dict[RCACategory, float] = defaultdict(float)

        for a in cluster:
            cat = _metric_to_category(a.metric_name)
            weight = 2.0 if a.severity == "critical" else 1.0
            scores[cat] += weight

            # Extra: if the metric name contains strong signal words
            mn = a.metric_name.lower()
            if "throttl" in mn:
                scores[RCACategory.THROTTLING] += 0.5
            if "error" in mn or "fail" in mn:
                scores[RCACategory.APPLICATION_ERROR] += 0.5
            if "latency" in mn or "response_time" in mn or "duration" in mn:
                scores[RCACategory.NETWORK_DEGRADATION] += 0.3
            if "memory" in mn and a.current_value < a.avg_value:
                # Falling freeable memory → exhaustion
                scores[RCACategory.RESOURCE_EXHAUSTION] += 0.5

        return dict(scores)

    # ── Root resource identification ──────────────────────────────────────────

    def _identify_root_resource(self, cluster: List[Anomaly], category: RCACategory) -> Anomaly:
        """
        Pick the anomaly most likely to be the root cause by scoring each one.
        Higher score = more likely root cause.
        """
        def score(a: Anomaly) -> float:
            s = 0.0
            # Upstream resources (no dependencies) score higher
            s += self._dep.upstream_score(a.resource_type) * 3.0
            # Critical anomalies score higher
            s += 2.0 if a.severity == "critical" else 0.0
            # Anomalies whose metric category matches the cluster's top category
            if _metric_to_category(a.metric_name) == category:
                s += 1.5
            # Algorithm confidence: IsoForest and Prophet > z-score
            algo_weight = {"isolation_forest": 1.2, "prophet": 1.1, "river": 1.0}.get(a.algorithm, 0.8)
            s += algo_weight
            # More data points = more reliable signal
            s += min(a.data_points / 100.0, 0.5)
            return s

        return max(cluster, key=score)

    # ── Evidence builder ──────────────────────────────────────────────────────

    def _build_evidence(
        self, cluster: List[Anomaly], root: Anomaly, category: RCACategory
    ) -> List[str]:
        ev: List[str] = []

        ev.append(
            f"Root resource '{root.resource_name}' ({root.resource_type}) has anomalous "
            f"{root.metric_name}: current={root.current_value:.4f}, "
            f"expected={root.avg_value:.4f}±{root.std_value:.4f}"
        )

        # Other affected resources
        others = [a for a in cluster if a.resource_id != root.resource_id]
        if others:
            names = ", ".join({a.resource_name for a in others})
            ev.append(f"{len(others)} downstream anomalies also firing: {names}")

        # Severity breakdown
        criticals = [a for a in cluster if a.severity == "critical"]
        if criticals:
            ev.append(f"{len(criticals)} critical-severity anomalies in this cluster")

        # Algorithm diversity
        algos = {a.algorithm for a in cluster}
        if len(algos) > 1:
            ev.append(f"Multiple detection algorithms agree: {', '.join(algos)}")

        # Category-specific evidence
        if category == RCACategory.CASCADING_FAILURE:
            n_resources = len({a.resource_id for a in cluster})
            ev.append(f"Cascade pattern: {n_resources} distinct resources affected simultaneously")

        if category == RCACategory.MEMORY_LEAK:
            ev.append("Memory baseline is steadily declining across multiple data points — leak pattern detected")

        if category == RCACategory.THROTTLING:
            throttle_anoms = [a for a in cluster if "throttl" in a.metric_name.lower()]
            ev.append(f"{len(throttle_anoms)} throttling metric(s) firing — quota/rate limit likely hit")

        if category == RCACategory.LOAD_SPIKE:
            load_anoms = [a for a in cluster if _metric_to_category(a.metric_name) == RCACategory.LOAD_SPIKE]
            if load_anoms:
                max_ratio = max(
                    a.current_value / max(a.avg_value, 1e-9) for a in load_anoms
                )
                ev.append(f"Peak load is {max_ratio:.1f}× baseline average")

        return ev

    # ── Suggested actions ─────────────────────────────────────────────────────

    def _suggest_actions(
        self, category: RCACategory, root: Anomaly, cluster: List[Anomaly]
    ) -> List[str]:
        rt = root.resource_type.lower()
        actions: List[str] = []

        base_actions: Dict[RCACategory, List[str]] = {
            RCACategory.RESOURCE_EXHAUSTION: [
                f"Scale up or add capacity to '{root.resource_name}'",
                "Review recent deployments that may have increased resource demand",
                "Enable auto-scaling if not already configured",
            ],
            RCACategory.NETWORK_DEGRADATION: [
                f"Check network path and DNS resolution for '{root.resource_name}'",
                "Review recent security group / firewall rule changes",
                "Inspect connection pool settings and timeouts",
            ],
            RCACategory.DEPENDENCY_FAILURE: [
                f"Check health of '{root.resource_name}' directly (status page / health endpoint)",
                "Verify IAM permissions and credentials have not expired",
                "Review VPC/subnet routing if this is an internal dependency",
            ],
            RCACategory.LOAD_SPIKE: [
                "Enable or trigger auto-scaling policy",
                "Activate CDN caching or rate limiting at the edge",
                "Check if a scheduled job or external event caused the traffic surge",
            ],
            RCACategory.MEMORY_LEAK: [
                f"Restart '{root.resource_name}' to reclaim leaked memory as a short-term fix",
                "Profile heap dumps to identify the leaking object type",
                "Review recent code deployments for unbounded cache or connection pool growth",
            ],
            RCACategory.STORAGE_PRESSURE: [
                f"Expand storage volume or partition for '{root.resource_name}'",
                "Run disk cleanup / archive old data to cold storage",
                "Review IOPS provisioning — consider io2 or provisioned IOPS upgrade",
            ],
            RCACategory.CASCADING_FAILURE: [
                "Identify and isolate the original failure point — check earliest-timestamped anomaly",
                "Enable circuit breakers between affected services",
                "Implement exponential backoff on retry logic to reduce amplification",
            ],
            RCACategory.APPLICATION_ERROR: [
                f"Review application logs for '{root.resource_name}' around the anomaly time",
                "Check for recent code deployments or configuration changes",
                "Trigger a rollback if a recent deploy correlates with error spike onset",
            ],
            RCACategory.THROTTLING: [
                f"Request quota increase for '{root.resource_name}' service",
                "Implement request queuing and exponential backoff",
                "Distribute load across multiple regions or accounts if possible",
            ],
            RCACategory.CONFIGURATION_DRIFT: [
                "Compare current configuration against last known-good baseline",
                "Check recent Infrastructure-as-Code changes (Terraform / CloudFormation diffs)",
                "Review IAM policy changes that may have restricted access",
            ],
            RCACategory.UNKNOWN: [
                "Inspect raw metrics and logs around the anomaly detection time",
                "Check cloud provider status page for regional incidents",
            ],
        }

        actions.extend(base_actions.get(category, base_actions[RCACategory.UNKNOWN]))

        # Resource-type-specific additions
        if rt == "rds" and category == RCACategory.RESOURCE_EXHAUSTION:
            actions.append("Consider enabling RDS Performance Insights for query-level profiling")
        if rt in ("lambda", "function_app", "cloud_run") and category == RCACategory.NETWORK_DEGRADATION:
            actions.append("Check cold start rates — high duration may be cold start latency, not network")
        if rt == "ec2" and category == RCACategory.MEMORY_LEAK:
            actions.append("Use 'free -h' or CloudWatch agent custom memory metrics to confirm exact usage")
        if category == RCACategory.CASCADING_FAILURE:
            actions.append("Consider activating a load-shedding policy until root resource recovers")

        return actions

    # ── Summary string ────────────────────────────────────────────────────────

    def _build_summary(
        self, category: RCACategory, root: Anomaly, cluster: List[Anomaly], confidence: float
    ) -> str:
        n = len(cluster)
        pct = int(confidence * 100)
        cat_label = category.value.replace("_", " ").title()
        return (
            f"{cat_label} on '{root.resource_name}' ({root.resource_type}) | "
            f"{n} anomal{'y' if n == 1 else 'ies'} | {pct}% confidence"
        )


# ─────────────────────────────────────────────────────────────────────────────
# RCA Store
# ─────────────────────────────────────────────────────────────────────────────

_DDL_RCA = """
CREATE TABLE IF NOT EXISTS rca_results (
    id                      INTEGER PRIMARY KEY AUTOINCREMENT,
    rca_id                  TEXT NOT NULL UNIQUE,
    analyzed_at             TEXT NOT NULL,
    cloud                   TEXT NOT NULL,
    region                  TEXT NOT NULL,
    category                TEXT NOT NULL,
    root_resource_id        TEXT NOT NULL,
    root_resource_name      TEXT NOT NULL,
    root_resource_type      TEXT NOT NULL,
    root_metric             TEXT NOT NULL,
    confidence              REAL NOT NULL,
    severity                TEXT NOT NULL,
    summary                 TEXT NOT NULL,
    affected_resource_ids   TEXT NOT NULL,
    contributing_anomaly_ids TEXT NOT NULL,
    category_scores         TEXT NOT NULL,
    evidence                TEXT NOT NULL,
    suggested_actions       TEXT NOT NULL,
    raw_anomaly_count       INTEGER NOT NULL,
    decision_taken          TEXT DEFAULT NULL,
    resolved                INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_rca_time ON rca_results(analyzed_at DESC);
CREATE INDEX IF NOT EXISTS idx_rca_resource ON rca_results(root_resource_id);
CREATE INDEX IF NOT EXISTS idx_rca_category ON rca_results(category);
"""


class RCAStore:
    """Persists RCAResult objects to the same database used by anomaly_detection."""

    def __init__(self, config_path: str = "config/cloud_observability.yaml") -> None:
        self._lock = threading.Lock()
        self._conn = self._connect(config_path)
        self._migrate()
        log.info("[RCAStore] Ready")

    def _connect(self, config_path: str):
        try:
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f)
            storage = cfg.get("storage", {})
            backend = storage.get("backend", "sqlite").lower()
        except Exception:
            backend = "sqlite"
            storage = {}

        if backend == "sqlite":
            db_path = storage.get("sqlite", {}).get("path", "observability_data/metrics.db")
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            self._backend = "sqlite"
            return conn
        else:
            import psycopg2
            pg = storage.get("postgres", {})
            dsn = pg.get("dsn") or os.getenv("DATABASE_URL") or (
                f"postgresql://{pg.get('user','postgres')}:{pg.get('password','')}@"
                f"{pg.get('host','localhost')}:{pg.get('port',5432)}/{pg.get('dbname','observability')}"
            )
            self._backend = "postgres"
            return psycopg2.connect(dsn)

    def _migrate(self) -> None:
        ddl = _DDL_RCA
        if self._backend == "postgres":
            ddl = ddl.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        if self._backend == "sqlite":
            self._conn.executescript(ddl)
            self._conn.commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute(ddl)
            self._conn.commit()

    def save(self, result: RCAResult) -> None:
        d = result.to_dict()
        cols = [c for c in d if c != "id"]
        vals = [d[c] for c in cols]
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = (
            f"INSERT OR REPLACE INTO rca_results ({','.join(cols)}) "
            f"VALUES ({','.join([ph]*len(cols))})"
        )
        if self._backend == "postgres":
            sql = sql.replace("INSERT OR REPLACE", "INSERT")
            sql += " ON CONFLICT (rca_id) DO UPDATE SET analyzed_at=EXCLUDED.analyzed_at"
        with self._lock:
            if self._backend == "sqlite":
                self._conn.execute(sql, vals)
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(sql, vals)
                self._conn.commit()

    def save_all(self, results: List[RCAResult]) -> None:
        for r in results:
            try:
                self.save(r)
            except Exception as e:
                log.error(f"[RCAStore] Failed to save {r.rca_id}: {e}")

    def get_recent(self, hours: int = 24, limit: int = 100) -> List[Dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = (
            f"SELECT * FROM rca_results WHERE analyzed_at >= {ph} "
            f"ORDER BY analyzed_at DESC LIMIT {ph}"
        )
        if self._backend == "sqlite":
            rows = self._conn.execute(sql, (cutoff, limit)).fetchall()
            cols = [d[0] for d in self._conn.execute(sql, (cutoff, limit)).description] if False else []
            # Use row_factory
            self._conn.row_factory = sqlite3.Row
            rows = self._conn.execute(sql, (cutoff, limit)).fetchall()
            return [dict(r) for r in rows]
        else:
            with self._conn.cursor() as cur:
                cur.execute(sql, (cutoff, limit))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    def mark_decision_taken(self, rca_id: str, decision: str) -> None:
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = f"UPDATE rca_results SET decision_taken={ph} WHERE rca_id={ph}"
        with self._lock:
            if self._backend == "sqlite":
                self._conn.execute(sql, (decision, rca_id))
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(sql, (decision, rca_id))
                self._conn.commit()

    def mark_resolved(self, rca_id: str) -> None:
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = f"UPDATE rca_results SET resolved=1 WHERE rca_id={ph}"
        with self._lock:
            if self._backend == "sqlite":
                self._conn.execute(sql, (rca_id,))
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(sql, (rca_id,))
                self._conn.commit()


# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner (for testing / one-shot)
# ─────────────────────────────────────────────────────────────────────────────

def run_rca_once(config_path: str = "config/cloud_observability.yaml") -> List[RCAResult]:
    """
    Pull recent anomalies from DB, run RCA, persist results.
    Called by main.py or directly for one-shot analysis.
    """
    from anomaly_detection import MetricsReader
    reader = MetricsReader(config_path)
    store = RCAStore(config_path)
    engine = RCAEngine(config_path)

    # Pull anomalies from the last 30 minutes that haven't been analysed yet
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    ph = "?"
    rows = reader._conn.execute(
        "SELECT * FROM anomalies WHERE detected_at >= ? ORDER BY detected_at ASC", (cutoff,)
    ).fetchall()

    if not rows:
        log.info("[RCA] No recent anomalies to analyze")
        return []

    reader._conn.row_factory = sqlite3.Row
    rows = reader._conn.execute(
        "SELECT * FROM anomalies WHERE detected_at >= ? ORDER BY detected_at ASC", (cutoff,)
    ).fetchall()

    anomalies: List[Anomaly] = []
    for r in rows:
        try:
            anomalies.append(Anomaly(
                detected_at=r["detected_at"],
                cloud=r["cloud"],
                region=r["region"],
                resource_type=r["resource_type"],
                resource_id=r["resource_id"],
                resource_name=r["resource_name"],
                metric_name=r["metric_name"],
                metric_unit=r["metric_unit"],
                current_value=float(r["current_value"]),
                avg_value=float(r["avg_value"]),
                std_value=float(r["std_value"]),
                upper_bound=float(r["upper_bound"]),
                lower_bound=float(r["lower_bound"]),
                severity=r["severity"],
                reason=r["reason"],
                data_points=int(r["data_points"]),
                algorithm=r.get("algorithm", "zscore"),
                correlation_id=r.get("correlation_id", ""),
            ))
        except Exception as e:
            log.warning(f"[RCA] Skipping malformed anomaly row: {e}")

    results = engine.analyze(anomalies)
    store.save_all(results)
    log.info(f"[RCA] Completed: {len(results)} RCA results saved")
    return results


if __name__ == "__main__":
    import sys
    cfg = sys.argv[1] if len(sys.argv) > 1 else "config/cloud_observability.yaml"
    results = run_rca_once(cfg)
    for r in results:
        print(f"\n{'='*60}")
        print(f"RCA ID   : {r.rca_id}")
        print(f"Category : {r.category.value}")
        print(f"Summary  : {r.summary}")
        print(f"Confidence: {r.confidence:.0%}")
        print(f"Evidence:")
        for e in r.evidence:
            print(f"  • {e}")
        print(f"Actions:")
        for a in r.suggested_actions:
            print(f"  → {a}")