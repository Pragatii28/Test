"""
collectors/unified_collector.py
────────────────────────────────
Single entry point for the self-healing infrastructure metrics pipeline.

Usage:
    from collectors.unified_collector import UnifiedCollector

    collector = UnifiedCollector(config)
    result    = collector.run()

    result.metrics       → List[MetricPoint]   (all infra layers)
    result.logs          → List[LogEntry]       (all log groups)
    result.health_scores → List[HealthScore]    (remediation signals)
    result.summary       → dict                 (counts by resource type)
"""
from __future__ import annotations
import os
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from collectors.models import HealthScore, LogEntry, MetricPoint

logger = logging.getLogger("collector.unified")


@dataclass
class CollectionResult:
    metrics:       List[MetricPoint]  = field(default_factory=list)
    logs:          List[LogEntry]     = field(default_factory=list)
    health_scores: List[HealthScore]  = field(default_factory=list)
    collected_at:  str                = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    duration_s:    float              = 0.0
    errors:        List[str]          = field(default_factory=list)

    @property
    def summary(self) -> Dict[str, Any]:
        metrics_by_type: Dict[str, int] = {}
        for mp in self.metrics:
            key = f"{mp.cloud}/{mp.resource_type}"
            metrics_by_type[key] = metrics_by_type.get(key, 0) + 1

        health_by_status: Dict[str, int] = {}
        for hs in self.health_scores:
            health_by_status[hs.status] = health_by_status.get(hs.status, 0) + 1

        critical_resources = [
            {"id": hs.resource_id, "name": hs.resource_name,
             "type": hs.resource_type, "score": hs.score, "signals": hs.signals}
            for hs in self.health_scores if hs.status == "critical"
        ]

        return {
            "collected_at":       self.collected_at,
            "duration_seconds":   self.duration_s,
            "total_metrics":      len(self.metrics),
            "total_logs":         len(self.logs),
            "total_resources":    len(self.health_scores),
            "metrics_by_type":    metrics_by_type,
            "health_by_status":   health_by_status,
            "critical_resources": critical_resources,
            "errors":             self.errors,
        }


class UnifiedCollector:
    """
    Orchestrates metric + log collection across all cloud providers.
    Currently supports: AWS (full infra coverage).
    Designed to be extended with GCP, Azure plugins.
    """
    config = {
    "aws": {
        # Credentials — leave empty to use IAM role / env vars
        "access_key_id":     os.getenv("AWS_ACCESS_KEY_ID", "AKIA5MPLW57ZJ3UQRYPP"),
        "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "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"),
        "session_token":     os.getenv("AWS_SESSION_TOKEN", ""),

        # Regions to scan
        "regions": ["us-east-1", "us-west-2"],

        # Resource types to collect (remove key or set [] to collect ALL)
        "resources": [
            "ec2", "ebs", "asg",
            "rds", "elasticache", "dynamodb",
            "lambda",
            "elb", "apigateway", "cloudfront", "natgateway",
            "sqs", "sns", "kinesis",
            "ecs", "eks",
            "s3",
        ],

        # How far back to look for CloudWatch metrics (minutes)
        "lookback_minutes": 10,

        # EC2: install node_exporter via SSM to collect memory + disk
        # (requires AmazonSSMManagedInstanceCore IAM policy on EC2 role)
        "node_exporter_via_ssm": True,
        "ssm_timeout_seconds":   120,

        # Log collection settings
        "logs_lookback_minutes":        15,
        "max_log_groups":               30,
        "max_log_events_per_group":     50,
    }
}


    def __init__(self, config: Dict[str, Any]):
        self.config  = config
        self.plugins = self._load_plugins(config)

    def _load_plugins(self, config: Dict) -> list:
        plugins = []

        if "aws" in config:
            try:
                from collectors.aws.plugin import AWSCollectorPlugin
                plugins.append(AWSCollectorPlugin(config))
                logger.info("[Unified] AWS plugin loaded")
            except Exception as exc:
                logger.error(f"[Unified] Failed to load AWS plugin: {exc}")

        # Future: GCP, Azure, Kubernetes plugins can be added here
        # if "gcp" in config:
        #     from collectors.gcp.plugin import GCPCollectorPlugin
        #     plugins.append(GCPCollectorPlugin(config))

        return plugins

    def run(self) -> CollectionResult:
        """
        Run a full collection cycle across all configured cloud providers.
        Returns a CollectionResult with metrics, logs, and health scores.
        """
        result    = CollectionResult()
        t_start   = time.time()

        logger.info(f"[Unified] Starting collection cycle with {len(self.plugins)} plugins")

        for plugin in self.plugins:
            try:
                logger.info(f"[Unified] Running plugin: {plugin.name}")
                metrics, logs, scores = plugin.collect_all()
                result.metrics.extend(metrics)
                result.logs.extend(logs)
                result.health_scores.extend(scores)
            except Exception as exc:
                msg = f"Plugin '{plugin.name}' failed: {exc}"
                logger.error(f"[Unified] {msg}", exc_info=True)
                result.errors.append(msg)

        result.duration_s = round(time.time() - t_start, 2)

        summary = result.summary
        logger.info(
            f"[Unified] Collection complete in {result.duration_s}s — "
            f"metrics={summary['total_metrics']} "
            f"logs={summary['total_logs']} "
            f"resources={summary['total_resources']} "
            f"health={summary['health_by_status']}"
        )

        if summary["critical_resources"]:
            logger.warning(
                f"[Unified] CRITICAL RESOURCES ({len(summary['critical_resources'])}): "
                + ", ".join(r["name"] for r in summary["critical_resources"])
            )

        return result

    def run_metrics_only(self) -> List[MetricPoint]:
        """Lightweight: collect metrics only, skip logs and health scoring."""
        metrics = []
        for plugin in self.plugins:
            try:
                resources = plugin.discover_resources()
                metrics.extend(plugin.collect_metrics(resources))
            except Exception as exc:
                logger.error(f"[Unified] Metrics-only plugin '{plugin.name}' failed: {exc}")
        return metrics

    def run_health_check(self) -> List[HealthScore]:
        """Fast health check: metrics + health scoring only, no logs."""
        all_scores = []
        for plugin in self.plugins:
            try:
                resources = plugin.discover_resources()
                metrics   = plugin.collect_metrics(resources)
                scores    = plugin.compute_health_scores(resources, metrics)
                all_scores.extend(scores)
            except Exception as exc:
                logger.error(f"[Unified] Health check plugin '{plugin.name}' failed: {exc}")
        return all_scores