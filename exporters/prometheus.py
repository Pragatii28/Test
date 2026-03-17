"""
exporters/prometheus.py
───────────────────────
Prometheus metrics exporter.
Exposes an HTTP /metrics endpoint consumed by a Prometheus scraper or Grafana.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Tuple

from collectors.models import LogEntry, MetricPoint

logger = logging.getLogger("collector.prometheus")

try:
    from prometheus_client import start_http_server, Gauge, REGISTRY
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False


class PrometheusExporter:
    LABEL_NAMES = ["cloud", "region", "resource_type", "resource_id", "resource_name"]

    def __init__(self, port: int = 8000, host: str = "0.0.0.0"):
        if not HAS_PROMETHEUS:
            raise ImportError("prometheus-client not installed: pip install prometheus-client")

        self._gauges: Dict[str, Gauge] = {}

        self._logs_gauge = Gauge(
            "cloud_logs_collected",
            "Log entries collected per cycle",
            ["cloud", "region", "log_level"],
        )
        self._discovery_gauge = Gauge(
            "cloud_resource_discovered",
            "Resource discovered this cycle (1=exists)",
            ["cloud", "region", "resource_type", "resource_id", "resource_name"],
        )
        self._up = Gauge("multi_cloud_collector_up", "Collector is running (1=yes, 0=no)")
        self._up.set(1)

        try:
            start_http_server(port, addr=host)
            logger.info(f"[Prometheus] Exporting → http://{host}:{port}/metrics")
        except OSError as exc:
            logger.warning(f"[Prometheus] Port {port} busy: {exc}")

    def _gauge_name(self, m: MetricPoint) -> str:
        # Normalise resource_type: replace hyphens/dots with underscores
        rtype = re.sub(r"[^a-zA-Z0-9_]", "_", m.resource_type)
        raw = f"cloud_{m.cloud}_{rtype}_{m.metric_name}"
        return re.sub(r"[^a-zA-Z0-9_]", "_", raw)

    @staticmethod
    def _safe_label(val: str, max_len: int = 128) -> str:
        """Truncate label but preserve the meaningful end for ARNs (e.g. Lambda)."""
        if len(val) <= max_len:
            return val
        # For ARNs keep everything after the last colon (function name)
        if ":" in val:
            short = val.split(":")[-1]
            if short:
                return short[:max_len]
        return val[-max_len:]

    def update(self, metrics: List[MetricPoint], logs: List[LogEntry]) -> None:
        for m in metrics:
            name = self._gauge_name(m)
            if name not in self._gauges:
                try:
                    self._gauges[name] = Gauge(
                        name,
                        f"{m.cloud} {m.resource_type} {m.metric_name} ({m.metric_unit})",
                        self.LABEL_NAMES,
                    )
                except ValueError:
                    existing = REGISTRY._names_to_collectors.get(name)
                    self._gauges[name] = existing
                    if existing is None:
                        continue
            try:
                self._gauges[name].labels(
                    cloud=m.cloud, region=m.region,
                    resource_type=m.resource_type,
                    resource_id=self._safe_label(m.resource_id),
                    resource_name=self._safe_label(m.resource_name),
                ).set(m.metric_value)
            except Exception as exc:
                logger.debug(f"[Prometheus] set {name}: {exc}")

        log_counts: Dict[Tuple, int] = {}
        for log in logs:
            key = (log.cloud, log.region, log.log_level)
            log_counts[key] = log_counts.get(key, 0) + 1
        for (cloud, region, level), count in log_counts.items():
            self._logs_gauge.labels(cloud=cloud, region=region, log_level=level).set(count)

    def update_resources(self, resources: List[Dict]) -> None:
        for r in resources:
            try:
                self._discovery_gauge.labels(
                    cloud=r.get("cloud", ""),
                    region=r.get("region", ""),
                    resource_type=r.get("type", ""),
                    resource_id=self._safe_label(r.get("id", "")),
                    resource_name=self._safe_label(r.get("name", r.get("id", ""))),
                ).set(1)
            except Exception as exc:
                logger.debug(f"[Prometheus] discovery gauge error: {exc}")

    def set_down(self) -> None:
        self._up.set(0)