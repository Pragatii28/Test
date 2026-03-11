"""
orchestrator.py
───────────────
Multi-cloud collection orchestrator.
Wires together cloud plugins, the database backend, and the Prometheus exporter.
"""
from __future__ import annotations

import concurrent.futures
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import yaml

from collectors.aws.plugin   import AWSCollectorPlugin
from collectors.azure.plugin import AzureCollectorPlugin
from collectors.gcp.plugin   import GCPCollectorPlugin
from collectors.base         import CloudCollectorPlugin
from collectors.models       import LogEntry, MetricPoint
from storage.db              import BaseStorage, create_storage

logger = logging.getLogger("collector.orchestrator")

try:
    from exporters.prometheus import PrometheusExporter
    HAS_PROMETHEUS = True
except ImportError:
    HAS_PROMETHEUS = False

PLUGIN_REGISTRY: Dict[str, type] = {
    "aws":   AWSCollectorPlugin,
    "azure": AzureCollectorPlugin,
    "gcp":   GCPCollectorPlugin,
}


class MultiCloudOrchestrator:

    def __init__(self, config_path: str = "config/cloud_observability.yaml"):
        with open(config_path, encoding="utf-8") as f:
            self.config = yaml.safe_load(f)

        self.plugins:  Dict[str, CloudCollectorPlugin] = {}
        self._storage: Optional[BaseStorage]           = None
        self._exporter = None
        self._prom_port = 8000

        self._init_plugins()
        self._init_storage()
        self._init_prometheus()

        logger.info("=" * 70)
        logger.info("MULTI-CLOUD COLLECTOR v6.0  (modular + database)")
        logger.info(f"Active plugins : {list(self.plugins.keys())}")
        logger.info(f"Storage backend: {self.config.get('storage', {}).get('backend', 'sqlite')}")
        logger.info(f"Prometheus     : http://0.0.0.0:{self._prom_port}/metrics")
        logger.info("=" * 70)

    # ── initialisation ────────────────────────────────────────────────────────

    def _init_plugins(self) -> None:
        clouds = self.config.get("clouds", {})
        for cloud_name, PluginClass in PLUGIN_REGISTRY.items():
            cfg = clouds.get(cloud_name, {})
            if not cfg.get("enabled", False):
                continue
            try:
                self.plugins[cloud_name] = PluginClass(clouds)
                logger.info(f"[Setup] ✓ {cloud_name.upper()} plugin ready")
            except ImportError as exc:
                logger.error(f"[Setup] {cloud_name.upper()} missing deps: {exc}")
            except Exception as exc:
                logger.error(f"[Setup] {cloud_name.upper()} init failed: {exc}")

    def _init_storage(self) -> None:
        storage_cfg = self.config.get("storage", {})
        try:
            self._storage = create_storage(storage_cfg)
            logger.info(f"[Setup] ✓ Storage ready "
                        f"(backend={storage_cfg.get('backend', 'sqlite')})")
        except Exception as exc:
            logger.error(f"[Setup] Storage init failed: {exc}. Data will not be persisted!")

    def _init_prometheus(self) -> None:
        if not HAS_PROMETHEUS:
            logger.warning("[Setup] prometheus-client not installed.")
            return
        prom_cfg = self.config.get("storage", {}).get("prometheus", {})
        if not prom_cfg.get("enabled", True):
            return
        self._prom_port = int(prom_cfg.get("port", 8000))
        host            = prom_cfg.get("host", "0.0.0.0")
        try:
            from exporters.prometheus import PrometheusExporter
            self._exporter = PrometheusExporter(port=self._prom_port, host=host)
        except Exception as exc:
            logger.warning(f"[Setup] Prometheus exporter failed: {exc}")

    # ── collection ────────────────────────────────────────────────────────────

    def run_once(self) -> Dict[str, Any]:
        summary: Dict[str, Any] = {"clouds": {}}
        all_metrics:   List[MetricPoint] = []
        all_logs:      List[LogEntry]    = []
        all_resources: List[Dict]        = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=len(self.plugins) or 1) as exe:
            def _collect(name: str, plugin: CloudCollectorPlugin):
                logger.info(f"[{name}] Starting collection…")
                resources = plugin.discover_resources()
                metrics   = plugin.collect_metrics(resources)
                logs      = plugin.collect_logs(resources)
                return name, resources, metrics, logs

            futs = {exe.submit(_collect, n, p): n for n, p in self.plugins.items()}
            for f in concurrent.futures.as_completed(futs):
                name = futs[f]
                try:
                    name, resources, metrics, logs = f.result()
                    all_resources.extend(resources)
                    all_metrics.extend(metrics)
                    all_logs.extend(logs)
                    summary["clouds"][name] = {
                        "resources": len(resources),
                        "metrics":   len(metrics),
                        "logs":      len(logs),
                    }
                except Exception as exc:
                    logger.error(f"[{name}] Collection failed: {exc}", exc_info=True)
                    summary["clouds"][name] = {
                        "resources": 0, "metrics": 0, "logs": 0, "error": str(exc)
                    }

        summary["total_metrics"] = len(all_metrics)
        summary["total_logs"]    = len(all_logs)

        # ── persist to database ───────────────────────────────────────────────
        if self._storage:
            try:
                self._storage.save(all_metrics, all_logs)
            except Exception as exc:
                logger.error(f"[Storage] Save failed: {exc}")
        else:
            logger.warning("[Storage] No storage backend — data not persisted")

        # ── update Prometheus ─────────────────────────────────────────────────
        if self._exporter:
            self._exporter.update_resources(all_resources)
            self._exporter.update(all_metrics, all_logs)
            logger.info(f"[Prometheus] Updated {len(all_metrics)} gauge values, "
                        f"{len(all_resources)} discovery entries")

        logger.info("=" * 70)
        logger.info("✓ Collection complete:")
        logger.info(f"  Total metrics : {len(all_metrics)}")
        logger.info(f"  Total logs    : {len(all_logs)}")
        for cloud, data in summary["clouds"].items():
            logger.info(
                f"  {cloud:10} resources={data['resources']}  "
                f"metrics={data['metrics']}  logs={data['logs']}"
            )
        logger.info("=" * 70)
        return summary

    def run_continuous(self, interval_seconds: int = 300) -> None:
        cycle = 0
        logger.info(f"Starting continuous collection (interval={interval_seconds}s)")
        logger.info("Press Ctrl+C to stop.")
        try:
            while True:
                cycle += 1
                logger.info(f"\n{'='*70}")
                logger.info(f"CYCLE #{cycle}  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                logger.info(f"{'='*70}")
                t0 = time.time()
                try:
                    self.run_once()
                except Exception as exc:
                    logger.error(f"Cycle #{cycle} failed: {exc}", exc_info=True)
                elapsed = time.time() - t0
                sleep   = max(0, interval_seconds - elapsed)
                logger.info(
                    f"Cycle took {elapsed:.1f}s. Next in {sleep:.0f}s "
                    f"({(datetime.now() + timedelta(seconds=sleep)).strftime('%H:%M:%S')})"
                )
                if sleep > 0:
                    time.sleep(sleep)
        except KeyboardInterrupt:
            logger.info("\nGraceful shutdown.")
            if self._exporter:
                self._exporter.set_down()
            if self._storage:
                self._storage.close()
