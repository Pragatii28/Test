#!/usr/bin/env python3
"""
main.py
───────
Entry point for the Multi-Cloud Observability Collector.

Environment variables:
  CONFIG_PATH        path to config YAML   (default: config/cloud_observability.yaml)
  COLLECTOR_MODE     "continuous" | "once" (default: continuous)
  COLLECTOR_INTERVAL seconds between runs  (default: 300)
"""
from __future__ import annotations

import logging
import os

import yaml

from orchestrator import MultiCloudOrchestrator

# ── logging ───────────────────────────────────────────────────────────────────
for _d in ("logs", "observability_data"):
    os.makedirs(_d, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler("logs/collector.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("collector")


# ── default config writer ─────────────────────────────────────────────────────

def _write_default_config(config_path: str) -> None:
    cfg = {
        "clouds": {
            "aws": {
                "enabled":  True,
                "regions":  os.getenv("AWS_REGIONS", "us-east-1").split(","),
                "resources": [
                    "ec2_instances", "rds_instances", "lambda_functions",
                    "dynamodb_tables", "elb_load_balancers",
                ],
                "lookback_minutes":        10,
                "logs_lookback_minutes":   15,
                "max_log_groups":          20,
                "max_log_events_per_group": 50,
            },
            "azure": {
                "enabled":         False,
                "subscription_id": os.getenv("AZURE_SUBSCRIPTION_ID", ""),
                "regions":         ["eastus", "westeurope"],
                "resources":       ["virtual_machines", "function_apps", "sql_databases", "app_services"],
                "lookback_minutes":      10,
                "logs_lookback_minutes": 15,
            },
            "gcp": {
                "enabled":    False,
                "project_id": os.getenv("GCP_PROJECT_ID", ""),
                "regions":    ["us-central1", "europe-west1"],
                "resources":  ["gce_instances", "cloud_functions", "cloud_run_services", "gke_clusters"],
                "lookback_minutes":     10,
                "logs_lookback_minutes": 15,
                "max_log_entries":       200,
            },
        },
        "storage": {
            # ── choose your backend ───────────────────────────────────────────
            # Options: sqlite | postgres | timescaledb
            "backend": "sqlite",

            "sqlite": {
                "path": "observability_data/metrics.db",
            },

            # Uncomment and fill in to use PostgreSQL or TimescaleDB:
            # "postgres": {
            #   "host":     "localhost",
            #   "port":     5432,
            #   "dbname":   "observability",
            #   "user":     "postgres",
            #   "password": "",
            # },

            "prometheus": {
                "enabled": True,
                "host":    "0.0.0.0",
                "port":    8000,
            },
        },
        "collection": {
            "parallel_clouds": True,
            "max_workers":     8,
        },
    }
    os.makedirs(os.path.dirname(config_path) if os.path.dirname(config_path) else ".", exist_ok=True)
    with open(config_path, "w") as f:
        yaml.dump(cfg, f, default_flow_style=False)
    logger.info(f"[Init] Wrote starter config → {config_path}")


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    config_path = os.getenv("CONFIG_PATH", "config/cloud_observability.yaml")
    mode        = os.getenv("COLLECTOR_MODE", "continuous")
    interval    = int(os.getenv("COLLECTOR_INTERVAL", "20"))

    if not os.path.isfile(config_path):
        logger.warning(f"Config not found at {config_path!r} — creating default")
        _write_default_config(config_path)

    collector = MultiCloudOrchestrator(config_path)

    print()
    print("=" * 70)
    print(f"  Multi-Cloud Collector v6.0  (AWS + Azure + GCP)")
    print(f"  Mode     : {mode.upper()}")
    print(f"  Interval : {interval}s")
    print(f"  Metrics  : http://localhost:8000/metrics")
    print(f"  Active   : {list(collector.plugins.keys())}")
    print("=" * 70)
    print()

    if mode == "continuous":
        collector.run_continuous(interval)
    else:
        collector.run_once()
        logger.info("Single run complete.")
