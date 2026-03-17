"""
collectors/gcp/plugin.py
────────────────────────
GCP cloud collector: GCE, Cloud Functions, Cloud Run, GKE.
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from collectors.base import CloudCollectorPlugin
from collectors.models import LogEntry, MetricPoint

logger = logging.getLogger("collector.gcp")


class GCPCollectorPlugin(CloudCollectorPlugin):

    def __init__(self, config: Dict):
        super().__init__("gcp", config)
        try:
            from google.cloud import monitoring_v3
            from google.cloud import logging as gcp_logging
            from google.cloud import compute_v1
            self._monitoring  = monitoring_v3
            self._gcp_logging = gcp_logging
            self._compute     = compute_v1
        except ImportError:
            raise ImportError(
                "GCP plugin requires: pip install google-cloud-monitoring "
                "google-cloud-logging google-cloud-compute"
            )

        gcp = config.get("gcp", {})
        self._project_id      = gcp.get("project_id") or os.getenv("GCP_PROJECT_ID", "")
        self._regions         = gcp.get("regions", [])
        self._res_types       = set(gcp.get("resources", []))
        self._lookback_min    = int(gcp.get("lookback_minutes", 10))
        self._max_log_entries = int(gcp.get("max_log_entries", 200))

        self._metric_defs = {
            "gce_instance": [
                {"name": "cpu_utilization",    "gcp_type": "compute.googleapis.com/instance/cpu/utilization",              "unit": "ratio",   "reducer": "REDUCE_MEAN"},
                {"name": "network_in_bytes",   "gcp_type": "compute.googleapis.com/instance/network/received_bytes_count", "unit": "bytes",   "reducer": "REDUCE_SUM"},
                {"name": "network_out_bytes",  "gcp_type": "compute.googleapis.com/instance/network/sent_bytes_count",     "unit": "bytes",   "reducer": "REDUCE_SUM"},
                {"name": "disk_read_bytes",    "gcp_type": "compute.googleapis.com/instance/disk/read_bytes_count",        "unit": "bytes",   "reducer": "REDUCE_SUM"},
                {"name": "disk_write_bytes",   "gcp_type": "compute.googleapis.com/instance/disk/write_bytes_count",       "unit": "bytes",   "reducer": "REDUCE_SUM"},
            ],
            "cloud_function": [
                {"name": "execution_count",    "gcp_type": "cloudfunctions.googleapis.com/function/execution_count",       "unit": "count",   "reducer": "REDUCE_SUM"},
                {"name": "execution_time_ms",  "gcp_type": "cloudfunctions.googleapis.com/function/execution_times",       "unit": "ms",      "reducer": "REDUCE_MEAN"},
                {"name": "active_instances",   "gcp_type": "cloudfunctions.googleapis.com/function/active_instances",      "unit": "count",   "reducer": "REDUCE_MAX"},
            ],
            "cloud_run_revision": [
                {"name": "request_count",      "gcp_type": "run.googleapis.com/request_count",                             "unit": "count",   "reducer": "REDUCE_SUM"},
                {"name": "request_latency_ms", "gcp_type": "run.googleapis.com/request_latencies",                        "unit": "ms",      "reducer": "REDUCE_MEAN"},
                {"name": "container_cpu",      "gcp_type": "run.googleapis.com/container/cpu/utilizations",                "unit": "ratio",   "reducer": "REDUCE_MEAN"},
            ],
            "gke_cluster": [
                {"name": "cpu_utilization",    "gcp_type": "kubernetes.io/node/cpu/allocatable_utilization",               "unit": "ratio",   "reducer": "REDUCE_MEAN"},
                {"name": "memory_utilization", "gcp_type": "kubernetes.io/node/memory/allocatable_utilization",            "unit": "ratio",   "reducer": "REDUCE_MEAN"},
                {"name": "node_count",         "gcp_type": "kubernetes.io/node/status/allocatable_cpus",                   "unit": "count",   "reducer": "REDUCE_COUNT"},
            ],
        }

        logger.info(f"[GCP] Project={self._project_id}  regions={self._regions}")

    # ── discovery ─────────────────────────────────────────────────────────────

    def discover_resources(self) -> List[Dict[str, Any]]:
        resources: List[Dict] = []
        if not self._res_types or "gce_instances"      in self._res_types:
            resources.extend(self._discover_gce())
        if not self._res_types or "cloud_functions"    in self._res_types:
            resources.extend(self._discover_cloud_functions())
        if not self._res_types or "cloud_run_services" in self._res_types:
            resources.extend(self._discover_cloud_run())
        if not self._res_types or "gke_clusters"       in self._res_types:
            resources.extend(self._discover_gke())

        by_type: Dict[str, int] = {}
        for r in resources:
            by_type[r["type"]] = by_type.get(r["type"], 0) + 1
        logger.info(f"[GCP] Discovered {len(resources)} resources: {by_type}")
        return resources

    def _discover_gce(self) -> List[Dict]:
        found: List[Dict] = []
        try:
            instances_client = self._compute.InstancesClient()
            request = self._compute.AggregatedListInstancesRequest(project=self._project_id)
            for zone_name, zone_data in instances_client.aggregated_list(request=request):
                for inst in getattr(zone_data, "instances", []):
                    if inst.status != "RUNNING":
                        continue
                    region = zone_name.split("/")[-1][:-2] if zone_name != "zones/-" else "global"
                    if self._regions and region not in self._regions:
                        continue
                    found.append({
                        "cloud": "gcp", "region": region, "type": "gce_instance",
                        "id":   str(inst.id), "name": inst.name,
                        "machine_type": inst.machine_type.split("/")[-1] if inst.machine_type else "",
                        "zone": zone_name.split("/")[-1],
                    })
        except Exception as exc:
            logger.error(f"[GCP] GCE discovery: {exc}")
        return found

    def _gcp_rest_get(self, url: str) -> dict:
        import requests as req_lib
        import google.auth
        from google.auth.transport.requests import Request as GRequest
        creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
        creds.refresh(GRequest())
        resp = req_lib.get(url, headers={"Authorization": f"Bearer {creds.token}"}, timeout=10)
        return resp.json() if resp.ok else {}

    def _discover_cloud_functions(self) -> List[Dict]:
        found: List[Dict] = []
        try:
            for region in (self._regions or ["us-central1"]):
                url = (f"https://cloudfunctions.googleapis.com/v1/projects/"
                       f"{self._project_id}/locations/{region}/functions")
                for fn in self._gcp_rest_get(url).get("functions", []):
                    found.append({
                        "cloud": "gcp", "region": region, "type": "cloud_function",
                        "id": fn["name"], "name": fn["name"].split("/")[-1],
                        "runtime": fn.get("runtime", ""),
                    })
        except Exception as exc:
            logger.debug(f"[GCP] Cloud Functions discovery: {exc}")
        return found

    def _discover_cloud_run(self) -> List[Dict]:
        found: List[Dict] = []
        try:
            for region in (self._regions or ["us-central1"]):
                url = (f"https://run.googleapis.com/v1/projects/{self._project_id}"
                       f"/locations/{region}/services")
                for svc in self._gcp_rest_get(url).get("items", []):
                    meta = svc.get("metadata", {})
                    found.append({
                        "cloud": "gcp", "region": region, "type": "cloud_run_revision",
                        "id": meta.get("selfLink", meta.get("name", "")),
                        "name": meta.get("name", ""),
                    })
        except Exception as exc:
            logger.debug(f"[GCP] Cloud Run discovery: {exc}")
        return found

    def _discover_gke(self) -> List[Dict]:
        found: List[Dict] = []
        try:
            url = (f"https://container.googleapis.com/v1/projects/"
                   f"{self._project_id}/locations/-/clusters")
            for cl in self._gcp_rest_get(url).get("clusters", []):
                if cl.get("status") != "RUNNING":
                    continue
                region = cl.get("location", "")
                if self._regions and region not in self._regions:
                    continue
                found.append({
                    "cloud": "gcp", "region": region, "type": "gke_cluster",
                    "id": cl.get("selfLink", cl.get("name", "")),
                    "name": cl.get("name", ""),
                    "node_count": cl.get("currentNodeCount", 0),
                })
        except Exception as exc:
            logger.debug(f"[GCP] GKE discovery: {exc}")
        return found

    # ── metrics ───────────────────────────────────────────────────────────────

    def collect_metrics(self, resources: List[Dict]) -> List[MetricPoint]:
        from google.cloud.monitoring_v3 import MetricServiceClient
        from google.cloud.monitoring_v3.types import TimeInterval, Aggregation
        from google.protobuf.timestamp_pb2 import Timestamp

        metrics: List[MetricPoint] = []
        client       = MetricServiceClient()
        project_name = f"projects/{self._project_id}"
        end_dt   = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(minutes=self._lookback_min)
        interval = TimeInterval(
            end_time=Timestamp(seconds=int(end_dt.timestamp())),
            start_time=Timestamp(seconds=int(start_dt.timestamp())),
        )

        by_type: Dict[str, List[Dict]] = {}
        for r in resources:
            by_type.setdefault(r["type"], []).append(r)

        for rtype, _res_list in by_type.items():
            defs = self._metric_defs.get(rtype, [])
            for mdef in defs:
                try:
                    results = client.list_time_series(
                        request={
                            "name":     project_name,
                            "filter":   f'metric.type = "{mdef["gcp_type"]}"',
                            "interval": interval,
                            "aggregation": {
                                "alignment_period":   {"seconds": max(60, self._lookback_min * 60)},
                                "per_series_aligner": "ALIGN_MEAN",
                                "cross_series_reducer": mdef["reducer"],
                            },
                            "view": "FULL",
                        }
                    )
                    for ts in results:
                        if not ts.points:
                            continue
                        pt         = ts.points[0]
                        value_type = ts.value_type.name
                        if value_type == "DOUBLE":
                            val = pt.value.double_value
                        elif value_type == "INT64":
                            val = float(pt.value.int64_value)
                        else:
                            continue
                        labels    = dict(ts.resource.labels)
                        res_name  = labels.get("instance_name", labels.get("function_name", labels.get("service_name", "unknown")))
                        region    = (labels.get("zone", "global")[:-2]
                                     if labels.get("zone")
                                     else labels.get("region", "global"))
                        metrics.append(MetricPoint(
                            timestamp=pt.interval.end_time.isoformat()
                                if hasattr(pt.interval.end_time, "isoformat") else self._utcnow(),
                            cloud="gcp", region=region,
                            resource_type=rtype,
                            resource_id=labels.get("instance_id", res_name),
                            resource_name=res_name,
                            metric_name=mdef["name"],
                            metric_value=round(val, 6),
                            metric_unit=mdef["unit"],
                            labels={k: str(v) for k, v in labels.items()},
                        ))
                except Exception as exc:
                    logger.debug(f"[GCP] Metric {mdef['gcp_type']}: {exc}")

        logger.info(f"[GCP] Collected {len(metrics)} metric points")
        return metrics

    # ── logs ──────────────────────────────────────────────────────────────────

    def collect_logs(self, resources: List[Dict]) -> List[LogEntry]:
        logs: List[LogEntry] = []
        try:
            from google.cloud import logging as gcp_logging
            client = gcp_logging.Client(project=self._project_id)
            end   = datetime.now(timezone.utc)
            start = end - timedelta(
                minutes=self.config.get("gcp", {}).get("logs_lookback_minutes", 15)
            )
            ts_filter = (
                f'timestamp >= "{start.isoformat()}" AND timestamp <= "{end.isoformat()}"'
            )
            for entry in client.list_entries(
                filter_=ts_filter,
                order_by=gcp_logging.DESCENDING,
                max_results=self._max_log_entries,
            ):
                payload = entry.payload
                msg = json.dumps(payload)[:1000] if isinstance(payload, dict) else str(payload)[:1000]
                res    = entry.resource
                region = res.labels.get("region", res.labels.get("location", "global"))
                logs.append(LogEntry(
                    timestamp=entry.timestamp.isoformat() if entry.timestamp else self._utcnow(),
                    cloud="gcp", region=region,
                    resource_type=res.type or "gcp_resource",
                    resource_id=res.labels.get("instance_id", res.type or ""),
                    resource_name=res.labels.get("instance_name",
                                                 res.labels.get("service_name", res.type or "")),
                    log_level=entry.severity.name if entry.severity else "INFO",
                    message=msg,
                    labels=dict(res.labels),
                ))
        except ImportError:
            logger.warning("[GCP] google-cloud-logging not installed — skipping logs")
        except Exception as exc:
            logger.error(f"[GCP] Logs: {exc}")

        logger.info(f"[GCP] Collected {len(logs)} log entries")
        return logs
