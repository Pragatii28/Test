"""
collectors/azure/plugin.py
──────────────────────────
Azure cloud collector: VMs, Function Apps, SQL Databases, App Services.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from collectors.base import CloudCollectorPlugin
from collectors.models import LogEntry, MetricPoint

logger = logging.getLogger("collector.azure")


class AzureCollectorPlugin(CloudCollectorPlugin):

    def __init__(self, config: Dict):
        super().__init__("azure", config)
        try:
            from azure.identity import DefaultAzureCredential, ClientSecretCredential
            from azure.mgmt.resource import ResourceManagementClient
            from azure.mgmt.compute import ComputeManagementClient
            from azure.mgmt.web import WebSiteManagementClient
            from azure.monitor.query import MetricsQueryClient
        except ImportError:
            raise ImportError(
                "Azure plugin requires: pip install azure-identity azure-mgmt-resource "
                "azure-mgmt-compute azure-mgmt-web azure-monitor-query"
            )

        azure = config.get("azure", {})
        self._sub_id        = azure.get("subscription_id") or os.getenv("AZURE_SUBSCRIPTION_ID", "")
        self._regions       = azure.get("regions", [])
        self._res_types     = set(azure.get("resources", []))
        self._lookback_min  = int(azure.get("lookback_minutes", 10))

        client_id     = os.getenv("AZURE_CLIENT_ID", "")
        client_secret = os.getenv("AZURE_CLIENT_SECRET", "")
        tenant_id     = os.getenv("AZURE_TENANT_ID", "")

        if client_id and client_secret and tenant_id:
            from azure.identity import ClientSecretCredential
            self._credential = ClientSecretCredential(
                tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
            )
        else:
            from azure.identity import DefaultAzureCredential
            self._credential = DefaultAzureCredential()

        from azure.mgmt.resource import ResourceManagementClient
        from azure.monitor.query import MetricsQueryClient

        self._resource_client = ResourceManagementClient(self._credential, self._sub_id)
        self._metrics_client  = MetricsQueryClient(self._credential)

        self._metric_defs = {
            "virtual_machine": [
                {"name": "cpu_utilization",   "az_name": "Percentage CPU",          "unit": "percent",  "agg": "Average"},
                {"name": "network_in_bytes",  "az_name": "Network In Total",        "unit": "bytes",    "agg": "Total"},
                {"name": "network_out_bytes", "az_name": "Network Out Total",       "unit": "bytes",    "agg": "Total"},
                {"name": "disk_read_bytes",   "az_name": "Disk Read Bytes",         "unit": "bytes",    "agg": "Total"},
                {"name": "disk_write_bytes",  "az_name": "Disk Write Bytes",        "unit": "bytes",    "agg": "Total"},
            ],
            "function_app": [
                {"name": "requests",          "az_name": "Requests",                "unit": "count",    "agg": "Total"},
                {"name": "http_5xx",          "az_name": "Http5xx",                 "unit": "count",    "agg": "Total"},
                {"name": "avg_response_ms",   "az_name": "AverageResponseTime",     "unit": "ms",       "agg": "Average"},
                {"name": "function_errors",   "az_name": "FunctionExecutionCount",  "unit": "count",    "agg": "Total"},
            ],
            "sql_database": [
                {"name": "cpu_utilization",   "az_name": "cpu_percent",             "unit": "percent",  "agg": "Average"},
                {"name": "dtu_utilization",   "az_name": "dtu_consumption_percent", "unit": "percent",  "agg": "Average"},
                {"name": "data_io_percent",   "az_name": "physical_data_read_percent","unit":"percent", "agg": "Average"},
                {"name": "connections",       "az_name": "connection_successful",   "unit": "count",    "agg": "Total"},
            ],
            "app_service": [
                {"name": "requests",          "az_name": "Requests",                "unit": "count",    "agg": "Total"},
                {"name": "http_5xx",          "az_name": "Http5xx",                 "unit": "count",    "agg": "Total"},
                {"name": "avg_response_ms",   "az_name": "AverageResponseTime",     "unit": "ms",       "agg": "Average"},
                {"name": "cpu_time_seconds",  "az_name": "CpuTime",                 "unit": "seconds",  "agg": "Total"},
            ],
        }

        logger.info(f"[Azure] SubscriptionId={self._sub_id}  regions={self._regions}")

    # ── discovery ─────────────────────────────────────────────────────────────

    def discover_resources(self) -> List[Dict[str, Any]]:
        resources: List[Dict] = []
        try:
            for r in self._resource_client.resources.list(expand="createdTime,changedTime"):
                rtype_raw = r.type.lower() if r.type else ""
                location  = (r.location or "").lower()

                if self._regions and location not in [reg.lower() for reg in self._regions]:
                    continue

                mapped_type = None
                if "microsoft.compute/virtualmachines" in rtype_raw and (
                    not self._res_types or "virtual_machines" in self._res_types
                ):
                    mapped_type = "virtual_machine"
                elif "microsoft.web/sites" in rtype_raw:
                    kind = (r.kind or "").lower()
                    if "functionapp" in kind and (not self._res_types or "function_apps" in self._res_types):
                        mapped_type = "function_app"
                    elif not self._res_types or "app_services" in self._res_types:
                        mapped_type = "app_service"
                elif "microsoft.sql/servers/databases" in rtype_raw and (
                    not self._res_types or "sql_databases" in self._res_types
                ):
                    mapped_type = "sql_database"

                if mapped_type:
                    resources.append({
                        "cloud":  "azure",
                        "region": location,
                        "type":   mapped_type,
                        "id":     r.id,
                        "name":   r.name,
                        "resource_group": r.id.split("/resourceGroups/")[1].split("/")[0]
                            if "/resourceGroups/" in (r.id or "") else "",
                    })
        except Exception as exc:
            logger.error(f"[Azure] Discovery error: {exc}")

        by_type: Dict[str, int] = {}
        for r in resources:
            by_type[r["type"]] = by_type.get(r["type"], 0) + 1
        logger.info(f"[Azure] Discovered {len(resources)} resources: {by_type}")
        return resources

    # ── metrics ───────────────────────────────────────────────────────────────

    def collect_metrics(self, resources: List[Dict]) -> List[MetricPoint]:
        from azure.monitor.query import MetricAggregationType

        metrics: List[MetricPoint] = []
        end   = datetime.now(timezone.utc)
        start = end - timedelta(minutes=self._lookback_min)

        for res in resources:
            rtype = res["type"]
            defs  = self._metric_defs.get(rtype, [])
            if not defs:
                continue
            metric_names = [d["az_name"] for d in defs]
            try:
                response = self._retry(lambda r=res, mn=metric_names, s=start, e=end: (
                    self._metrics_client.query_resource(
                        r["id"],
                        metric_names=mn,
                        timespan=(s, e),
                        granularity=timedelta(minutes=max(5, self._lookback_min)),
                    )
                ))
                az_name_to_def = {d["az_name"]: d for d in defs}
                for metric in response.metrics:
                    mdef = az_name_to_def.get(metric.name)
                    if not mdef:
                        continue
                    for ts in metric.timeseries:
                        for dp in ts.data:
                            val = getattr(dp, mdef["agg"].lower(), None)
                            if val is None:
                                continue
                            metrics.append(MetricPoint(
                                timestamp=dp.timestamp.isoformat() if dp.timestamp else self._utcnow(),
                                cloud="azure", region=res["region"],
                                resource_type=rtype,
                                resource_id=res["id"], resource_name=res["name"],
                                metric_name=mdef["name"],
                                metric_value=round(float(val), 6),
                                metric_unit=mdef["unit"],
                                labels={"resource_group": res.get("resource_group", "")},
                            ))
            except Exception as exc:
                logger.debug(f"[Azure] Metrics for {res['name']}: {exc}")

        logger.info(f"[Azure] Collected {len(metrics)} metric points")
        return metrics

    # ── logs ──────────────────────────────────────────────────────────────────

    def collect_logs(self, resources: List[Dict]) -> List[LogEntry]:
        logs: List[LogEntry] = []
        try:
            from azure.monitor.query import LogsQueryClient, LogsQueryStatus

            workspace_id = self.config.get("azure", {}).get("log_analytics_workspace_id", "")
            if not workspace_id:
                logger.info("[Azure] No log_analytics_workspace_id configured — skipping logs")
                return logs

            logs_client = LogsQueryClient(self._credential)
            end   = datetime.now(timezone.utc)
            start = end - timedelta(
                minutes=self.config.get("azure", {}).get("logs_lookback_minutes", 15)
            )
            query = (
                "AzureActivity "
                "| where TimeGenerated >= ago(15m) "
                "| project TimeGenerated, OperationNameValue, ActivityStatusValue, ResourceId, Caller, Properties "
                "| limit 200"
            )
            try:
                result = logs_client.query_workspace(workspace_id, query, timespan=(start, end))
                if result.status == LogsQueryStatus.SUCCESS:
                    for row in result.tables[0].rows:
                        ts, op, status, rid, caller, props = row
                        level = "ERROR" if str(status).upper() in ("FAILED", "FAILURE") else "INFO"
                        msg   = f"{op} | status={status} | caller={caller}"
                        logs.append(LogEntry(
                            timestamp=ts.isoformat() if hasattr(ts, "isoformat") else str(ts),
                            cloud="azure", region="global",
                            resource_type="activity_log",
                            resource_id=str(rid), resource_name=str(rid).split("/")[-1],
                            log_level=level, message=msg[:1000],
                            labels={"caller": str(caller)},
                        ))
            except Exception as exc:
                logger.warning(f"[Azure] Log Analytics query failed: {exc}")
        except ImportError:
            logger.warning("[Azure] azure-monitor-query not installed — skipping logs")

        logger.info(f"[Azure] Collected {len(logs)} log entries")
        return logs
