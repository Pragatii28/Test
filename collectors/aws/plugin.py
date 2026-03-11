"""
collectors/aws/plugin.py
────────────────────────
AWS cloud collector: EC2, RDS, Lambda, ELB (ALB), DynamoDB.
"""
from __future__ import annotations

import logging
import os
import concurrent.futures
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple

from collectors.base import CloudCollectorPlugin
from collectors.models import LogEntry, MetricPoint
import time

logger = logging.getLogger("collector.aws")


class AWSCollectorPlugin(CloudCollectorPlugin):

    def __init__(self, config: Dict):
        super().__init__("aws", config)
        try:
            import boto3
            from botocore.exceptions import ClientError
            self._boto3 = boto3
            self._ClientError = ClientError
        except ImportError:
            raise ImportError("AWS plugin requires boto3: pip install boto3")

        aws = config.get("aws", {})
        session_kwargs: Dict[str, Any] = {}

        # Config file takes priority; fall back to environment variables
        access_key    = aws.get("access_key_id")     or os.getenv("AWS_ACCESS_KEY_ID", "")
        secret_key    = aws.get("secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY", "")
        session_token = aws.get("session_token")     or os.getenv("AWS_SESSION_TOKEN", "")

        if access_key:
            session_kwargs["aws_access_key_id"]     = access_key
        if secret_key:
            session_kwargs["aws_secret_access_key"] = secret_key
        if session_token:
            session_kwargs["aws_session_token"]     = session_token
        if aws.get("profile"):
            session_kwargs["profile_name"] = aws["profile"]

        logger.info(f"[AWS] Credential source: {'config file' if aws.get('access_key_id') else 'environment / IAM role'}")

        import boto3
        self._session          = boto3.Session(**session_kwargs)
        self._regions          = aws.get("regions", [os.getenv("AWS_DEFAULT_REGION", "us-east-1")])
        self._res_types        = set(aws.get("resources", []))
        self._lookback_min     = int(aws.get("lookback_minutes", 10))
        self._logs_lookback    = int(aws.get("logs_lookback_minutes", 15))
        self._max_log_groups   = int(aws.get("max_log_groups", 20))
        self._max_log_events   = int(aws.get("max_log_events_per_group", 50))
        self._metric_defs      = aws.get("metrics", self._default_metric_defs())

        logger.info(f"[AWS] Regions={self._regions}  resource_types={self._res_types}")

    # ── metric definitions ────────────────────────────────────────────────────

    @staticmethod
    def _default_metric_defs() -> Dict:
        return {
            "ec2": {
                "namespace": "AWS/EC2",
                "dimension_key": "InstanceId",
                "definitions": [
                    {"name": "cpu_utilization_percent",          "cw_name": "CPUUtilization",              "stat": "Average", "unit": "percent"},
                    {"name": "network_in_bytes",                 "cw_name": "NetworkIn",                   "stat": "Sum",     "unit": "bytes"},
                    {"name": "network_out_bytes",                "cw_name": "NetworkOut",                  "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_read_bytes",                  "cw_name": "DiskReadBytes",               "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_write_bytes",                 "cw_name": "DiskWriteBytes",              "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_read_ops",                    "cw_name": "DiskReadOps",                 "stat": "Sum",     "unit": "count"},
                    {"name": "disk_write_ops",                   "cw_name": "DiskWriteOps",                "stat": "Sum",     "unit": "count"},
                    {"name": "network_packets_in",               "cw_name": "NetworkPacketsIn",            "stat": "Sum",     "unit": "count"},
                    {"name": "network_packets_out",              "cw_name": "NetworkPacketsOut",           "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed",              "cw_name": "StatusCheckFailed",           "stat": "Sum",     "unit": "count"},
                    {"name": "memory_used_percent",              "cw_name": "MemoryUtilization",           "stat": "Average", "unit": "percent"},
                    {"name": "disk_used_percent",                "cw_name": "DiskUtilization",             "stat": "Average", "unit": "percent"},
                    {"name": "status_check_failed_instance",     "cw_name": "StatusCheckFailed_Instance",  "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed_system",       "cw_name": "StatusCheckFailed_System",    "stat": "Sum",     "unit": "count"},
                ],
            },
            "rds": {
                "namespace": "AWS/RDS",
                "dimension_key": "DBInstanceIdentifier",
                "definitions": [
                    # ── Confirmed correct from CloudWatch debug (period=60s, matches console) ──
                    # CPU: Average/60s → matches console exactly
                    {"name": "cpu_utilization_percent",          "cw_name": "CPUUtilization",              "stat": "Average", "unit": "percent",  "period": 60},
                    # Connections: Maximum/60s → 0.0 is correct (no active connections on this idle DB)
                    {"name": "database_connections",             "cw_name": "DatabaseConnections",         "stat": "Maximum", "unit": "count",    "period": 60},
                    # Free Storage: Average/60s → 19502764032 bytes (~18.16 GB) — stable value
                    {"name": "free_storage_bytes",               "cw_name": "FreeStorageSpace",            "stat": "Average", "unit": "bytes",    "period": 60},
                    # IOPS: Average/60s → ReadIOPS=0.0 (no reads), WriteIOPS=0.4
                    {"name": "read_iops",                        "cw_name": "ReadIOPS",                    "stat": "Average", "unit": "count",    "period": 60},
                    {"name": "write_iops",                       "cw_name": "WriteIOPS",                   "stat": "Average", "unit": "count",    "period": 60},
                    # Latency: Average/60s → Read=0.0, Write=0.0005s
                    {"name": "read_latency_seconds",             "cw_name": "ReadLatency",                 "stat": "Average", "unit": "seconds",  "period": 60},
                    {"name": "write_latency_seconds",            "cw_name": "WriteLatency",                "stat": "Average", "unit": "seconds",  "period": 60},
                    # Freeable Memory: Average/60s → 166174720 bytes (~158.4 MB)
                    {"name": "freeable_memory_bytes",            "cw_name": "FreeableMemory",              "stat": "Average", "unit": "bytes",    "period": 60},
                    # Network: Average/60s → Receive=1010 B/s, Transmit=11129 B/s
                    {"name": "network_receive_bytes_per_sec",    "cw_name": "NetworkReceiveThroughput",    "stat": "Average", "unit": "bytes",    "period": 60},
                    {"name": "network_transmit_bytes_per_sec",   "cw_name": "NetworkTransmitThroughput",   "stat": "Average", "unit": "bytes",    "period": 60},
                    # Swap: Average/60s → 135053312 bytes (~128.8 MB)
                    {"name": "swap_usage_bytes",                 "cw_name": "SwapUsage",                   "stat": "Average", "unit": "bytes",    "period": 60},
                    # Disk Queue: Average/60s → 0.0004
                    {"name": "disk_queue_depth",                 "cw_name": "DiskQueueDepth",              "stat": "Average", "unit": "count",    "period": 60},
                    # Burst Balance: Average/60s → 100.0%
                    {"name": "burst_balance_percent",            "cw_name": "BurstBalance",                "stat": "Average", "unit": "percent",  "period": 60},
                    # BinLog: only populated on replicas with binary logging enabled
                    {"name": "binlog_disk_usage_bytes",          "cw_name": "BinLogDiskUsage",             "stat": "Average", "unit": "bytes",    "period": 60},
                    # Replica Lag: NO DATA on this primary MySQL instance — correct, not a replica
                    # Both ReplicaLag and AuroraBinlogReplicaLag return empty for primary instances.
                    # They will show data only when this DB has read replicas.
                    {"name": "replica_lag_seconds",              "cw_name": "ReplicaLag",                  "stat": "Average", "unit": "seconds",  "period": 60},
                    {"name": "replica_lag_aurora_seconds",       "cw_name": "AuroraBinlogReplicaLag",      "stat": "Average", "unit": "seconds",  "period": 60},
                ],
            },
            "lambda": {
                "namespace": "AWS/Lambda",
                "dimension_key": "FunctionName",
                "definitions": [
                    {"name": "invocations_total",                "cw_name": "Invocations",                 "stat": "Sum",     "unit": "count"},
                    {"name": "errors_total",                     "cw_name": "Errors",                      "stat": "Sum",     "unit": "count"},
                    {"name": "duration_avg_ms",                  "cw_name": "Duration",                    "stat": "Average", "unit": "ms"},
                    {"name": "duration_max_ms",                  "cw_name": "Duration",                    "stat": "Maximum", "unit": "ms"},
                    {"name": "concurrent_executions",            "cw_name": "ConcurrentExecutions",        "stat": "Maximum", "unit": "count"},
                    {"name": "throttles_total",                  "cw_name": "Throttles",                   "stat": "Sum",     "unit": "count"},
                    {"name": "iterator_age_ms",                  "cw_name": "IteratorAge",                 "stat": "Maximum", "unit": "ms"},
                    {"name": "init_duration_ms",                 "cw_name": "InitDuration",                "stat": "Average", "unit": "ms"},
                    {"name": "unreserved_concurrent_executions", "cw_name": "UnreservedConcurrentExecutions","stat":"Maximum","unit": "count"},
                ],
            },
            "elb": {
                "namespace": "AWS/ApplicationELB",
                "dimension_key": "LoadBalancer",
                "definitions": [
                    {"name": "request_count",                    "cw_name": "RequestCount",                "stat": "Sum",     "unit": "count"},
                    {"name": "target_response_time_s",           "cw_name": "TargetResponseTime",          "stat": "Average", "unit": "seconds"},
                    {"name": "http_2xx_count",                   "cw_name": "HTTPCode_Target_2XX_Count",   "stat": "Sum",     "unit": "count"},
                    {"name": "http_4xx_count",                   "cw_name": "HTTPCode_Target_4XX_Count",   "stat": "Sum",     "unit": "count"},
                    {"name": "http_5xx_count",                   "cw_name": "HTTPCode_Target_5XX_Count",   "stat": "Sum",     "unit": "count"},
                    {"name": "active_connections",               "cw_name": "ActiveConnectionCount",       "stat": "Sum",     "unit": "count"},
                    {"name": "healthy_host_count",               "cw_name": "HealthyHostCount",            "stat": "Average", "unit": "count"},
                    {"name": "unhealthy_host_count",             "cw_name": "UnHealthyHostCount",          "stat": "Average", "unit": "count"},
                    {"name": "processed_bytes",                  "cw_name": "ProcessedBytes",              "stat": "Sum",     "unit": "bytes"},
                ],
            },
            "dynamodb": {
                "namespace": "AWS/DynamoDB",
                "dimension_key": "TableName",
                "definitions": [
                    {"name": "consumed_read_capacity",           "cw_name": "ConsumedReadCapacityUnits",   "stat": "Sum",     "unit": "count"},
                    {"name": "consumed_write_capacity",          "cw_name": "ConsumedWriteCapacityUnits",  "stat": "Sum",     "unit": "count"},
                    {"name": "request_latency_ms",               "cw_name": "SuccessfulRequestLatency",    "stat": "Average", "unit": "ms"},
                    {"name": "throttled_requests",               "cw_name": "ThrottledRequests",           "stat": "Sum",     "unit": "count"},
                    {"name": "system_errors",                    "cw_name": "SystemErrors",                "stat": "Sum",     "unit": "count"},
                    {"name": "user_errors",                      "cw_name": "UserErrors",                  "stat": "Sum",     "unit": "count"},
                    {"name": "provisioned_read_capacity",        "cw_name": "ProvisionedReadCapacityUnits","stat": "Average", "unit": "count"},
                    {"name": "provisioned_write_capacity",       "cw_name": "ProvisionedWriteCapacityUnits","stat":"Average", "unit": "count"},
                    {"name": "returned_item_count",              "cw_name": "ReturnedItemCount",           "stat": "Sum",     "unit": "count"},
                ],
            },
        }

    # ── resource discovery ────────────────────────────────────────────────────

    def discover_resources(self) -> List[Dict[str, Any]]:
        resources: List[Dict] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(self._regions), 8)) as exe:
            futs = {exe.submit(self._discover_region, r): r for r in self._regions}
            for f in concurrent.futures.as_completed(futs):
                try:
                    resources.extend(f.result())
                except Exception as exc:
                    logger.error(f"[AWS] Discovery error: {exc}")

        by_type: Dict[str, int] = {}
        for r in resources:
            by_type[r["type"]] = by_type.get(r["type"], 0) + 1
        logger.info(f"[AWS] Discovered {len(resources)} resources: {by_type}")
        return resources

    def _discover_region(self, region: str) -> List[Dict]:
        found: List[Dict] = []

        if not self._res_types or "ec2_instances" in self._res_types:
            try:
                ec2 = self._session.client("ec2", region_name=region)
                for page in ec2.get_paginator("describe_instances").paginate(
                    Filters=[{"Name": "instance-state-name", "Values": ["running"]}]
                ):
                    for res in page["Reservations"]:
                        for inst in res["Instances"]:
                            tags = {t["Key"]: t["Value"] for t in inst.get("Tags", [])}
                            found.append({
                                "cloud": "aws", "region": region, "type": "ec2",
                                "id":    inst["InstanceId"],
                                "name":  tags.get("Name", inst["InstanceId"]),
                                "instance_type": inst.get("InstanceType", ""),
                                "az":    inst.get("Placement", {}).get("AvailabilityZone", ""),
                                "tags":  tags,
                            })
            except Exception as exc:
                logger.error(f"[AWS/{region}] EC2: {exc}")

        if not self._res_types or "rds_instances" in self._res_types:
            try:
                rds = self._session.client("rds", region_name=region)
                for page in rds.get_paginator("describe_db_instances").paginate():
                    for db in page["DBInstances"]:
                        if db["DBInstanceStatus"] in ("available", "backing-up"):
                            found.append({
                                "cloud": "aws", "region": region, "type": "rds",
                                "id":   db["DBInstanceIdentifier"],
                                "name": db["DBInstanceIdentifier"],
                                "engine":         db.get("Engine", ""),
                                "instance_class": db.get("DBInstanceClass", ""),
                            })
            except Exception as exc:
                logger.error(f"[AWS/{region}] RDS: {exc}")

        if not self._res_types or "lambda_functions" in self._res_types:
            try:
                lam = self._session.client("lambda", region_name=region)
                for page in lam.get_paginator("list_functions").paginate():
                    for fn in page["Functions"]:
                        found.append({
                            "cloud": "aws", "region": region, "type": "lambda",
                            "id":      fn["FunctionArn"],
                            "name":    fn["FunctionName"],
                            "runtime": fn.get("Runtime", ""),
                        })
            except Exception as exc:
                logger.error(f"[AWS/{region}] Lambda: {exc}")

        if not self._res_types or "elb_load_balancers" in self._res_types:
            try:
                elb = self._session.client("elbv2", region_name=region)
                for page in elb.get_paginator("describe_load_balancers").paginate():
                    for lb in page["LoadBalancers"]:
                        if lb["State"]["Code"] == "active":
                            found.append({
                                "cloud": "aws", "region": region, "type": "elb",
                                "id":      lb["LoadBalancerArn"],
                                "name":    lb["LoadBalancerName"],
                                "lb_type": lb.get("Type", "application"),
                            })
            except Exception as exc:
                logger.error(f"[AWS/{region}] ELB: {exc}")

        if not self._res_types or "dynamodb_tables" in self._res_types:
            try:
                ddb = self._session.client("dynamodb", region_name=region)
                table_names: List[str] = []
                for page in ddb.get_paginator("list_tables").paginate():
                    table_names.extend(page["TableNames"])
                for tname in table_names:
                    try:
                        info = ddb.describe_table(TableName=tname)["Table"]
                        if info["TableStatus"] == "ACTIVE":
                            found.append({
                                "cloud": "aws", "region": region, "type": "dynamodb",
                                "id": tname, "name": tname,
                            })
                    except Exception:
                        pass
            except Exception as exc:
                logger.error(f"[AWS/{region}] DynamoDB: {exc}")

        return found

    # ── metrics ───────────────────────────────────────────────────────────────

    def collect_metrics(self, resources: List[Dict]) -> List[MetricPoint]:
        end   = datetime.now(timezone.utc)
        start = end - timedelta(minutes=self._lookback_min)
        metrics: List[MetricPoint] = []

        by_region: Dict[str, List[Dict]] = {}
        for r in resources:
            by_region.setdefault(r["region"], []).append(r)

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(by_region) or 1, 8)) as exe:
            futs = {
                exe.submit(self._collect_region_metrics, region, res_list, start, end): region
                for region, res_list in by_region.items()
            }
            for f in concurrent.futures.as_completed(futs):
                try:
                    metrics.extend(f.result())
                except Exception as exc:
                    logger.error(f"[AWS] Metrics error: {exc}")

        logger.info(f"[AWS] Collected {len(metrics)} metric points")
        return metrics

    def _cw_dim_value(self, resource: Dict) -> str:
        rtype = resource["type"]
        if rtype == "elb":
            arn = resource.get("id", "")
            return arn.split(":loadbalancer/")[-1] if ":loadbalancer/" in arn else arn
        if rtype in ("lambda", "rds", "dynamodb"):
            return resource.get("name", resource.get("id", ""))
        return resource.get("id", "")

    def _collect_region_metrics(self, region: str, resources: List[Dict],
                                 start: datetime, end: datetime) -> List[MetricPoint]:
        # RDS metrics with 300s period need at least a 30-min window to guarantee
        # at least 2 data points are available regardless of collection timing.
        # For all other resource types 15 min is sufficient.
        has_rds = any(r["type"] == "rds" for r in resources)
        min_window = 30 if has_rds else 15
        effective_start = end - timedelta(minutes=max(self._lookback_min, min_window))
        cw = self._session.client("cloudwatch", region_name=region)
        metrics:    List[MetricPoint] = []
        queries:    List[Dict]        = []
        query_meta: Dict[str, Tuple]  = {}
        queries_by_type: Dict[str, int] = {}

        for res in resources:
            rtype   = res["type"]
            cfg     = self._metric_defs.get(rtype, {})
            defs    = cfg.get("definitions", [])
            dim_key = cfg.get("dimension_key", "InstanceId")
            if not defs:
                continue
            dim_val = self._cw_dim_value(res)
            ns      = cfg.get("namespace", "AWS/EC2")

            for mdef in defs:
                qid = f"q{len(queries)}"
                # Use per-metric period if defined, else:
                #   EC2/Lambda/ELB: 60s (high-res metrics exist)
                #   RDS/DynamoDB:   300s (basic monitoring only publishes every 5 min)
                default_period = 60  # 60s period confirmed correct for all resource types
                period = mdef.get("period", default_period)
                queries.append({
                    "Id": qid,
                    "MetricStat": {
                        "Metric": {
                            "Namespace":  ns,
                            "MetricName": mdef["cw_name"],
                            "Dimensions": [{"Name": dim_key, "Value": dim_val}],
                        },
                        "Period": period,
                        "Stat":   mdef["stat"],
                    },
                    "ReturnData": True,
                })
                query_meta[qid] = (res, mdef)
                queries_by_type[rtype] = queries_by_type.get(rtype, 0) + 1

        logger.info(f"[AWS/{region}] Query plan: {queries_by_type}  (total={len(queries)})")

        results_by_type: Dict[str, int] = {}
        empty_by_type:   Dict[str, int] = {}

        for batch_start in range(0, len(queries), 100):
            batch_q = queries[batch_start: batch_start + 100]
            if not batch_q:
                continue
            try:
                resp = self._retry(lambda bq=batch_q: cw.get_metric_data(
                    MetricDataQueries=bq,
                    StartTime=effective_start,
                    EndTime=end,
                ))
            except Exception as exc:
                logger.warning(f"[AWS/{region}] GetMetricData failed: {exc}")
                continue

            all_results = list(resp.get("MetricDataResults", []))
            while resp.get("NextToken"):
                try:
                    resp = self._retry(lambda nt=resp["NextToken"], bq=batch_q: cw.get_metric_data(
                        MetricDataQueries=bq,
                        StartTime=effective_start,
                        EndTime=end,
                        NextToken=nt,
                    ))
                    all_results.extend(resp.get("MetricDataResults", []))
                except Exception as exc:
                    logger.warning(f"[AWS/{region}] Pagination failed: {exc}")
                    break

            for result in all_results:
                try:
                    rid  = result.get("Id", "")
                    vals = result.get("Values", [])
                    tsts = result.get("Timestamps", [])
                    if rid not in query_meta:
                        continue
                    res, mdef = query_meta[rid]
                    rtype = res["type"]
                    if not vals:
                        empty_by_type[rtype] = empty_by_type.get(rtype, 0) + 1
                        continue
                    idx    = tsts.index(max(tsts))
                    ts     = tsts[idx]
                    ts_str = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)
                    metrics.append(MetricPoint(
                        timestamp=ts_str,
                        cloud="aws", region=region,
                        resource_type=rtype,
                        resource_id=res["id"], resource_name=res["name"],
                        metric_name=mdef["name"],
                        metric_value=round(float(vals[idx]), 6),
                        metric_unit=mdef["unit"],
                        labels={k: str(v) for k, v in res.items()
                                if k not in ("cloud", "region", "type", "id", "name", "tags")},
                    ))
                    results_by_type[rtype] = results_by_type.get(rtype, 0) + 1
                except Exception as exc:
                    logger.warning(f"[AWS/{region}] Result error: {exc}")

        logger.info(f"[AWS/{region}] Results with data: {results_by_type}")
        logger.info(f"[AWS/{region}] Results empty   : {empty_by_type}")
        return metrics

    # ── logs ──────────────────────────────────────────────────────────────────

    def collect_logs(self, resources: List[Dict]) -> List[LogEntry]:
        logs: List[LogEntry] = []
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - self._logs_lookback * 60 * 1000

        by_region: Dict[str, List[Dict]] = {}
        for r in resources:
            by_region.setdefault(r["region"], []).append(r)

        for region, _res_list in by_region.items():
            try:
                cw_logs   = self._session.client("logs", region_name=region)
                log_groups: List[Dict] = []
                for page in cw_logs.get_paginator("describe_log_groups").paginate():
                    log_groups.extend(page["logGroups"])
                    if len(log_groups) >= self._max_log_groups:
                        break
                log_groups = log_groups[: self._max_log_groups]

                for lg in log_groups:
                    lg_name  = lg["logGroupName"]
                    res_type = "cloudwatch_logs"
                    res_id   = lg_name
                    res_name = lg_name.split("/")[-1] or lg_name

                    if "/aws/lambda/" in lg_name:
                        fn = lg_name.split("/aws/lambda/")[-1]
                        res_type, res_id, res_name = "lambda", fn, fn
                    elif "/aws/rds/" in lg_name:
                        res_type = "rds"
                    elif "/aws/ecs/" in lg_name:
                        res_type = "ecs"

                    try:
                        resp = cw_logs.filter_log_events(
                            logGroupName=lg_name,
                            startTime=start_ms, endTime=end_ms,
                            limit=self._max_log_events,
                        )
                        for ev in resp.get("events", []):
                            msg = ev.get("message", "").strip()
                            if not msg:
                                continue
                            logs.append(LogEntry(
                                timestamp=datetime.fromtimestamp(
                                    ev["timestamp"] / 1000, tz=timezone.utc
                                ).isoformat(),
                                cloud="aws", region=region,
                                resource_type=res_type,
                                resource_id=res_id, resource_name=res_name,
                                log_level=self._detect_log_level(msg),
                                message=msg[:1000],
                                labels={"log_group": lg_name,
                                        "log_stream": ev.get("logStreamName", "")},
                            ))
                    except Exception as exc:
                        logger.debug(f"[AWS/{region}] filter_log_events({lg_name}): {exc}")
            except Exception as exc:
                logger.error(f"[AWS/{region}] Logs: {exc}")

        logger.info(f"[AWS] Collected {len(logs)} log entries")
        return logs