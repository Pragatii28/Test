"""
collectors/aws/plugin.py
────────────────────────
AWS cloud collector: EC2, RDS, Lambda, ELB (ALB), DynamoDB.
UPDATED: All patches applied (v2)

Fixes applied:
  1. Correct metric aggregation — Sum/Average/Maximum across ALL data points
  2. RDS/DynamoDB default period is 300s; EC2/Lambda/ELB default is 60s
  3. effective_start derived from maximum period for >= 2 datapoints
  4. Lambda resource id uses FunctionName; ARN in labels
  5. ELB dimension extraction uses robust index-based strip
  6. GetMetricData pagination lambda captures batch_q via default arg
  7. describe_log_groups pagination respects max_log_groups at API level
  
  PATCH #4: AWS timestamp handling — ensures ISO 8601 format always
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

        logger.info(
            f"[AWS] Credential source: "
            f"{'config file' if aws.get('access_key_id') else 'environment / IAM role'}"
        )

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
                    {"name": "cpu_utilization_percent",          "cw_name": "CPUUtilization",               "stat": "Average", "unit": "percent"},
                    {"name": "network_in_bytes",                 "cw_name": "NetworkIn",                    "stat": "Sum",     "unit": "bytes"},
                    {"name": "network_out_bytes",                "cw_name": "NetworkOut",                   "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_read_bytes",                  "cw_name": "DiskReadBytes",                "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_write_bytes",                 "cw_name": "DiskWriteBytes",               "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_read_ops",                    "cw_name": "DiskReadOps",                  "stat": "Sum",     "unit": "count"},
                    {"name": "disk_write_ops",                   "cw_name": "DiskWriteOps",                 "stat": "Sum",     "unit": "count"},
                    {"name": "network_packets_in",               "cw_name": "NetworkPacketsIn",             "stat": "Sum",     "unit": "count"},
                    {"name": "network_packets_out",              "cw_name": "NetworkPacketsOut",            "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed",              "cw_name": "StatusCheckFailed",            "stat": "Sum",     "unit": "count"},
                    {"name": "memory_used_percent",              "cw_name": "MemoryUtilization",            "stat": "Average", "unit": "percent"},
                    {"name": "disk_used_percent",                "cw_name": "DiskUtilization",              "stat": "Average", "unit": "percent"},
                    {"name": "status_check_failed_instance",     "cw_name": "StatusCheckFailed_Instance",   "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed_system",       "cw_name": "StatusCheckFailed_System",     "stat": "Sum",     "unit": "count"},
                ],
            },
            "rds": {
                "namespace": "AWS/RDS",
                "dimension_key": "DBInstanceIdentifier",
                "definitions": [
                    {"name": "cpu_utilization_percent",          "cw_name": "CPUUtilization",               "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "database_connections",             "cw_name": "DatabaseConnections",          "stat": "Maximum", "unit": "count",    "period": 300},
                    {"name": "free_storage_bytes",               "cw_name": "FreeStorageSpace",             "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "read_iops",                        "cw_name": "ReadIOPS",                     "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "write_iops",                       "cw_name": "WriteIOPS",                    "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "read_latency_seconds",             "cw_name": "ReadLatency",                  "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "write_latency_seconds",            "cw_name": "WriteLatency",                 "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "freeable_memory_bytes",            "cw_name": "FreeableMemory",               "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "network_receive_bytes_per_sec",    "cw_name": "NetworkReceiveThroughput",     "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "network_transmit_bytes_per_sec",   "cw_name": "NetworkTransmitThroughput",    "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "swap_usage_bytes",                 "cw_name": "SwapUsage",                    "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "disk_queue_depth",                 "cw_name": "DiskQueueDepth",               "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "burst_balance_percent",            "cw_name": "BurstBalance",                 "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "binlog_disk_usage_bytes",          "cw_name": "BinLogDiskUsage",              "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "replica_lag_seconds",              "cw_name": "ReplicaLag",                   "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "replica_lag_aurora_seconds",       "cw_name": "AuroraBinlogReplicaLag",       "stat": "Average", "unit": "seconds",  "period": 300},
                ],
            },
            "lambda": {
                "namespace": "AWS/Lambda",
                "dimension_key": "FunctionName",
                "definitions": [
                    {"name": "invocations_total",                "cw_name": "Invocations",                  "stat": "Sum",     "unit": "count"},
                    {"name": "errors_total",                     "cw_name": "Errors",                       "stat": "Sum",     "unit": "count"},
                    {"name": "duration_avg_ms",                  "cw_name": "Duration",                     "stat": "Average", "unit": "ms"},
                    {"name": "duration_max_ms",                  "cw_name": "Duration",                     "stat": "Maximum", "unit": "ms"},
                    {"name": "concurrent_executions",            "cw_name": "ConcurrentExecutions",         "stat": "Maximum", "unit": "count"},
                    {"name": "throttles_total",                  "cw_name": "Throttles",                    "stat": "Sum",     "unit": "count"},
                    {"name": "iterator_age_ms",                  "cw_name": "IteratorAge",                  "stat": "Maximum", "unit": "ms"},
                    {"name": "init_duration_ms",                 "cw_name": "InitDuration",                 "stat": "Average", "unit": "ms"},
                    {"name": "unreserved_concurrent_executions", "cw_name": "UnreservedConcurrentExecutions","stat": "Maximum","unit": "count"},
                ],
            },
            "elb": {
                "namespace": "AWS/ApplicationELB",
                "dimension_key": "LoadBalancer",
                "definitions": [
                    {"name": "request_count",                    "cw_name": "RequestCount",                 "stat": "Sum",     "unit": "count"},
                    {"name": "target_response_time_s",           "cw_name": "TargetResponseTime",           "stat": "Average", "unit": "seconds"},
                    {"name": "http_2xx_count",                   "cw_name": "HTTPCode_Target_2XX_Count",    "stat": "Sum",     "unit": "count"},
                    {"name": "http_4xx_count",                   "cw_name": "HTTPCode_Target_4XX_Count",    "stat": "Sum",     "unit": "count"},
                    {"name": "http_5xx_count",                   "cw_name": "HTTPCode_Target_5XX_Count",    "stat": "Sum",     "unit": "count"},
                    {"name": "active_connections",               "cw_name": "ActiveConnectionCount",        "stat": "Sum",     "unit": "count"},
                    {"name": "healthy_host_count",               "cw_name": "HealthyHostCount",             "stat": "Average", "unit": "count"},
                    {"name": "unhealthy_host_count",             "cw_name": "UnHealthyHostCount",           "stat": "Average", "unit": "count"},
                    {"name": "processed_bytes",                  "cw_name": "ProcessedBytes",               "stat": "Sum",     "unit": "bytes"},
                ],
            },
            "dynamodb": {
                "namespace": "AWS/DynamoDB",
                "dimension_key": "TableName",
                "definitions": [
                    {"name": "consumed_read_capacity",           "cw_name": "ConsumedReadCapacityUnits",    "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "consumed_write_capacity",          "cw_name": "ConsumedWriteCapacityUnits",   "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "request_latency_ms",               "cw_name": "SuccessfulRequestLatency",     "stat": "Average", "unit": "ms",      "period": 300},
                    {"name": "throttled_requests",               "cw_name": "ThrottledRequests",            "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "system_errors",                    "cw_name": "SystemErrors",                 "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "user_errors",                      "cw_name": "UserErrors",                   "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "provisioned_read_capacity",        "cw_name": "ProvisionedReadCapacityUnits", "stat": "Average", "unit": "count",   "period": 300},
                    {"name": "provisioned_write_capacity",       "cw_name": "ProvisionedWriteCapacityUnits","stat": "Average", "unit": "count",   "period": 300},
                    {"name": "returned_item_count",              "cw_name": "ReturnedItemCount",            "stat": "Sum",     "unit": "count",   "period": 300},
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
                                "vpc_id": inst.get("VpcId", ""),
                                "subnet_id": inst.get("SubnetId", ""),
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
                                "cluster_id":     db.get("DBClusterIdentifier", ""),
                                "vpc_id":         db.get("DBSubnetGroup", {}).get("VpcId", ""),
                            })
            except Exception as exc:
                logger.error(f"[AWS/{region}] RDS: {exc}")

        if not self._res_types or "lambda_functions" in self._res_types:
            try:
                lam = self._session.client("lambda", region_name=region)
                for page in lam.get_paginator("list_functions").paginate():
                    for fn in page["Functions"]:
                        fn_name = fn["FunctionName"]
                        
                        # Fetch more details (VPC, Env vars)
                        vpc_id = ""
                        subnet_ids = ""
                        depends_on = []
                        try:
                            config = lam.get_function_configuration(FunctionName=fn_name)
                            vpc_cfg = config.get("VpcConfig", {})
                            vpc_id = vpc_cfg.get("VpcId", "")
                            subnet_ids = ",".join(vpc_cfg.get("SubnetIds", []))
                            
                            # Inspect env vars for dependency hints (e.g. DB_HOST, RDS_INSTANCE)
                            env = config.get("Environment", {}).get("Variables", {})
                            for k, v in env.items():
                                if any(x in k.lower() for x in ["db", "host", "table", "queue", "topic"]):
                                    depends_on.append(v)
                        except Exception:
                            pass

                        found.append({
                            "cloud": "aws", "region": region, "type": "lambda",
                            "id":      fn_name,
                            "name":    fn_name,
                            "arn":     fn["FunctionArn"],
                            "runtime": fn.get("Runtime", ""),
                            "vpc_id":  vpc_id,
                            "subnets": subnet_ids,
                            "depends_on": ",".join(depends_on),
                        })
            except Exception as exc:
                logger.error(f"[AWS/{region}] Lambda: {exc}")

        if not self._res_types or "elb_load_balancers" in self._res_types:
            try:
                elb = self._session.client("elbv2", region_name=region)
                ec2 = self._session.client("ec2", region_name=region)
                
                # First, get all Target Groups in region
                target_groups = {}
                try:
                    for tg_page in elb.get_paginator("describe_target_groups").paginate():
                        for tg in tg_page["TargetGroups"]:
                            tg_arn = tg["TargetGroupArn"]
                            # Find instances in this target group
                            targets = []
                            try:
                                health = elb.describe_target_health(TargetGroupArn=tg_arn)
                                for th in health["TargetHealthDescriptions"]:
                                    if "Id" in th["Target"]:
                                        targets.append(th["Target"]["Id"])
                            except Exception:
                                pass
                            target_groups[tg_arn] = targets
                except Exception as e:
                    logger.warning(f"[AWS/{region}] Failed to fetch Target Groups: {e}")

                for page in elb.get_paginator("describe_load_balancers").paginate():
                    for lb in page["LoadBalancers"]:
                        if lb["State"]["Code"] == "active":
                            lb_arn = lb["LoadBalancerArn"]
                            
                            # Find which target groups are associated with this LB
                            lb_targets = []
                            try:
                                listeners = elb.describe_listeners(LoadBalancerArn=lb_arn)
                                for lis in listeners["Listeners"]:
                                    lis_arn = lis["ListenerArn"]
                                    rules = elb.describe_rules(ListenerArn=lis_arn)
                                    for rule in rules["Rules"]:
                                        for action in rule["Actions"]:
                                            tg_arn = action.get("TargetGroupArn")
                                            if tg_arn in target_groups:
                                                lb_targets.extend(target_groups[tg_arn])
                            except Exception:
                                pass

                            found.append({
                                "cloud": "aws", "region": region, "type": "elb",
                                "id":      lb_arn,
                                "name":    lb["LoadBalancerName"],
                                "lb_type": lb.get("Type", "application"),
                                "depends_on": ",".join(list(set(lb_targets))),
                                "vpc_id": lb.get("VpcId", ""),
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
            arn    = resource.get("id", "")
            prefix = "loadbalancer/"
            idx    = arn.find(prefix)
            return arn[idx + len(prefix):] if idx != -1 else arn
        if rtype in ("lambda", "rds", "dynamodb"):
            return resource.get("name", resource.get("id", ""))
        return resource.get("id", "")

    @staticmethod
    def _aggregate(values: List[float], stat: str) -> float:
        """
        Aggregate all data points in the window according to the CloudWatch stat.
        FIX: Previously picked only the single most-recent data point.
        """
        if not values:
            return 0.0
        if stat == "Sum":
            return sum(values)
        if stat == "Maximum":
            return max(values)
        if stat == "Minimum":
            return min(values)
        return sum(values) / len(values)

    # ── PATCH #4: Timestamp Handling ──────────────────────────────────────────
    @staticmethod
    def _ensure_iso_timestamp(ts: Any) -> str:
        """
        Convert any timestamp format to ISO 8601.
        
        Handles:
        - datetime objects → .isoformat()
        - Unix timestamps (int/float) → parse to datetime
        - ISO strings → validate and return
        
        Returns:
            ISO 8601 string (e.g., "2026-04-09T06:32:00+00:00")
        
        Raises:
            ValueError: If timestamp format cannot be determined
        """
        # Already ISO string?
        if isinstance(ts, str):
            if "T" in ts:
                if "+00:00" in ts or "Z" in ts or ts.endswith("Z"):
                    return ts.replace("Z", "+00:00")
            raise ValueError(f"String timestamp not ISO 8601: {ts}")
        
        # Unix timestamp (seconds or milliseconds)?
        if isinstance(ts, (int, float)):
            ts_s = ts / 1000 if ts > 1e10 else ts
            dt = datetime.fromtimestamp(ts_s, tz=timezone.utc)
            return dt.isoformat()
        
        # datetime object?
        if hasattr(ts, "isoformat") and callable(ts.isoformat):
            result = ts.isoformat()
            if "+" not in result and "Z" not in result:
                result += "+00:00"
            return result
        
        raise ValueError(f"Cannot parse timestamp: {ts!r} (type: {type(ts).__name__})")

    def _collect_region_metrics(
        self,
        region: str,
        resources: List[Dict],
        start: datetime,
        end: datetime,
    ) -> List[MetricPoint]:
        cw = self._session.client("cloudwatch", region_name=region)
        metrics:    List[MetricPoint] = []
        queries:    List[Dict]        = []
        query_meta: Dict[str, Tuple]  = {}
        queries_by_type: Dict[str, int] = {}

        _default_period: Dict[str, int] = {
            "ec2":      60,
            "lambda":   60,
            "elb":      60,
            "rds":      300,
            "dynamodb": 300,
        }

        for res in resources:
            rtype   = res["type"]
            cfg     = self._metric_defs.get(rtype, {})
            defs    = cfg.get("definitions", [])
            dim_key = cfg.get("dimension_key", "InstanceId")
            if not defs:
                continue
            dim_val = self._cw_dim_value(res)
            ns      = cfg.get("namespace", "AWS/EC2")
            default_period = _default_period.get(rtype, 60)

            for mdef in defs:
                qid    = f"q{len(queries)}"
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
                query_meta[qid] = (res, mdef, period)
                queries_by_type[rtype] = queries_by_type.get(rtype, 0) + 1

        logger.info(f"[AWS/{region}] Query plan: {queries_by_type}  (total={len(queries)})")

        if queries:
            max_period_s = max(q["MetricStat"]["Period"] for q in queries)
        else:
            max_period_s = 60
        min_window_s   = max(max_period_s * 2, self._lookback_min * 60)
        effective_start = end - timedelta(seconds=min_window_s)

        results_by_type: Dict[str, int] = {}
        empty_by_type:   Dict[str, int] = {}

        for batch_start in range(0, len(queries), 100):
            batch_q = queries[batch_start: batch_start + 100]
            if not batch_q:
                continue
            try:
                resp = self._retry(
                    lambda bq=batch_q: cw.get_metric_data(
                        MetricDataQueries=bq,
                        StartTime=effective_start,
                        EndTime=end,
                    )
                )
            except Exception as exc:
                logger.warning(f"[AWS/{region}] GetMetricData failed: {exc}")
                continue

            all_results = list(resp.get("MetricDataResults", []))
            while resp.get("NextToken"):
                try:
                    resp = self._retry(
                        lambda nt=resp["NextToken"], bq=batch_q: cw.get_metric_data(
                            MetricDataQueries=bq,
                            StartTime=effective_start,
                            EndTime=end,
                            NextToken=nt,
                        )
                    )
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
                    res, mdef, _period = query_meta[rid]
                    rtype = res["type"]
                    if not vals:
                        empty_by_type[rtype] = empty_by_type.get(rtype, 0) + 1
                        continue

                    aggregated_value = self._aggregate(vals, mdef["stat"])

                    ts = max(tsts)
                    try:
                        ts_str = self._ensure_iso_timestamp(ts)
                    except (ValueError, TypeError) as e:
                        logger.warning(
                            f"[AWS/{region}] Bad timestamp {ts!r} for {res['name']}/{mdef['name']}: {e}. "
                            f"Using current time."
                        )
                        ts_str = datetime.now(timezone.utc).isoformat()

                    metrics.append(MetricPoint(
                        timestamp=ts_str,
                        cloud="aws", region=region,
                        resource_type=rtype,
                        resource_id=res["id"], resource_name=res["name"],
                        metric_name=mdef["name"],
                        metric_value=round(float(aggregated_value), 6),
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
                cw_logs    = self._session.client("logs", region_name=region)
                log_groups: List[Dict] = []

                for page in cw_logs.get_paginator("describe_log_groups").paginate(
                    PaginationConfig={"MaxItems": self._max_log_groups}
                ):
                    log_groups.extend(page["logGroups"])

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