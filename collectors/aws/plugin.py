"""
collectors/aws/plugin.py
────────────────────────
Complete AWS infrastructure metrics collector.

Covers ALL infrastructure layers:

  COMPUTE
    • EC2 instances      — CloudWatch (CPU/Net/Disk/Status) + Node Exporter via SSM
                           (Memory %, Disk %, Load avg — no CW Agent needed)
    • ECS clusters        — Task count, CPU/memory reservation & utilisation
    • EKS clusters        — Node metrics via CloudWatch Container Insights

  DATABASES
    • RDS instances       — 16 CloudWatch metrics + replica lag
    • ElastiCache         — Redis/Memcached: hit ratio, evictions, connections,
                            replication lag, engine CPU
    • DynamoDB            — Capacity, throttles, per-operation latency

  MESSAGING / STREAMING
    • SQS queues          — Depth, age of oldest message, DLQ depth
    • SNS topics          — Publish/delivery counts, failures
    • Kinesis streams     — Iterator age, throttled records, shard-level metrics

  SERVERLESS
    • Lambda functions    — Invocations, errors, duration, throttles, cold starts

  NETWORKING
    • ELB / ALB           — Request count, latency, 2xx/4xx/5xx, host health
    • API Gateway         — Count, latency, 4xx/5xx, cache hit ratio
    • CloudFront          — Requests, cache hit rate, error rate, origin latency
    • VPC NAT Gateway     — Bytes in/out, connections, errors

  STORAGE
    • S3 buckets          — Bucket size, object count, request errors, latency
    • EBS volumes         — IOPS, throughput, queue depth, burst balance

  PLATFORM
    • Auto Scaling Groups — Desired / InService / Pending / Terminating capacity
    • CloudWatch Logs     — Log group event collection

  HEALTH ROLLUP
    • HealthScore per resource (0.0 critical → 1.0 healthy) for remediation engine

Version: 1.0
"""
from __future__ import annotations

import logging
import os
import re
import time
import random
import concurrent.futures
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from collectors.base import CloudCollectorPlugin
from collectors.models import LogEntry, MetricPoint, HealthScore

logger = logging.getLogger("collector.aws")

# ── retry ─────────────────────────────────────────────────────────────────────
_MAX_RETRIES  = 5
_BASE_BACKOFF = 0.5
_MAX_BACKOFF  = 30.0

_RETRIABLE_CODES = {
    "ThrottlingException", "RequestLimitExceeded", "Throttling",
    "ProvisionedThroughputExceededException", "TransactionInProgressException",
    "RequestThrottled", "SlowDown", "TooManyRequestsException",
    "ServiceUnavailable", "InternalFailure", "InternalError",
}

_DYNAMODB_LATENCY_OPS = [
    "GetItem", "PutItem", "UpdateItem", "DeleteItem",
    "Query",   "Scan",    "BatchGetItem", "BatchWriteItem",
]

# ── Node Exporter via SSM ─────────────────────────────────────────────────────
_NODE_EXPORTER_METRICS: Dict[str, Tuple[str, str, Optional[str]]] = {
    "node_memory_MemTotal_bytes":        ("memory_total_bytes",        "bytes",   None),
    "node_memory_MemAvailable_bytes":    ("memory_available_bytes",    "bytes",   None),
    "node_memory_MemFree_bytes":         ("memory_free_bytes",         "bytes",   None),
    "node_memory_Cached_bytes":          ("memory_cached_bytes",       "bytes",   None),
    "node_filesystem_size_bytes":        ("disk_total_bytes",          "bytes",   'mountpoint="/"'),
    "node_filesystem_avail_bytes":       ("disk_available_bytes",      "bytes",   'mountpoint="/"'),
    "node_filesystem_files_free":        ("disk_inodes_free",          "count",   'mountpoint="/"'),
    "node_disk_read_bytes_total":        ("disk_read_bytes_total",     "bytes",   None),
    "node_disk_written_bytes_total":     ("disk_write_bytes_total",    "bytes",   None),
    "node_disk_reads_completed_total":   ("disk_reads_completed",      "count",   None),
    "node_disk_writes_completed_total":  ("disk_writes_completed",     "count",   None),
    "node_network_receive_bytes_total":  ("network_receive_bytes",     "bytes",   'device="eth0"'),
    "node_network_transmit_bytes_total": ("network_transmit_bytes",    "bytes",   'device="eth0"'),
    "node_load1":                        ("load_avg_1m",               "count",   None),
    "node_load5":                        ("load_avg_5m",               "count",   None),
    "node_load15":                       ("load_avg_15m",              "count",   None),
    "node_cpu_seconds_total":            ("cpu_seconds_total",         "seconds", 'mode="idle"'),
}

_SSM_INSTALL_AND_SCRAPE = r"""
#!/bin/bash
if systemctl is-active --quiet node_exporter 2>/dev/null; then
    echo "---METRICS_START---"
    curl -sf http://localhost:9100/metrics 2>/dev/null
    echo
    echo "---METRICS_END---"
    exit 0
fi
NE_BIN="/usr/local/bin/node_exporter"
if [ ! -f "$NE_BIN" ]; then
    if command -v dnf &>/dev/null; then
        dnf install -y golang-github-prometheus-node-exporter 2>/dev/null && \
            NE_BIN=$(which node_exporter 2>/dev/null || echo "$NE_BIN")
    fi
fi
if [ -f "$NE_BIN" ]; then
    $NE_BIN --web.listen-address=127.0.0.1:9100 &
    NE_PID=$!
    sleep 2
    echo "---METRICS_START---"
    curl -sf http://localhost:9100/metrics 2>/dev/null
    echo
    echo "---METRICS_END---"
    kill $NE_PID 2>/dev/null
    wait $NE_PID 2>/dev/null
    exit 0
fi
echo "---METRICS_START---"
awk '/MemTotal/{printf "node_memory_MemTotal_bytes %.0f\n",$2*1024}
     /MemAvailable/{printf "node_memory_MemAvailable_bytes %.0f\n",$2*1024}
     /MemFree/{printf "node_memory_MemFree_bytes %.0f\n",$2*1024}
     /^Cached:/{printf "node_memory_Cached_bytes %.0f\n",$2*1024}' /proc/meminfo
awk '{printf "node_load1 %s\nnode_load5 %s\nnode_load15 %s\n",$1,$2,$3}' /proc/loadavg
df -B1 / 2>/dev/null | awk 'NR==2{printf "node_filesystem_size_bytes{mountpoint=\"/\"} %s\nnode_filesystem_avail_bytes{mountpoint=\"/\"} %s\n",$2,$4}'
awk '/eth0:/{printf "node_network_receive_bytes_total{device=\"eth0\"} %s\nnode_network_transmit_bytes_total{device=\"eth0\"} %s\n",$2,$10}' /proc/net/dev
awk 'NR==1{printf "node_cpu_seconds_total{mode=\"idle\"} %.2f\n",$5/100}' /proc/stat
echo "---METRICS_END---"
"""

# ── Health thresholds for score rollup ────────────────────────────────────────
_HEALTH_THRESHOLDS: Dict[str, Dict[str, Tuple[float, float]]] = {
    # metric_name → (warn_threshold, critical_threshold)
    "cpu_utilization_percent":    (75.0, 90.0),
    "memory_used_percent":        (80.0, 95.0),
    "disk_used_percent":          (75.0, 90.0),
    "load_avg_1m":                (4.0,  8.0),
    "database_connections":       (80.0, 95.0),   # percent of max
    "free_storage_bytes":         (5e9,  1e9),     # inverted: lower = worse
    "unhealthy_host_count":       (1.0,  3.0),
    "status_check_failed":        (0.5,  1.0),
    "throttled_requests":         (10.0, 50.0),
    "errors_total":               (5.0,  20.0),
    "cache_hit_rate":             (0.5,  0.2),     # inverted
    "elasticache_cache_hits":     (0.0,  0.0),
    "sqs_oldest_message_age_s":   (300,  900),
    "sqs_queue_depth":            (1000, 5000),
    "iterator_age_ms":            (60000, 300000),
}


class AWSCollectorPlugin(CloudCollectorPlugin):
    """
    Complete AWS infrastructure metrics collector.
    Single entry point: collect_all() → (metrics, logs, health_scores)
    """

    def __init__(self, config: Dict):
        super().__init__("aws", config)
        try:
            import boto3
            from botocore.exceptions import ClientError
            self._boto3       = boto3
            self._ClientError = ClientError
        except ImportError:
            raise ImportError("AWS plugin requires boto3: pip install boto3")

        aws = config.get("aws", {})
        session_kwargs: Dict[str, Any] = {}

        access_key    = aws.get("access_key_id")     or os.getenv("AWS_ACCESS_KEY_ID", "")
        secret_key    = aws.get("secret_access_key") or os.getenv("AWS_SECRET_ACCESS_KEY", "")
        session_token = aws.get("session_token")     or os.getenv("AWS_SESSION_TOKEN", "")

        if access_key:    session_kwargs["aws_access_key_id"]     = access_key
        if secret_key:    session_kwargs["aws_secret_access_key"] = secret_key
        if session_token: session_kwargs["aws_session_token"]     = session_token
        if aws.get("profile"): session_kwargs["profile_name"]     = aws["profile"]

        self._session             = self._boto3.Session(**session_kwargs)
        self._regions             = aws.get("regions", [os.getenv("AWS_DEFAULT_REGION", "us-east-1")])
        self._res_types           = set(aws.get("resources", []))
        self._lookback_min        = int(aws.get("lookback_minutes", 10))
        self._logs_lookback       = int(aws.get("logs_lookback_minutes", 15))
        self._max_log_groups      = int(aws.get("max_log_groups", 20))
        self._max_log_events      = int(aws.get("max_log_events_per_group", 50))
        self._node_exporter_on    = aws.get("node_exporter_via_ssm", True)
        self._ssm_timeout         = int(aws.get("ssm_timeout_seconds", 180))
        self._metric_defs         = aws.get("metrics", self._default_metric_defs())

        logger.info(
            f"[AWS] regions={self._regions} "
            f"resource_types={self._res_types or 'ALL'} "
            f"node_exporter_via_ssm={self._node_exporter_on}"
        )

    # ═══════════════════════════════════════════════════════════════════════════
    # PUBLIC API
    # ═══════════════════════════════════════════════════════════════════════════

    def collect_all(self) -> Tuple[List[MetricPoint], List[LogEntry], List[HealthScore]]:
        """Single call that returns metrics + logs + health scores."""
        resources = self.discover_resources()
        metrics   = self.collect_metrics(resources)
        logs      = self.collect_logs(resources)
        scores    = self.compute_health_scores(resources, metrics)
        return metrics, logs, scores

    # ═══════════════════════════════════════════════════════════════════════════
    # RETRY
    # ═══════════════════════════════════════════════════════════════════════════

    def _retry(self, fn, *args, **kwargs):
        attempt = 0
        while True:
            try:
                return fn(*args, **kwargs)
            except self._ClientError as exc:
                code = exc.response.get("Error", {}).get("Code", "")
                if code not in _RETRIABLE_CODES or attempt >= _MAX_RETRIES:
                    raise
                delay = min(_BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 0.3), _MAX_BACKOFF)
                logger.warning(f"[AWS] Retrying '{code}' (attempt {attempt+1}), wait {delay:.1f}s")
                time.sleep(delay)
                attempt += 1
            except Exception:
                raise

    # ═══════════════════════════════════════════════════════════════════════════
    # METRIC DEFINITIONS
    # ═══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def _default_metric_defs() -> Dict:
        return {

            # ── EC2 ──────────────────────────────────────────────────────────
            "ec2": {
                "namespace":     "AWS/EC2",
                "dimension_key": "InstanceId",
                "definitions": [
                    {"name": "cpu_utilization_percent",       "cw_name": "CPUUtilization",             "stat": "Average", "unit": "percent"},
                    {"name": "network_in_bytes",              "cw_name": "NetworkIn",                  "stat": "Sum",     "unit": "bytes"},
                    {"name": "network_out_bytes",             "cw_name": "NetworkOut",                 "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_read_bytes",               "cw_name": "DiskReadBytes",              "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_write_bytes",              "cw_name": "DiskWriteBytes",             "stat": "Sum",     "unit": "bytes"},
                    {"name": "disk_read_ops",                 "cw_name": "DiskReadOps",                "stat": "Sum",     "unit": "count"},
                    {"name": "disk_write_ops",                "cw_name": "DiskWriteOps",               "stat": "Sum",     "unit": "count"},
                    {"name": "network_packets_in",            "cw_name": "NetworkPacketsIn",           "stat": "Sum",     "unit": "count"},
                    {"name": "network_packets_out",           "cw_name": "NetworkPacketsOut",          "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed",           "cw_name": "StatusCheckFailed",          "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed_instance",  "cw_name": "StatusCheckFailed_Instance", "stat": "Sum",     "unit": "count"},
                    {"name": "status_check_failed_system",    "cw_name": "StatusCheckFailed_System",   "stat": "Sum",     "unit": "count"},
                    {"name": "cpu_credit_balance",            "cw_name": "CPUCreditBalance",           "stat": "Average", "unit": "count"},
                    {"name": "cpu_credit_usage",              "cw_name": "CPUCreditUsage",             "stat": "Average", "unit": "count"},
                    # Memory & Disk come from Node Exporter via SSM (see below)
                ],
            },

            # ── EBS volumes ───────────────────────────────────────────────────
            "ebs": {
                "namespace":     "AWS/EBS",
                "dimension_key": "VolumeId",
                "definitions": [
                    {"name": "read_ops",          "cw_name": "VolumeReadOps",          "stat": "Sum",     "unit": "count"},
                    {"name": "write_ops",         "cw_name": "VolumeWriteOps",         "stat": "Sum",     "unit": "count"},
                    {"name": "read_bytes",        "cw_name": "VolumeReadBytes",        "stat": "Sum",     "unit": "bytes"},
                    {"name": "write_bytes",       "cw_name": "VolumeWriteBytes",       "stat": "Sum",     "unit": "bytes"},
                    {"name": "total_read_time",   "cw_name": "VolumeTotalReadTime",    "stat": "Sum",     "unit": "seconds"},
                    {"name": "total_write_time",  "cw_name": "VolumeTotalWriteTime",   "stat": "Sum",     "unit": "seconds"},
                    {"name": "idle_time",         "cw_name": "VolumeIdleTime",         "stat": "Sum",     "unit": "seconds"},
                    {"name": "queue_length",      "cw_name": "VolumeQueueLength",      "stat": "Average", "unit": "count"},
                    {"name": "throughput_percent","cw_name": "VolumeThroughputPercentage","stat":"Average","unit": "percent"},
                    {"name": "burst_balance",     "cw_name": "BurstBalance",           "stat": "Average", "unit": "percent"},
                ],
            },

            # ── Auto Scaling ──────────────────────────────────────────────────
            "asg": {
                "namespace":     "AWS/AutoScaling",
                "dimension_key": "AutoScalingGroupName",
                "definitions": [
                    {"name": "group_desired_capacity",    "cw_name": "GroupDesiredCapacity",    "stat": "Average", "unit": "count"},
                    {"name": "group_in_service_instances","cw_name": "GroupInServiceInstances", "stat": "Average", "unit": "count"},
                    {"name": "group_pending_instances",   "cw_name": "GroupPendingInstances",   "stat": "Average", "unit": "count"},
                    {"name": "group_terminating_instances","cw_name":"GroupTerminatingInstances","stat": "Average", "unit": "count"},
                    {"name": "group_standby_instances",   "cw_name": "GroupStandbyInstances",   "stat": "Average", "unit": "count"},
                    {"name": "group_total_instances",     "cw_name": "GroupTotalInstances",     "stat": "Average", "unit": "count"},
                    {"name": "warm_pool_total_capacity",  "cw_name": "WarmPoolTotalCapacity",   "stat": "Average", "unit": "count"},
                ],
            },

            # ── RDS ───────────────────────────────────────────────────────────
            "rds": {
                "namespace":     "AWS/RDS",
                "dimension_key": "DBInstanceIdentifier",
                "definitions": [
                    {"name": "cpu_utilization_percent",        "cw_name": "CPUUtilization",            "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "database_connections",           "cw_name": "DatabaseConnections",       "stat": "Maximum", "unit": "count",    "period": 300},
                    {"name": "free_storage_bytes",             "cw_name": "FreeStorageSpace",          "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "read_iops",                      "cw_name": "ReadIOPS",                  "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "write_iops",                     "cw_name": "WriteIOPS",                 "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "read_latency_seconds",           "cw_name": "ReadLatency",               "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "write_latency_seconds",          "cw_name": "WriteLatency",              "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "freeable_memory_bytes",          "cw_name": "FreeableMemory",            "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "network_receive_bytes_per_sec",  "cw_name": "NetworkReceiveThroughput",  "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "network_transmit_bytes_per_sec", "cw_name": "NetworkTransmitThroughput", "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "swap_usage_bytes",               "cw_name": "SwapUsage",                 "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "disk_queue_depth",               "cw_name": "DiskQueueDepth",            "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "burst_balance_percent",          "cw_name": "BurstBalance",              "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "binlog_disk_usage_bytes",        "cw_name": "BinLogDiskUsage",           "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "replica_lag_seconds",            "cw_name": "ReplicaLag",                "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "replica_lag_aurora_seconds",     "cw_name": "AuroraBinlogReplicaLag",    "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "maximum_used_tx_ids",            "cw_name": "MaximumUsedTransactionIDs", "stat": "Maximum", "unit": "count",    "period": 300},
                    {"name": "deadlocks",                      "cw_name": "Deadlocks",                 "stat": "Sum",     "unit": "count",    "period": 300},
                ],
            },

            # ── ElastiCache ───────────────────────────────────────────────────
            "elasticache": {
                "namespace":     "AWS/ElastiCache",
                "dimension_key": "CacheClusterId",
                "definitions": [
                    {"name": "cpu_utilization_percent",   "cw_name": "CPUUtilization",        "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "engine_cpu_utilization",    "cw_name": "EngineCPUUtilization",  "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "freeable_memory_bytes",     "cw_name": "FreeableMemory",        "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "bytes_used_for_cache",      "cw_name": "BytesUsedForCache",     "stat": "Average", "unit": "bytes",    "period": 300},
                    {"name": "cache_hits",                "cw_name": "CacheHits",             "stat": "Sum",     "unit": "count",    "period": 300},
                    {"name": "cache_misses",              "cw_name": "CacheMisses",           "stat": "Sum",     "unit": "count",    "period": 300},
                    {"name": "cache_hit_rate",            "cw_name": "CacheHitRate",          "stat": "Average", "unit": "percent",  "period": 300},
                    {"name": "evictions",                 "cw_name": "Evictions",             "stat": "Sum",     "unit": "count",    "period": 300},
                    {"name": "curr_connections",          "cw_name": "CurrConnections",       "stat": "Maximum", "unit": "count",    "period": 300},
                    {"name": "new_connections",           "cw_name": "NewConnections",        "stat": "Sum",     "unit": "count",    "period": 300},
                    {"name": "network_bytes_in",          "cw_name": "NetworkBytesIn",        "stat": "Sum",     "unit": "bytes",    "period": 300},
                    {"name": "network_bytes_out",         "cw_name": "NetworkBytesOut",       "stat": "Sum",     "unit": "bytes",    "period": 300},
                    {"name": "replication_lag_seconds",   "cw_name": "ReplicationLag",        "stat": "Average", "unit": "seconds",  "period": 300},
                    {"name": "save_in_progress",          "cw_name": "SaveInProgress",        "stat": "Maximum", "unit": "count",    "period": 300},
                    {"name": "curr_items",                "cw_name": "CurrItems",             "stat": "Average", "unit": "count",    "period": 300},
                    {"name": "reclaimed",                 "cw_name": "Reclaimed",             "stat": "Sum",     "unit": "count",    "period": 300},
                ],
            },

            # ── DynamoDB ──────────────────────────────────────────────────────
            "dynamodb": {
                "namespace":     "AWS/DynamoDB",
                "dimension_key": "TableName",
                "definitions": [
                    {"name": "consumed_read_capacity",     "cw_name": "ConsumedReadCapacityUnits",     "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "consumed_write_capacity",    "cw_name": "ConsumedWriteCapacityUnits",    "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "throttled_requests",         "cw_name": "ThrottledRequests",             "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "system_errors",              "cw_name": "SystemErrors",                  "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "user_errors",                "cw_name": "UserErrors",                    "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "provisioned_read_capacity",  "cw_name": "ProvisionedReadCapacityUnits",  "stat": "Average", "unit": "count",   "period": 300},
                    {"name": "provisioned_write_capacity", "cw_name": "ProvisionedWriteCapacityUnits", "stat": "Average", "unit": "count",   "period": 300},
                    {"name": "returned_item_count",        "cw_name": "ReturnedItemCount",             "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "conditional_check_failed",   "cw_name": "ConditionalCheckFailedRequests","stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "online_index_throttle_events","cw_name":"OnlineIndexThrottleEvents",     "stat": "Sum",     "unit": "count",   "period": 300},
                ],
            },

            # ── Lambda ────────────────────────────────────────────────────────
            "lambda": {
                "namespace":     "AWS/Lambda",
                "dimension_key": "FunctionName",
                "definitions": [
                    {"name": "invocations_total",                "cw_name": "Invocations",                    "stat": "Sum",     "unit": "count"},
                    {"name": "errors_total",                     "cw_name": "Errors",                         "stat": "Sum",     "unit": "count"},
                    {"name": "duration_avg_ms",                  "cw_name": "Duration",                       "stat": "Average", "unit": "ms"},
                    {"name": "duration_max_ms",                  "cw_name": "Duration",                       "stat": "Maximum", "unit": "ms"},
                    {"name": "concurrent_executions",            "cw_name": "ConcurrentExecutions",           "stat": "Maximum", "unit": "count"},
                    {"name": "throttles_total",                  "cw_name": "Throttles",                      "stat": "Sum",     "unit": "count"},
                    {"name": "iterator_age_ms",                  "cw_name": "IteratorAge",                    "stat": "Maximum", "unit": "ms"},
                    {"name": "init_duration_ms",                 "cw_name": "InitDuration",                   "stat": "Average", "unit": "ms"},
                    {"name": "unreserved_concurrent_executions", "cw_name": "UnreservedConcurrentExecutions", "stat": "Maximum", "unit": "count"},
                    {"name": "async_events_received",            "cw_name": "AsyncEventsReceived",            "stat": "Sum",     "unit": "count"},
                    {"name": "async_event_age_ms",               "cw_name": "AsyncEventAge",                  "stat": "Maximum", "unit": "ms"},
                    {"name": "async_events_dropped",             "cw_name": "AsyncEventsDropped",             "stat": "Sum",     "unit": "count"},
                ],
            },

            # ── ELB / ALB ─────────────────────────────────────────────────────
            "elb": {
                "namespace":     "AWS/ApplicationELB",
                "dimension_key": "LoadBalancer",
                "definitions": [
                    {"name": "request_count",            "cw_name": "RequestCount",               "stat": "Sum",     "unit": "count"},
                    {"name": "target_response_time_s",   "cw_name": "TargetResponseTime",         "stat": "Average", "unit": "seconds"},
                    {"name": "http_2xx_count",           "cw_name": "HTTPCode_Target_2XX_Count",  "stat": "Sum",     "unit": "count"},
                    {"name": "http_4xx_count",           "cw_name": "HTTPCode_Target_4XX_Count",  "stat": "Sum",     "unit": "count"},
                    {"name": "http_5xx_count",           "cw_name": "HTTPCode_Target_5XX_Count",  "stat": "Sum",     "unit": "count"},
                    {"name": "elb_5xx_count",            "cw_name": "HTTPCode_ELB_5XX_Count",     "stat": "Sum",     "unit": "count"},
                    {"name": "active_connections",       "cw_name": "ActiveConnectionCount",      "stat": "Sum",     "unit": "count"},
                    {"name": "new_connections",          "cw_name": "NewConnectionCount",         "stat": "Sum",     "unit": "count"},
                    {"name": "healthy_host_count",       "cw_name": "HealthyHostCount",           "stat": "Average", "unit": "count"},
                    {"name": "unhealthy_host_count",     "cw_name": "UnHealthyHostCount",         "stat": "Average", "unit": "count"},
                    {"name": "processed_bytes",          "cw_name": "ProcessedBytes",             "stat": "Sum",     "unit": "bytes"},
                    {"name": "rejected_connections",     "cw_name": "RejectedConnectionCount",    "stat": "Sum",     "unit": "count"},
                    {"name": "target_tls_errors",        "cw_name": "TargetTLSNegotiationErrorCount","stat":"Sum",   "unit": "count"},
                ],
            },

            # ── API Gateway ───────────────────────────────────────────────────
            "apigateway": {
                "namespace":     "AWS/ApiGateway",
                "dimension_key": "ApiName",
                "definitions": [
                    {"name": "count",             "cw_name": "Count",            "stat": "Sum",     "unit": "count"},
                    {"name": "latency_avg_ms",    "cw_name": "Latency",          "stat": "Average", "unit": "ms"},
                    {"name": "latency_p99_ms",    "cw_name": "Latency",          "stat": "p99",     "unit": "ms"},
                    {"name": "integration_latency_avg_ms", "cw_name": "IntegrationLatency", "stat": "Average", "unit": "ms"},
                    {"name": "4xx_errors",        "cw_name": "4XXError",         "stat": "Sum",     "unit": "count"},
                    {"name": "5xx_errors",        "cw_name": "5XXError",         "stat": "Sum",     "unit": "count"},
                    {"name": "cache_hit_count",   "cw_name": "CacheHitCount",    "stat": "Sum",     "unit": "count"},
                    {"name": "cache_miss_count",  "cw_name": "CacheMissCount",   "stat": "Sum",     "unit": "count"},
                ],
            },

            # ── CloudFront ────────────────────────────────────────────────────
            "cloudfront": {
                "namespace":     "AWS/CloudFront",
                "dimension_key": "DistributionId",
                "definitions": [
                    {"name": "requests",             "cw_name": "Requests",            "stat": "Sum",     "unit": "count",   "region_override": "us-east-1"},
                    {"name": "bytes_downloaded",     "cw_name": "BytesDownloaded",     "stat": "Sum",     "unit": "bytes",   "region_override": "us-east-1"},
                    {"name": "bytes_uploaded",       "cw_name": "BytesUploaded",       "stat": "Sum",     "unit": "bytes",   "region_override": "us-east-1"},
                    {"name": "total_error_rate",     "cw_name": "TotalErrorRate",      "stat": "Average", "unit": "percent", "region_override": "us-east-1"},
                    {"name": "4xx_error_rate",       "cw_name": "4xxErrorRate",        "stat": "Average", "unit": "percent", "region_override": "us-east-1"},
                    {"name": "5xx_error_rate",       "cw_name": "5xxErrorRate",        "stat": "Average", "unit": "percent", "region_override": "us-east-1"},
                    {"name": "cache_hit_rate",       "cw_name": "CacheHitRate",        "stat": "Average", "unit": "percent", "region_override": "us-east-1"},
                    {"name": "origin_latency_ms",    "cw_name": "OriginLatency",       "stat": "Average", "unit": "ms",      "region_override": "us-east-1"},
                ],
            },

            # ── SQS ───────────────────────────────────────────────────────────
            "sqs": {
                "namespace":     "AWS/SQS",
                "dimension_key": "QueueName",
                "definitions": [
                    {"name": "messages_sent",          "cw_name": "NumberOfMessagesSent",                "stat": "Sum",     "unit": "count"},
                    {"name": "messages_received",      "cw_name": "NumberOfMessagesReceived",            "stat": "Sum",     "unit": "count"},
                    {"name": "messages_deleted",       "cw_name": "NumberOfMessagesDeleted",             "stat": "Sum",     "unit": "count"},
                    {"name": "sqs_queue_depth",        "cw_name": "ApproximateNumberOfMessagesVisible",  "stat": "Maximum", "unit": "count"},
                    {"name": "not_visible_count",      "cw_name": "ApproximateNumberOfMessagesNotVisible","stat":"Maximum",  "unit": "count"},
                    {"name": "sqs_oldest_message_age_s","cw_name":"ApproximateAgeOfOldestMessage",       "stat": "Maximum", "unit": "seconds"},
                    {"name": "empty_receives",         "cw_name": "NumberOfEmptyReceives",               "stat": "Sum",     "unit": "count"},
                    {"name": "sent_message_size_bytes","cw_name": "SentMessageSize",                     "stat": "Average", "unit": "bytes"},
                ],
            },

            # ── SNS ───────────────────────────────────────────────────────────
            "sns": {
                "namespace":     "AWS/SNS",
                "dimension_key": "TopicName",
                "definitions": [
                    {"name": "messages_published",          "cw_name": "NumberOfMessagesPublished",         "stat": "Sum",     "unit": "count"},
                    {"name": "notifications_delivered",     "cw_name": "NumberOfNotificationsDelivered",    "stat": "Sum",     "unit": "count"},
                    {"name": "notifications_failed",        "cw_name": "NumberOfNotificationsFailed",       "stat": "Sum",     "unit": "count"},
                    {"name": "notifications_filtered_out",  "cw_name": "NumberOfNotificationsFilteredOut",  "stat": "Sum",     "unit": "count"},
                    {"name": "publish_size_bytes",          "cw_name": "PublishSize",                       "stat": "Average", "unit": "bytes"},
                ],
            },

            # ── Kinesis Data Streams ──────────────────────────────────────────
            "kinesis": {
                "namespace":     "AWS/Kinesis",
                "dimension_key": "StreamName",
                "definitions": [
                    {"name": "get_records_bytes",            "cw_name": "GetRecords.Bytes",                "stat": "Sum",     "unit": "bytes"},
                    {"name": "get_records_latency_ms",       "cw_name": "GetRecords.IteratorAgeMilliseconds","stat":"Maximum", "unit": "ms"},
                    {"name": "iterator_age_ms",              "cw_name": "GetRecords.IteratorAgeMilliseconds","stat":"Maximum", "unit": "ms"},
                    {"name": "get_records_count",            "cw_name": "GetRecords.Records",              "stat": "Sum",     "unit": "count"},
                    {"name": "get_records_success",          "cw_name": "GetRecords.Success",              "stat": "Average", "unit": "count"},
                    {"name": "put_record_bytes",             "cw_name": "PutRecord.Bytes",                 "stat": "Sum",     "unit": "bytes"},
                    {"name": "put_record_latency_ms",        "cw_name": "PutRecord.Latency",               "stat": "Average", "unit": "ms"},
                    {"name": "put_record_success",           "cw_name": "PutRecord.Success",               "stat": "Average", "unit": "count"},
                    {"name": "put_records_bytes",            "cw_name": "PutRecords.Bytes",                "stat": "Sum",     "unit": "bytes"},
                    {"name": "put_records_throttled",        "cw_name": "PutRecords.ThrottledRecords",     "stat": "Sum",     "unit": "count"},
                    {"name": "put_records_failed",           "cw_name": "PutRecords.FailedRecords",        "stat": "Sum",     "unit": "count"},
                    {"name": "read_throughput_exceeded",     "cw_name": "ReadProvisionedThroughputExceeded","stat":"Sum",     "unit": "count"},
                    {"name": "write_throughput_exceeded",    "cw_name": "WriteProvisionedThroughputExceeded","stat":"Sum",    "unit": "count"},
                    {"name": "incoming_bytes",               "cw_name": "IncomingBytes",                   "stat": "Sum",     "unit": "bytes"},
                    {"name": "incoming_records",             "cw_name": "IncomingRecords",                 "stat": "Sum",     "unit": "count"},
                ],
            },

            # ── ECS ───────────────────────────────────────────────────────────
            "ecs": {
                "namespace":     "AWS/ECS",
                "dimension_key": "ClusterName",
                "definitions": [
                    {"name": "cpu_utilization_percent",    "cw_name": "CPUUtilization",       "stat": "Average", "unit": "percent"},
                    {"name": "memory_utilization_percent", "cw_name": "MemoryUtilization",    "stat": "Average", "unit": "percent"},
                    {"name": "cpu_reservation_percent",    "cw_name": "CPUReservation",       "stat": "Average", "unit": "percent"},
                    {"name": "memory_reservation_percent", "cw_name": "MemoryReservation",    "stat": "Average", "unit": "percent"},
                    {"name": "running_task_count",         "cw_name": "RunningTaskCount",     "stat": "Average", "unit": "count"},
                    {"name": "pending_task_count",         "cw_name": "PendingTaskCount",     "stat": "Average", "unit": "count"},
                    {"name": "service_desired_count",      "cw_name": "DesiredTaskCount",     "stat": "Average", "unit": "count"},
                ],
            },

            # ── EKS (Container Insights) ──────────────────────────────────────
            "eks": {
                "namespace":     "ContainerInsights",
                "dimension_key": "ClusterName",
                "definitions": [
                    {"name": "node_cpu_utilization_percent",    "cw_name": "node_cpu_utilization",           "stat": "Average", "unit": "percent"},
                    {"name": "node_memory_utilization_percent", "cw_name": "node_memory_utilization",        "stat": "Average", "unit": "percent"},
                    {"name": "node_network_total_bytes",        "cw_name": "node_network_total_bytes",       "stat": "Sum",     "unit": "bytes"},
                    {"name": "node_filesystem_utilization",     "cw_name": "node_filesystem_utilization",    "stat": "Average", "unit": "percent"},
                    {"name": "pod_cpu_utilization_percent",     "cw_name": "pod_cpu_utilization",            "stat": "Average", "unit": "percent"},
                    {"name": "pod_memory_utilization_percent",  "cw_name": "pod_memory_utilization",         "stat": "Average", "unit": "percent"},
                    {"name": "pod_restart_count",               "cw_name": "pod_number_of_container_restarts","stat":"Sum",     "unit": "count"},
                    {"name": "cluster_failed_node_count",       "cw_name": "cluster_failed_node_count",      "stat": "Maximum", "unit": "count"},
                    {"name": "cluster_node_count",              "cw_name": "cluster_node_count",             "stat": "Average", "unit": "count"},
                    {"name": "namespace_cpu_utilization",       "cw_name": "namespace_cpu_utilization",      "stat": "Average", "unit": "percent"},
                    {"name": "namespace_memory_utilization",    "cw_name": "namespace_memory_utilization",   "stat": "Average", "unit": "percent"},
                ],
            },

            # ── S3 ────────────────────────────────────────────────────────────
            "s3": {
                "namespace":     "AWS/S3",
                "dimension_key": "BucketName",
                "definitions": [
                    {"name": "bucket_size_bytes",    "cw_name": "BucketSizeBytes",       "stat": "Average", "unit": "bytes",   "period": 86400, "extra_dims": [{"Name": "StorageType", "Value": "StandardStorage"}]},
                    {"name": "object_count",         "cw_name": "NumberOfObjects",       "stat": "Average", "unit": "count",   "period": 86400, "extra_dims": [{"Name": "StorageType", "Value": "AllStorageTypes"}]},
                    {"name": "all_requests",         "cw_name": "AllRequests",           "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "get_requests",         "cw_name": "GetRequests",           "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "put_requests",         "cw_name": "PutRequests",           "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "delete_requests",      "cw_name": "DeleteRequests",        "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "4xx_errors",           "cw_name": "4xxErrors",             "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "5xx_errors",           "cw_name": "5xxErrors",             "stat": "Sum",     "unit": "count",   "period": 300},
                    {"name": "first_byte_latency_ms","cw_name": "FirstByteLatency",      "stat": "Average", "unit": "ms",      "period": 300},
                    {"name": "total_request_latency_ms","cw_name":"TotalRequestLatency", "stat": "Average", "unit": "ms",      "period": 300},
                    {"name": "bytes_downloaded",     "cw_name": "BytesDownloaded",       "stat": "Sum",     "unit": "bytes",   "period": 300},
                    {"name": "bytes_uploaded",       "cw_name": "BytesUploaded",         "stat": "Sum",     "unit": "bytes",   "period": 300},
                ],
            },

            # ── NAT Gateway ───────────────────────────────────────────────────
            "natgateway": {
                "namespace":     "AWS/NATGateway",
                "dimension_key": "NatGatewayId",
                "definitions": [
                    {"name": "bytes_in_from_destination", "cw_name": "BytesInFromDestination", "stat": "Sum",     "unit": "bytes"},
                    {"name": "bytes_in_from_source",      "cw_name": "BytesInFromSource",      "stat": "Sum",     "unit": "bytes"},
                    {"name": "bytes_out_to_destination",  "cw_name": "BytesOutToDestination",  "stat": "Sum",     "unit": "bytes"},
                    {"name": "bytes_out_to_source",       "cw_name": "BytesOutToSource",       "stat": "Sum",     "unit": "bytes"},
                    {"name": "active_connections",        "cw_name": "ActiveConnectionCount",  "stat": "Maximum", "unit": "count"},
                    {"name": "connection_attempt_count",  "cw_name": "ConnectionAttemptCount", "stat": "Sum",     "unit": "count"},
                    {"name": "connection_established_count","cw_name":"ConnectionEstablishedCount","stat":"Sum",   "unit": "count"},
                    {"name": "error_port_allocation",     "cw_name": "ErrorPortAllocation",    "stat": "Sum",     "unit": "count"},
                    {"name": "idle_timeout_count",        "cw_name": "IdleTimeoutCount",       "stat": "Sum",     "unit": "count"},
                    {"name": "packets_drop_count",        "cw_name": "PacketsDropCount",       "stat": "Sum",     "unit": "count"},
                ],
            },
        }

    # ═══════════════════════════════════════════════════════════════════════════
    # RESOURCE DISCOVERY
    # ═══════════════════════════════════════════════════════════════════════════

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

    def _want(self, rtype: str) -> bool:
        """Return True if this resource type should be collected."""
        return not self._res_types or rtype in self._res_types

    def _discover_region(self, region: str) -> List[Dict]:
        found: List[Dict] = []
        found.extend(self._discover_ec2(region))
        found.extend(self._discover_ebs(region))
        found.extend(self._discover_asg(region))
        found.extend(self._discover_rds(region))
        found.extend(self._discover_elasticache(region))
        found.extend(self._discover_dynamodb(region))
        found.extend(self._discover_lambda(region))
        found.extend(self._discover_elb(region))
        found.extend(self._discover_apigateway(region))
        found.extend(self._discover_cloudfront(region))
        found.extend(self._discover_sqs(region))
        found.extend(self._discover_sns(region))
        found.extend(self._discover_kinesis(region))
        found.extend(self._discover_ecs(region))
        found.extend(self._discover_eks(region))
        found.extend(self._discover_s3(region))
        found.extend(self._discover_natgateway(region))
        return found

    # ── EC2 ───────────────────────────────────────────────────────────────────
    def _discover_ec2(self, region: str) -> List[Dict]:
        if not self._want("ec2"):
            return []
        found = []
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
                            "tags": tags,
                        })
        except Exception as exc:
            logger.error(f"[AWS/{region}] EC2: {exc}")
        return found

    # ── EBS ───────────────────────────────────────────────────────────────────
    def _discover_ebs(self, region: str) -> List[Dict]:
        if not self._want("ebs"):
            return []
        found = []
        try:
            ec2 = self._session.client("ec2", region_name=region)
            for page in ec2.get_paginator("describe_volumes").paginate(
                Filters=[{"Name": "status", "Values": ["in-use"]}]
            ):
                for vol in page["Volumes"]:
                    tags = {t["Key"]: t["Value"] for t in vol.get("Tags", [])}
                    attachment = vol.get("Attachments", [{}])[0]
                    found.append({
                        "cloud": "aws", "region": region, "type": "ebs",
                        "id":    vol["VolumeId"],
                        "name":  tags.get("Name", vol["VolumeId"]),
                        "volume_type": vol.get("VolumeType", ""),
                        "size_gb":     vol.get("Size", 0),
                        "attached_to": attachment.get("InstanceId", ""),
                        "az":          vol.get("AvailabilityZone", ""),
                        "iops":        vol.get("Iops", 0),
                        "tags": tags,
                    })
        except Exception as exc:
            logger.error(f"[AWS/{region}] EBS: {exc}")
        return found

    # ── Auto Scaling Groups ───────────────────────────────────────────────────
    def _discover_asg(self, region: str) -> List[Dict]:
        if not self._want("asg"):
            return []
        found = []
        try:
            asg_client = self._session.client("autoscaling", region_name=region)
            for page in asg_client.get_paginator("describe_auto_scaling_groups").paginate():
                for asg in page["AutoScalingGroups"]:
                    tags = {t["Key"]: t["Value"] for t in asg.get("Tags", [])}
                    found.append({
                        "cloud": "aws", "region": region, "type": "asg",
                        "id":    asg["AutoScalingGroupName"],
                        "name":  asg["AutoScalingGroupName"],
                        "min_size":     asg.get("MinSize", 0),
                        "max_size":     asg.get("MaxSize", 0),
                        "desired":      asg.get("DesiredCapacity", 0),
                        "tags": tags,
                    })
        except Exception as exc:
            logger.error(f"[AWS/{region}] ASG: {exc}")
        return found

    # ── RDS ───────────────────────────────────────────────────────────────────
    def _discover_rds(self, region: str) -> List[Dict]:
        if not self._want("rds"):
            return []
        found = []
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
                            "engine_version": db.get("EngineVersion", ""),
                            "instance_class": db.get("DBInstanceClass", ""),
                            "cluster_id":     db.get("DBClusterIdentifier", ""),
                            "vpc_id":         db.get("DBSubnetGroup", {}).get("VpcId", ""),
                            "multi_az":       str(db.get("MultiAZ", False)),
                            "storage_gb":     db.get("AllocatedStorage", 0),
                        })
        except Exception as exc:
            logger.error(f"[AWS/{region}] RDS: {exc}")
        return found

    # ── ElastiCache ───────────────────────────────────────────────────────────
    def _discover_elasticache(self, region: str) -> List[Dict]:
        if not self._want("elasticache"):
            return []
        found = []
        try:
            ec = self._session.client("elasticache", region_name=region)
            for page in ec.get_paginator("describe_cache_clusters").paginate(
                ShowCacheNodeInfo=True
            ):
                for cluster in page["CacheClusters"]:
                    if cluster["CacheClusterStatus"] == "available":
                        found.append({
                            "cloud": "aws", "region": region, "type": "elasticache",
                            "id":    cluster["CacheClusterId"],
                            "name":  cluster["CacheClusterId"],
                            "engine":        cluster.get("Engine", "redis"),
                            "engine_version":cluster.get("EngineVersion", ""),
                            "node_type":     cluster.get("CacheNodeType", ""),
                            "num_nodes":     cluster.get("NumCacheNodes", 1),
                            "replication_group": cluster.get("ReplicationGroupId", ""),
                        })
        except Exception as exc:
            logger.error(f"[AWS/{region}] ElastiCache: {exc}")
        return found

    # ── DynamoDB ──────────────────────────────────────────────────────────────
    def _discover_dynamodb(self, region: str) -> List[Dict]:
        if not self._want("dynamodb"):
            return []
        found = []
        try:
            ddb = self._session.client("dynamodb", region_name=region)
            for page in ddb.get_paginator("list_tables").paginate():
                for tname in page["TableNames"]:
                    try:
                        info = ddb.describe_table(TableName=tname)["Table"]
                        if info["TableStatus"] == "ACTIVE":
                            found.append({
                                "cloud": "aws", "region": region, "type": "dynamodb",
                                "id": tname, "name": tname,
                                "billing_mode": info.get("BillingModeSummary", {}).get("BillingMode", "PROVISIONED"),
                                "item_count": info.get("ItemCount", 0),
                                "size_bytes": info.get("TableSizeBytes", 0),
                            })
                    except Exception:
                        pass
        except Exception as exc:
            logger.error(f"[AWS/{region}] DynamoDB: {exc}")
        return found

    # ── Lambda ────────────────────────────────────────────────────────────────
    def _discover_lambda(self, region: str) -> List[Dict]:
        if not self._want("lambda"):
            return []
        found = []
        try:
            lam = self._session.client("lambda", region_name=region)
            for page in lam.get_paginator("list_functions").paginate():
                for fn in page["Functions"]:
                    found.append({
                        "cloud": "aws", "region": region, "type": "lambda",
                        "id":      fn["FunctionName"],
                        "name":    fn["FunctionName"],
                        "arn":     fn["FunctionArn"],
                        "runtime": fn.get("Runtime", ""),
                        "memory_mb": fn.get("MemorySize", 128),
                        "timeout_s": fn.get("Timeout", 3),
                    })
        except Exception as exc:
            logger.error(f"[AWS/{region}] Lambda: {exc}")
        return found

    # ── ELB / ALB ─────────────────────────────────────────────────────────────
    def _discover_elb(self, region: str) -> List[Dict]:
        if not self._want("elb"):
            return []
        found = []
        try:
            elb = self._session.client("elbv2", region_name=region)
            for page in elb.get_paginator("describe_load_balancers").paginate():
                for lb in page["LoadBalancers"]:
                    if lb["State"]["Code"] == "active":
                        lb_arn = lb["LoadBalancerArn"]
                        found.append({
                            "cloud": "aws", "region": region, "type": "elb",
                            "id":      self._elb_arn_to_dim(lb_arn),
                            "name":    lb["LoadBalancerName"],
                            "arn":     lb_arn,
                            "lb_type": lb.get("Type", "application"),
                            "vpc_id":  lb.get("VpcId", ""),
                            "scheme":  lb.get("Scheme", ""),
                        })
        except Exception as exc:
            logger.error(f"[AWS/{region}] ELB: {exc}")
        return found

    # ── API Gateway ───────────────────────────────────────────────────────────
    def _discover_apigateway(self, region: str) -> List[Dict]:
        if not self._want("apigateway"):
            return []
        found = []
        try:
            apigw = self._session.client("apigateway", region_name=region)
            for page in apigw.get_paginator("get_rest_apis").paginate():
                for api in page["items"]:
                    found.append({
                        "cloud": "aws", "region": region, "type": "apigateway",
                        "id":   api["name"],
                        "name": api["name"],
                        "api_id":      api["id"],
                        "description": api.get("description", ""),
                    })
        except Exception as exc:
            logger.error(f"[AWS/{region}] API Gateway: {exc}")
        return found

    # ── CloudFront (global — only query from one region) ─────────────────────
    def _discover_cloudfront(self, region: str) -> List[Dict]:
        if not self._want("cloudfront") or region != self._regions[0]:
            return []
        found = []
        try:
            cf = self._session.client("cloudfront", region_name="us-east-1")
            for page in cf.get_paginator("list_distributions").paginate():
                dist_list = page.get("DistributionList", {})
                for dist in dist_list.get("Items", []):
                    if dist.get("Status") == "Deployed":
                        found.append({
                            "cloud": "aws", "region": "us-east-1", "type": "cloudfront",
                            "id":     dist["Id"],
                            "name":   dist.get("Comment") or dist["Id"],
                            "domain": dist.get("DomainName", ""),
                            "origins": ",".join(
                                o.get("DomainName", "") for o in
                                dist.get("Origins", {}).get("Items", [])
                            ),
                        })
        except Exception as exc:
            logger.error(f"[AWS] CloudFront: {exc}")
        return found

    # ── SQS ───────────────────────────────────────────────────────────────────
    def _discover_sqs(self, region: str) -> List[Dict]:
        if not self._want("sqs"):
            return []
        found = []
        try:
            sqs = self._session.client("sqs", region_name=region)
            resp = sqs.list_queues()
            for url in resp.get("QueueUrls", []):
                name = url.split("/")[-1]
                attrs = {}
                try:
                    attrs = sqs.get_queue_attributes(
                        QueueUrl=url,
                        AttributeNames=["All"]
                    ).get("Attributes", {})
                except Exception:
                    pass
                found.append({
                    "cloud": "aws", "region": region, "type": "sqs",
                    "id":    name,
                    "name":  name,
                    "url":   url,
                    "is_dlq": "RedrivePolicy" not in attrs and name.endswith("-dlq"),
                    "fifo":  str(attrs.get("FifoQueue", "false")).lower() == "true",
                })
        except Exception as exc:
            logger.error(f"[AWS/{region}] SQS: {exc}")
        return found

    # ── SNS ───────────────────────────────────────────────────────────────────
    def _discover_sns(self, region: str) -> List[Dict]:
        if not self._want("sns"):
            return []
        found = []
        try:
            sns = self._session.client("sns", region_name=region)
            for page in sns.get_paginator("list_topics").paginate():
                for topic in page["Topics"]:
                    arn  = topic["TopicArn"]
                    name = arn.split(":")[-1]
                    found.append({
                        "cloud": "aws", "region": region, "type": "sns",
                        "id":   name,
                        "name": name,
                        "arn":  arn,
                    })
        except Exception as exc:
            logger.error(f"[AWS/{region}] SNS: {exc}")
        return found

    # ── Kinesis ───────────────────────────────────────────────────────────────
    def _discover_kinesis(self, region: str) -> List[Dict]:
        if not self._want("kinesis"):
            return []
        found = []
        try:
            kin = self._session.client("kinesis", region_name=region)
            for page in kin.get_paginator("list_streams").paginate():
                for name in page.get("StreamNames", []):
                    try:
                        desc = kin.describe_stream_summary(StreamName=name)
                        summary = desc["StreamDescriptionSummary"]
                        if summary["StreamStatus"] == "ACTIVE":
                            found.append({
                                "cloud": "aws", "region": region, "type": "kinesis",
                                "id":    name,
                                "name":  name,
                                "shards":  summary.get("OpenShardCount", 0),
                                "retention_hours": summary.get("RetentionPeriodHours", 24),
                            })
                    except Exception:
                        pass
        except Exception as exc:
            logger.error(f"[AWS/{region}] Kinesis: {exc}")
        return found

    # ── ECS ───────────────────────────────────────────────────────────────────
    def _discover_ecs(self, region: str) -> List[Dict]:
        if not self._want("ecs"):
            return []
        found = []
        try:
            ecs = self._session.client("ecs", region_name=region)
            for page in ecs.get_paginator("list_clusters").paginate():
                clusters = page.get("clusterArns", [])
                if not clusters:
                    continue
                details = ecs.describe_clusters(clusters=clusters)
                for cl in details.get("clusters", []):
                    if cl["status"] == "ACTIVE":
                        found.append({
                            "cloud": "aws", "region": region, "type": "ecs",
                            "id":    cl["clusterName"],
                            "name":  cl["clusterName"],
                            "arn":   cl["clusterArn"],
                            "running_tasks":  cl.get("runningTasksCount", 0),
                            "pending_tasks":  cl.get("pendingTasksCount", 0),
                            "registered_containers": cl.get("registeredContainerInstancesCount", 0),
                        })
        except Exception as exc:
            logger.error(f"[AWS/{region}] ECS: {exc}")
        return found

    # ── EKS ───────────────────────────────────────────────────────────────────
    def _discover_eks(self, region: str) -> List[Dict]:
        if not self._want("eks"):
            return []
        found = []
        try:
            eks = self._session.client("eks", region_name=region)
            for page in eks.get_paginator("list_clusters").paginate():
                for name in page.get("clusters", []):
                    try:
                        desc = eks.describe_cluster(name=name)["cluster"]
                        if desc["status"] == "ACTIVE":
                            found.append({
                                "cloud": "aws", "region": region, "type": "eks",
                                "id":    name,
                                "name":  name,
                                "arn":   desc["arn"],
                                "k8s_version": desc.get("version", ""),
                                "vpc_id":      desc.get("resourcesVpcConfig", {}).get("vpcId", ""),
                                "endpoint":    desc.get("endpoint", ""),
                            })
                    except Exception:
                        pass
        except Exception as exc:
            logger.error(f"[AWS/{region}] EKS: {exc}")
        return found

    # ── S3 (enumerate once — global service) ─────────────────────────────────
    def _discover_s3(self, region: str) -> List[Dict]:
        if not self._want("s3") or region != self._regions[0]:
            return []
        found = []
        try:
            s3 = self._session.client("s3", region_name=region)
            buckets = s3.list_buckets().get("Buckets", [])
            for b in buckets:
                try:
                    loc = s3.get_bucket_location(Bucket=b["Name"])
                    bucket_region = loc.get("LocationConstraint") or "us-east-1"
                except Exception:
                    bucket_region = region

                found.append({
                    "cloud": "aws", "region": bucket_region, "type": "s3",
                    "id":   b["Name"],
                    "name": b["Name"],
                    "created": str(b.get("CreationDate", "")),
                })
        except Exception as exc:
            logger.error(f"[AWS] S3: {exc}")
        return found

    # ── NAT Gateway ───────────────────────────────────────────────────────────
    def _discover_natgateway(self, region: str) -> List[Dict]:
        if not self._want("natgateway"):
            return []
        found = []
        try:
            ec2 = self._session.client("ec2", region_name=region)
            for page in ec2.get_paginator("describe_nat_gateways").paginate(
                Filters=[{"Name": "state", "Values": ["available"]}]
            ):
                for ngw in page["NatGateways"]:
                    tags = {t["Key"]: t["Value"] for t in ngw.get("Tags", [])}
                    found.append({
                        "cloud": "aws", "region": region, "type": "natgateway",
                        "id":    ngw["NatGatewayId"],
                        "name":  tags.get("Name", ngw["NatGatewayId"]),
                        "vpc_id":    ngw.get("VpcId", ""),
                        "subnet_id": ngw.get("SubnetId", ""),
                    })
        except Exception as exc:
            logger.error(f"[AWS/{region}] NAT Gateway: {exc}")
        return found

    # ═══════════════════════════════════════════════════════════════════════════
    # METRICS COLLECTION
    # ═══════════════════════════════════════════════════════════════════════════

    def collect_metrics(self, resources: List[Dict]) -> List[MetricPoint]:
        end   = datetime.now(timezone.utc)
        start = end - timedelta(minutes=self._lookback_min)
        metrics: List[MetricPoint] = []

        # Group by region for parallel CloudWatch collection
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
                    logger.error(f"[AWS] CW metrics error: {exc}")

        logger.info(f"[AWS] CloudWatch: {len(metrics)} metric points")

        # Node Exporter via SSM for EC2 (memory, disk, load)
        if self._node_exporter_on:
            ne = self._collect_node_exporter_via_ssm(resources)
            metrics.extend(ne)
            logger.info(f"[AWS] Node Exporter: {len(ne)} metric points")

        logger.info(f"[AWS] Total metrics: {len(metrics)}")
        return metrics

    def _collect_region_metrics(
        self,
        region: str,
        resources: List[Dict],
        start: datetime,
        end: datetime,
    ) -> List[MetricPoint]:
        cw          = self._session.client("cloudwatch", region_name=region)
        metrics:    List[MetricPoint] = []
        queries:    List[Dict]        = []
        query_meta: Dict[str, Tuple]  = {}

        _default_period: Dict[str, int] = {
            "ec2": 60, "ebs": 60, "asg": 60,
            "lambda": 60, "elb": 60, "apigateway": 60,
            "cloudfront": 300, "sqs": 60, "sns": 60, "kinesis": 60,
            "ecs": 60, "eks": 60, "natgateway": 60,
            "rds": 300, "dynamodb": 300, "elasticache": 300,
            "s3": 86400,
        }

        for res in resources:
            rtype   = res["type"]
            cfg     = self._metric_defs.get(rtype, {})
            defs    = cfg.get("definitions", [])
            dim_key = cfg.get("dimension_key", "InstanceId")
            ns      = cfg.get("namespace", "AWS/EC2")
            if not defs:
                continue

            dim_val        = self._cw_dim_value(res)
            default_period = _default_period.get(rtype, 60)

            for mdef in defs:
                qid    = f"q{len(queries)}"
                period = mdef.get("period", default_period)
                cw_region = mdef.get("region_override", region)

                dims = [{"Name": dim_key, "Value": dim_val}]
                dims.extend(mdef.get("extra_dims", []))

                queries.append({
                    "Id": qid,
                    "MetricStat": {
                        "Metric": {
                            "Namespace":  ns,
                            "MetricName": mdef["cw_name"],
                            "Dimensions": dims,
                        },
                        "Period": period,
                        "Stat":   mdef["stat"],
                    },
                    "ReturnData": True,
                })
                query_meta[qid] = (res, mdef, period)

        # DynamoDB per-operation latency
        ddb_resources = [r for r in resources if r["type"] == "dynamodb"]
        if ddb_resources:
            ddb_q = self._build_dynamodb_latency_queries(ddb_resources, query_meta)
            queries.extend(ddb_q)

        logger.info(f"[AWS/{region}] Total CW queries: {len(queries)}")

        for batch_start in range(0, len(queries), 100):
            batch_q = queries[batch_start: batch_start + 100]
            if not batch_q:
                continue

            batch_max_period = max(q["MetricStat"]["Period"] for q in batch_q)
            min_window_s     = max(batch_max_period * 2, self._lookback_min * 60)
            effective_start  = end - timedelta(seconds=min_window_s)

            try:
                resp = self._retry(
                    cw.get_metric_data,
                    MetricDataQueries=batch_q,
                    StartTime=effective_start,
                    EndTime=end,
                )
            except Exception as exc:
                logger.warning(f"[AWS/{region}] GetMetricData batch failed: {exc}")
                continue

            all_results = list(resp.get("MetricDataResults", []))
            while resp.get("NextToken"):
                try:
                    resp = self._retry(
                        cw.get_metric_data,
                        MetricDataQueries=batch_q,
                        StartTime=effective_start,
                        EndTime=end,
                        NextToken=resp["NextToken"],
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
                    if rid not in query_meta or not vals:
                        continue
                    res, mdef, _period = query_meta[rid]
                    agg_value = self._aggregate(vals, mdef["stat"])
                    ts        = max(tsts)
                    try:
                        ts_str = self._ensure_iso_timestamp(ts)
                    except Exception:
                        ts_str = datetime.now(timezone.utc).isoformat()

                    metrics.append(MetricPoint(
                        timestamp=ts_str,
                        cloud="aws", region=region,
                        resource_type=res["type"],
                        resource_id=res["id"], resource_name=res["name"],
                        metric_name=mdef["name"],
                        metric_value=round(float(agg_value), 6),
                        metric_unit=mdef["unit"],
                        labels={k: str(v) for k, v in res.items()
                                if k not in ("cloud", "region", "type", "id", "name", "tags")},
                    ))
                except Exception as exc:
                    logger.warning(f"[AWS/{region}] Result parse error: {exc}")

        return metrics

    # ── DynamoDB per-operation latency queries ────────────────────────────────
    def _build_dynamodb_latency_queries(
        self,
        resources: List[Dict],
        query_meta: Dict[str, Tuple],
        period: int = 300,
    ) -> List[Dict]:
        queries = []
        for res in resources:
            table_name = res["name"]
            for op in _DYNAMODB_LATENCY_OPS:
                qid = f"q{len(query_meta) + len(queries)}"
                queries.append({
                    "Id": qid,
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/DynamoDB",
                            "MetricName": "SuccessfulRequestLatency",
                            "Dimensions": [
                                {"Name": "TableName", "Value": table_name},
                                {"Name": "Operation", "Value": op},
                            ],
                        },
                        "Period": period,
                        "Stat":   "Average",
                    },
                    "ReturnData": True,
                })
                query_meta[qid] = (res, {
                    "name": f"request_latency_{op.lower()}_ms",
                    "cw_name": "SuccessfulRequestLatency",
                    "stat":    "Average",
                    "unit":    "ms",
                }, period)
        return queries

    # ═══════════════════════════════════════════════════════════════════════════
    # NODE EXPORTER VIA SSM (EC2 memory / disk / load)
    # ═══════════════════════════════════════════════════════════════════════════

    def _collect_node_exporter_via_ssm(self, resources: List[Dict]) -> List[MetricPoint]:
        ec2_resources = [r for r in resources if r["type"] == "ec2"]
        if not ec2_resources:
            return []
        metrics: List[MetricPoint] = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(ec2_resources), 8)) as exe:
            futs = {exe.submit(self._ssm_collect_single, res): res for res in ec2_resources}
            for f in concurrent.futures.as_completed(futs):
                res = futs[f]
                try:
                    result = f.result()
                    metrics.extend(result)
                except Exception as exc:
                    logger.warning(f"[AWS/SSM] {res['name']} ({res['id']}): {exc}")
        return metrics

    def _is_ssm_ready(self, ssm, instance_id: str) -> bool:
        try:
            resp = ssm.describe_instance_information(
                Filters=[{"Key": "InstanceIds", "Values": [instance_id]}]
            )
            info_list = resp.get("InstanceInformationList", [])
            if not info_list:
                logger.warning(f"[AWS/SSM] {instance_id}: not in SSM fleet")
                return False
            status = info_list[0].get("PingStatus", "")
            if status != "Online":
                logger.warning(f"[AWS/SSM] {instance_id}: PingStatus={status!r}")
                return False
            return True
        except self._ClientError:
            return False

    def _ssm_collect_single(self, res: Dict) -> List[MetricPoint]:
        instance_id = res["id"]
        region      = res["region"]
        ssm         = self._session.client("ssm", region_name=region)

        if not self._is_ssm_ready(ssm, instance_id):
            return []
        try:
            resp = self._retry(
                ssm.send_command,
                InstanceIds=[instance_id],
                DocumentName="AWS-RunShellScript",
                Parameters={"commands": [_SSM_INSTALL_AND_SCRAPE]},
                Comment=f"node_exporter {instance_id}",
                TimeoutSeconds=self._ssm_timeout,
            )
            command_id  = resp["Command"]["CommandId"]
            raw_output  = self._wait_ssm_command(ssm, command_id, instance_id)
            metrics_txt = self._extract_metrics_section(raw_output)
            if not metrics_txt:
                logger.warning(f"[AWS/SSM] {instance_id}: no METRICS_START marker")
                return []
            return self._parse_node_exporter_output(metrics_txt, res)
        except self._ClientError as exc:
            code = exc.response["Error"]["Code"]
            logger.warning(f"[AWS/SSM] {instance_id}: ClientError [{code}]")
            return []
        except (TimeoutError, RuntimeError) as exc:
            logger.warning(f"[AWS/SSM] {instance_id}: {exc}")
            return []

    def _wait_ssm_command(self, ssm, command_id: str, instance_id: str) -> str:
        poll_interval = 5
        waited        = 0
        while waited < self._ssm_timeout:
            time.sleep(poll_interval)
            waited += poll_interval
            try:
                result = ssm.get_command_invocation(
                    CommandId=command_id, InstanceId=instance_id,
                )
            except self._ClientError as exc:
                if exc.response["Error"]["Code"] == "InvocationDoesNotExist":
                    continue
                raise
            status = result["Status"]
            if status == "Success":
                return result.get("StandardOutputContent", "")
            if status in ("Failed", "Cancelled", "TimedOut", "DeliveryTimedOut",
                          "ExecutionTimedOut", "Undeliverable", "Terminated"):
                raise RuntimeError(f"SSM command {status}")
        raise TimeoutError(f"SSM command {command_id} timed out after {self._ssm_timeout}s")

    @staticmethod
    def _extract_metrics_section(raw: str) -> str:
        start_marker = "---METRICS_START---"
        end_marker   = "---METRICS_END---"
        s = raw.find(start_marker)
        e = raw.find(end_marker)
        if s == -1 or e == -1:
            return ""
        return raw[s + len(start_marker): e].strip()

    def _parse_node_exporter_output(self, raw: str, res: Dict) -> List[MetricPoint]:
        metrics:  List[MetricPoint] = []
        ts_str    = datetime.now(timezone.utc).isoformat()
        raw_vals: Dict[str, float] = {}
        _skip_devices = ("loop", "ram", "dm-", "sr")

        for line in raw.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            for prom_name, (metric_name, unit, label_filter) in _NODE_EXPORTER_METRICS.items():
                if not line.startswith(prom_name):
                    continue
                if label_filter and label_filter not in line:
                    continue
                if prom_name.startswith("node_disk_"):
                    m = re.search(r'device="([^"]+)"', line)
                    if m and any(m.group(1).startswith(s) for s in _skip_devices):
                        continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                value_str = parts[-1]
                if len(parts) >= 3:
                    try:
                        if float(parts[-1]) > 1e9:
                            value_str = parts[-2]
                    except ValueError:
                        pass
                try:
                    value = float(value_str)
                except ValueError:
                    continue
                if metric_name in raw_vals and prom_name.startswith("node_disk_"):
                    continue
                metrics.append(MetricPoint(
                    timestamp=ts_str, cloud="aws", region=res["region"],
                    resource_type="ec2", resource_id=res["id"], resource_name=res["name"],
                    metric_name=metric_name, metric_value=round(value, 6), metric_unit=unit,
                    labels={"source": "node_exporter"},
                ))
                raw_vals[metric_name] = value
                break

        # Derived: memory_used_percent
        if "memory_total_bytes" in raw_vals and "memory_available_bytes" in raw_vals:
            total = raw_vals["memory_total_bytes"]
            avail = raw_vals["memory_available_bytes"]
            if total > 0:
                pct = round((total - avail) / total * 100, 2)
                metrics.append(MetricPoint(
                    timestamp=ts_str, cloud="aws", region=res["region"],
                    resource_type="ec2", resource_id=res["id"], resource_name=res["name"],
                    metric_name="memory_used_percent", metric_value=pct, metric_unit="percent",
                    labels={"source": "node_exporter"},
                ))

        # Derived: disk_used_percent
        if "disk_total_bytes" in raw_vals and "disk_available_bytes" in raw_vals:
            total = raw_vals["disk_total_bytes"]
            avail = raw_vals["disk_available_bytes"]
            if total > 0:
                pct = round((total - avail) / total * 100, 2)
                metrics.append(MetricPoint(
                    timestamp=ts_str, cloud="aws", region=res["region"],
                    resource_type="ec2", resource_id=res["id"], resource_name=res["name"],
                    metric_name="disk_used_percent", metric_value=pct, metric_unit="percent",
                    labels={"source": "node_exporter"},
                ))

        return metrics

    # ═══════════════════════════════════════════════════════════════════════════
    # LOGS
    # ═══════════════════════════════════════════════════════════════════════════

    def collect_logs(self, resources: List[Dict]) -> List[LogEntry]:
        logs: List[LogEntry] = []
        end_ms   = int(time.time() * 1000)
        start_ms = end_ms - self._logs_lookback * 60 * 1000

        by_region: Dict[str, List[Dict]] = {}
        for r in resources:
            by_region.setdefault(r["region"], []).append(r)

        for region in by_region:
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
                    elif "/aws/eks/" in lg_name:
                        res_type = "eks"
                    elif "/aws/elasticache/" in lg_name:
                        res_type = "elasticache"
                    elif "/aws/kinesis/" in lg_name:
                        res_type = "kinesis"
                    elif "CloudTrail" in lg_name:
                        res_type = "cloudtrail"

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
                            ts_ms = int(ev["timestamp"])
                            logs.append(LogEntry(
                                timestamp=datetime.fromtimestamp(
                                    ts_ms / 1000, tz=timezone.utc
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

    # ═══════════════════════════════════════════════════════════════════════════
    # HEALTH SCORE ROLLUP
    # ═══════════════════════════════════════════════════════════════════════════

    def compute_health_scores(
        self,
        resources: List[Dict],
        metrics: List[MetricPoint],
    ) -> List[HealthScore]:
        """
        Aggregate raw metrics into a single health score per resource.
        Score: 1.0 = fully healthy, 0.0 = critical.
        Status: "healthy" ≥ 0.7, "degraded" ≥ 0.4, "critical" < 0.4
        """
        # Index metrics by (resource_id, metric_name) → latest value
        latest: Dict[Tuple[str, str], float] = {}
        for mp in metrics:
            key = (mp.resource_id, mp.metric_name)
            latest[key] = mp.metric_value   # last-write wins; collector already sorted by time

        ts_str = datetime.now(timezone.utc).isoformat()
        scores: List[HealthScore] = []

        for res in resources:
            rid   = res["id"]
            rtype = res["type"]

            # Collect all metric values for this resource
            resource_metrics = {
                k[1]: v for k, v in latest.items() if k[0] == rid
            }
            if not resource_metrics:
                continue

            # Compute per-metric penalty scores (0 = critical, 1 = ok)
            penalties: List[float] = []
            signals: Dict[str, Any] = {}

            for mname, (warn_thresh, crit_thresh) in _HEALTH_THRESHOLDS.items():
                if mname not in resource_metrics:
                    continue
                val = resource_metrics[mname]
                signals[mname] = val

                # Inverted metrics: lower = worse (free storage, cache hit rate)
                if mname in ("free_storage_bytes", "freeable_memory_bytes",
                             "cache_hit_rate", "elasticache_cache_hits"):
                    if val <= crit_thresh:
                        penalties.append(0.0)
                    elif val <= warn_thresh:
                        penalties.append(0.5)
                    else:
                        penalties.append(1.0)
                else:
                    # Normal: higher = worse
                    if val >= crit_thresh:
                        penalties.append(0.0)
                    elif val >= warn_thresh:
                        penalties.append(0.5)
                    else:
                        penalties.append(1.0)

            # If no known thresholds matched, assume healthy
            if not penalties:
                score = 1.0
            else:
                # Weighted average — critical signals weigh more
                score = sum(penalties) / len(penalties)
                # Hard-zero: any single critical metric tanks the score
                if 0.0 in penalties:
                    score = min(score, 0.3)

            score  = round(max(0.0, min(1.0, score)), 4)
            status = "healthy" if score >= 0.7 else ("degraded" if score >= 0.4 else "critical")

            scores.append(HealthScore(
                timestamp=ts_str,
                cloud="aws", region=res["region"],
                resource_type=rtype,
                resource_id=rid, resource_name=res["name"],
                score=score, status=status,
                signals=signals,
                labels={k: str(v) for k, v in res.items()
                        if k not in ("cloud", "region", "type", "id", "name", "tags")},
            ))

        healthy  = sum(1 for s in scores if s.status == "healthy")
        degraded = sum(1 for s in scores if s.status == "degraded")
        critical = sum(1 for s in scores if s.status == "critical")
        logger.info(
            f"[AWS] Health scores: {len(scores)} resources — "
            f"healthy={healthy} degraded={degraded} critical={critical}"
        )
        return scores

    # ═══════════════════════════════════════════════════════════════════════════
    # HELPERS
    # ═══════════════════════════════════════════════════════════════════════════

    @staticmethod
    def _elb_arn_to_dim(arn: str) -> str:
        marker = "/loadbalancer/"
        idx    = arn.find(marker)
        if idx != -1:
            return arn[idx + len(marker):]
        return arn

    def _cw_dim_value(self, resource: Dict) -> str:
        rtype = resource["type"]
        if rtype == "elb":
            return resource.get("id", "")
        if rtype in ("lambda", "rds", "dynamodb", "sqs", "sns",
                     "kinesis", "ecs", "eks", "apigateway", "natgateway"):
            return resource.get("name", resource.get("id", ""))
        if rtype == "cloudfront":
            return resource.get("id", "")
        if rtype in ("ebs",):
            return resource.get("id", "")
        return resource.get("id", "")

    @staticmethod
    def _aggregate(values: List[float], stat: str) -> float:
        if not values:
            return 0.0
        if stat == "Sum":
            return sum(values)
        if stat == "Maximum":
            return max(values)
        if stat == "Minimum":
            return min(values)
        return sum(values) / len(values)

    @staticmethod
    def _ensure_iso_timestamp(ts: Any) -> str:
        if hasattr(ts, "isoformat") and callable(ts.isoformat):
            if getattr(ts, "tzinfo", None) is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return ts.isoformat().replace("Z", "+00:00")
        if isinstance(ts, str):
            ts_clean = ts.strip()
            if "T" not in ts_clean:
                raise ValueError(f"No T separator: {ts_clean!r}")
            if ts_clean.endswith("Z"):
                ts_clean = ts_clean[:-1] + "+00:00"
            if "+" not in ts_clean and (len(ts_clean) < 6 or ts_clean[-6] != "-"):
                ts_clean += "+00:00"
            return ts_clean
        if isinstance(ts, (int, float)):
            ts_s = float(ts)
            if ts_s > 1e12:
                ts_s /= 1000.0
            return datetime.fromtimestamp(ts_s, tz=timezone.utc).isoformat()
        raise ValueError(f"Cannot parse timestamp: {ts!r}")