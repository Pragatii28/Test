"""
example_usage.py
────────────────
Example: how to run the unified metrics collector.
"""
import json
import logging
import os

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(name)-25s  %(levelname)-8s  %(message)s",
)
from collectors.aws.ssm_auto_attach import ensure_ssm_role_for_all_instances
from unified_collector import UnifiedCollector

# ── Config ────────────────────────────────────────────────────────────────────
config = {
    "aws": {
        # Credentials — leave empty to use IAM role / env vars
        "access_key_id":     os.getenv("AWS_ACCESS_KEY_ID", ""),
        "secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
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
# 🔥 NEW STEP (before collection)
ensure_ssm_role_for_all_instances()
# ── Run ───────────────────────────────────────────────────────────────────────
collector = UnifiedCollector(config)

# Option A: Full collection (metrics + logs + health scores)
result = collector.run()

# Option B: Metrics only (faster)
# metrics = collector.run_metrics_only()

# Option C: Health check only (for remediation engine)
# scores = collector.run_health_check()

# ── Output summary ────────────────────────────────────────────────────────────
summary = result.summary
print(json.dumps(summary, indent=2, default=str))

# ── Example: feed to remediation engine ──────────────────────────────────────
for hs in result.health_scores:
    if hs.status == "critical":
        print(f"\n🔴 CRITICAL: {hs.resource_type}/{hs.resource_name}")
        print(f"   Score:   {hs.score}")
        print(f"   Signals: {hs.signals}")
        # → trigger_remediation(hs)

    elif hs.status == "degraded":
        print(f"\n🟡 DEGRADED: {hs.resource_type}/{hs.resource_name}")
        print(f"   Score:   {hs.score}")
        print(f"   Signals: {hs.signals}")
        # → alert_on_call(hs)
