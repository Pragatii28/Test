"""
remediation_executor.py  —  Phase 5: Automated Remediation Executor
=====================================================================

Accepts a Decision (from Phase 4) and dispatches to the correct
playbook handler.  Every handler returns a RemediationResult that
Phase 6 (Validation) will consume.

Design principles
-----------------
* DRY-RUN by default  — set REMEDIATION_DRY_RUN=false in production.
* Per-action timeouts — a hanging boto3 call never blocks the pipeline.
* Isolated failure    — one bad action raises RemediationError; the
                        caller catches it; the pipeline continues.
* Full audit trail    — every execution written to `remediation_log`
                        table in the existing observability SQLite / PG DB.
* IAM-role aware      — uses boto3 session + optional role assumption;
                        credentials never live in this file.

Credential / IAM strategy
--------------------------
Option A (recommended for EC2/ECS/Lambda)  — Instance Profile / Pod Role
    The machine running this code carries an IAM Role.
    boto3 picks it up automatically via the metadata endpoint.
    No keys, no secrets manager calls needed.

Option B (cross-account or local dev)  — STS AssumeRole
    Set REMEDIATION_ASSUME_ROLE_ARN=arn:aws:iam::123:role/AutoHealRole
    The executor calls sts.assume_role() and creates a scoped session.

Option C (CI / local testing without real AWS)
    Set AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY environment variables.
    These are picked up by boto3 automatically.

Minimum IAM permissions required by the Role
---------------------------------------------
    autoscaling:DescribeAutoScalingGroups
    autoscaling:SetDesiredCapacity
    autoscaling:UpdateAutoScalingGroup
    ec2:RebootInstances
    ec2:DescribeInstances
    lambda:GetFunctionConfiguration
    lambda:PutFunctionConcurrency
    lambda:UpdateFunctionConfiguration
    rds:DescribeDBInstances
    rds:ModifyDBInstance
    rds:RebootDBInstance
    rds:FailoverDBCluster
    ecs:UpdateService
    ecs:DescribeServices
    dynamodb:DescribeTable
    dynamodb:UpdateTable
    ssm:SendCommand
    ssm:GetCommandInvocation
    route53:ChangeResourceRecordSets
    route53:ListResourceRecordSets
    sts:AssumeRole  (if using Option B)

Usage
-----
    from remediation_executor import RemediationExecutor
    executor = RemediationExecutor(config_path="config/cloud_observability.yaml")
    result = executor.execute_from_decision(playbook_id, parameters)
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
import traceback
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional

import yaml

# ── Optional imports ──────────────────────────────────────────────────────────
try:
    import boto3
    from botocore.exceptions import ClientError, BotoCoreError
    BOTO3_AVAILABLE = True
except ImportError:
    BOTO3_AVAILABLE = False

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [remediation] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/remediation_executor.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("remediation")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration — all from environment, never hardcoded
# ─────────────────────────────────────────────────────────────────────────────
DRY_RUN             = os.getenv("REMEDIATION_DRY_RUN", "true").lower() != "false"
DEFAULT_TIMEOUT_S   = int(os.getenv("REMEDIATION_TIMEOUT_SECONDS", "120"))
ASSUME_ROLE_ARN     = os.getenv("REMEDIATION_ASSUME_ROLE_ARN", "")
ASSUME_ROLE_SESSION = os.getenv("REMEDIATION_ASSUME_ROLE_SESSION", "AIOps-AutoHeal")
SSM_POLL_INTERVAL   = int(os.getenv("REMEDIATION_SSM_POLL_INTERVAL", "5"))


# ─────────────────────────────────────────────────────────────────────────────
# Custom exception
# ─────────────────────────────────────────────────────────────────────────────
class RemediationError(Exception):
    """Raised when a remediation handler fails in a known, non-retryable way."""


# ─────────────────────────────────────────────────────────────────────────────
# Result dataclass  (consumed by Phase 6 Validation)
# ─────────────────────────────────────────────────────────────────────────────
@dataclass
class RemediationResult:
    """
    Structured output of every remediation attempt.
    Phase 6 (Validation) reads status, validation_metric, and
    validation_window_minutes to know what health-check to run and when.
    """
    execution_id:   str
    decision_id:    str
    playbook_id:    str
    resource_id:    str
    resource_name:  str
    cloud:          str
    region:         str

    # Outcome
    status:         str         # "success" | "dry_run" | "failed" | "timeout" | "skipped"
    started_at:     str
    finished_at:    str
    elapsed_seconds: float

    # Detail
    action_taken:   str         # Human-readable description of what was done
    output_log:     List[str]   # Ordered log lines from the handler
    error_message:  str         # Non-empty only when status == "failed" | "timeout"

    # Phase 6 hints — tells the validator what to check and when
    validation_metric:          str
    validation_window_minutes:  int

    # Raw cloud-provider response (serialised to JSON in DB)
    raw_response:   Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["output_log"]   = json.dumps(d["output_log"])
        d["raw_response"] = json.dumps(d["raw_response"])
        return d

    def is_success(self) -> bool:
        return self.status in ("success", "dry_run")

    def summary_line(self) -> str:
        return (
            f"[{self.status.upper()}] {self.playbook_id} → {self.resource_name} "
            f"({self.elapsed_seconds:.1f}s) | {self.action_taken}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# DB persistence  (audit trail + Phase 6 lookup)
# ─────────────────────────────────────────────────────────────────────────────
_DDL_REMEDIATION_LOG = """
CREATE TABLE IF NOT EXISTS remediation_log (
    id                          INTEGER PRIMARY KEY AUTOINCREMENT,
    execution_id                TEXT NOT NULL UNIQUE,
    decision_id                 TEXT NOT NULL,
    playbook_id                 TEXT NOT NULL,
    resource_id                 TEXT NOT NULL,
    resource_name               TEXT NOT NULL,
    cloud                       TEXT NOT NULL,
    region                      TEXT NOT NULL,
    status                      TEXT NOT NULL,
    started_at                  TEXT NOT NULL,
    finished_at                 TEXT NOT NULL,
    elapsed_seconds             REAL NOT NULL,
    action_taken                TEXT NOT NULL,
    output_log                  TEXT NOT NULL,
    error_message               TEXT NOT NULL,
    validation_metric           TEXT NOT NULL,
    validation_window_minutes   INTEGER NOT NULL,
    raw_response                TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_rem_decision  ON remediation_log(decision_id);
CREATE INDEX IF NOT EXISTS idx_rem_resource  ON remediation_log(resource_id);
CREATE INDEX IF NOT EXISTS idx_rem_started   ON remediation_log(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_rem_status    ON remediation_log(status);
"""


class RemediationStore:
    """Writes RemediationResult rows to the observability database."""

    def __init__(self, config_path: str) -> None:
        self._lock = threading.Lock()
        self._conn, self._backend = self._connect(config_path)
        self._migrate()

    def _connect(self, config_path: str):
        try:
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            storage = cfg.get("storage", {})
            backend = storage.get("backend", "sqlite").lower()
        except Exception:
            backend, storage = "sqlite", {}

        if backend == "sqlite":
            db_path = storage.get("sqlite", {}).get("path", "observability_data/metrics.db")
            os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
            conn = sqlite3.connect(db_path, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            return conn, "sqlite"
        else:
            import psycopg2
            pg = storage.get("postgres", {})
            dsn = (
                pg.get("dsn")
                or os.getenv("DATABASE_URL")
                or (
                    f"postgresql://{pg.get('user','postgres')}:{pg.get('password','')}@"
                    f"{pg.get('host','localhost')}:{pg.get('port',5432)}"
                    f"/{pg.get('dbname','observability')}"
                )
            )
            return psycopg2.connect(dsn), "postgres"

    def _migrate(self) -> None:
        ddl = _DDL_REMEDIATION_LOG
        if self._backend == "postgres":
            ddl = ddl.replace(
                "INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY"
            )
        if self._backend == "sqlite":
            self._conn.executescript(ddl)
            self._conn.commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute(ddl)
            self._conn.commit()

    def save(self, result: RemediationResult) -> None:
        row  = result.to_dict()
        cols = [c for c in row if c != "id"]
        vals = [row[c] for c in cols]
        ph   = "?" if self._backend == "sqlite" else "%s"
        sql  = (
            f"INSERT OR IGNORE INTO remediation_log ({','.join(cols)}) "
            f"VALUES ({','.join([ph] * len(cols))})"
        )
        if self._backend == "postgres":
            sql = sql.replace("INSERT OR IGNORE", "INSERT").rstrip()
            sql += " ON CONFLICT (execution_id) DO NOTHING"
        with self._lock:
            if self._backend == "sqlite":
                self._conn.execute(sql, vals)
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(sql, vals)
                self._conn.commit()

    def get_recent(self, hours: int = 24) -> List[Dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        ph  = "?" if self._backend == "sqlite" else "%s"
        sql = (
            f"SELECT * FROM remediation_log "
            f"WHERE started_at>={ph} ORDER BY started_at DESC"
        )
        if self._backend == "sqlite":
            self._conn.row_factory = sqlite3.Row
            rows = self._conn.execute(sql, (cutoff,)).fetchall()
            return [dict(r) for r in rows]
        else:
            with self._conn.cursor() as cur:
                cur.execute(sql, (cutoff,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    def get_pending_validation(self) -> List[Dict[str, Any]]:
        """
        Returns successful remediations that have not yet been validated.
        Phase 6 polls this.
        """
        sql = (
            "SELECT * FROM remediation_log "
            "WHERE status='success' "
            "ORDER BY finished_at DESC LIMIT 100"
        )
        if self._backend == "sqlite":
            self._conn.row_factory = sqlite3.Row
            return [dict(r) for r in self._conn.execute(sql).fetchall()]
        else:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Internal helper — timeout wrapper
# ─────────────────────────────────────────────────────────────────────────────
def _run_with_timeout(fn: Callable, timeout_s: int, *args, **kwargs) -> Any:
    """
    Run fn(*args, **kwargs) in a daemon thread.
    Raises TimeoutError if it does not return within timeout_s seconds.
    The underlying cloud API call may still be running — the pipeline is
    unblocked regardless.
    """
    result_box: List[Any]                   = [None]
    error_box:  List[Optional[Exception]]   = [None]

    def _target():
        try:
            result_box[0] = fn(*args, **kwargs)
        except Exception as exc:
            error_box[0] = exc

    t = threading.Thread(target=_target, daemon=True)
    t.start()
    t.join(timeout=timeout_s)

    if t.is_alive():
        raise TimeoutError(
            f"Handler did not return within {timeout_s}s — "
            "the action may still be in progress in AWS."
        )
    if error_box[0] is not None:
        raise error_box[0]
    return result_box[0]


# ─────────────────────────────────────────────────────────────────────────────
# Main executor class
# ─────────────────────────────────────────────────────────────────────────────
class RemediationExecutor:
    """
    Phase 5 entry point.

    Wire into Phase 4 in main.py:

        from remediation_executor import RemediationExecutor
        executor = RemediationExecutor(config_path)
        decision_engine.remediation_executor = executor.execute_from_decision
    """

    # Maps every playbook_id defined in decision_engine.py → handler method
    _HANDLER_MAP: Dict[str, str] = {
        "scale_out_asg":                    "_handle_scale_out_asg",
        "restart_ec2_instance":             "_handle_restart_ec2",
        "restart_ecs_service":              "_handle_restart_ecs_service",
        "restart_lambda":                   "_handle_restart_lambda",
        "increase_lambda_concurrency":      "_handle_increase_lambda_concurrency",
        "expand_rds_storage":               "_handle_expand_rds_storage",
        "trigger_disk_cleanup":             "_handle_disk_cleanup_ssm",
        "enable_connection_pool_reset":     "_handle_connection_pool_reset",
        "raise_dynamodb_capacity":          "_handle_raise_dynamodb_capacity",
        "failover_to_replica":              "_handle_rds_failover",
        "trigger_rollback":                 "_handle_trigger_rollback",
        "enable_route53_health_failover":   "_handle_route53_failover",
        "enable_circuit_breaker":           "_handle_circuit_breaker_flag",
        "activate_cdn_cache":               "_handle_activate_cdn_cache",
        "enable_sqs_dlq_redrive":           "_handle_sqs_dlq_redrive",
        "notify_only":                      "_handle_notify_only",
    }

    def __init__(
        self, config_path: str = "config/cloud_observability.yaml"
    ) -> None:
        self._config_path = config_path
        self._store = RemediationStore(config_path)

        log.info(
            f"[RemediationExecutor] Ready | dry_run={DRY_RUN} | "
            f"timeout={DEFAULT_TIMEOUT_S}s | "
            f"iam={'assume-role:' + ASSUME_ROLE_ARN if ASSUME_ROLE_ARN else 'instance-profile/env'}"
        )

    # ─────────────────────────────────────────────────────────────────────────
    # Public entry point  (matches DecisionEngine.remediation_executor hook)
    # ─────────────────────────────────────────────────────────────────────────
    def execute_from_decision(
        self,
        playbook_id: str,
        parameters:  Dict[str, Any],
    ) -> RemediationResult:
        """
        Called by DecisionEngine._decide_one() for every AUTO_APPROVED decision.

        playbook_id  — matches keys in PLAYBOOKS dict in decision_engine.py
        parameters   — dict merged by _make_decision(); always contains at least:
                       resource_id, resource_name, cloud, region
        """
        execution_id = str(uuid.uuid4())
        started_at   = datetime.now(timezone.utc)
        logs: List[str] = []

        log.info("=" * 65)
        log.info(
            f"[Phase5] EXECUTE | execution={execution_id[:8]} "
            f"| playbook={playbook_id} "
            f"| resource={parameters.get('resource_name', '?')} "
            f"| dry_run={DRY_RUN}"
        )

        # ── Resolve handler ───────────────────────────────────────────────────
        handler_name = self._HANDLER_MAP.get(playbook_id)
        if handler_name is None:
            msg = f"No handler registered for playbook_id='{playbook_id}'"
            log.error(f"[Phase5] {msg}")
            result = self._build_result(
                execution_id, playbook_id, parameters, started_at,
                status="skipped",
                action_taken=msg,
                output_log=[msg],
                error_message=msg,
                validation_metric="",
                validation_window_minutes=0,
            )
            self._persist(result)
            return result

        handler: Callable = getattr(self, handler_name)

        # ── Dry-run fast-path ─────────────────────────────────────────────────
        if DRY_RUN:
            log.info(f"[Phase5] DRY RUN — would execute '{playbook_id}' but not calling AWS")
            result = self._build_result(
                execution_id, playbook_id, parameters, started_at,
                status="dry_run",
                action_taken=f"[DRY RUN] Would execute: {playbook_id}",
                output_log=[
                    f"[DRY RUN] playbook  : {playbook_id}",
                    f"[DRY RUN] parameters: {json.dumps(parameters, indent=2)}",
                    "[DRY RUN] Set REMEDIATION_DRY_RUN=false to enable real execution",
                ],
                error_message="",
                validation_metric=self._default_validation_metric(playbook_id),
                validation_window_minutes=self._default_validation_window(playbook_id),
            )
            self._persist(result)
            log.info(f"[Phase5] {result.summary_line()}")
            return result

        # ── Live execution with timeout + full error isolation ─────────────────
        try:
            raw = _run_with_timeout(handler, DEFAULT_TIMEOUT_S, parameters, logs)

            result = self._build_result(
                execution_id, playbook_id, parameters, started_at,
                status="success",
                action_taken=raw.get("action", playbook_id),
                output_log=logs,
                error_message="",
                validation_metric=raw.get(
                    "validation_metric",
                    self._default_validation_metric(playbook_id),
                ),
                validation_window_minutes=raw.get(
                    "validation_window_minutes",
                    self._default_validation_window(playbook_id),
                ),
                raw_response=raw.get("raw_response", {}),
            )
            log.info(f"[Phase5] ✓ {result.summary_line()}")

        except TimeoutError as exc:
            msg = str(exc)
            log.error(f"[Phase5] TIMEOUT: {msg}")
            result = self._build_result(
                execution_id, playbook_id, parameters, started_at,
                status="timeout",
                action_taken="Action timed out",
                output_log=logs + [f"TIMEOUT: {msg}"],
                error_message=msg,
                validation_metric=self._default_validation_metric(playbook_id),
                validation_window_minutes=self._default_validation_window(playbook_id),
            )

        except RemediationError as exc:
            msg = str(exc)
            log.error(f"[Phase5] REMEDIATION ERROR: {msg}")
            result = self._build_result(
                execution_id, playbook_id, parameters, started_at,
                status="failed",
                action_taken="Action failed (known error)",
                output_log=logs + [f"ERROR: {msg}"],
                error_message=msg,
                validation_metric=self._default_validation_metric(playbook_id),
                validation_window_minutes=self._default_validation_window(playbook_id),
            )

        except Exception as exc:
            msg = f"{type(exc).__name__}: {exc}"
            tb  = traceback.format_exc()
            log.error(f"[Phase5] UNEXPECTED: {msg}\n{tb}")
            result = self._build_result(
                execution_id, playbook_id, parameters, started_at,
                status="failed",
                action_taken="Unexpected error",
                output_log=logs + [f"UNEXPECTED: {msg}", tb],
                error_message=msg,
                validation_metric=self._default_validation_metric(playbook_id),
                validation_window_minutes=self._default_validation_window(playbook_id),
            )

        self._persist(result)
        log.info("=" * 65)
        return result

    # ─────────────────────────────────────────────────────────────────────────
    # IAM / boto3 session (lazy, role-aware)
    # ─────────────────────────────────────────────────────────────────────────
    def _get_session(self, region: str):
        """
        Returns a boto3 Session scoped to the correct region and credentials.

        Credential resolution order (boto3 standard chain):
          1. ASSUME_ROLE_ARN env var  → STS AssumeRole → scoped session
          2. AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY env vars
          3. ~/.aws/credentials (developer workstation)
          4. EC2 Instance Metadata / ECS Task Role / Lambda execution role
        """
        if not BOTO3_AVAILABLE:
            raise RemediationError(
                "boto3 is not installed. Run: pip install boto3"
            )

        if ASSUME_ROLE_ARN:
            # Option B — cross-account or scoped role
            sts = boto3.client("sts", region_name=region)
            try:
                resp  = sts.assume_role(
                    RoleArn=ASSUME_ROLE_ARN,
                    RoleSessionName=ASSUME_ROLE_SESSION,
                    DurationSeconds=900,   # 15 min — sufficient for any single action
                )
                creds = resp["Credentials"]
                return boto3.Session(
                    aws_access_key_id=creds["AccessKeyId"],
                    aws_secret_access_key=creds["SecretAccessKey"],
                    aws_session_token=creds["SessionToken"],
                    region_name=region,
                )
            except (ClientError, BotoCoreError) as exc:
                raise RemediationError(f"STS AssumeRole failed: {exc}") from exc

        # Option A/C — instance profile or env vars
        return boto3.Session(region_name=region)

    def _client(self, service: str, region: str):
        return self._get_session(region).client(service)

    # ─────────────────────────────────────────────────────────────────────────
    # Result builder
    # ─────────────────────────────────────────────────────────────────────────
    def _build_result(
        self,
        execution_id:           str,
        playbook_id:            str,
        params:                 Dict[str, Any],
        started_at:             datetime,
        status:                 str,
        action_taken:           str,
        output_log:             List[str],
        error_message:          str,
        validation_metric:      str,
        validation_window_minutes: int,
        raw_response:           Optional[Dict] = None,
    ) -> RemediationResult:
        finished_at = datetime.now(timezone.utc)
        return RemediationResult(
            execution_id=execution_id,
            decision_id=params.get("decision_id", ""),
            playbook_id=playbook_id,
            resource_id=params.get("resource_id", ""),
            resource_name=params.get("resource_name", ""),
            cloud=params.get("cloud", "aws"),
            region=params.get("region", "us-east-1"),
            status=status,
            started_at=started_at.isoformat(),
            finished_at=finished_at.isoformat(),
            elapsed_seconds=round(
                (finished_at - started_at).total_seconds(), 2
            ),
            action_taken=action_taken,
            output_log=output_log,
            error_message=error_message,
            validation_metric=validation_metric,
            validation_window_minutes=validation_window_minutes,
            raw_response=raw_response or {},
        )

    def _persist(self, result: RemediationResult) -> None:
        try:
            self._store.save(result)
            log.info(
                f"[Phase5] Persisted → execution_id={result.execution_id[:8]} "
                f"status={result.status}"
            )
        except Exception as exc:
            log.error(f"[Phase5] DB persist failed: {exc}")

    # =========================================================================
    # PLAYBOOK HANDLERS
    # =========================================================================
    # Each handler signature: (self, params: Dict, logs: List[str]) -> Dict
    # Append to `logs` for observability.
    # Return dict with keys:
    #   action                    : str   human-readable description
    #   raw_response              : dict  cloud-provider response (for audit)
    #   validation_metric         : str   metric Phase 6 should re-check
    #   validation_window_minutes : int   how many minutes to wait before checking
    # =========================================================================

    # ── 1. Auto Scaling Group — scale out ────────────────────────────────────
    def _handle_scale_out_asg(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")
        delta       = int(params.get("delta_instances", 2))

        asg_client = self._client("autoscaling", region)
        logs.append(f"Resolving ASG for resource_id={resource_id}")
        asg_name = self._resolve_asg_name(asg_client, resource_id, logs)

        resp   = asg_client.describe_auto_scaling_groups(
            AutoScalingGroupNames=[asg_name]
        )
        groups = resp.get("AutoScalingGroups", [])
        if not groups:
            raise RemediationError(f"ASG '{asg_name}' not found in {region}")

        group       = groups[0]
        current     = group["DesiredCapacity"]
        maximum     = group["MaxSize"]
        new_desired = min(current + delta, maximum)

        logs.append(
            f"ASG={asg_name} | current_desired={current} "
            f"max={maximum} → target={new_desired}"
        )

        if new_desired == current:
            logs.append("Already at max capacity — no scale-out performed")
            return {
                "action": (
                    f"Scale-out skipped: '{asg_name}' "
                    f"already at max capacity ({maximum})"
                ),
                "raw_response": {},
                "validation_metric": "cpu_utilization_percent",
                "validation_window_minutes": 5,
            }

        scale_resp = asg_client.set_desired_capacity(
            AutoScalingGroupName=asg_name,
            DesiredCapacity=new_desired,
            HonorCooldown=False,
        )
        http_status = scale_resp["ResponseMetadata"]["HTTPStatusCode"]
        logs.append(
            f"SetDesiredCapacity({new_desired}) → HTTP {http_status}"
        )

        return {
            "action": (
                f"Scaled out ASG '{asg_name}': "
                f"{current} → {new_desired} instances"
            ),
            "raw_response": {
                "asg_name": asg_name,
                "old_desired": current,
                "new_desired": new_desired,
            },
            "validation_metric": "cpu_utilization_percent",
            "validation_window_minutes": 5,
        }

    def _resolve_asg_name(
        self, asg_client, resource_id: str, logs: List[str]
    ) -> str:
        """
        If resource_id looks like an EC2 instance ID (i-xxxx), find its ASG.
        Otherwise treat resource_id as the ASG name directly.
        """
        if not resource_id.startswith("i-"):
            return resource_id

        logs.append(f"Resolving ASG via EC2 instance {resource_id}")
        resp = asg_client.describe_auto_scaling_instances(
            InstanceIds=[resource_id]
        )
        instances = resp.get("AutoScalingInstances", [])
        if not instances:
            raise RemediationError(
                f"Instance {resource_id} is not part of any Auto Scaling Group"
            )
        asg_name = instances[0]["AutoScalingGroupName"]
        logs.append(f"Resolved ASG: {asg_name}")
        return asg_name

    # ── 2. EC2 — reboot instance ──────────────────────────────────────────────
    def _handle_restart_ec2(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")
        reboot_type = params.get("reboot_type", "soft")  # soft | hard

        ec2 = self._client("ec2", region)
        logs.append(
            f"EC2 reboot | instance={resource_id} type={reboot_type}"
        )

        resp  = ec2.describe_instances(InstanceIds=[resource_id])
        resv  = resp.get("Reservations", [])
        if not resv:
            raise RemediationError(
                f"EC2 instance {resource_id} not found in {region}"
            )
        state = resv[0]["Instances"][0]["State"]["Name"]
        logs.append(f"Instance state: {state}")
        if state not in ("running", "stopped"):
            raise RemediationError(
                f"Instance {resource_id} is in state '{state}' — cannot reboot"
            )

        if reboot_type == "hard":
            logs.append("Hard reboot: stopping instance...")
            ec2.stop_instances(InstanceIds=[resource_id])
            waiter = ec2.get_waiter("instance_stopped")
            waiter.wait(
                InstanceIds=[resource_id],
                WaiterConfig={"Delay": 5, "MaxAttempts": 24},
            )
            logs.append("Stopped. Starting...")
            start_resp = ec2.start_instances(InstanceIds=[resource_id])
            new_state  = start_resp["StartingInstances"][0]["CurrentState"]["Name"]
            logs.append(f"New state: {new_state}")
        else:
            reboot_resp = ec2.reboot_instances(InstanceIds=[resource_id])
            logs.append(
                f"Soft reboot sent | HTTP {reboot_resp['ResponseMetadata']['HTTPStatusCode']}"
            )

        return {
            "action": (
                f"{reboot_type.capitalize()} reboot of EC2 {resource_id}"
            ),
            "raw_response": {
                "instance_id": resource_id,
                "reboot_type": reboot_type,
            },
            "validation_metric": "status_check_failed",
            "validation_window_minutes": 5,
        }

    # ── 3. ECS — force new deployment ────────────────────────────────────────
    def _handle_restart_ecs_service(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")
        force_new   = params.get("force_new_deployment", True)

        # resource_id format: "cluster-name/service-name"  or just service ARN
        if "/" in resource_id:
            cluster, service = resource_id.split("/", 1)
        else:
            cluster, service = "default", resource_id

        ecs = self._client("ecs", region)
        logs.append(
            f"ECS force-deploy | cluster={cluster} service={service}"
        )

        resp    = ecs.update_service(
            cluster=cluster,
            service=service,
            forceNewDeployment=force_new,
        )
        svc_arn = resp["service"]["serviceArn"]
        logs.append(f"Deployment triggered: {svc_arn}")

        return {
            "action": (
                f"Forced new ECS deployment: "
                f"service '{service}' on cluster '{cluster}'"
            ),
            "raw_response": {"service_arn": svc_arn},
            "validation_metric": "http_5xx_count",
            "validation_window_minutes": 5,
        }

    # ── 4. Lambda — recycle execution environments ────────────────────────────
    def _handle_restart_lambda(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")
        strategy    = params.get("strategy", "touch_env_var")

        lam = self._client("lambda", region)
        logs.append(
            f"Lambda recycle | fn={resource_id} strategy={strategy}"
        )

        cfg      = lam.get_function_configuration(FunctionName=resource_id)
        env_vars = cfg.get("Environment", {}).get("Variables", {})

        # Bumping a harmless env var forces all warm containers to drain
        env_vars["_AIOPS_RESTART_TS"] = str(int(time.time()))
        resp = lam.update_function_configuration(
            FunctionName=resource_id,
            Environment={"Variables": env_vars},
        )
        logs.append(f"Config updated | LastModified={resp.get('LastModified', '')}")

        return {
            "action": (
                f"Lambda '{resource_id}' recycled "
                f"(env-var bump forces execution environment refresh)"
            ),
            "raw_response": {
                "function_name": resource_id,
                "strategy": strategy,
            },
            "validation_metric": "errors_total",
            "validation_window_minutes": 3,
        }

    # ── 5. Lambda — increase reserved concurrency ─────────────────────────────
    def _handle_increase_lambda_concurrency(
        self, params: Dict, logs: List[str]
    ) -> Dict:
        region       = params.get("region", "us-east-1")
        resource_id  = params.get("resource_id", "")
        scale_factor = float(params.get("scale_factor", 1.25))

        lam = self._client("lambda", region)
        logs.append(
            f"Lambda concurrency | fn={resource_id} scale_factor={scale_factor}"
        )

        cfg     = lam.get_function_concurrency(FunctionName=resource_id)
        current = cfg.get("ReservedConcurrentExecutions", 0)

        if current == 0:
            new_concurrency = 100
            logs.append(
                "No reserved concurrency set — initialising to 100"
            )
        else:
            new_concurrency = int(current * scale_factor)
            logs.append(f"Concurrency: {current} → {new_concurrency}")

        resp = lam.put_function_concurrency(
            FunctionName=resource_id,
            ReservedConcurrentExecutions=new_concurrency,
        )
        logs.append(
            f"Concurrency set to {resp['ReservedConcurrentExecutions']}"
        )

        return {
            "action": (
                f"Lambda '{resource_id}' concurrency: "
                f"{current} → {new_concurrency}"
            ),
            "raw_response": {
                "function": resource_id,
                "old": current,
                "new": new_concurrency,
            },
            "validation_metric": "throttles_total",
            "validation_window_minutes": 3,
        }

    # ── 6. RDS — expand allocated storage ─────────────────────────────────────
    def _handle_expand_rds_storage(self, params: Dict, logs: List[str]) -> Dict:
        region       = params.get("region", "us-east-1")
        resource_id  = params.get("resource_id", "")
        scale_factor = float(params.get("scale_factor", 1.20))

        rds = self._client("rds", region)
        logs.append(
            f"RDS storage expand | db={resource_id} scale_factor={scale_factor}"
        )

        resp      = rds.describe_db_instances(DBInstanceIdentifier=resource_id)
        instances = resp.get("DBInstances", [])
        if not instances:
            raise RemediationError(
                f"RDS instance '{resource_id}' not found in {region}"
            )

        db         = instances[0]
        current_gb = db["AllocatedStorage"]
        new_gb     = max(int(current_gb * scale_factor), current_gb + 10)
        max_gb     = db.get("MaxAllocatedStorage", new_gb + 1000)

        if new_gb > max_gb:
            raise RemediationError(
                f"Requested {new_gb}GB exceeds max_allocated_storage {max_gb}GB"
            )

        logs.append(f"Expanding: {current_gb}GB → {new_gb}GB")
        mod_resp = rds.modify_db_instance(
            DBInstanceIdentifier=resource_id,
            AllocatedStorage=new_gb,
            ApplyImmediately=True,
        )
        logs.append(
            f"Modify status: {mod_resp['DBInstance']['DBInstanceStatus']}"
        )

        return {
            "action": (
                f"RDS '{resource_id}' storage expanded: "
                f"{current_gb}GB → {new_gb}GB"
            ),
            "raw_response": {
                "db_id": resource_id,
                "old_gb": current_gb,
                "new_gb": new_gb,
            },
            "validation_metric": "free_storage_bytes",
            "validation_window_minutes": 10,
        }

    # ── 7. Disk cleanup via SSM Run Command ───────────────────────────────────
    def _handle_disk_cleanup_ssm(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")
        ssm_doc     = params.get("ssm_document", "AWS-RunShellScript")
        commands    = params.get("commands", [
            "find /tmp -type f -atime +1 -delete",
            "find /var/log -name '*.gz' -mtime +7 -delete",
            "journalctl --vacuum-time=3d 2>/dev/null || true",
            "df -h /",
        ])

        ssm = self._client("ssm", region)
        logs.append(
            f"SSM Run Command | instance={resource_id} doc={ssm_doc}"
        )
        logs.append(f"Commands: {commands}")

        send_resp  = ssm.send_command(
            InstanceIds=[resource_id],
            DocumentName=ssm_doc,
            Parameters={"commands": commands},
            TimeoutSeconds=min(DEFAULT_TIMEOUT_S - 10, 110),
            Comment="AIOps automated disk cleanup",
        )
        command_id = send_resp["Command"]["CommandId"]
        logs.append(f"SSM command submitted: {command_id}")

        # Poll for completion (up to timeout)
        inv: Dict = {}
        for attempt in range(
            max(1, int((DEFAULT_TIMEOUT_S - 15) / SSM_POLL_INTERVAL))
        ):
            time.sleep(SSM_POLL_INTERVAL)
            inv    = ssm.get_command_invocation(
                CommandId=command_id, InstanceId=resource_id
            )
            status = inv.get("StatusDetails", "")
            logs.append(f"  SSM poll [{attempt + 1}]: {status}")
            if status in (
                "Success", "Failed", "TimedOut", "Cancelled", "Undeliverable"
            ):
                break

        stdout = inv.get("StandardOutputContent", "").strip()
        stderr = inv.get("StandardErrorContent", "").strip()
        if stdout:
            logs.append(f"STDOUT:\n{stdout}")
        if stderr:
            logs.append(f"STDERR:\n{stderr}")

        final = inv.get("StatusDetails", "Unknown")
        if final != "Success":
            raise RemediationError(
                f"SSM command {command_id} ended with '{final}'. "
                f"STDERR: {stderr[:400]}"
            )

        return {
            "action": (
                f"Disk cleanup via SSM on {resource_id} — status: {final}"
            ),
            "raw_response": {
                "command_id": command_id,
                "status": final,
            },
            "validation_metric": "free_storage_bytes",
            "validation_window_minutes": 5,
        }

    # ── 8. RDS — reset connection pool (reboot) ───────────────────────────────
    def _handle_connection_pool_reset(
        self, params: Dict, logs: List[str]
    ) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")

        rds = self._client("rds", region)
        logs.append(f"Rebooting RDS to reset connections: {resource_id}")

        resp   = rds.reboot_db_instance(DBInstanceIdentifier=resource_id)
        status = resp["DBInstance"]["DBInstanceStatus"]
        logs.append(f"Reboot initiated — status: {status}")

        return {
            "action": (
                f"RDS '{resource_id}' rebooted to drain connection pool"
            ),
            "raw_response": {"db_id": resource_id, "status": status},
            "validation_metric": "database_connections",
            "validation_window_minutes": 5,
        }

    # ── 9. DynamoDB — raise read/write capacity ───────────────────────────────
    def _handle_raise_dynamodb_capacity(
        self, params: Dict, logs: List[str]
    ) -> Dict:
        region       = params.get("region", "us-east-1")
        resource_id  = params.get("resource_id", "")
        scale_factor = float(params.get("scale_factor", 2.0))

        ddb = self._client("dynamodb", region)
        logs.append(
            f"DynamoDB capacity | table={resource_id} scale={scale_factor}"
        )

        desc    = ddb.describe_table(TableName=resource_id)["Table"]
        billing = (
            desc.get("BillingModeSummary", {})
            .get("BillingMode", "PROVISIONED")
        )

        if billing == "PAY_PER_REQUEST":
            logs.append("Table is on-demand — no capacity adjustment needed")
            return {
                "action": (
                    f"DynamoDB '{resource_id}' is on-demand mode "
                    "— no capacity change required"
                ),
                "raw_response": {},
                "validation_metric": "throttles_total",
                "validation_window_minutes": 3,
            }

        tp      = desc["ProvisionedThroughput"]
        cur_rcu = tp["ReadCapacityUnits"]
        cur_wcu = tp["WriteCapacityUnits"]
        new_rcu = int(cur_rcu * scale_factor)
        new_wcu = int(cur_wcu * scale_factor)
        logs.append(
            f"RCU: {cur_rcu} → {new_rcu} | WCU: {cur_wcu} → {new_wcu}"
        )

        ddb.update_table(
            TableName=resource_id,
            ProvisionedThroughput={
                "ReadCapacityUnits": new_rcu,
                "WriteCapacityUnits": new_wcu,
            },
        )
        logs.append("Table update submitted")

        return {
            "action": (
                f"DynamoDB '{resource_id}': "
                f"RCU {cur_rcu}→{new_rcu}, WCU {cur_wcu}→{new_wcu}"
            ),
            "raw_response": {
                "table": resource_id,
                "rcu": new_rcu,
                "wcu": new_wcu,
            },
            "validation_metric": "throttles_total",
            "validation_window_minutes": 3,
        }

    # ── 10. RDS — Multi-AZ cluster failover ───────────────────────────────────
    def _handle_rds_failover(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")

        rds = self._client("rds", region)
        logs.append(f"RDS Multi-AZ failover: cluster={resource_id}")

        resp   = rds.failover_db_cluster(DBClusterIdentifier=resource_id)
        status = resp["DBCluster"]["Status"]
        logs.append(f"Failover initiated — cluster status: {status}")

        return {
            "action": (
                f"RDS cluster '{resource_id}' failover initiated "
                f"(status: {status})"
            ),
            "raw_response": {"cluster_id": resource_id, "status": status},
            "validation_metric": "database_connections",
            "validation_window_minutes": 8,
        }

    # ── 11. Lambda — version rollback via alias pointer ───────────────────────
    def _handle_trigger_rollback(self, params: Dict, logs: List[str]) -> Dict:
        region      = params.get("region", "us-east-1")
        resource_id = params.get("resource_id", "")

        lam = self._client("lambda", region)
        logs.append(f"Lambda rollback | fn={resource_id}")

        versions_resp = lam.list_versions_by_function(
            FunctionName=resource_id
        )
        versions = sorted(
            [
                v
                for v in versions_resp.get("Versions", [])
                if v["Version"] != "$LATEST"
            ],
            key=lambda v: v["LastModified"],
            reverse=True,
        )

        if len(versions) < 2:
            raise RemediationError(
                f"Function '{resource_id}' has < 2 published versions "
                "— cannot roll back"
            )

        previous = versions[1]["Version"]
        logs.append(
            f"Latest={versions[0]['Version']} | "
            f"Rolling back LIVE alias to version {previous}"
        )

        resp = lam.update_alias(
            FunctionName=resource_id,
            Name="LIVE",
            FunctionVersion=previous,
        )
        logs.append(f"LIVE alias now points to {resp['FunctionVersion']}")

        return {
            "action": (
                f"Lambda '{resource_id}' LIVE alias rolled back "
                f"to version {previous}"
            ),
            "raw_response": {
                "function": resource_id,
                "version": previous,
            },
            "validation_metric": "errors_total",
            "validation_window_minutes": 5,
        }

    # ── 12. Route53 — health-check-based failover ─────────────────────────────
    def _handle_route53_failover(self, params: Dict, logs: List[str]) -> Dict:
        hosted_zone_id = params.get("hosted_zone_id", "")
        record_name    = params.get("resource_name", "")
        record_type    = params.get("record_type", "A")

        if not hosted_zone_id:
            raise RemediationError(
                "Route53 failover requires 'hosted_zone_id' in playbook parameters"
            )

        r53 = boto3.client("route53")  # global service — no region needed
        logs.append(
            f"Route53 failover | zone={hosted_zone_id} "
            f"record={record_name}/{record_type}"
        )

        resp = r53.list_resource_record_sets(
            HostedZoneId=hosted_zone_id,
            StartRecordName=record_name,
            StartRecordType=record_type,
            MaxItems="10",
        )
        primary_records = [
            rr
            for rr in resp.get("ResourceRecordSets", [])
            if rr.get("Failover") == "PRIMARY"
            and rr.get("Name", "").rstrip(".") == record_name.rstrip(".")
        ]
        if not primary_records:
            raise RemediationError(
                f"No PRIMARY failover record found for "
                f"{record_name}/{record_type} in zone {hosted_zone_id}"
            )

        hc_id = primary_records[0].get("HealthCheckId", "")
        logs.append(f"PRIMARY health check: {hc_id}")

        if hc_id:
            r53.update_health_check(HealthCheckId=hc_id, Disabled=True)
            logs.append(
                f"Health check {hc_id} disabled "
                "→ Route53 will route traffic to SECONDARY"
            )

        return {
            "action": (
                f"Route53 health check '{hc_id}' disabled "
                "— traffic failing over to secondary region"
            ),
            "raw_response": {
                "health_check_id": hc_id,
                "zone_id": hosted_zone_id,
            },
            "validation_metric": "healthy_host_count",
            "validation_window_minutes": 5,
        }

    # ── 13. Circuit breaker via SSM Parameter Store ───────────────────────────
    def _handle_circuit_breaker_flag(
        self, params: Dict, logs: List[str]
    ) -> Dict:
        region     = params.get("region", "us-east-1")
        flag_name  = params.get("flag_name", "circuit_breaker_open")
        param_path = f"/aiops/circuit_breakers/{flag_name}"

        ssm = self._client("ssm", region)
        logs.append(f"Setting circuit breaker: {param_path} = 'true'")

        ssm.put_parameter(
            Name=param_path,
            Value="true",
            Type="String",
            Overwrite=True,
            Description=(
                f"AIOps auto-set at "
                f"{datetime.now(timezone.utc).isoformat()}"
            ),
        )
        logs.append(
            "Parameter written — application must poll SSM to honour this flag"
        )

        return {
            "action": (
                f"Circuit breaker '{flag_name}' opened "
                f"via SSM Parameter Store ({param_path})"
            ),
            "raw_response": {"param_path": param_path, "value": "true"},
            "validation_metric": "http_5xx_count",
            "validation_window_minutes": 3,
        }

    # ── 14. CloudFront — invalidate cache ────────────────────────────────────
    def _handle_activate_cdn_cache(
        self, params: Dict, logs: List[str]
    ) -> Dict:
        distribution_id = params.get("distribution_id", "")

        if not distribution_id:
            raise RemediationError(
                "CDN cache activation requires 'distribution_id' in parameters"
            )

        cf  = boto3.client("cloudfront")  # global service
        ref = str(uuid.uuid4())
        logs.append(
            f"CloudFront invalidation | dist={distribution_id} paths=['/*']"
        )

        resp   = cf.create_invalidation(
            DistributionId=distribution_id,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": ["/*"]},
                "CallerReference": ref,
            },
        )
        inv_id = resp["Invalidation"]["Id"]
        logs.append(f"Invalidation created: {inv_id}")

        return {
            "action": (
                f"CloudFront distribution '{distribution_id}' invalidated "
                "— fresh cache will populate at all edge locations"
            ),
            "raw_response": {
                "distribution_id": distribution_id,
                "invalidation_id": inv_id,
            },
            "validation_metric": "target_response_time_s",
            "validation_window_minutes": 5,
        }

    # ── 15. SQS — redrive dead-letter queue ──────────────────────────────────
    def _handle_sqs_dlq_redrive(self, params: Dict, logs: List[str]) -> Dict:
        region           = params.get("region", "us-east-1")
        dlq_url          = params.get("dlq_url", "")
        source_queue_url = params.get("source_queue_url", "")
        max_mps          = int(params.get("max_messages_per_second", 10))

        if not dlq_url or not source_queue_url:
            raise RemediationError(
                "SQS DLQ redrive requires both 'dlq_url' and "
                "'source_queue_url' in parameters"
            )

        sqs = self._client("sqs", region)
        logs.append(
            f"SQS DLQ redrive | dlq={dlq_url} → source={source_queue_url} "
            f"| rate={max_mps} msg/s"
        )

        resp        = sqs.start_message_move_task(
            SourceArn=dlq_url,
            DestinationArn=source_queue_url,
            MaxNumberOfMessagesPerSecond=max_mps,
        )
        task_handle = resp.get("TaskHandle", "")
        logs.append(f"Move task started: {task_handle}")

        return {
            "action": (
                f"SQS DLQ redrive started at {max_mps} msg/s"
            ),
            "raw_response": {
                "task_handle": task_handle,
                "dlq_url": dlq_url,
            },
            "validation_metric": "errors_total",
            "validation_window_minutes": 10,
        }

    # ── 16. Notify-only ───────────────────────────────────────────────────────
    def _handle_notify_only(self, params: Dict, logs: List[str]) -> Dict:
        import urllib.request

        slack_webhook = os.getenv("SLACK_WEBHOOK_URL", "")
        resource_name = params.get("resource_name", "unknown")
        summary       = params.get(
            "summary", "AIOps alert — requires human review"
        )

        logs.append(f"Notify-only action for resource: {resource_name}")

        if slack_webhook:
            payload = json.dumps({
                "text": (
                    f"🔔 *AIOps Alert — Notify Only*\n"
                    f">Resource: `{resource_name}`\n"
                    f">{summary}\n"
                    f">No automated action taken — human review required."
                )
            }).encode()
            req = urllib.request.Request(
                slack_webhook,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            try:
                urllib.request.urlopen(req, timeout=5)
                logs.append("Slack notification sent")
            except Exception as exc:
                logs.append(f"Slack notification failed: {exc}")
        else:
            logs.append("SLACK_WEBHOOK_URL not set — notification skipped")

        return {
            "action": f"On-call notified for {resource_name}",
            "raw_response": {},
            "validation_metric": "",
            "validation_window_minutes": 0,
        }

    # ─────────────────────────────────────────────────────────────────────────
    # Phase 6 hint tables
    # ─────────────────────────────────────────────────────────────────────────
    _VALIDATION_METRICS: Dict[str, str] = {
        "scale_out_asg":                    "cpu_utilization_percent",
        "restart_ec2_instance":             "status_check_failed",
        "restart_ecs_service":              "http_5xx_count",
        "restart_lambda":                   "errors_total",
        "increase_lambda_concurrency":      "throttles_total",
        "expand_rds_storage":               "free_storage_bytes",
        "trigger_disk_cleanup":             "free_storage_bytes",
        "enable_connection_pool_reset":     "database_connections",
        "raise_dynamodb_capacity":          "throttles_total",
        "failover_to_replica":              "database_connections",
        "trigger_rollback":                 "errors_total",
        "enable_route53_health_failover":   "healthy_host_count",
        "enable_circuit_breaker":           "http_5xx_count",
        "activate_cdn_cache":               "target_response_time_s",
        "enable_sqs_dlq_redrive":           "errors_total",
        "notify_only":                      "",
    }
    _VALIDATION_WINDOWS: Dict[str, int] = {
        "scale_out_asg":                    5,
        "restart_ec2_instance":             5,
        "restart_ecs_service":              5,
        "restart_lambda":                   3,
        "increase_lambda_concurrency":      3,
        "expand_rds_storage":               10,
        "trigger_disk_cleanup":             5,
        "enable_connection_pool_reset":     5,
        "raise_dynamodb_capacity":          3,
        "failover_to_replica":              8,
        "trigger_rollback":                 5,
        "enable_route53_health_failover":   5,
        "enable_circuit_breaker":           3,
        "activate_cdn_cache":               5,
        "enable_sqs_dlq_redrive":           10,
        "notify_only":                      0,
    }

    def _default_validation_metric(self, playbook_id: str) -> str:
        return self._VALIDATION_METRICS.get(playbook_id, "")

    def _default_validation_window(self, playbook_id: str) -> int:
        return self._VALIDATION_WINDOWS.get(playbook_id, 5)
