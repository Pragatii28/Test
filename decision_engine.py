"""
decision_engine.py — Phase 4: Decision Engine

Takes RCAResult objects from root_cause_analysis.py and decides:
  1. WHAT action (playbook) to take
  2. WHETHER to act automatically or require human approval
  3. HOW to record the decision for the Learning Store (Phase 7)

Key design principles:
  - Dry-run by default: DECISION_DRY_RUN=true (set false to enable auto-remediation)
  - Confidence gate: only auto-act if RCA confidence >= DECISION_MIN_CONFIDENCE
  - Cooldown: same resource won't be acted on more than once per DECISION_COOLDOWN_MINUTES
  - Full audit trail: every decision written to decisions table in DB
  - Pluggable playbooks: add your own by subclassing RemediationPlaybook

Integration with Phase 5 (Remediation):
  When remediation.py is implemented, DecisionEngine calls:
      remediation.execute(decision.playbook_id, decision.parameters)
  Until then, decisions are logged + sent to Slack/webhook in dry-run mode.

Usage:
  from decision_engine import DecisionEngine
  engine = DecisionEngine(config_path="config/cloud_observability.yaml")
  decisions = engine.decide(rca_results)           # List[Decision]
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple

import yaml

from root_cause_analysis import RCAResult, RCACategory

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [decision] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/decision_engine.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("decision")

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────

# Set DECISION_DRY_RUN=false in production to enable auto-remediation
DRY_RUN = os.getenv("DECISION_DRY_RUN", "true").lower() != "false"

# Only auto-act when RCA confidence meets or exceeds this threshold
MIN_CONFIDENCE = float(os.getenv("DECISION_MIN_CONFIDENCE", "0.65"))

# Cooldown: don't act on the same resource more than once within this window
COOLDOWN_MINUTES = int(os.getenv("DECISION_COOLDOWN_MINUTES", "30"))

# Slack webhook (shared with anomaly_detection.py)
SLACK_WEBHOOK = os.getenv("SLACK_WEBHOOK_URL", "")

# Maximum auto-actions per engine run (safety cap)
MAX_AUTO_ACTIONS_PER_RUN = int(os.getenv("DECISION_MAX_AUTO_ACTIONS", "5"))


# ─────────────────────────────────────────────────────────────────────────────
# Decision outcome enum
# ─────────────────────────────────────────────────────────────────────────────

class DecisionOutcome(str, Enum):
    AUTO_APPROVED   = "auto_approved"    # confidence high → automatically queued for remediation
    NEEDS_APPROVAL  = "needs_approval"   # confidence low or high-risk → human must approve
    DRY_RUN         = "dry_run"          # dry-run mode active → logged only
    SKIPPED_COOLDOWN= "skipped_cooldown" # resource was acted on recently
    SKIPPED_NO_PLAY = "skipped_no_play"  # no playbook defined for this RCA category
    SKIPPED_LOW_CONF= "skipped_low_conf" # confidence below minimum threshold


# ─────────────────────────────────────────────────────────────────────────────
# Playbook definitions
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Playbook:
    """
    A Playbook describes what remediation action to take for a given situation.

    playbook_id:     Unique string identifier (used by remediation.py to dispatch)
    name:            Human-readable name
    description:     What this playbook does
    risk_level:      low | medium | high  — affects auto-approval threshold
    auto_approvable: Whether this can ever be auto-approved (some actions require humans)
    parameters:      Dict of parameters to pass to the remediation executor
    """
    playbook_id: str
    name: str
    description: str
    risk_level: str                        # low | medium | high
    auto_approvable: bool
    parameters: Dict[str, Any] = field(default_factory=dict)

    @property
    def confidence_required(self) -> float:
        """Higher-risk playbooks need higher confidence before auto-acting."""
        return {"low": 0.60, "medium": 0.75, "high": 0.90}.get(self.risk_level, 0.80)


# ── Built-in playbook library ─────────────────────────────────────────────────

PLAYBOOKS: Dict[str, Playbook] = {

    # Resource exhaustion
    "scale_out_asg": Playbook(
        playbook_id="scale_out_asg",
        name="Scale out Auto Scaling Group",
        description="Increase desired capacity of the ASG by 2 instances",
        risk_level="medium",
        auto_approvable=True,
        parameters={"delta_instances": 2},
    ),
    "increase_lambda_concurrency": Playbook(
        playbook_id="increase_lambda_concurrency",
        name="Raise Lambda reserved concurrency",
        description="Increase Lambda reserved concurrency limit by 25%",
        risk_level="low",
        auto_approvable=True,
        parameters={"scale_factor": 1.25},
    ),

    # Memory leak
    "restart_ec2_instance": Playbook(
        playbook_id="restart_ec2_instance",
        name="Restart EC2 instance",
        description="Gracefully reboot the EC2 instance to reclaim leaked memory",
        risk_level="high",
        auto_approvable=False,   # restart always needs human approval
        parameters={"reboot_type": "soft"},
    ),
    "restart_ecs_service": Playbook(
        playbook_id="restart_ecs_service",
        name="Force new ECS deployment",
        description="Force a new ECS task deployment to replace leaky container",
        risk_level="medium",
        auto_approvable=True,
        parameters={"force_new_deployment": True},
    ),
    "restart_lambda": Playbook(
        playbook_id="restart_lambda",
        name="Recycle Lambda execution environment",
        description="Deploy a no-op Lambda update to force cold start and reclaim memory",
        risk_level="low",
        auto_approvable=True,
        parameters={"strategy": "touch_env_var"},
    ),

    # Storage pressure
    "expand_rds_storage": Playbook(
        playbook_id="expand_rds_storage",
        name="Expand RDS storage",
        description="Enable RDS storage autoscaling or expand allocated storage by 20%",
        risk_level="low",
        auto_approvable=True,
        parameters={"scale_factor": 1.20},
    ),
    "trigger_disk_cleanup": Playbook(
        playbook_id="trigger_disk_cleanup",
        name="Trigger disk cleanup SSM command",
        description="Run SSM Run Command to clear /tmp and old log files on EC2",
        risk_level="low",
        auto_approvable=True,
        parameters={"ssm_document": "AWS-RunShellScript",
                    "commands": ["find /tmp -mtime +1 -delete",
                                 "journalctl --vacuum-time=2d"]},
    ),

    # Network / latency
    "enable_connection_pool_reset": Playbook(
        playbook_id="enable_connection_pool_reset",
        name="Reset DB connection pool",
        description="Bounce the application connection pool by updating a config parameter",
        risk_level="medium",
        auto_approvable=True,
        parameters={"target": "connection_pool"},
    ),

    # Throttling
    "raise_dynamodb_capacity": Playbook(
        playbook_id="raise_dynamodb_capacity",
        name="Raise DynamoDB provisioned capacity",
        description="Temporarily double DynamoDB read/write capacity units",
        risk_level="low",
        auto_approvable=True,
        parameters={"scale_factor": 2.0, "revert_after_minutes": 60},
    ),
    "enable_sqs_dlq_redrive": Playbook(
        playbook_id="enable_sqs_dlq_redrive",
        name="Start DLQ redrive",
        description="Begin redriving messages from the dead-letter queue back to source",
        risk_level="low",
        auto_approvable=True,
        parameters={"max_messages_per_second": 10},
    ),

    # Application errors
    "trigger_rollback": Playbook(
        playbook_id="trigger_rollback",
        name="Trigger deployment rollback",
        description="Roll back to the last successful deployment via CodeDeploy / ArgoCD",
        risk_level="high",
        auto_approvable=False,   # rollbacks always need human sign-off
        parameters={"strategy": "previous_version"},
    ),

    # Dependency failure
    "failover_to_replica": Playbook(
        playbook_id="failover_to_replica",
        name="Promote RDS read replica",
        description="Initiate RDS Multi-AZ failover to the standby replica",
        risk_level="high",
        auto_approvable=False,
        parameters={"failover_type": "multi_az"},
    ),
    "enable_route53_health_failover": Playbook(
        playbook_id="enable_route53_health_failover",
        name="Activate Route53 failover routing",
        description="Flip Route53 failover record to secondary region endpoint",
        risk_level="medium",
        auto_approvable=True,
        parameters={"record_type": "A", "policy": "failover"},
    ),

    # Cascading failure
    "enable_circuit_breaker": Playbook(
        playbook_id="enable_circuit_breaker",
        name="Enable circuit breaker flag",
        description="Set a feature flag to open the circuit breaker between services",
        risk_level="medium",
        auto_approvable=True,
        parameters={"flag_name": "circuit_breaker_open"},
    ),

    # Load spike
    "activate_cdn_cache": Playbook(
        playbook_id="activate_cdn_cache",
        name="Increase CDN cache TTL",
        description="Raise CloudFront default TTL to 300s to absorb traffic spike",
        risk_level="low",
        auto_approvable=True,
        parameters={"ttl_seconds": 300},
    ),

    # Fallback
    "notify_only": Playbook(
        playbook_id="notify_only",
        name="Notify on-call team",
        description="Page the on-call engineer via PagerDuty / Slack with full RCA context",
        risk_level="low",
        auto_approvable=True,
        parameters={"channel": "slack"},
    ),
}


# ─────────────────────────────────────────────────────────────────────────────
# Playbook selector
# ─────────────────────────────────────────────────────────────────────────────

def _select_playbook(rca: RCAResult) -> Optional[Playbook]:
    """
    Choose the most appropriate playbook for a given RCAResult.
    Rules are evaluated in priority order — first match wins.
    """
    rt  = rca.root_resource_type.lower()
    cat = rca.category
    metric = rca.root_metric.lower()

    # ── Specific resource + category overrides ─────────────────────────────
    if cat == RCACategory.MEMORY_LEAK:
        if rt == "ec2":
            return PLAYBOOKS["restart_ec2_instance"]
        if rt in ("ecs", "ecs_service"):
            return PLAYBOOKS["restart_ecs_service"]
        if rt in ("lambda", "function_app", "cloud_run"):
            return PLAYBOOKS["restart_lambda"]

    if cat == RCACategory.STORAGE_PRESSURE:
        if rt == "rds":
            return PLAYBOOKS["expand_rds_storage"]
        if rt == "ec2":
            return PLAYBOOKS["trigger_disk_cleanup"]

    if cat == RCACategory.RESOURCE_EXHAUSTION:
        if rt in ("lambda", "function_app", "cloud_run"):
            return PLAYBOOKS["increase_lambda_concurrency"]
        if rt in ("ec2", "asg", "ecs"):
            return PLAYBOOKS["scale_out_asg"]
        if rt == "dynamodb":
            return PLAYBOOKS["raise_dynamodb_capacity"]

    if cat == RCACategory.THROTTLING:
        if rt == "dynamodb":
            return PLAYBOOKS["raise_dynamodb_capacity"]
        if rt == "sqs":
            return PLAYBOOKS["enable_sqs_dlq_redrive"]

    if cat == RCACategory.DEPENDENCY_FAILURE:
        if rt == "rds":
            return PLAYBOOKS["failover_to_replica"]
        return PLAYBOOKS["enable_route53_health_failover"]

    if cat == RCACategory.APPLICATION_ERROR:
        return PLAYBOOKS["trigger_rollback"]

    if cat == RCACategory.CASCADING_FAILURE:
        return PLAYBOOKS["enable_circuit_breaker"]

    if cat == RCACategory.NETWORK_DEGRADATION:
        if "latency" in metric or "duration" in metric:
            return PLAYBOOKS["enable_connection_pool_reset"]

    if cat == RCACategory.LOAD_SPIKE:
        return PLAYBOOKS["activate_cdn_cache"]

    # ── Fallback: notify only ─────────────────────────────────────────────
    return PLAYBOOKS["notify_only"]


# ─────────────────────────────────────────────────────────────────────────────
# Decision dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Decision:
    decision_id: str
    decided_at: str
    rca_id: str
    cloud: str
    region: str
    resource_id: str
    resource_name: str
    resource_type: str
    rca_category: str
    rca_confidence: float
    severity: str
    playbook_id: str
    playbook_name: str
    playbook_risk: str
    parameters: Dict[str, Any]
    outcome: DecisionOutcome
    outcome_reason: str
    dry_run: bool
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["outcome"] = self.outcome.value
        d["parameters"] = json.dumps(self.parameters)
        return d

    def slack_message(self) -> dict:
        icon_map = {
            DecisionOutcome.AUTO_APPROVED:    "🟢",
            DecisionOutcome.NEEDS_APPROVAL:   "🟡",
            DecisionOutcome.DRY_RUN:          "🔵",
            DecisionOutcome.SKIPPED_COOLDOWN: "⏸️",
            DecisionOutcome.SKIPPED_NO_PLAY:  "⚪",
            DecisionOutcome.SKIPPED_LOW_CONF: "🔻",
        }
        icon = icon_map.get(self.outcome, "⚪")
        dry_tag = " [DRY RUN]" if self.dry_run else ""

        return {
            "text": f"{icon} Decision{dry_tag}: {self.playbook_name} on {self.resource_name}",
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text", "text": f"{icon} Decision Engine{dry_tag}"},
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Resource:*\n{self.resource_name}"},
                        {"type": "mrkdwn", "text": f"*Type:*\n{self.resource_type}"},
                        {"type": "mrkdwn", "text": f"*RCA Category:*\n{self.rca_category}"},
                        {"type": "mrkdwn", "text": f"*Confidence:*\n{self.rca_confidence:.0%}"},
                        {"type": "mrkdwn", "text": f"*Playbook:*\n{self.playbook_name}"},
                        {"type": "mrkdwn", "text": f"*Outcome:*\n{self.outcome.value}"},
                    ],
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": f"*Reason:*\n{self.outcome_reason}"},
                },
                {
                    "type": "context",
                    "elements": [{"type": "mrkdwn",
                                  "text": f"RCA: {self.rca_id} | Decision: {self.decision_id} | {self.decided_at}"}],
                },
            ],
        }


# ─────────────────────────────────────────────────────────────────────────────
# Decision Engine
# ─────────────────────────────────────────────────────────────────────────────

class DecisionEngine:
    """
    Core decision-making engine.

    For each RCAResult:
      1. Select playbook via _select_playbook()
      2. Check cooldown (was this resource acted on recently?)
      3. Check confidence gate
      4. Check risk level vs confidence
      5. Emit Decision(outcome=AUTO_APPROVED | NEEDS_APPROVAL | DRY_RUN | SKIPPED_*)
      6. Notify via Slack
      7. Persist decision to DB
    """

    def __init__(self, config_path: str = "config/cloud_observability.yaml") -> None:
        self._config_path = config_path
        self._store = DecisionStore(config_path)
        self._cooldown_cache: Dict[str, datetime] = {}  # resource_id → last action time
        self._lock = threading.Lock()
        self._run_auto_count = 0

        # Optional: hook for Phase 5 remediation executor
        # Set this to a callable: engine.remediation_executor = remediation.execute
        self.remediation_executor: Optional[Callable[[str, Dict], Any]] = None

        log.info(
            f"[DecisionEngine] Ready | dry_run={DRY_RUN} "
            f"min_confidence={MIN_CONFIDENCE:.0%} cooldown={COOLDOWN_MINUTES}m"
        )

    # ── Public entry point ────────────────────────────────────────────────────

    def decide(self, rca_results: List[RCAResult]) -> List[Decision]:
        """
        Process a list of RCAResults and produce one Decision per result.
        Resets per-run auto action counter.
        """
        self._run_auto_count = 0
        decisions: List[Decision] = []

        for rca in rca_results:
            try:
                d = self._decide_one(rca)
                decisions.append(d)
                self._store.save(d)
                self._notify_slack(d)
                log.info(
                    f"[DecisionEngine] {d.decision_id[:8]} | {d.outcome.value} | "
                    f"{d.playbook_id} → {d.resource_name}"
                )
            except Exception as e:
                log.error(f"[DecisionEngine] Failed for RCA {rca.rca_id}: {e}", exc_info=True)

        return decisions

    # ── Single decision ───────────────────────────────────────────────────────

    def _decide_one(self, rca: RCAResult) -> Decision:
        now = datetime.now(timezone.utc)
        decision_id = str(uuid.uuid4())

        # 1. Select playbook
        playbook = _select_playbook(rca)
        if playbook is None:
            return self._make_decision(
                decision_id, now, rca, None,
                DecisionOutcome.SKIPPED_NO_PLAY,
                "No playbook defined for this RCA category and resource type",
            )

        # 2. Cooldown check
        if self._in_cooldown(rca.root_resource_id, now):
            return self._make_decision(
                decision_id, now, rca, playbook,
                DecisionOutcome.SKIPPED_COOLDOWN,
                f"Resource acted on within last {COOLDOWN_MINUTES} minutes — cooldown active",
            )

        # 3. Confidence gate
        if rca.confidence < MIN_CONFIDENCE:
            return self._make_decision(
                decision_id, now, rca, playbook,
                DecisionOutcome.SKIPPED_LOW_CONF,
                f"Confidence {rca.confidence:.0%} < minimum {MIN_CONFIDENCE:.0%}",
            )

        # 4. Dry-run mode
        if DRY_RUN:
            return self._make_decision(
                decision_id, now, rca, playbook,
                DecisionOutcome.DRY_RUN,
                "DRY_RUN mode active — set DECISION_DRY_RUN=false to enable auto-remediation",
            )

        # 5. Risk + confidence gate
        if not playbook.auto_approvable:
            return self._make_decision(
                decision_id, now, rca, playbook,
                DecisionOutcome.NEEDS_APPROVAL,
                f"Playbook '{playbook.name}' is marked requires human approval (high-risk action)",
            )

        if rca.confidence < playbook.confidence_required:
            return self._make_decision(
                decision_id, now, rca, playbook,
                DecisionOutcome.NEEDS_APPROVAL,
                (
                    f"Confidence {rca.confidence:.0%} < "
                    f"{playbook.confidence_required:.0%} required for {playbook.risk_level}-risk playbook"
                ),
            )

        # 6. Safety cap
        if self._run_auto_count >= MAX_AUTO_ACTIONS_PER_RUN:
            return self._make_decision(
                decision_id, now, rca, playbook,
                DecisionOutcome.NEEDS_APPROVAL,
                f"Auto-action cap ({MAX_AUTO_ACTIONS_PER_RUN}) reached for this run — deferring to human",
            )

        # 7. AUTO_APPROVED — execute remediation
        d = self._make_decision(
            decision_id, now, rca, playbook,
            DecisionOutcome.AUTO_APPROVED,
            f"All gates passed | confidence={rca.confidence:.0%} | risk={playbook.risk_level}",
        )

        self._run_auto_count += 1
        self._record_cooldown(rca.root_resource_id, now)

        # Invoke remediation executor if wired up (Phase 5)
        if self.remediation_executor is not None:
            try:
                self.remediation_executor(playbook.playbook_id, d.parameters)
            except Exception as e:
                log.error(f"[DecisionEngine] Remediation executor failed: {e}", exc_info=True)

        return d

    # ── Builder helper ────────────────────────────────────────────────────────

    def _make_decision(
        self,
        decision_id: str,
        now: datetime,
        rca: RCAResult,
        playbook: Optional[Playbook],
        outcome: DecisionOutcome,
        reason: str,
    ) -> Decision:
        pb_id   = playbook.playbook_id if playbook else "none"
        pb_name = playbook.name if playbook else "No playbook"
        pb_risk = playbook.risk_level if playbook else "n/a"
        params  = {**playbook.parameters, "resource_id": rca.root_resource_id,
                   "resource_name": rca.root_resource_name,
                   "cloud": rca.cloud, "region": rca.region} if playbook else {}

        return Decision(
            decision_id=decision_id,
            decided_at=now.isoformat(),
            rca_id=rca.rca_id,
            cloud=rca.cloud,
            region=rca.region,
            resource_id=rca.root_resource_id,
            resource_name=rca.root_resource_name,
            resource_type=rca.root_resource_type,
            rca_category=rca.category.value,
            rca_confidence=rca.confidence,
            severity=rca.severity,
            playbook_id=pb_id,
            playbook_name=pb_name,
            playbook_risk=pb_risk,
            parameters=params,
            outcome=outcome,
            outcome_reason=reason,
            dry_run=DRY_RUN,
            summary=(
                f"{outcome.value}: {pb_name} on {rca.root_resource_name} "
                f"[{rca.category.value} | conf={rca.confidence:.0%}]"
            ),
        )

    # ── Cooldown helpers ──────────────────────────────────────────────────────

    def _in_cooldown(self, resource_id: str, now: datetime) -> bool:
        with self._lock:
            last = self._cooldown_cache.get(resource_id)
        if last is None:
            # Also check DB for cross-process cooldown
            return self._store.was_acted_recently(resource_id, COOLDOWN_MINUTES)
        return (now - last).total_seconds() < COOLDOWN_MINUTES * 60

    def _record_cooldown(self, resource_id: str, now: datetime) -> None:
        with self._lock:
            self._cooldown_cache[resource_id] = now

    # ── Slack notification ────────────────────────────────────────────────────

    def _notify_slack(self, decision: Decision) -> None:
        if not SLACK_WEBHOOK:
            return
        # Only notify on actionable outcomes
        if decision.outcome in (
            DecisionOutcome.SKIPPED_COOLDOWN,
            DecisionOutcome.SKIPPED_NO_PLAY,
        ):
            return
        try:
            import urllib.request
            payload = json.dumps(decision.slack_message()).encode()
            req = urllib.request.Request(
                SLACK_WEBHOOK, data=payload,
                headers={"Content-Type": "application/json"}, method="POST"
            )
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            log.debug(f"[DecisionEngine] Slack notify failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Decision Store
# ─────────────────────────────────────────────────────────────────────────────

_DDL_DECISIONS = """
CREATE TABLE IF NOT EXISTS decisions (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id       TEXT NOT NULL UNIQUE,
    decided_at        TEXT NOT NULL,
    rca_id            TEXT NOT NULL,
    cloud             TEXT NOT NULL,
    region            TEXT NOT NULL,
    resource_id       TEXT NOT NULL,
    resource_name     TEXT NOT NULL,
    resource_type     TEXT NOT NULL,
    rca_category      TEXT NOT NULL,
    rca_confidence    REAL NOT NULL,
    severity          TEXT NOT NULL,
    playbook_id       TEXT NOT NULL,
    playbook_name     TEXT NOT NULL,
    playbook_risk     TEXT NOT NULL,
    parameters        TEXT NOT NULL,
    outcome           TEXT NOT NULL,
    outcome_reason    TEXT NOT NULL,
    dry_run           INTEGER NOT NULL,
    summary           TEXT NOT NULL,
    executed_at       TEXT DEFAULT NULL,
    execution_result  TEXT DEFAULT NULL
);
CREATE INDEX IF NOT EXISTS idx_dec_time     ON decisions(decided_at DESC);
CREATE INDEX IF NOT EXISTS idx_dec_resource ON decisions(resource_id);
CREATE INDEX IF NOT EXISTS idx_dec_rca      ON decisions(rca_id);
CREATE INDEX IF NOT EXISTS idx_dec_outcome  ON decisions(outcome);
"""


class DecisionStore:
    """Persists Decision objects to the observability database."""

    def __init__(self, config_path: str = "config/cloud_observability.yaml") -> None:
        self._lock = threading.Lock()
        self._conn, self._backend = self._connect(config_path)
        self._migrate()
        log.info("[DecisionStore] Ready")

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
            dsn = pg.get("dsn") or os.getenv("DATABASE_URL") or (
                f"postgresql://{pg.get('user','postgres')}:{pg.get('password','')}@"
                f"{pg.get('host','localhost')}:{pg.get('port',5432)}/{pg.get('dbname','observability')}"
            )
            return psycopg2.connect(dsn), "postgres"

    def _migrate(self) -> None:
        ddl = _DDL_DECISIONS
        if self._backend == "postgres":
            ddl = ddl.replace("INTEGER PRIMARY KEY AUTOINCREMENT", "BIGSERIAL PRIMARY KEY")
        if self._backend == "sqlite":
            self._conn.executescript(ddl)
            self._conn.commit()
        else:
            with self._conn.cursor() as cur:
                cur.execute(ddl)
            self._conn.commit()

    def save(self, d: Decision) -> None:
        row = d.to_dict()
        row["dry_run"] = 1 if row["dry_run"] else 0
        cols = [c for c in row if c not in ("id",)]
        vals = [row[c] for c in cols]
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = (
            f"INSERT OR IGNORE INTO decisions ({','.join(cols)}) "
            f"VALUES ({','.join([ph]*len(cols))})"
        )
        if self._backend == "postgres":
            sql = sql.replace("INSERT OR IGNORE", "INSERT").rstrip()
            sql += " ON CONFLICT (decision_id) DO NOTHING"
        with self._lock:
            if self._backend == "sqlite":
                self._conn.execute(sql, vals)
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(sql, vals)
                self._conn.commit()

    def was_acted_recently(self, resource_id: str, minutes: int) -> bool:
        """Check DB for recent AUTO_APPROVED decisions on this resource."""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = (
            f"SELECT COUNT(*) FROM decisions "
            f"WHERE resource_id={ph} AND outcome='auto_approved' AND decided_at>={ph}"
        )
        if self._backend == "sqlite":
            row = self._conn.execute(sql, (resource_id, cutoff)).fetchone()
        else:
            with self._conn.cursor() as cur:
                cur.execute(sql, (resource_id, cutoff))
                row = cur.fetchone()
        return bool(row and int(row[0]) > 0)

    def get_pending_approvals(self) -> List[Dict[str, Any]]:
        """Return all decisions awaiting human approval."""
        sql = (
            "SELECT * FROM decisions WHERE outcome='needs_approval' "
            "AND executed_at IS NULL ORDER BY decided_at DESC"
        )
        if self._backend == "sqlite":
            self._conn.row_factory = sqlite3.Row
            rows = self._conn.execute(sql).fetchall()
            return [dict(r) for r in rows]
        else:
            with self._conn.cursor() as cur:
                cur.execute(sql)
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]

    def record_execution(self, decision_id: str, result: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = (
            f"UPDATE decisions SET executed_at={ph}, execution_result={ph} "
            f"WHERE decision_id={ph}"
        )
        with self._lock:
            if self._backend == "sqlite":
                self._conn.execute(sql, (now, result, decision_id))
                self._conn.commit()
            else:
                with self._conn.cursor() as cur:
                    cur.execute(sql, (now, result, decision_id))
                self._conn.commit()

    def get_recent(self, hours: int = 24) -> List[Dict[str, Any]]:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        ph = "?" if self._backend == "sqlite" else "%s"
        sql = f"SELECT * FROM decisions WHERE decided_at>={ph} ORDER BY decided_at DESC"
        if self._backend == "sqlite":
            self._conn.row_factory = sqlite3.Row
            rows = self._conn.execute(sql, (cutoff,)).fetchall()
            return [dict(r) for r in rows]
        else:
            with self._conn.cursor() as cur:
                cur.execute(sql, (cutoff,))
                cols = [d[0] for d in cur.description]
                return [dict(zip(cols, r)) for r in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────────────────
# Standalone runner
# ─────────────────────────────────────────────────────────────────────────────

def run_decision_once(
    config_path: str = "config/cloud_observability.yaml",
    rca_results: Optional[List[RCAResult]] = None,
) -> List[Decision]:
    """
    If rca_results is not provided, pulls recent unresolved RCA results from DB.
    Called by main.py after run_rca_once().
    """
    if rca_results is None:
        from root_cause_analysis import RCAStore
        store = RCAStore(config_path)
        raw = store.get_recent(hours=1)
        rca_results = []
        for r in raw:
            if r.get("decision_taken"):
                continue   # already processed
            try:
                rca_results.append(RCAResult(
                    rca_id=r["rca_id"],
                    analyzed_at=r["analyzed_at"],
                    cloud=r["cloud"],
                    region=r["region"],
                    category=RCACategory(r["category"]),
                    root_resource_id=r["root_resource_id"],
                    root_resource_name=r["root_resource_name"],
                    root_resource_type=r["root_resource_type"],
                    root_metric=r["root_metric"],
                    confidence=float(r["confidence"]),
                    severity=r["severity"],
                    summary=r["summary"],
                    affected_resource_ids=json.loads(r["affected_resource_ids"]),
                    contributing_anomaly_ids=json.loads(r["contributing_anomaly_ids"]),
                    category_scores=json.loads(r["category_scores"]),
                    evidence=json.loads(r["evidence"]),
                    suggested_actions=json.loads(r["suggested_actions"]),
                    raw_anomaly_count=int(r["raw_anomaly_count"]),
                ))
            except Exception as e:
                log.warning(f"[Decision] Skipping malformed RCA row: {e}")

    engine = DecisionEngine(config_path)
    decisions = engine.decide(rca_results)

    # Mark decisions taken in RCA store
    from root_cause_analysis import RCAStore
    rca_store = RCAStore(config_path)
    for d in decisions:
        rca_store.mark_decision_taken(d.rca_id, d.outcome.value)

    return decisions


if __name__ == "__main__":
    import sys
    cfg = sys.argv[1] if len(sys.argv) > 1 else "config/cloud_observability.yaml"
    decisions = run_decision_once(cfg)
    for d in decisions:
        print(f"\n{'='*60}")
        print(f"Decision   : {d.decision_id}")
        print(f"Outcome    : {d.outcome.value}")
        print(f"Playbook   : {d.playbook_name} (risk={d.playbook_risk})")
        print(f"Resource   : {d.resource_name} ({d.resource_type})")
        print(f"Reason     : {d.outcome_reason}")
        print(f"Parameters : {json.dumps(d.parameters, indent=2)}")