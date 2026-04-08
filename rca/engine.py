from __future__ import annotations

import json
import logging
import os
import sqlite3
import statistics
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

log = logging.getLogger("rca.engine")


# ── Optional MCP client import ───────────────────────────────────────────────
# This file ONLY calls MCP if the client/tool is available.
# It does not implement MCP server logic here.
try:
    from mcp_client import MCPClient  # expected external wrapper/client
    MCP_CLIENT_AVAILABLE = True
except Exception:
    MCPClient = None
    MCP_CLIENT_AVAILABLE = False


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class CausalCandidate:
    resource_id: str
    resource_name: str
    resource_type: str
    cloud: str
    region: str
    metric_name: str
    metric_value: float
    baseline_avg: float
    deviation_pct: float
    first_seen_at: str
    time_offset_seconds: float
    correlation_score: float
    is_root_cause: bool = False


@dataclass
class ErrorLogEntry:
    cloud: str
    resource_name: str
    log_level: str
    message: str
    collected_at: str


@dataclass
class RCAReport:
    report_id: str
    generated_at: str
    trigger_anomaly_time: str
    trigger_resource: str
    trigger_resource_type: str
    trigger_metric: str
    trigger_value: float
    trigger_severity: str
    trigger_cloud: str
    trigger_region: str

    root_cause: Optional[CausalCandidate]
    cascading_effects: List[CausalCandidate]
    related_errors: List[ErrorLogEntry]
    timeline: List[Dict[str, Any]]
    summary: str
    recommended_actions: List[str]
    confidence: float

    metric_history: Dict[str, List[Tuple[str, float]]]

    # NEW: MCP enrichment result
    mcp_analysis: Optional[Dict[str, Any]] = None


# ── RCA Engine ────────────────────────────────────────────────────────────────

class RCAEngine:
    """
    Reads from your existing SQLite DB and produces a structured RCA report.
    Optional MCP enrichment can be applied on top of heuristic RCA.
    """

    LOOKBACK_MINUTES = 30
    LOOKAHEAD_MINUTES = 10
    DEVIATION_THRESHOLD = 30.0
    CORRELATION_DECAY = 0.05

    def __init__(self, db_path: str):
        self.db_path = db_path
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row

        self.enable_mcp = os.getenv("RCA_USE_MCP", "true").lower() == "true"
        self.mcp_timeout_seconds = int(os.getenv("RCA_MCP_TIMEOUT_SECONDS", "30"))
        self.mcp_tool_name = os.getenv("RCA_MCP_TOOL_NAME", "analyze_rca")

        self._mcp_client = None
        if self.enable_mcp and MCP_CLIENT_AVAILABLE:
            try:
                self._mcp_client = MCPClient(timeout=self.mcp_timeout_seconds)
                log.info("MCP client initialized for RCA enrichment")
            except Exception as e:
                log.warning(f"Failed to initialize MCP client: {e}")
                self._mcp_client = None

    # ── Public API ─────────────────────────────────────────────────────────────

    def run_rca(
        self,
        trigger_resource_id: str,
        trigger_metric: str,
        trigger_time: Optional[str] = None,
        window_minutes: int = LOOKBACK_MINUTES,
    ) -> RCAReport:
        if trigger_time is None:
            trigger_time = datetime.now(timezone.utc).isoformat()

        trigger_dt = _parse_ts(trigger_time)
        window_start = (trigger_dt - timedelta(minutes=window_minutes)).isoformat()
        window_end = (trigger_dt + timedelta(minutes=self.LOOKAHEAD_MINUTES)).isoformat()

        trigger_row = self._get_latest_anomaly(trigger_resource_id, trigger_metric)
        all_metrics = self._get_all_metrics_in_window(window_start, window_end)

        baseline_start = (trigger_dt - timedelta(hours=1, minutes=window_minutes)).isoformat()
        baseline_end = window_start
        baselines = self._compute_baselines(baseline_start, baseline_end)

        candidates = self._find_candidates(
            all_metrics,
            baselines,
            trigger_dt,
            trigger_resource_id,
            trigger_metric,
        )

        root_cause, cascading = self._rank_candidates(candidates, trigger_dt)

        error_logs = self._get_error_logs(window_start, window_end)

        timeline = self._build_timeline(
            root_cause,
            cascading,
            error_logs,
            trigger_row,
            trigger_time,
        )

        metric_history = self._build_metric_history(
            root_cause,
            cascading,
            window_start,
            window_end,
        )

        summary, actions = self._generate_summary(
            trigger_row,
            root_cause,
            cascading,
            error_logs,
        )

        confidence = self._compute_confidence(root_cause, candidates, error_logs)

        report_id = f"RCA-{trigger_dt.strftime('%Y%m%d-%H%M%S')}-{trigger_resource_id[:8]}"

        mcp_analysis = self._run_mcp_analysis(
            trigger_row=trigger_row,
            trigger_resource_id=trigger_resource_id,
            trigger_metric=trigger_metric,
            trigger_time=trigger_time,
            root_cause=root_cause,
            cascading=cascading,
            error_logs=error_logs,
            timeline=timeline,
            metric_history=metric_history,
            heuristic_summary=summary,
            heuristic_actions=actions,
            heuristic_confidence=confidence,
            candidates=candidates,
        )

        # Optional merge: use MCP output only as enrichment, not hard override
        if mcp_analysis:
            summary, actions, confidence = self._merge_mcp_into_report(
                summary,
                actions,
                confidence,
                mcp_analysis,
            )

        return RCAReport(
            report_id=report_id,
            generated_at=datetime.now(timezone.utc).isoformat(),
            trigger_anomaly_time=trigger_time,
            trigger_resource=trigger_row["resource_name"] if trigger_row else trigger_resource_id,
            trigger_resource_type=trigger_row["resource_type"] if trigger_row else "unknown",
            trigger_metric=trigger_metric,
            trigger_value=float(trigger_row["current_value"]) if trigger_row else 0.0,
            trigger_severity=trigger_row["severity"] if trigger_row else "warning",
            trigger_cloud=trigger_row["cloud"] if trigger_row else "unknown",
            trigger_region=trigger_row["region"] if trigger_row else "unknown",
            root_cause=root_cause,
            cascading_effects=cascading,
            related_errors=error_logs,
            timeline=timeline,
            summary=summary,
            recommended_actions=actions,
            confidence=confidence,
            metric_history=metric_history,
            mcp_analysis=mcp_analysis,
        )

    # ── MCP integration ────────────────────────────────────────────────────────

    def _run_mcp_analysis(
        self,
        trigger_row: Optional[sqlite3.Row],
        trigger_resource_id: str,
        trigger_metric: str,
        trigger_time: str,
        root_cause: Optional[CausalCandidate],
        cascading: List[CausalCandidate],
        error_logs: List[ErrorLogEntry],
        timeline: List[Dict[str, Any]],
        metric_history: Dict[str, List[Tuple[str, float]]],
        heuristic_summary: str,
        heuristic_actions: List[str],
        heuristic_confidence: float,
        candidates: List[CausalCandidate],
    ) -> Optional[Dict[str, Any]]:
        """
        Only CALLS an external MCP tool/client.
        No MCP server logic is implemented here.
        """
        if not self.enable_mcp:
            log.info("MCP enrichment disabled by RCA_USE_MCP=false")
            return None

        if self._mcp_client is None:
            log.info("MCP client not available, skipping MCP enrichment")
            return None

        payload = {
            "incident": {
                "trigger_time": trigger_time,
                "trigger_resource_id": trigger_resource_id,
                "trigger_metric": trigger_metric,
                "trigger_resource_name": trigger_row["resource_name"] if trigger_row else trigger_resource_id,
                "trigger_resource_type": trigger_row["resource_type"] if trigger_row else "unknown",
                "trigger_cloud": trigger_row["cloud"] if trigger_row else "unknown",
                "trigger_region": trigger_row["region"] if trigger_row else "unknown",
                "trigger_value": float(trigger_row["current_value"]) if trigger_row else 0.0,
                "trigger_severity": trigger_row["severity"] if trigger_row else "warning",
            },
            "heuristic_rca": {
                "root_cause": asdict(root_cause) if root_cause else None,
                "cascading_effects": [asdict(c) for c in cascading],
                "summary": heuristic_summary,
                "recommended_actions": heuristic_actions,
                "confidence": heuristic_confidence,
                "candidates": [asdict(c) for c in candidates[:15]],
            },
            "error_logs": [asdict(e) for e in error_logs[:20]],
            "timeline": timeline[:30],
            "metric_history": metric_history,
        }

        try:
            result = self._mcp_client.call_tool(
                self.mcp_tool_name,
                payload,
            )

            if not result:
                log.info("MCP returned empty result")
                return None

            if isinstance(result, str):
                try:
                    result = json.loads(result)
                except Exception:
                    result = {"raw_result": result}

            if not isinstance(result, dict):
                result = {"raw_result": result}

            log.info("MCP RCA enrichment completed successfully")
            return result

        except Exception as e:
            log.warning(f"MCP RCA enrichment failed: {e}")
            return None

    def _merge_mcp_into_report(
        self,
        summary: str,
        actions: List[str],
        confidence: float,
        mcp_analysis: Dict[str, Any],
    ) -> Tuple[str, List[str], float]:
        """
        Merge MCP output conservatively.
        Keeps heuristic RCA as base and appends MCP insights.
        """
        merged_summary = summary
        merged_actions = list(actions)
        merged_confidence = confidence

        mcp_summary = mcp_analysis.get("summary") or mcp_analysis.get("explanation")
        if mcp_summary:
            merged_summary = f"{summary}\n\nMCP Analysis: {mcp_summary}"

        mcp_actions = mcp_analysis.get("recommended_actions") or mcp_analysis.get("actions")
        if isinstance(mcp_actions, list):
            for action in mcp_actions:
                if action and action not in merged_actions:
                    merged_actions.append(str(action))

        mcp_conf = mcp_analysis.get("confidence")
        if isinstance(mcp_conf, (int, float)):
            merged_confidence = round((confidence * 0.7) + (float(mcp_conf) * 0.3), 2)

        return merged_summary, merged_actions, merged_confidence

    # ── DB Queries ─────────────────────────────────────────────────────────────

    def _get_latest_anomaly(self, resource_id: str, metric_name: str) -> Optional[sqlite3.Row]:
        return self._conn.execute(
            """
            SELECT * FROM anomalies
            WHERE resource_id=? AND metric_name=?
            ORDER BY detected_at DESC LIMIT 1
            """,
            (resource_id, metric_name),
        ).fetchone()

    def _get_all_metrics_in_window(self, start: str, end: str) -> List[sqlite3.Row]:
        return self._conn.execute(
            """
            SELECT cloud, region, resource_type, resource_id, resource_name,
                   metric_name, metric_value, metric_unit, collected_at
            FROM metrics
            WHERE collected_at BETWEEN ? AND ?
            ORDER BY collected_at ASC
            """,
            (start, end),
        ).fetchall()

    def _compute_baselines(
        self,
        baseline_start: str,
        baseline_end: str,
    ) -> Dict[str, Dict[str, float]]:
        rows = self._conn.execute(
            """
            SELECT resource_id, metric_name, metric_value
            FROM metrics
            WHERE collected_at BETWEEN ? AND ?
            """,
            (baseline_start, baseline_end),
        ).fetchall()

        buckets: Dict[str, List[float]] = {}
        for r in rows:
            key = f"{r['resource_id']}::{r['metric_name']}"
            buckets.setdefault(key, []).append(float(r["metric_value"]))

        result: Dict[str, Dict[str, float]] = {}
        for key, vals in buckets.items():
            if vals:
                result[key] = {
                    "mean": statistics.mean(vals),
                    "std": statistics.stdev(vals) if len(vals) > 1 else 0.0,
                }
        return result

    def _get_error_logs(self, start: str, end: str) -> List[ErrorLogEntry]:
        try:
            rows = self._conn.execute(
                """
                SELECT cloud, resource_name, log_level, message, collected_at
                FROM logs
                WHERE collected_at BETWEEN ? AND ?
                  AND log_level IN ('ERROR','CRITICAL','FATAL')
                ORDER BY collected_at ASC
                """,
                (start, end),
            ).fetchall()

            return [
                ErrorLogEntry(
                    cloud=r["cloud"],
                    resource_name=r["resource_name"],
                    log_level=r["log_level"],
                    message=r["message"],
                    collected_at=r["collected_at"],
                )
                for r in rows
            ]
        except Exception:
            return []

    # ── Analysis ───────────────────────────────────────────────────────────────

    def _find_candidates(
        self,
        all_metrics: List[sqlite3.Row],
        baselines: Dict[str, Dict[str, float]],
        trigger_dt: datetime,
        trigger_resource_id: str,
        trigger_metric: str,
    ) -> List[CausalCandidate]:
        series: Dict[str, List[sqlite3.Row]] = {}
        for r in all_metrics:
            key = f"{r['resource_id']}::{r['metric_name']}"
            series.setdefault(key, []).append(r)

        candidates: List[CausalCandidate] = []

        for key, rows in series.items():
            resource_id, metric_name = key.split("::", 1)
            baseline = baselines.get(key, {})
            if not baseline:
                continue

            mean = baseline["mean"]
            std = baseline["std"]
            if mean == 0 and std == 0:
                continue

            for row in rows:
                val = float(row["metric_value"])
                if mean != 0:
                    dev_pct = abs(val - mean) / max(abs(mean), 1e-9) * 100
                else:
                    dev_pct = abs(val - mean) / max(std, 1e-9) * 100

                if dev_pct < self.DEVIATION_THRESHOLD:
                    continue

                first_seen_dt = _parse_ts(row["collected_at"])
                offset_sec = (trigger_dt - first_seen_dt).total_seconds()
                corr = self._correlation_score(offset_sec, dev_pct)

                candidates.append(
                    CausalCandidate(
                        resource_id=resource_id,
                        resource_name=row["resource_name"],
                        resource_type=row["resource_type"],
                        cloud=row["cloud"],
                        region=row["region"],
                        metric_name=metric_name,
                        metric_value=val,
                        baseline_avg=round(mean, 4),
                        deviation_pct=round(dev_pct, 1),
                        first_seen_at=row["collected_at"],
                        time_offset_seconds=offset_sec,
                        correlation_score=corr,
                    )
                )
                break

        return candidates

    def _correlation_score(self, offset_sec: float, dev_pct: float) -> float:
        severity_factor = min(dev_pct / 100, 1.0)
        minutes = offset_sec / 60.0

        if minutes < 0:
            time_factor = max(0, 1 - abs(minutes) * 0.15)
        elif 0 <= minutes <= 5:
            time_factor = 0.95
        elif 5 < minutes <= 15:
            time_factor = 0.80
        elif 15 < minutes <= 30:
            time_factor = 0.60
        else:
            time_factor = max(0.1, 0.6 - (minutes - 30) * self.CORRELATION_DECAY)

        return round(min(severity_factor * 0.4 + time_factor * 0.6, 1.0), 3)

    def _rank_candidates(
        self,
        candidates: List[CausalCandidate],
        trigger_dt: datetime,
    ) -> Tuple[Optional[CausalCandidate], List[CausalCandidate]]:
        if not candidates:
            return None, []

        sorted_c = sorted(candidates, key=lambda c: c.correlation_score, reverse=True)

        root_cause = None
        effects: List[CausalCandidate] = []

        for c in sorted_c:
            if c.time_offset_seconds > 0 and root_cause is None:
                root_cause = c
                c.is_root_cause = True
            else:
                effects.append(c)

        if root_cause is None and sorted_c:
            root_cause = sorted_c[0]
            root_cause.is_root_cause = True
            effects = sorted_c[1:]

        return root_cause, effects[:8]

    def _build_timeline(
        self,
        root_cause: Optional[CausalCandidate],
        cascading: List[CausalCandidate],
        errors: List[ErrorLogEntry],
        trigger_row: Optional[sqlite3.Row],
        trigger_time: str,
    ) -> List[Dict[str, Any]]:
        events: List[Dict[str, Any]] = []

        if root_cause:
            events.append(
                {
                    "time": root_cause.first_seen_at,
                    "type": "root_cause",
                    "resource": root_cause.resource_name,
                    "metric": root_cause.metric_name,
                    "value": root_cause.metric_value,
                    "label": f"ROOT CAUSE: {root_cause.metric_name} deviated {root_cause.deviation_pct:.0f}% on {root_cause.resource_name}",
                }
            )

        for e in errors:
            events.append(
                {
                    "time": e.collected_at,
                    "type": "error_log",
                    "resource": e.resource_name,
                    "metric": "log",
                    "value": None,
                    "label": f"[{e.log_level}] {e.resource_name}: {e.message[:80]}",
                }
            )

        if trigger_row:
            events.append(
                {
                    "time": trigger_time,
                    "type": "trigger",
                    "resource": trigger_row["resource_name"],
                    "metric": trigger_row["metric_name"],
                    "value": float(trigger_row["current_value"]),
                    "label": f"ANOMALY DETECTED: {trigger_row['metric_name']} on {trigger_row['resource_name']}",
                }
            )

        for c in cascading[:5]:
            if c.time_offset_seconds < 0:
                events.append(
                    {
                        "time": c.first_seen_at,
                        "type": "cascade",
                        "resource": c.resource_name,
                        "metric": c.metric_name,
                        "value": c.metric_value,
                        "label": f"CASCADE: {c.metric_name} affected on {c.resource_name} ({c.deviation_pct:.0f}% deviation)",
                    }
                )

        events.sort(key=lambda e: e["time"])
        return events

    def _build_metric_history(
        self,
        root_cause: Optional[CausalCandidate],
        cascading: List[CausalCandidate],
        window_start: str,
        window_end: str,
    ) -> Dict[str, List[Tuple[str, float]]]:
        result: Dict[str, List[Tuple[str, float]]] = {}

        targets: List[Tuple[str, str]] = []
        if root_cause:
            targets.append((root_cause.resource_id, root_cause.metric_name))
        for c in cascading[:3]:
            targets.append((c.resource_id, c.metric_name))

        for resource_id, metric_name in targets:
            rows = self._conn.execute(
                """
                SELECT collected_at, metric_value FROM metrics
                WHERE resource_id=? AND metric_name=?
                  AND collected_at BETWEEN ? AND ?
                ORDER BY collected_at ASC
                """,
                (resource_id, metric_name, window_start, window_end),
            ).fetchall()

            key = f"{resource_id[:12]}::{metric_name}"
            result[key] = [
                (r["collected_at"], float(r["metric_value"]))
                for r in rows
            ]

        return result

    # ── Summary / Actions ──────────────────────────────────────────────────────

    def _generate_summary(
        self,
        trigger_row: Optional[sqlite3.Row],
        root_cause: Optional[CausalCandidate],
        cascading: List[CausalCandidate],
        errors: List[ErrorLogEntry],
    ) -> Tuple[str, List[str]]:
        trigger_name = trigger_row["resource_name"] if trigger_row else "unknown resource"
        trigger_metric = trigger_row["metric_name"] if trigger_row else "unknown metric"

        if root_cause:
            rc_desc = (
                f"The analysis identified {root_cause.resource_name} ({root_cause.resource_type}) "
                f"as the most likely root cause. Its {root_cause.metric_name} deviated "
                f"{root_cause.deviation_pct:.0f}% from baseline approximately "
                f"{abs(root_cause.time_offset_seconds / 60):.1f} minutes before the primary anomaly "
                f"was triggered on {trigger_name}/{trigger_metric}."
            )
        else:
            rc_desc = (
                f"No clear upstream root cause was identified in the {self.LOOKBACK_MINUTES}-minute "
                f"lookback window. The anomaly on {trigger_name}/{trigger_metric} may be an "
                f"originating event rather than a downstream effect."
            )

        cascade_desc = ""
        if cascading:
            affected = ", ".join(f"{c.resource_name}/{c.metric_name}" for c in cascading[:3])
            cascade_desc = f" Cascading effects were observed on: {affected}."

        error_desc = ""
        if errors:
            error_desc = (
                f" {len(errors)} error log entries were found in the incident window, "
                f"including errors on {', '.join({e.resource_name for e in errors[:3]})}."
            )

        summary = rc_desc + cascade_desc + error_desc

        actions: List[str] = []

        if root_cause:
            rt = root_cause.resource_type.lower()
            if "ec2" in rt or "vm" in rt or "gce" in rt:
                actions += [
                    f"Investigate CPU/memory pressure on {root_cause.resource_name}",
                    "Check for runaway processes or scheduled jobs that triggered at this time",
                    "Consider auto-scaling policy review if CPU was the trigger",
                ]
            elif "rds" in rt or "sql" in rt or "database" in rt:
                actions += [
                    f"Review slow query logs on {root_cause.resource_name}",
                    "Check for lock contention or missing indexes",
                    "Verify connection pool limits and current active connections",
                ]
            elif "lambda" in rt or "function" in rt:
                actions += [
                    f"Check Lambda cold-start rate and concurrency limits for {root_cause.resource_name}",
                    "Review function timeout settings and memory allocation",
                    "Check downstream dependencies called by this function",
                ]
            elif "elb" in rt or "alb" in rt or "load" in rt:
                actions += [
                    f"Review backend target health on {root_cause.resource_name}",
                    "Check for 5xx error spikes and identify failing targets",
                ]
            else:
                actions.append(f"Investigate {root_cause.metric_name} on {root_cause.resource_name}")

        if errors:
            actions.append("Review error logs in the incident window (included in this report)")
        if cascading:
            actions.append("Validate that cascading resources have returned to normal baseline values")
        actions.append("Acknowledge this anomaly in the dashboard once remediation is confirmed")

        return summary, actions

    def _compute_confidence(
        self,
        root_cause: Optional[CausalCandidate],
        all_candidates: List[CausalCandidate],
        errors: List[ErrorLogEntry],
    ) -> float:
        if root_cause is None:
            return 0.2

        score = root_cause.correlation_score
        if errors:
            score = min(score + 0.10, 1.0)

        if len(all_candidates) > 1:
            sorted_c = sorted(all_candidates, key=lambda c: c.correlation_score, reverse=True)
            if len(sorted_c) > 1:
                gap = sorted_c[0].correlation_score - sorted_c[1].correlation_score
                score = min(score + gap * 0.3, 1.0)

        return round(score, 2)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_ts(ts: str) -> datetime:
    ts = ts.replace("Z", "+00:00")
    if "+" not in ts[10:] and len(ts) == 19:
        ts += "+00:00"
    return datetime.fromisoformat(ts)