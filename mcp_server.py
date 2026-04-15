"""
mcp_server.py
─────────────
MCP Server for Infra Healing Tool.

Run it: python mcp_server.py
"""

import os
import sqlite3
import json
import statistics
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from mcp.server.fastmcp import FastMCP

# ── Config ─────────────────────────────────────────────────────────
# ───────────
DB_PATH = os.getenv("DB_PATH", "observability_data/metrics.db")

mcp = FastMCP("InfraHealingServer")

# ── Helper: connect to YOUR existing SQLite DB ────────────────────────────────
def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── TOOL 1: Get recent anomalies ──────────────────────────────────────────────
@mcp.tool()
def get_recent_anomalies(hours: int = 24, severity: str = "") -> List[Dict]:
    """
    Get anomalies detected in the last N hours from the database.
    severity: 'critical', 'warning', or '' for all.
    """
    conn = get_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    query = "SELECT * FROM anomalies WHERE detected_at >= ?"
    params = [cutoff]
    if severity:
        query += " AND severity = ?"
        params.append(severity)
    query += " ORDER BY detected_at DESC LIMIT 50"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── TOOL 2: Get metric history for a resource ─────────────────────────────────
@mcp.tool()
def get_metric_history(resource_id: str, metric_name: str, hours: int = 2) -> List[Dict]:
    """
    Get historical metric values for a specific resource and metric.
    Example: resource_id='i-1234', metric_name='cpu_utilization_percent'
    """
    conn = get_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    rows = conn.execute(
        "SELECT collected_at, metric_value, metric_unit FROM metrics "
        "WHERE resource_id=? AND metric_name=? AND collected_at>=? "
        "ORDER BY collected_at ASC",
        (resource_id, metric_name, cutoff)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── TOOL 3: Compute confidence score dynamically ──────────────────────────────
@mcp.tool()
def compute_confidence_score(resource_id: str, metric_name: str, current_value: float) -> Dict:
    """
    Compute a dynamic confidence score for an anomaly.
    Uses historical data from your DB — NOT a static value.
    Returns score 0.0-1.0 plus a breakdown of contributing factors.
    """
    conn = get_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()

    # Factor 1: How far is current value from historical average?
    rows = conn.execute(
        "SELECT metric_value FROM metrics WHERE resource_id=? AND metric_name=? AND collected_at>=?",
        (resource_id, metric_name, cutoff)
    ).fetchall()
    values = [r[0] for r in rows]

    severity_score = 0.0
    if len(values) >= 5:
        avg = statistics.mean(values)
        std = statistics.stdev(values) if len(values) > 1 else 1.0
        z_score = abs(current_value - avg) / max(std, 0.001)
        severity_score = min(z_score / 5.0, 1.0)  # normalize to 0-1

    # Factor 2: How many times has this anomaly fired recently?
    recurrence_cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    recurrence_count = conn.execute(
        "SELECT COUNT(*) FROM anomalies WHERE resource_id=? AND metric_name=? AND detected_at>=?",
        (resource_id, metric_name, recurrence_cutoff)
    ).fetchone()[0]
    recurrence_score = min(recurrence_count / 5.0, 1.0)

    # Factor 3: Is this a critical metric type?
    critical_metrics = {"cpu_utilization_percent", "status_check_failed",
                        "errors_total", "unhealthy_host_count", "disk_queue_depth"}
    criticality_score = 1.0 if metric_name in critical_metrics else 0.5

    conn.close()

    # Weighted final score
    final_score = round(
        (severity_score * 0.5) +
        (recurrence_score * 0.3) +
        (criticality_score * 0.2),
        3
    )

    return {
        "confidence_score": final_score,
        "factors": {
            "severity_deviation": round(severity_score, 3),
            "recurrence_in_24h": recurrence_count,
            "recurrence_score": round(recurrence_score, 3),
            "is_critical_metric": metric_name in critical_metrics,
            "data_points_used": len(values)
        },
        "interpretation": (
            "HIGH confidence — strong anomaly signal" if final_score > 0.7
            else "MEDIUM confidence — monitor closely" if final_score > 0.4
            else "LOW confidence — may be a false positive"
        )
    }


# ── TOOL 4: Get RCA data (dynamic, not static!) ───────────────────────────────
@mcp.tool()
def get_rca_data(resource_id: str, metric_name: str) -> Dict:
    """
    Fetch real data to support Root Cause Analysis.
    Returns recent metric trend, anomaly history, and correlated metrics.
    This is what makes RCA dynamic instead of returning static text.
    """
    conn = get_db()
    cutoff_2h = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    cutoff_24h = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()

    # Recent values for this metric
    recent = conn.execute(
        "SELECT collected_at, metric_value FROM metrics "
        "WHERE resource_id=? AND metric_name=? AND collected_at>=? ORDER BY collected_at ASC",
        (resource_id, metric_name, cutoff_2h)
    ).fetchall()

    # Past anomalies for this resource (all metrics)
    past_anomalies = conn.execute(
        "SELECT metric_name, severity, reason, detected_at FROM anomalies "
        "WHERE resource_id=? AND detected_at>=? ORDER BY detected_at DESC LIMIT 10",
        (resource_id, cutoff_24h)
    ).fetchall()

    # All metrics for this resource right now (to find correlated issues)
    all_metrics = conn.execute(
        "SELECT metric_name, metric_value, collected_at FROM metrics "
        "WHERE resource_id=? AND collected_at>=? "
        "GROUP BY metric_name ORDER BY metric_name",
        (resource_id, cutoff_2h)
    ).fetchall()

    # Trend: is value increasing, decreasing, or flat?
    trend = "insufficient data"
    if len(recent) >= 3:
        vals = [r[1] for r in recent]
        first_half = statistics.mean(vals[:len(vals)//2])
        second_half = statistics.mean(vals[len(vals)//2:])
        diff_pct = ((second_half - first_half) / max(abs(first_half), 0.001)) * 100
        if diff_pct > 10:
            trend = f"INCREASING (+{diff_pct:.1f}% in last 2h)"
        elif diff_pct < -10:
            trend = f"DECREASING ({diff_pct:.1f}% in last 2h)"
        else:
            trend = f"STABLE (±{abs(diff_pct):.1f}%)"

    # Relationships
    deps: List[str] = []
    try:
        sql = "SELECT labels FROM metrics WHERE resource_id = ? ORDER BY collected_at DESC LIMIT 1"
        row = conn.execute(sql, (resource_id,)).fetchone()
        if row and row["labels"]:
            labels = json.loads(row["labels"])
            if "depends_on" in labels:
                deps.extend([d.strip() for d in labels["depends_on"].split(",") if d.strip()])
            if "vpc_id" in labels:
                deps.append(f"vpc:{labels['vpc_id']}")
    except Exception:
        pass

    conn.close()

    return {
        "resource_id": resource_id,
        "metric_name": metric_name,
        "data_points_last_2h": len(recent),
        "trend": trend,
        "recent_values": [{"time": r[0], "value": r[1]} for r in recent[-10:]],
        "past_anomalies_24h": [dict(r) for r in past_anomalies],
        "all_current_metrics": [
            {"metric": r[0], "value": r[1], "at": r[2]} for r in all_metrics
        ],
        "discovered_dependencies": deps,
        "rca_prompt_hint": (
            f"The metric '{metric_name}' on '{resource_id}' is {trend}. "
            f"Known dependencies: {', '.join(deps) if deps else 'none'}. "
            f"Use the correlated metrics to identify if this is isolated or systemic."
        )
    }


# ── TOOL 5: List all resources currently in DB ────────────────────────────────
@mcp.tool()
def list_resources(cloud: str = "", resource_type: str = "") -> List[Dict]:
    """
    List all resources tracked in your observability DB.
    Filter by cloud ('aws', 'azure', 'gcp') or resource_type ('ec2', 'rds', etc.)
    """
    conn = get_db()
    query = """
        SELECT DISTINCT cloud, region, resource_type, resource_id, resource_name,
               MAX(collected_at) as last_seen
        FROM metrics
    """
    params = []
    where = []
    if cloud:
        where.append("cloud=?"); params.append(cloud)
    if resource_type:
        where.append("resource_type=?"); params.append(resource_type)
    if where:
        query += " WHERE " + " AND ".join(where)
    query += " GROUP BY resource_id ORDER BY cloud, resource_type, resource_name"

    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ── TOOL 6: Suggest remediation based on anomaly type ────────────────────────
@mcp.tool()
def suggest_remediation(metric_name: str, severity: str, current_value: float) -> Dict:
    """
    Suggest a remediation action based on the anomaly type.
    Returns action type, description, and whether it is safe to auto-execute.
    """
    suggestions = {
        "cpu_utilization_percent": {
            "action": "scale_up_or_investigate_process",
            "description": "CPU is high. Check for runaway processes. If on EC2, consider scaling vertically or horizontally.",
            "auto_safe": False,
            "commands": ["top -b -n 1 | head -20", "ps aux --sort=-%cpu | head -10"]
        },
        "database_connections": {
            "action": "check_connection_pool",
            "description": "Too many DB connections. Check app connection pool settings and idle connections.",
            "auto_safe": False,
            "commands": ["SHOW PROCESSLIST;", "SELECT count(*) FROM information_schema.processlist;"]
        },
        "errors_total": {
            "action": "check_logs_and_restart",
            "description": "Lambda errors spiking. Check CloudWatch logs for stack traces.",
            "auto_safe": False,
            "commands": ["aws logs tail /aws/lambda/{function} --follow"]
        },
        "disk_queue_depth": {
            "action": "check_io_bottleneck",
            "description": "Disk I/O queue is growing. Check for heavy read/write operations.",
            "auto_safe": False,
            "commands": ["iostat -x 1 5", "iotop -o"]
        },
        "status_check_failed": {
            "action": "restart_instance_or_alert",
            "description": "EC2 status check failed. This usually requires manual intervention or instance restart.",
            "auto_safe": False,
            "commands": ["aws ec2 reboot-instances --instance-ids {resource_id}"]
        }
    }

    default = {
        "action": "investigate_manually",
        "description": f"Anomaly detected on '{metric_name}' with value {current_value}. Manual investigation required.",
        "auto_safe": False,
        "commands": []
    }

    result = suggestions.get(metric_name, default).copy()
    result["metric_name"] = metric_name
    result["severity"] = severity
    result["current_value"] = current_value
    result["confidence_threshold_for_auto"] = "0.85+ required for any auto-remediation"
    return result

@mcp.tool()
def get_resource_relationships(resource_id: str) -> Dict:
    """
    Fetch automatically discovered relationships for a resource (e.g. LB targets).
    """
    conn = get_db()
    row = conn.execute(
        "SELECT labels FROM metrics WHERE resource_id=? ORDER BY collected_at DESC LIMIT 1",
        (resource_id,)
    ).fetchone()
    conn.close()
    
    if not row or not row["labels"]:
        return {"resource_id": resource_id, "relationships": {}}
    
    labels = json.loads(row["labels"])
    return {
        "resource_id": resource_id,
        "relationships": {
            "depends_on": [d.strip() for d in labels.get("depends_on", "").split(",") if d.strip()],
            "vpc_id": labels.get("vpc_id"),
            "subnets": [s.strip() for s in labels.get("subnets", "").split(",") if s.strip()],
            "cluster_id": labels.get("cluster_id")
        }
    }
@mcp.tool()
def get_regional_context(region: str, trigger_resource_id: str, minutes: int = 10) -> Dict:
    """
    Fetch all anomalies in the same region within the last N minutes.
    Gives Gemini cross-resource context for RCA.
    """
    conn = get_db()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()

    rows = conn.execute(
        """
        SELECT resource_id, resource_name, resource_type,
               metric_name, current_value, severity, detected_at, reason
        FROM anomalies
        WHERE region = ?
          AND detected_at >= ?
          AND resource_id != ?
        ORDER BY detected_at DESC
        LIMIT 20
        """,
        (region, cutoff, trigger_resource_id)
    ).fetchall()
    conn.close()

    return {
        "region": region,
        "window_minutes": minutes,
        "other_anomalies_in_region": [dict(r) for r in rows],
        "note": (
            "Cross-resource anomalies in the same region may be related. "
            "Check for discovered dependencies (depends_on) to find causal links. "
            "Note: CPU spikes on a DB can cause latency spikes on a web server in the same VPC."
        )
    }

# ── Entry point ────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print(f"Starting MCP server... DB: {DB_PATH}")
    mcp.run()
