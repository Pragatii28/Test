"""
collectors/models.py
────────────────────
Shared data models for all collector plugins.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class MetricPoint:
    timestamp: str
    cloud: str
    region: str
    resource_type: str
    resource_id: str
    resource_name: str
    metric_name: str
    metric_value: float
    metric_unit: str
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class LogEntry:
    timestamp: str
    cloud: str
    region: str
    resource_type: str
    resource_id: str
    resource_name: str
    log_level: str
    message: str
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass
class HealthScore:
    """Rolled-up health signal per resource for the remediation engine."""
    timestamp: str
    cloud: str
    region: str
    resource_type: str
    resource_id: str
    resource_name: str
    score: float          # 0.0 (critical) → 1.0 (healthy)
    status: str           # "healthy" | "degraded" | "critical"
    signals: Dict[str, Any] = field(default_factory=dict)   # contributing metrics
    labels: Dict[str, str]  = field(default_factory=dict)