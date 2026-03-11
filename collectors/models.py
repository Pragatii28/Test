"""
collectors/models.py
────────────────────
Shared, cloud-agnostic data models used across all plugins.
No cloud SDK types leak into this file.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass
class MetricPoint:
    timestamp:     str
    cloud:         str          # "aws" | "azure" | "gcp"
    region:        str
    resource_type: str          # e.g. "ec2", "virtual_machine", "gce_instance"
    resource_id:   str
    resource_name: str
    metric_name:   str          # normalised snake_case name
    metric_value:  float
    metric_unit:   str
    labels:        Dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {"pillar": "metrics", **self.__dict__}


@dataclass
class LogEntry:
    timestamp:     str
    cloud:         str
    region:        str
    resource_type: str
    resource_id:   str
    resource_name: str
    log_level:     str
    message:       str
    labels:        Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict:
        return {"pillar": "logs", **self.__dict__}
