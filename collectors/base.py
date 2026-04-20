"""
collectors/base.py
──────────────────
Abstract base class for all cloud collector plugins.
"""
from __future__ import annotations

import re
from abc import ABC, abstractmethod
from typing import Any, Dict, List

from collectors.models import LogEntry, MetricPoint


class CloudCollectorPlugin(ABC):

    def __init__(self, name: str, config: Dict):
        self.name   = name
        self.config = config

    @abstractmethod
    def discover_resources(self) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def collect_metrics(self, resources: List[Dict]) -> List[MetricPoint]:
        ...

    @abstractmethod
    def collect_logs(self, resources: List[Dict]) -> List[LogEntry]:
        ...

    # ── shared helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _detect_log_level(message: str) -> str:
        msg = message.upper()
        for level in ("CRITICAL", "FATAL", "ERROR", "WARN", "WARNING",
                      "INFO", "DEBUG", "TRACE"):
            if level in msg:
                return level.replace("WARNING", "WARN").replace("FATAL", "CRITICAL")
        return "INFO"