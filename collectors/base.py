"""
collectors/base.py
──────────────────
Abstract base class that every cloud plugin must implement.
"""
from __future__ import annotations

import time
import threading
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, Dict, List

from collectors.models import LogEntry, MetricPoint


class CloudCollectorPlugin(ABC):
    def __init__(self, cloud_name: str, config: Dict):
        self.cloud_name = cloud_name
        self.config     = config
        self._lock      = threading.Lock()

    @property
    def name(self) -> str:
        return self.cloud_name

    @abstractmethod
    def discover_resources(self) -> List[Dict[str, Any]]: ...

    @abstractmethod
    def collect_metrics(self, resources: List[Dict]) -> List[MetricPoint]: ...

    @abstractmethod
    def collect_logs(self, resources: List[Dict]) -> List[LogEntry]: ...

    # ── helpers shared across all plugins ─────────────────────────────────────

    @staticmethod
    def _detect_log_level(text: str) -> str:
        t = text.upper()
        for lvl in ("CRITICAL", "FATAL", "ERROR", "WARN", "WARNING", "DEBUG"):
            if lvl in t:
                return lvl.replace("FATAL", "CRITICAL").replace("WARNING", "WARN")
        return "INFO"

    @staticmethod
    def _utcnow() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _retry(fn, attempts: int = 3, delay: float = 2.0):
        last_exc = None
        for i in range(attempts):
            try:
                return fn()
            except Exception as exc:
                last_exc = exc
                if i < attempts - 1:
                    time.sleep(delay * (i + 1))
        raise last_exc
