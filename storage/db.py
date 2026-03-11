"""
storage/db.py
─────────────
Database storage backend for metrics and logs.

Supports three backends selected via config / environment variable:
  - sqlite    (default, zero dependencies, great for development)
  - postgres  (pip install psycopg2-binary)
  - timescaledb  (same driver as postgres — TimescaleDB is a PG extension)

Schema (identical for all backends):
  Table: metrics
  Table: logs

TimescaleDB hypertables are created automatically if the extension is enabled.

Usage in config YAML:
  storage:
    backend: postgres          # sqlite | postgres | timescaledb
    sqlite:
      path: observability_data/metrics.db
    postgres:
      host:     localhost
      port:     5432
      dbname:   observability
      user:     postgres
      password: ""
      # OR use DSN:
      # dsn: "postgresql://user:pass@host:5432/dbname"
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional

from collectors.models import LogEntry, MetricPoint

logger = logging.getLogger("collector.storage")


# ══════════════════════════════════════════════════════════════════════════════
# Base
# ══════════════════════════════════════════════════════════════════════════════

class BaseStorage:
    def save(self, metrics: List[MetricPoint], logs: List[LogEntry]) -> None:
        raise NotImplementedError

    def close(self) -> None:
        pass


# ══════════════════════════════════════════════════════════════════════════════
# SQLite
# ══════════════════════════════════════════════════════════════════════════════

class SQLiteStorage(BaseStorage):
    """
    Zero-dependency local storage. Perfect for development and single-node use.
    Data is stored in a single .db file; WAL mode enables concurrent readers.
    """

    _DDL_METRICS = """
    CREATE TABLE IF NOT EXISTS metrics (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        collected_at  TEXT    NOT NULL,
        cloud         TEXT    NOT NULL,
        region        TEXT    NOT NULL,
        resource_type TEXT    NOT NULL,
        resource_id   TEXT    NOT NULL,
        resource_name TEXT    NOT NULL,
        metric_name   TEXT    NOT NULL,
        metric_value  REAL    NOT NULL,
        metric_unit   TEXT    NOT NULL,
        labels        TEXT    NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_metrics_cloud_type
        ON metrics(cloud, resource_type, collected_at);
    """

    _DDL_LOGS = """
    CREATE TABLE IF NOT EXISTS logs (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        collected_at  TEXT    NOT NULL,
        cloud         TEXT    NOT NULL,
        region        TEXT    NOT NULL,
        resource_type TEXT    NOT NULL,
        resource_id   TEXT    NOT NULL,
        resource_name TEXT    NOT NULL,
        log_level     TEXT    NOT NULL,
        message       TEXT    NOT NULL,
        labels        TEXT    NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_logs_cloud_level
        ON logs(cloud, log_level, collected_at);
    """

    def __init__(self, cfg: Dict):
        import sqlite3
        path = cfg.get("path", "observability_data/metrics.db")
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(self._DDL_METRICS)
        self._conn.executescript(self._DDL_LOGS)
        self._conn.commit()
        logger.info(f"[SQLite] Connected → {path}")

    def save(self, metrics: List[MetricPoint], logs: List[LogEntry]) -> None:
        cur = self._conn.cursor()

        if metrics:
            cur.executemany(
                """INSERT INTO metrics
                   (collected_at, cloud, region, resource_type, resource_id,
                    resource_name, metric_name, metric_value, metric_unit, labels)
                   VALUES (?,?,?,?,?,?,?,?,?,?)""",
                [
                    (m.timestamp, m.cloud, m.region, m.resource_type,
                     m.resource_id, m.resource_name, m.metric_name,
                     m.metric_value, m.metric_unit, json.dumps(m.labels))
                    for m in metrics
                ],
            )

        if logs:
            cur.executemany(
                """INSERT INTO logs
                   (collected_at, cloud, region, resource_type, resource_id,
                    resource_name, log_level, message, labels)
                   VALUES (?,?,?,?,?,?,?,?,?)""",
                [
                    (l.timestamp, l.cloud, l.region, l.resource_type,
                     l.resource_id, l.resource_name, l.log_level,
                     l.message, json.dumps(l.labels))
                    for l in logs
                ],
            )

        self._conn.commit()
        logger.info(f"[SQLite] Saved {len(metrics)} metrics, {len(logs)} log entries")

    def close(self) -> None:
        self._conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# PostgreSQL / TimescaleDB
# ══════════════════════════════════════════════════════════════════════════════

class PostgresStorage(BaseStorage):
    """
    Production-grade backend. Works with vanilla PostgreSQL and TimescaleDB.

    When backend = "timescaledb" the metrics and logs tables are promoted to
    hypertables on first run (requires the TimescaleDB extension to be installed
    and enabled in the target database).

    Connection can be specified via:
      - Individual fields: host / port / dbname / user / password
      - DSN string: dsn: "postgresql://user:pass@host:5432/dbname"
      - Environment: PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD
    """

    _DDL_METRICS = """
    CREATE TABLE IF NOT EXISTS metrics (
        id            BIGSERIAL PRIMARY KEY,
        collected_at  TIMESTAMPTZ NOT NULL,
        cloud         TEXT        NOT NULL,
        region        TEXT        NOT NULL,
        resource_type TEXT        NOT NULL,
        resource_id   TEXT        NOT NULL,
        resource_name TEXT        NOT NULL,
        metric_name   TEXT        NOT NULL,
        metric_value  DOUBLE PRECISION NOT NULL,
        metric_unit   TEXT        NOT NULL,
        labels        JSONB       NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_metrics_ts  ON metrics (collected_at DESC);
    CREATE INDEX IF NOT EXISTS idx_metrics_cls ON metrics (cloud, resource_type, metric_name, collected_at DESC);
    """

    _DDL_LOGS = """
    CREATE TABLE IF NOT EXISTS logs (
        id            BIGSERIAL PRIMARY KEY,
        collected_at  TIMESTAMPTZ NOT NULL,
        cloud         TEXT        NOT NULL,
        region        TEXT        NOT NULL,
        resource_type TEXT        NOT NULL,
        resource_id   TEXT        NOT NULL,
        resource_name TEXT        NOT NULL,
        log_level     TEXT        NOT NULL,
        message       TEXT        NOT NULL,
        labels        JSONB       NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS idx_logs_ts  ON logs (collected_at DESC);
    CREATE INDEX IF NOT EXISTS idx_logs_cls ON logs (cloud, log_level, collected_at DESC);
    """

    _HYPERTABLE_METRICS = """
    SELECT create_hypertable('metrics', 'collected_at', if_not_exists => TRUE,
                              migrate_data => TRUE);
    """
    _HYPERTABLE_LOGS = """
    SELECT create_hypertable('logs', 'collected_at', if_not_exists => TRUE,
                              migrate_data => TRUE);
    """

    def __init__(self, cfg: Dict, timescale: bool = False):
        try:
            import psycopg2
            import psycopg2.extras
            self._psycopg2 = psycopg2
            self._extras   = psycopg2.extras
        except ImportError:
            raise ImportError("PostgreSQL backend requires: pip install psycopg2-binary")

        dsn = (
            cfg.get("dsn")
            or os.getenv("DATABASE_URL", "")
            or self._build_dsn(cfg)
        )
        self._conn = psycopg2.connect(dsn)
        self._conn.autocommit = False
        self._timescale = timescale

        with self._conn.cursor() as cur:
            cur.execute(self._DDL_METRICS)
            cur.execute(self._DDL_LOGS)
            if timescale:
                try:
                    cur.execute(self._HYPERTABLE_METRICS)
                    cur.execute(self._HYPERTABLE_LOGS)
                    logger.info("[TimescaleDB] Hypertables created / verified")
                except Exception as exc:
                    logger.warning(f"[TimescaleDB] Could not create hypertables: {exc}")
        self._conn.commit()
        tag = "TimescaleDB" if timescale else "PostgreSQL"
        logger.info(f"[{tag}] Connected → {self._safe_dsn(dsn)}")

    @staticmethod
    def _build_dsn(cfg: Dict) -> str:
        host     = cfg.get("host",     os.getenv("PGHOST",     "localhost"))
        port     = cfg.get("port",     os.getenv("PGPORT",     "5432"))
        dbname   = cfg.get("dbname",   os.getenv("PGDATABASE", "observability"))
        user     = cfg.get("user",     os.getenv("PGUSER",     "postgres"))
        password = cfg.get("password", os.getenv("PGPASSWORD", ""))
        return f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    @staticmethod
    def _safe_dsn(dsn: str) -> str:
        """Redact password from DSN for logging."""
        import re
        return re.sub(r":[^:@]+@", ":***@", dsn)

    def save(self, metrics: List[MetricPoint], logs: List[LogEntry]) -> None:
        try:
            with self._conn.cursor() as cur:
                if metrics:
                    self._extras.execute_values(
                        cur,
                        """INSERT INTO metrics
                           (collected_at, cloud, region, resource_type, resource_id,
                            resource_name, metric_name, metric_value, metric_unit, labels)
                           VALUES %s""",
                        [
                            (m.timestamp, m.cloud, m.region, m.resource_type,
                             m.resource_id, m.resource_name, m.metric_name,
                             m.metric_value, m.metric_unit,
                             self._psycopg2.extras.Json(m.labels))
                            for m in metrics
                        ],
                        page_size=500,
                    )

                if logs:
                    self._extras.execute_values(
                        cur,
                        """INSERT INTO logs
                           (collected_at, cloud, region, resource_type, resource_id,
                            resource_name, log_level, message, labels)
                           VALUES %s""",
                        [
                            (l.timestamp, l.cloud, l.region, l.resource_type,
                             l.resource_id, l.resource_name, l.log_level,
                             l.message, self._psycopg2.extras.Json(l.labels))
                            for l in logs
                        ],
                        page_size=500,
                    )
            self._conn.commit()
            tag = "TimescaleDB" if self._timescale else "PostgreSQL"
            logger.info(f"[{tag}] Saved {len(metrics)} metrics, {len(logs)} log entries")
        except Exception as exc:
            self._conn.rollback()
            logger.error(f"[Postgres] Save failed: {exc}")
            raise

    def close(self) -> None:
        self._conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# Factory
# ══════════════════════════════════════════════════════════════════════════════

def create_storage(storage_cfg: Dict) -> BaseStorage:
    """
    Instantiate the correct storage backend from config.

    storage_cfg example:
        backend: postgres
        postgres:
          host: localhost
          port: 5432
          dbname: observability
          user: postgres
          password: secret
    """
    backend = storage_cfg.get("backend", "sqlite").lower()

    if backend == "sqlite":
        return SQLiteStorage(storage_cfg.get("sqlite", {}))

    if backend in ("postgres", "postgresql"):
        return PostgresStorage(storage_cfg.get("postgres", {}), timescale=False)

    if backend == "timescaledb":
        return PostgresStorage(storage_cfg.get("postgres", {}), timescale=True)

    raise ValueError(
        f"Unknown storage backend {backend!r}. "
        f"Supported: sqlite, postgres, timescaledb"
    )
