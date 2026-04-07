"""
main.py — Self-Healing Infrastructure entry point

Phases wired here:
  Phase 1: Telemetry Collection   (orchestrator.py)
  Phase 2: Anomaly Detection      (anomaly_detection.py)
  Phase 3: Root Cause Analysis    (root_cause_analysis.py)
  Phase 4: Decision Engine        (decision_engine.py)

Run modes:
  python main.py                  → continuous (all phases, every COLLECTOR_INTERVAL seconds)
  COLLECTOR_MODE=once python main.py → single cycle then exit

Environment variables:
  COLLECTOR_INTERVAL   seconds between full cycles (default: 300)
  COLLECTOR_MODE       continuous | once
  DECISION_DRY_RUN     true | false  (default: true — log only, no cloud API calls)
  DECISION_MIN_CONFIDENCE  0.0-1.0  (default: 0.65)
  SLACK_WEBHOOK_URL    optional Slack incoming webhook for alerts + decisions
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime

import yaml

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s [main] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler("logs/main.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("main")

CONFIG_PATH      = os.getenv("CONFIG_PATH", "config/cloud_observability.yaml")
INTERVAL         = int(os.getenv("COLLECTOR_INTERVAL", "300"))
MODE             = os.getenv("COLLECTOR_MODE", "continuous")
ANOMALY_INTERVAL = int(os.getenv("DETECTOR_INTERVAL", "60"))
RCA_EVERY_N      = max(1, INTERVAL // max(ANOMALY_INTERVAL, 1))  # run RCA every N anomaly cycles


def _load_config():
    try:
        with open(CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except FileNotFoundError:
        log.warning(f"Config not found at {CONFIG_PATH!r}. Using defaults.")
        return {}


def run_cycle(
    orchestrator,
    anomaly_runner,
    rca_runner,
    decision_runner,
    cycle: int,
) -> None:
    """Execute one full pipeline cycle."""
    log.info(f"\n{'='*70}")
    log.info(f"CYCLE #{cycle} — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f"{'='*70}")

    # ── Phase 1: Telemetry Collection ────────────────────────────────────────
    log.info("[Phase 1] Telemetry collection starting...")
    try:
        summary = orchestrator.run_once()
        total_metrics = summary.get("total_metrics", 0)
        log.info(f"[Phase 1] Complete — {total_metrics} metrics collected")
    except Exception as e:
        log.error(f"[Phase 1] Collection failed: {e}", exc_info=True)

    # ── Phase 2: Anomaly Detection ───────────────────────────────────────────
    log.info("[Phase 2] Anomaly detection starting...")
    anomalies = []
    try:
        anomalies = anomaly_runner()
        log.info(f"[Phase 2] Complete — {len(anomalies)} anomalies detected")
    except Exception as e:
        log.error(f"[Phase 2] Anomaly detection failed: {e}", exc_info=True)

    if not anomalies:
        log.info("[Phase 3+4] Skipping — no anomalies to analyze")
        return

    # ── Phase 3: Root Cause Analysis ─────────────────────────────────────────
    log.info("[Phase 3] Root cause analysis starting...")
    rca_results = []
    try:
        rca_results = rca_runner(anomalies)
        log.info(f"[Phase 3] Complete — {len(rca_results)} RCA results")
        for r in rca_results:
            log.info(
                f"  → [{r.severity.upper()}] {r.category.value} | "
                f"root={r.root_resource_name} | conf={r.confidence:.0%}"
            )
    except Exception as e:
        log.error(f"[Phase 3] RCA failed: {e}", exc_info=True)

    if not rca_results:
        log.info("[Phase 4] Skipping — no RCA results to decide on")
        return

    # ── Phase 4: Decision Engine ──────────────────────────────────────────────
    log.info("[Phase 4] Decision engine starting...")
    try:
        decisions = decision_runner(rca_results)
        log.info(f"[Phase 4] Complete — {len(decisions)} decisions made")
        for d in decisions:
            log.info(
                f"  → [{d.outcome.value}] {d.playbook_name} | "
                f"resource={d.resource_name} | reason={d.outcome_reason[:60]}"
            )
    except Exception as e:
        log.error(f"[Phase 4] Decision engine failed: {e}", exc_info=True)


def main() -> None:
    cfg = _load_config()
    dry_run_flag = "DRY RUN" if os.getenv("DECISION_DRY_RUN", "true").lower() != "false" else "LIVE"

    log.info("=" * 70)
    log.info("SELF-HEALING INFRASTRUCTURE v1.0")
    log.info(f"  Config     : {CONFIG_PATH}")
    log.info(f"  Mode       : {MODE}")
    log.info(f"  Interval   : {INTERVAL}s")
    log.info(f"  Decision   : {dry_run_flag}")
    log.info("=" * 70)

    # ── Phase 1: Orchestrator ─────────────────────────────────────────────────
    from orchestrator import MultiCloudOrchestrator
    orchestrator = MultiCloudOrchestrator(CONFIG_PATH)

    # ── Phase 2: Anomaly detection ────────────────────────────────────────────
    from anomaly_detection import (
        MetricsReader, ModelRegistry, ContinuousTrainer,
        _load_detection_config, run_detection,
    )
    _load_detection_config(CONFIG_PATH)
    reader  = MetricsReader(CONFIG_PATH)
    registry = ModelRegistry()
    trainer  = ContinuousTrainer(registry)

    def _run_anomaly_detection():
        return run_detection(reader, trainer)

    # ── Phase 3: RCA ──────────────────────────────────────────────────────────
    from root_cause_analysis import RCAEngine, RCAStore
    rca_engine = RCAEngine(CONFIG_PATH)
    rca_store  = RCAStore(CONFIG_PATH)

    def _run_rca(anomalies):
        results = rca_engine.analyze(anomalies)
        rca_store.save_all(results)
        return results

    # ── Phase 4: Decision Engine ──────────────────────────────────────────────
    from decision_engine import DecisionEngine
    decision_engine = DecisionEngine(CONFIG_PATH)

    def _run_decision(rca_results):
        decisions = decision_engine.decide(rca_results)
        # Mark decisions taken in RCA store
        for d in decisions:
            rca_store.mark_decision_taken(d.rca_id, d.outcome.value)
        return decisions

    # ── Run ───────────────────────────────────────────────────────────────────
    if MODE == "once":
        run_cycle(orchestrator, _run_anomaly_detection, _run_rca, _run_decision, cycle=1)
        return

    cycle = 0
    try:
        while True:
            cycle += 1
            t0 = time.time()
            try:
                run_cycle(
                    orchestrator,
                    _run_anomaly_detection,
                    _run_rca,
                    _run_decision,
                    cycle,
                )
            except Exception as e:
                log.error(f"Cycle #{cycle} failed: {e}", exc_info=True)

            elapsed = time.time() - t0
            sleep = max(0, INTERVAL - elapsed)
            log.info(f"Cycle took {elapsed:.1f}s. Next in {sleep:.0f}s")
            if sleep > 0:
                time.sleep(sleep)

    except KeyboardInterrupt:
        log.info("\nGraceful shutdown.")


if __name__ == "__main__":
    main()