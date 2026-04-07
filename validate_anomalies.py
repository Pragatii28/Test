#!/usr/bin/env python3
"""
validate_detector.py
====================
End-to-end validation script that:
  1. Injects a normal baseline (warm-up) for each resource type
  2. Injects each anomaly scenario
  3. Runs anomaly_detection.run_detection()
  4. Checks which anomalies were detected and which weren't
  5. Prints a pass/fail report

Run this to get a quantitative view of:
  - True Positive Rate  (TPR):  anomalies correctly detected
  - False Positive Rate (FPR):  normal traffic flagged as anomaly
  - Algorithm breakdown: which detector caught what

Usage:
    python3 scripts/validate_detector.py --config config/cloud_observability.yaml

Requirements:
    Same as anomaly_detection.py + injector/inject_anomalies.py
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import yaml

# ── Load injector and detector from local paths ───────────────────────────────
def _load_module(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod  = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ── Test cases ────────────────────────────────────────────────────────────────
# (scenario_name, should_detect, expected_algorithm_hints, description)
TEST_CASES: List[Tuple[str, bool, List[str], str]] = [
    # Scenarios that MUST trigger an anomaly
    ("cpu_spike",           True,  ["hard_limit"],      "EC2 CPU > 90% → hard_limit"),
    ("memory_low",          True,  ["hard_limit"],      "EC2 free mem < 100 MB → hard_limit"),
    ("low_storage",         True,  ["hard_limit"],      "RDS free storage < 1 GB → hard_limit"),
    ("db_connections_high", True,  ["hard_limit"],      "RDS connections > 80 → hard_limit"),
    ("db_latency_high",     True,  ["hard_limit"],      "RDS latency > 1s → hard_limit"),
    ("lambda_errors",       True,  ["always_bad"],      "Lambda errors non-zero → always_bad"),
    ("lambda_throttles",    True,  ["always_bad"],      "Lambda throttles non-zero → always_bad"),
    ("lambda_timeout",      True,  ["hard_limit"],      "Lambda duration > 28s → hard_limit"),
    ("alb_5xx",             True,  ["always_bad"],      "ALB 5xx non-zero → always_bad"),
    ("alb_latency",         True,  ["hard_limit"],      "ALB response > 5s → hard_limit"),
    ("alb_unhealthy",       True,  ["always_bad"],      "Unhealthy hosts → always_bad"),

    # Scenarios that must NOT trigger (false positive test)
    ("fp_probe_ec2",        False, [],                  "Normal EC2 — expect SILENCE"),
    ("fp_probe_rds",        False, [],                  "Normal RDS — expect SILENCE"),
]


def run_validation(
    config_path: str,
    detector_path: str = "anomaly_detection.py",
    injector_path: str = "injector/inject_anomalies.py",
    backfill_minutes: int = 90,
) -> Dict[str, Any]:
    """
    Run all test cases and return a report dict.
    """
    # Load modules
    try:
        detector = _load_module("anomaly_detection", detector_path)
        injector = _load_module("inject_anomalies", injector_path)
    except FileNotFoundError as e:
        print(f"ERROR: Could not load module — {e}")
        sys.exit(1)

    # Load config + DB
    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    db_path = (config.get("storage", {})
               .get("sqlite", {})
               .get("path", "observability_data/metrics.db"))
    os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")

    # Pick one resource of each type from config
    resources_by_type: Dict[str, Dict] = {}
    for r in config.get("resources", []):
        rtype = r.get("resource_type", "").lower()
        resources_by_type.setdefault(rtype, r)

    if not resources_by_type:
        print("ERROR: No resources in config. Run terraform and update the config.")
        sys.exit(1)

    # ── Phase 1: Inject warm-up baselines ─────────────────────────────────────
    print("\n" + "═" * 70)
    print("  PHASE 1: Injecting warm-up baselines")
    print("═" * 70)
    warmup_scenarios = {
        "ec2":    "normal_ec2",
        "rds":    "normal_rds",
        "lambda": "normal_lambda",
        "alb":    "normal_alb",
    }
    for rtype, sc_name in warmup_scenarios.items():
        resource = resources_by_type.get(rtype)
        if not resource:
            print(f"  SKIP  {rtype} — no resource in config")
            continue
        sc = injector.SCENARIOS[sc_name].copy()
        sc["_name"] = sc_name
        injector.inject_db(conn, resource, sc, backfill_minutes=backfill_minutes)
        print(f"  OK    {rtype}: {sc_name} ({backfill_minutes}min backfill)")

    # ── Phase 2: Set up detector ───────────────────────────────────────────────
    reader  = detector.MetricsReader(config_path)
    registry = detector.ModelRegistry()
    trainer  = detector.ContinuousTrainer(registry)

    results = []
    passed = failed = 0

    # ── Phase 3: Run each test case ────────────────────────────────────────────
    print("\n" + "═" * 70)
    print("  PHASE 2: Running test cases")
    print("═" * 70)

    for sc_name, should_detect, algo_hints, description in TEST_CASES:
        sc = injector.SCENARIOS.get(sc_name)
        if sc is None:
            print(f"  SKIP  {sc_name} — scenario not found")
            continue

        resource = resources_by_type.get(sc["resource_type"])
        if resource is None:
            print(f"  SKIP  {sc_name} — no {sc['resource_type']} resource in config")
            continue

        # Inject the scenario
        sc_copy = sc.copy()
        sc_copy["_name"] = sc_name
        injector.inject_db(conn, resource, sc_copy, backfill_minutes=0)

        # Run detector on this resource
        detected_anomalies: List[detector.Anomaly] = detector.run_detection(reader, trainer)
        resource_anomalies = [
            a for a in detected_anomalies
            if a.resource_id == resource["resource_id"]
        ]

        detected       = len(resource_anomalies) > 0
        status_correct = detected == should_detect
        algos_found    = list({a.algorithm for a in resource_anomalies})

        # Check algorithm hint if specified
        algo_correct = True
        if should_detect and algo_hints:
            algo_correct = any(
                any(hint in a.algorithm for hint in algo_hints)
                for a in resource_anomalies
            )

        overall_pass = status_correct and algo_correct

        if overall_pass:
            passed += 1
            status = "✅ PASS"
        else:
            failed += 1
            if not status_correct:
                status = "❌ FAIL (detection mismatch)"
            else:
                status = "⚠️  FAIL (wrong algorithm)"

        result = {
            "scenario":       sc_name,
            "description":    description,
            "should_detect":  should_detect,
            "detected":       detected,
            "algorithms":     algos_found,
            "expected_algos": algo_hints,
            "pass":           overall_pass,
            "anomaly_count":  len(resource_anomalies),
            "anomaly_details": [
                {
                    "metric":    a.metric_name,
                    "value":     a.current_value,
                    "severity":  a.severity,
                    "algorithm": a.algorithm,
                    "reason":    a.reason[:80],
                }
                for a in resource_anomalies
            ],
        }
        results.append(result)

        print(f"\n  {status}  [{sc_name}]")
        print(f"    {description}")
        if resource_anomalies:
            for a in resource_anomalies:
                print(f"    → [{a.algorithm}] {a.metric_name} = {a.current_value:.4f} "
                      f"({a.severity}) — {a.reason[:60]}")
        elif should_detect:
            print("    → NO anomaly detected (expected one)")

    # ── Report ─────────────────────────────────────────────────────────────────
    total = passed + failed
    tpr_scenarios = [r for r in results if r["should_detect"]]
    fpr_scenarios = [r for r in results if not r["should_detect"]]

    tpr = sum(1 for r in tpr_scenarios if r["detected"]) / max(len(tpr_scenarios), 1)
    fpr = sum(1 for r in fpr_scenarios if r["detected"]) / max(len(fpr_scenarios), 1)

    report = {
        "timestamp":    datetime.now(timezone.utc).isoformat(),
        "total_tests":  total,
        "passed":       passed,
        "failed":       failed,
        "tpr":          round(tpr, 3),
        "fpr":          round(fpr, 3),
        "results":      results,
    }

    print("\n" + "═" * 70)
    print("  VALIDATION REPORT")
    print("═" * 70)
    print(f"  Total tests : {total}")
    print(f"  Passed      : {passed}")
    print(f"  Failed      : {failed}")
    print(f"  TPR (True Positive Rate)  : {tpr*100:.1f}%  (higher is better)")
    print(f"  FPR (False Positive Rate) : {fpr*100:.1f}%  (lower is better)")

    if fpr > 0:
        print(f"\n  ⚠️  FALSE POSITIVES DETECTED — review sensitivity settings")
    if tpr < 1.0:
        print(f"\n  ⚠️  MISSED DETECTIONS — check hard_limits / always_bad config")
    if tpr == 1.0 and fpr == 0.0:
        print(f"\n  ✅ All tests passed. Detector is working correctly.")

    print("═" * 70)

    # Save JSON report
    report_path = f"logs/validation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"\n  Full report saved → {report_path}")

    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Validate anomaly_detection.py")
    parser.add_argument("--config",    default="config/cloud_observability.yaml")
    parser.add_argument("--detector",  default="anomaly_detection.py")
    parser.add_argument("--injector",  default="injector/inject_anomalies.py")
    parser.add_argument("--backfill",  type=int, default=90,
                        help="Minutes of baseline to inject before anomalies (default 90)")
    args = parser.parse_args()

    run_validation(
        config_path    = args.config,
        detector_path  = args.detector,
        injector_path  = args.injector,
        backfill_minutes = args.backfill,
    )


if __name__ == "__main__":
    main()
