# test_phase1.py
import logging
import yaml
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")

from orchestrator import MultiCloudOrchestrator

orchestrator = MultiCloudOrchestrator("config/cloud_observability.yaml")
summary = orchestrator.run_once()

print("\n=== SUMMARY ===")
for cloud, data in summary["clouds"].items():
    print(f"{cloud}: {data}")
print(f"Total metrics : {summary['total_metrics']}")
print(f"Total logs    : {summary['total_logs']}")