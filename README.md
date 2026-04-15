# Self-Healing Infrastructure 

A continuous operations platform that collects telemetry across multi-cloud environments (AWS, Azure, GCP), performs AI-driven anomaly detection, executes Root Cause Analysis (RCA), and automatically proposes remediation actions via a Decision Engine. 

It also includes a real-time CloudOps Infrastructure Health Dashboard and exposes a Prometheus `/metrics` endpoint.

---

## Architecture phases

The system runs continuously through the following pipeline (`main.py`):

1. **Phase 1: Telemetry Collection** (`orchestrator.py`)
   - Collects metrics and logs from cloud providers spanning EC2, RDS, Lambda, etc.
   - Stores data locally (SQLite) or remotely (PostgreSQL / TimescaleDB).
2. **Phase 2: Anomaly Detection** (`anomaly_detection.py`)
   - Continuously trains models in the background to detect threshold deviations.
3. **Phase 3: Root Cause Analysis** (`root_cause_analysis.py`)
   - Maps anomalies to root resources and scores confidences. Outputs RCA reports.
4. **Phase 4: Decision Engine** (`decision_engine.py`)
   - Evaluates RCA outcomes to decide upon standard remediation playbooks.
5. **Phase 5: Remediation Executor** (`remediation_executor.py`)
   - Executes structural fixes or alerts via webhook. Currently supports safe DRY RUN modes.

---

## Directory layout

```text
.
├── main.py                        ← Continuous execution entry point
├── dashboard.py                   ← Run proxy UI (http://localhost:5000)
├── orchestrator.py                ← Collector integration and routing
├── anomaly_detection.py           ← Phase 2: Anomaly Detection
├── root_cause_analysis.py         ← Phase 3: RCA
├── decision_engine.py             ← Phase 4: Decision Engine
├── remediation_executor.py        ← Phase 5: Remediation Executor
├── mcp_server.py                  ← MCP Server integration
├── requirements.txt
├── config/
│   └── cloud_observability.yaml   ← All settings
│
├── collectors/                    ← Cloud integration plugins (AWS, Azure, GCP)
├── storage/                       ← Database adapters
├── exporters/                     ← Prometheus gauge exporter
├── logs/                          ← Core execution logs
├── models/                        ← Pickled ML models for tracking anomalies
├── rca_reports/                   ← Persistent RCA analysis artifacts
└── observability_data/            ← Local metrics storage (SQLite)
```

---

## Quick start

### 1. Install dependencies

```bash
# Core requirements
pip install -r requirements.txt
```

### 2. Configure

Edit `config/cloud_observability.yaml` (auto-generated on first run):

```yaml
clouds:
  aws:
    enabled: true
    regions: [us-east-1]
    ...
storage:
  backend: sqlite 
```

### 3. Run the orchestration pipeline

```bash
# Continuous self-healing loop (default interval = 300 s)
python main.py

# One-shot diagnostic execution
COLLECTOR_MODE=once python main.py
```

### 4. Run the Dashboard

In a separate terminal, launch the web dashboard:

```bash
python dashboard.py
```
View the dashboard at `http://localhost:5000`.

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_PATH` | `config/cloud_observability.yaml` | configuration file path |
| `COLLECTOR_MODE` | `continuous` | `continuous` or `once` |
| `COLLECTOR_INTERVAL` | `300` | seconds between collection cycles (Phase 1-5) |
| `DECISION_DRY_RUN` | `true` | `true` or `false` — log only vs active remediation |
| `DECISION_MIN_CONFIDENCE` | `0.65` | Minimum confidence score for remediation actions |
| `AWS_REGIONS` | `us-east-1` | comma-separated region list |
| `AWS_ACCESS_KEY_ID` | — | AWS credentials for collector plugin |
| `AWS_SECRET_ACCESS_KEY` | — | AWS credentials for collector plugin |