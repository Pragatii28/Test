# Multi-Cloud Observability Collector v6.0

Collects metrics and logs from **AWS**, **Azure**, and **GCP** on a schedule and stores them in a **database** (SQLite, PostgreSQL, or TimescaleDB) while also exposing a **Prometheus `/metrics` endpoint**.

---

## Directory layout

```
.
├── main.py                        ← entry point
├── orchestrator.py                ← wires everything together
├── requirements.txt
├── config/
│   └── cloud_observability.yaml   ← all settings
│
├── collectors/                    ← one sub-package per cloud
│   ├── __init__.py
│   ├── base.py                    ← abstract CloudCollectorPlugin
│   ├── models.py                  ← MetricPoint, LogEntry dataclasses
│   ├── aws/
│   │   ├── __init__.py
│   │   └── plugin.py              ← EC2, RDS, Lambda, ELB, DynamoDB
│   ├── azure/
│   │   ├── __init__.py
│   │   └── plugin.py              ← VMs, Function Apps, SQL, App Services
│   └── gcp/
│       ├── __init__.py
│       └── plugin.py              ← GCE, Cloud Functions, Cloud Run, GKE
│
├── storage/
│   ├── __init__.py
│   └── db.py                      ← SQLite / PostgreSQL / TimescaleDB backends
│
└── exporters/
    ├── __init__.py
    └── prometheus.py              ← Prometheus gauge exporter
```

---

## Quick start

### 1. Install dependencies

```bash
# minimum (AWS + SQLite + Prometheus)
pip install PyYAML boto3 prometheus-client

# all clouds
pip install -r requirements.txt

# PostgreSQL or TimescaleDB backend
pip install psycopg2-binary
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
  backend: sqlite        # sqlite | postgres | timescaledb

  sqlite:
    path: observability_data/metrics.db

  # postgres:
  #   host: localhost
  #   port: 5432
  #   dbname: observability
  #   user: postgres
  #   password: secret
```

### 3. Run

```bash
# continuous (default interval = 300 s)
python main.py

# one-shot
COLLECTOR_MODE=once python main.py

# custom interval
COLLECTOR_INTERVAL=60 python main.py
```

---

## Storage backends

| Backend | Connection | Notes |
|---------|-----------|-------|
| `sqlite` | local file | default, zero extra deps |
| `postgres` | TCP | `pip install psycopg2-binary` |
| `timescaledb` | TCP | same driver; creates hypertables automatically |

### Database schema

Both backends use the same two tables:

**`metrics`**

| column | type | description |
|--------|------|-------------|
| `id` | bigint / rowid | primary key |
| `collected_at` | timestamptz | ISO-8601 timestamp from the cloud API |
| `cloud` | text | aws / azure / gcp |
| `region` | text | cloud region |
| `resource_type` | text | ec2, rds, lambda, … |
| `resource_id` | text | cloud resource ID |
| `resource_name` | text | human-friendly name |
| `metric_name` | text | snake_case metric name |
| `metric_value` | float | numeric value |
| `metric_unit` | text | percent, bytes, count, … |
| `labels` | JSON | extra key-value pairs |

**`logs`** — same shape, with `log_level` and `message` instead of metric columns.

### Useful queries

```sql
-- latest CPU across all EC2 instances
SELECT resource_name, metric_value, collected_at
FROM   metrics
WHERE  resource_type = 'ec2' AND metric_name = 'cpu_utilization_percent'
ORDER  BY collected_at DESC
LIMIT  20;

-- error log entries in the last hour
SELECT cloud, resource_name, message, collected_at
FROM   logs
WHERE  log_level = 'ERROR'
  AND  collected_at > NOW() - INTERVAL '1 hour'
ORDER  BY collected_at DESC;
```

---

## What metrics are collected?

| Cloud | Resource | Key metrics |
|-------|----------|-------------|
| AWS | EC2 | CPU, network in/out, disk R/W, status checks |
| AWS | RDS | CPU, connections, IOPS, latency, free storage |
| AWS | Lambda | invocations, errors, duration, throttles |
| AWS | ELB (ALB) | requests, response time, 2xx/4xx/5xx, healthy hosts |
| AWS | DynamoDB | read/write capacity, latency, throttles |
| Azure | Virtual Machine | CPU, network in/out, disk R/W |
| Azure | Function App | requests, errors, response time |
| Azure | SQL Database | CPU, DTU, data IO, connections |
| Azure | App Service | requests, errors, response time, CPU time |
| GCP | GCE Instance | CPU, network in/out, disk R/W |
| GCP | Cloud Function | executions, execution time, active instances |
| GCP | Cloud Run | requests, latency, CPU |
| GCP | GKE Cluster | CPU, memory utilisation, node count |

---

## Adding a new cloud

1. Create `collectors/newcloud/plugin.py` extending `CloudCollectorPlugin`
2. Implement `discover_resources`, `collect_metrics`, `collect_logs`
3. Register it in `orchestrator.py`:
   ```python
   PLUGIN_REGISTRY["newcloud"] = NewCloudCollectorPlugin
   ```
4. Add a section to `config/cloud_observability.yaml`

---

## Environment variables

| Variable | Default | Description |
|----------|---------|-------------|
| `CONFIG_PATH` | `config/cloud_observability.yaml` | config file path |
| `COLLECTOR_MODE` | `continuous` | `continuous` or `once` |
| `COLLECTOR_INTERVAL` | `300` | seconds between collection cycles |
| `AWS_REGIONS` | `us-east-1` | comma-separated region list |
| `AWS_ACCESS_KEY_ID` | — | AWS credentials |
| `AWS_SECRET_ACCESS_KEY` | — | AWS credentials |
| `AZURE_SUBSCRIPTION_ID` | — | Azure subscription |
| `AZURE_CLIENT_ID` | — | Azure service principal |
| `AZURE_CLIENT_SECRET` | — | Azure service principal |
| `AZURE_TENANT_ID` | — | Azure tenant |
| `GCP_PROJECT_ID` | — | GCP project |
| `DATABASE_URL` | — | PostgreSQL DSN (overrides config file) |
| `PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD` | — | individual PG params |
