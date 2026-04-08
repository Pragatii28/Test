import sqlite3
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse

app = FastAPI()

# Update this path to your actual db location
DB_PATH = r"C:\Users\Pragati.Sagar\Downloads\multi_cloud_collector\observability_data\metrics.db"

@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    try:
        conn = sqlite3.connect(DB_PATH)
        rows = conn.execute("""
            SELECT
                m.resource_id,
                m.resource_name,
                m.resource_type,
                m.metric_name,
                m.metric_value,
                m.cloud,
                m.region
            FROM metrics m
            INNER JOIN (
                SELECT
                    resource_id   AS sub_rid,
                    metric_name   AS sub_mn,
                    MAX(collected_at) AS max_ts
                FROM metrics
                WHERE collected_at >= datetime('now', '-15 minutes')
                GROUP BY resource_id, metric_name
            ) latest
              ON  m.resource_id  = latest.sub_rid
              AND m.metric_name  = latest.sub_mn
              AND m.collected_at = latest.max_ts
        """).fetchall()
        conn.close()
    except Exception as e:
        return f"# ERROR: {e}\n"

    if not rows:
        return "# No recent metrics found in last 15 minutes\n"

    lines = []
    for resource_id, resource_name, resource_type, metric_name, value, cloud, region in rows:
        # sanitize for Prometheus format
        metric_name   = str(metric_name).replace(" ", "_").replace("-", "_")
        resource_id   = str(resource_id).replace('"', '').replace("\\", "")
        resource_name = str(resource_name).replace('"', '').replace("\\", "")
        resource_type = str(resource_type).replace('"', '').replace("\\", "")
        cloud         = str(cloud).replace('"', '')
        region        = str(region).replace('"', '')

        lines.append(
            f'infra_{metric_name}{{'
            f'resource_id="{resource_id}",'
            f'resource_name="{resource_name}",'
            f'resource_type="{resource_type}",'
            f'cloud="{cloud}",'
            f'region="{region}"'
            f'}} {value}'
        )

    return "\n".join(lines) + "\n"


@app.get("/health")
def health():
    return {"status": "ok", "metrics_count": 0}