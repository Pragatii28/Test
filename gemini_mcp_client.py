import os
import json
import asyncio
from google import genai

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


PYTHON_EXE = r"C:\Users\Pragati.Sagar\AppData\Local\Programs\Python\Python312\python.exe"
MCP_SERVER = r"C:\Users\Pragati.Sagar\Downloads\multi_cloud_collector\mcp_server.py"
DB_PATH = r"C:\Users\Pragati.Sagar\Downloads\multi_cloud_collector\observability_data\metrics.db"


def parse_concatenated_json(text: str):
    """
    Parse:
    1. JSON list
    2. Single JSON dict
    3. Multiple JSON dicts concatenated with newlines
    """
    text = text.strip()
    if not text:
        return None

    # Case 1: valid JSON directly
    try:
        return json.loads(text)
    except Exception:
        pass

    # Case 2: multiple JSON objects one after another
    decoder = json.JSONDecoder()
    idx = 0
    results = []

    while idx < len(text):
        while idx < len(text) and text[idx].isspace():
            idx += 1
        if idx >= len(text):
            break
        try:
            obj, end = decoder.raw_decode(text, idx)
            results.append(obj)
            idx = end
        except Exception:
            break

    if results:
        return results

    return text


def extract_tool_payload(tool_result):
    """
    Extract text payload from MCP tool result.
    """
    if hasattr(tool_result, "content") and tool_result.content:
        texts = []
        for item in tool_result.content:
            if hasattr(item, "text") and item.text:
                texts.append(item.text)
            else:
                texts.append(str(item))
        joined = "\n".join(texts).strip()
        return parse_concatenated_json(joined)

    if hasattr(tool_result, "model_dump"):
        dumped = tool_result.model_dump()

        if isinstance(dumped, dict) and "content" in dumped:
            texts = []
            for item in dumped["content"]:
                if isinstance(item, dict) and "text" in item:
                    texts.append(item["text"])
            joined = "\n".join(texts).strip()
            return parse_concatenated_json(joined)

        return dumped

    return str(tool_result)


def dedupe_latest_anomalies(anomalies):
    latest = {}
    for a in anomalies:
        key = (a.get("resource_id"), a.get("metric_name"))
        existing = latest.get(key)
        if not existing or a.get("detected_at", "") > existing.get("detected_at", ""):
            latest[key] = a

    return sorted(
        latest.values(),
        key=lambda x: x.get("detected_at", ""),
        reverse=True
    )


async def call_tool(session, name, args):
    result = await session.call_tool(name, arguments=args)
    return extract_tool_payload(result)


async def main():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set")

    client = genai.Client(api_key=api_key)

    server_params = StdioServerParameters(
        command=PYTHON_EXE,
        args=[MCP_SERVER],
        env={"DB_PATH": DB_PATH}
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            print("Fetching recent critical anomalies...\n")

            anomalies = await call_tool(
                session,
                "get_recent_anomalies",
                {"hours": 24, "severity": "critical"}
            )

            if not anomalies:
                print("No critical anomalies found.")
                return

            if isinstance(anomalies, dict):
                anomalies = [anomalies]

            if not isinstance(anomalies, list):
                print("Unexpected anomalies response:")
                print(json.dumps(anomalies, indent=2, default=str))
                return

            anomalies = [a for a in anomalies if isinstance(a, dict)]

            if not anomalies:
                print("No valid anomaly objects found.")
                return

            anomalies = dedupe_latest_anomalies(anomalies)

            top_anomaly = anomalies[0]
            resource_id = top_anomaly["resource_id"]
            metric_name = top_anomaly["metric_name"]
            current_value = float(top_anomaly.get("current_value", 0) or 0)
            severity = top_anomaly.get("severity", "unknown")

            print("Selected anomaly:")
            print(json.dumps(top_anomaly, indent=2, default=str))
            print()

            rca_data = await call_tool(
                session,
                "get_rca_data",
                {
                    "resource_id": resource_id,
                    "metric_name": metric_name
                }
            )

            confidence_data = await call_tool(
                session,
                "compute_confidence_score",
                {
                    "resource_id": resource_id,
                    "metric_name": metric_name,
                    "current_value": current_value
                }
            )

            remediation_data = await call_tool(
                session,
                "suggest_remediation",
                {
                    "metric_name": metric_name,
                    "severity": severity,
                    "current_value": current_value
                }
            )

            prompt = f"""
You are an AIOps root cause analysis assistant.

Analyze the anomaly and provide operational RCA.

Return exactly these sections:
1. Incident Summary
2. Key Evidence
3. Likely Root Cause
4. Confidence Assessment
5. Recommended Mitigation
6. Auto-remediation Suitability

Selected anomaly:
{json.dumps(top_anomaly, indent=2, default=str)}

RCA data:
{json.dumps(rca_data, indent=2, default=str)}

Confidence data:
{json.dumps(confidence_data, indent=2, default=str)}

Remediation data:
{json.dumps(remediation_data, indent=2, default=str)}
"""

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )

            print("\n=== RCA SUMMARY ===\n")
            print(response.text)


if __name__ == "__main__":
    asyncio.run(main())