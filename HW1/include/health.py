import json
import urllib.request

with urllib.request.urlopen("http://api-server:8080/api/v2/monitor/health", timeout=10) as response:
    health = json.load(response)
print(json.dumps(health, indent=2))
required = ("metadatabase", "scheduler", "triggerer", "dag_processor")
if not all(health.get(name, {}).get("status") == "healthy" for name in required):
    raise SystemExit("Airflow is not ready: inspect unhealthy component logs")
