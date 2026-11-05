import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "dags"))
from batch_common import predict

rows = json.loads((ROOT / "data/basic/2026-09-01.json").read_text())["rows"]
model = json.loads((ROOT / "data/models/v1.json").read_text())
print("rows:", len(rows), "features:", model["feature_names"])
print(json.dumps(predict(rows[:3], model), indent=2))
