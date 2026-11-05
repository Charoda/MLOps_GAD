import hashlib
import json
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[1] / "data"
FEATURES = ["age", "tenure_months", "monthly_spend", "support_tickets_30d"]


def write(path, value):
    content = json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
    path = ROOT / path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


def sample(rng, count):
    return np.column_stack([rng.integers(18, 81, count), rng.integers(0, 121, count),
                            rng.integers(100, 5001, count) / 10, rng.integers(0, 16, count)])


def model(version):
    rng = np.random.default_rng(2026 + version)
    x = sample(rng, 3000)
    mean, scale = x.mean(axis=0), x.std(axis=0)
    z = (x - mean) / scale
    logits = z @ np.array([-0.2, -0.8, 0.3, 1.4]) - 0.5
    y = rng.binomial(1, 1 / (1 + np.exp(-logits)))
    weights, bias = np.zeros(4), 0.0
    for _ in range(1200):
        error = 1 / (1 + np.exp(-(z @ weights + bias))) - y
        weights -= 0.1 * (z.T @ error / len(y) + 0.01 * weights)
        bias -= 0.1 * error.mean()
    return {"feature_names": FEATURES, "mean": mean.round(12).tolist(),
            "scale": scale.round(12).tolist(), "weights": weights.round(12).tolist(),
            "bias": round(float(bias), 12), "training": {"seed": 2026 + version, "rows": 3000,
            "steps": 1200, "kind": "synthetic logistic regression", "l2": 0.01}}


def main():
    write("models/v1.json", model(1))
    for index, count in enumerate((125, 129, 97), start=1):
        day = f"2026-09-{index:02d}"
        matrix = sample(np.random.default_rng(42 + index), count)
        rows = [{"customer_id": f"c{i:04d}", "business_date": day,
                 **{name: float(value) for name, value in zip(FEATURES, row)}}
                for i, row in enumerate(matrix)]
        write(f"basic/{day}.json", {"business_date": day, "rows": rows})


if __name__ == "__main__":
    main()
