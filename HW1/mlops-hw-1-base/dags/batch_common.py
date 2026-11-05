import hashlib
import json
import logging
import math
import os
import re
import tempfile
from datetime import date
from pathlib import Path

import numpy as np

FEATURES = ["age", "tenure_months", "monthly_spend", "support_tickets_30d"]
LIMITS = [(18, 100), (0, 240), (0, 10000), (0, 100)]
ARTIFACTS = Path(os.environ.get("BATCH_ARTIFACTS", "/opt/airflow/include/artifacts"))
LOG = logging.getLogger(__name__)


def canonical(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False, allow_nan=False,
                      separators=(",", ":")).encode()


def digest(content):
    return hashlib.sha256(content).hexdigest()


def save(value):
    content = canonical(value)
    sha = digest(content)
    ARTIFACTS.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=ARTIFACTS, delete=False) as file:
        file.write(content)
        temporary = Path(file.name)
    temporary.replace(ARTIFACTS / f"{sha}.json")
    return {"sha256": sha}


def read(ref):
    sha = ref["sha256"]
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{64}", sha):
        raise ValueError("Invalid artifact reference")
    content = (ARTIFACTS / f"{sha}.json").read_bytes()
    if digest(content) != sha:
        raise ValueError("Artifact checksum mismatch")
    return json.loads(content)


def business_date(explicit, interval_start):
    if explicit:
        return date.fromisoformat(explicit).isoformat()
    if interval_start is None:
        raise ValueError("business_date is required without a data interval")
    return interval_start.in_timezone("Europe/Moscow").to_date_string()


def validate_rows(rows, day):
    if not rows:
        raise ValueError("Empty rows")
    seen = set()
    for row in rows:
        customer_id = row["customer_id"]
        if type(customer_id) is not str or not customer_id:
            raise ValueError("Invalid customer_id")
        if customer_id in seen:
            raise ValueError(f"Duplicate customer_id: {customer_id}")
        seen.add(customer_id)
        if row["business_date"] != day:
            raise ValueError(
                f"business_date mismatch: expected {day}, got {row['business_date']}"
            )
        for name, (lo, hi) in zip(FEATURES, LIMITS):
            value = row[name]
            if type(value) not in (int, float) or not math.isfinite(value):
                raise ValueError(f"Invalid feature {name}: {value!r}")
            if not (lo <= value <= hi):
                raise ValueError(f"Feature {name} out of range: {value}")
    return rows


def validate_model(model):
    if model["feature_names"] != FEATURES:
        raise ValueError("Feature order mismatch")
    for field in ("mean", "scale", "weights"):
        if len(model[field]) != len(FEATURES):
            raise ValueError("Model dimensions mismatch")
        if any(type(v) not in (int, float) or not math.isfinite(v) for v in model[field]):
            raise ValueError("Invalid model coefficient")
    if any(v <= 0 for v in model["scale"]) or type(model["bias"]) not in (int, float) or not math.isfinite(model["bias"]):
        raise ValueError("Invalid model scale or bias")


def predict(rows, model):
    validate_model(model)
    matrix = np.asarray([[row[name] for name in FEATURES] for row in rows], dtype=np.float64)
    normalized = (matrix - np.asarray(model["mean"])) / np.asarray(model["scale"])
    logits = normalized @ np.asarray(model["weights"]) + model["bias"]
    scores = np.round(1 / (1 + np.exp(-np.clip(logits, -700, 700))), 8)
    return [{"customer_id": row["customer_id"], "score": float(score), "label": int(score >= 0.5)}
            for row, score in zip(rows, scores)]


def infer_batches(rows, model, batch_size):
    if type(batch_size) is not int or not (1 <= batch_size <= 1000):
        raise ValueError(f"Invalid batch_size: {batch_size!r}")
    predictions = []
    for index in range(0, len(rows), batch_size):
        batch = rows[index:index + batch_size]
        batch_number = index // batch_size + 1
        LOG.info("batch %s size %s", batch_number, len(batch))
        predictions.extend(predict(batch, model))
    return predictions


def validate_predictions(predictions, expected_ids):
    if len(predictions) != len(expected_ids):
        raise ValueError(
            f"Prediction count mismatch: got {len(predictions)}, expected {len(expected_ids)}"
        )
    ids = [item["customer_id"] for item in predictions]
    if len(ids) != len(set(ids)):
        raise ValueError("Duplicate customer_id in predictions")
    if set(ids) != set(expected_ids):
        raise ValueError("customer_id set mismatch")
    for item in predictions:
        score = item["score"]
        label = item["label"]
        if type(score) not in (int, float) or not math.isfinite(score):
            raise ValueError(f"Invalid score: {score!r}")
        if not (0 <= score <= 1):
            raise ValueError(f"Score out of range: {score}")
        if type(label) is not int or label not in (0, 1):
            raise ValueError(f"Invalid label: {label!r}")
        if label != int(score >= 0.5):
            raise ValueError(f"Label does not match score: label={label}, score={score}")


def replace_day(conn, day, model_version, predictions):
    if not predictions:
        raise ValueError("Empty predictions")
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                (f"{day}:{model_version}",),
            )
            cur.execute(
                "DELETE FROM predictions WHERE business_date = %s AND model_version = %s",
                (day, model_version),
            )
            cur.executemany(
                """
                INSERT INTO predictions
                    (business_date, model_version, customer_id, score, label)
                VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (day, model_version, item["customer_id"], item["score"], item["label"])
                    for item in predictions
                ],
            )

