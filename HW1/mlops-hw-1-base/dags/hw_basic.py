from datetime import timedelta

import pendulum
import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import BaseHook, Param, dag, task

import batch_common as core


@dag(
    dag_id="hw_basic_batch_inference",
    start_date=pendulum.datetime(2026, 9, 1, tz="Europe/Moscow"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["homework", "basic"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(seconds=5),
        "execution_timeout": timedelta(minutes=2),
    },
    params={
        "business_date": Param("2026-09-01", type="string", format="date"),
        "batch_size": Param(32, type="integer", minimum=1, maximum=1000),
    },
)
def hw_basic_batch_inference():
    @task
    def extract(**context):
        day = core.business_date(
            context["params"].get("business_date"),
            context.get("data_interval_start"),
        )
        host = BaseHook.get_connection("source_api").host.rstrip("/")

        rows_response = requests.get(f"{host}/basic/{day}.json", timeout=10)
        rows_response.raise_for_status()
        payload = rows_response.json()
        if payload.get("business_date") != day:
            raise ValueError(
                f"business_date mismatch: expected {day}, got {payload.get('business_date')}"
            )

        model_response = requests.get(f"{host}/models/v1.json", timeout=10)
        model_response.raise_for_status()
        model_sha256 = core.digest(model_response.content)
        model = model_response.json()

        return core.save(
            {
                "business_date": day,
                "rows": payload["rows"],
                "model": model,
                "model_sha256": model_sha256,
            }
        )

    @task(retries=0)
    def infer(extract_ref, **context):
        data = core.read(extract_ref)
        batch_size = context["params"]["batch_size"]
        rows = core.validate_rows(data["rows"], data["business_date"])
        predictions = core.infer_batches(rows, data["model"], batch_size)
        core.validate_predictions(predictions, [row["customer_id"] for row in rows])
        return core.save(
            {
                "business_date": data["business_date"],
                "model_version": "v1",
                "predictions": predictions,
                "model_sha256": data["model_sha256"],
            }
        )

    @task
    def publish(infer_ref):
        data = core.read(infer_ref)
        conn = PostgresHook(postgres_conn_id="warehouse").get_conn()
        try:
            core.replace_day(
                conn,
                data["business_date"],
                data["model_version"],
                data["predictions"],
            )
        finally:
            conn.close()
        return {
            "business_date": data["business_date"],
            "model_version": data["model_version"],
            "rows": len(data["predictions"]),
        }

    publish(infer(extract()))


hw_basic_batch_inference()
