from datetime import timedelta

import pendulum
import pytest
from airflow.dag_processing.dagbag import DagBag

DAG_FOLDER = "/opt/airflow/dags"
DAG_ID = "hw_basic_batch_inference"
TASKS = {"extract", "infer", "publish"}


@pytest.fixture(scope="module")
def bag():
    return DagBag(dag_folder=DAG_FOLDER)


@pytest.fixture(scope="module")
def dag(bag):
    assert not bag.import_errors, f"Ошибки импорта DAG-файлов: {bag.import_errors}"
    if DAG_ID not in bag.dags:
        pytest.fail(
            f"DAG '{DAG_ID}' не найден в {DAG_FOLDER} (найдены: {sorted(bag.dags) or 'ни одного'}). "
            "Выполните Задание 1 в dags/hw_basic.py как просят в ASSIGNMENT.md.",
            pytrace=False,
        )
    return bag.dags[DAG_ID]


def test_no_import_errors(bag):
    assert not bag.import_errors, f"Ошибки импорта DAG-файлов: {bag.import_errors}"


def test_task_ids(dag):
    assert set(dag.task_ids) == TASKS, f"Нужны ровно задачи {sorted(TASKS)}, найдено {sorted(dag.task_ids)}"


def test_edges(dag):
    assert dag.get_task("extract").upstream_task_ids == set()
    assert dag.get_task("infer").upstream_task_ids == {"extract"}
    assert dag.get_task("publish").upstream_task_ids == {"infer"}
    assert dag.get_task("publish").downstream_task_ids == set()


def test_dag_settings(dag):
    assert type(dag.timetable).__name__ == "NullTimetable", "DAG запускается только вручную: schedule=None"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.start_date == pendulum.datetime(2026, 9, 1, tz="Europe/Moscow")


def test_params(dag):
    day = dag.params.get_param("business_date")
    assert day.value == "2026-09-01"
    assert day.schema.get("type") == "string"
    assert day.schema.get("format") == "date"
    size = dag.params.get_param("batch_size")
    assert size.value == 32
    assert size.schema.get("type") == "integer"
    assert size.schema.get("minimum") == 1
    assert size.schema.get("maximum") == 1000


def test_retry_and_timeout_policy(dag):
    for task_id in TASKS:
        task = dag.get_task(task_id)
        assert task.execution_timeout == timedelta(minutes=2), task_id
        assert task.retry_delay == timedelta(seconds=5), task_id
    assert dag.get_task("extract").retries == 2
    assert dag.get_task("publish").retries == 2
    assert dag.get_task("infer").retries == 0, "Ошибка контракта данных детерминирована: infer без повторов"


def test_trigger_rules(dag):
    for task_id in TASKS:
        assert str(dag.get_task(task_id).trigger_rule) in {"all_success", "TriggerRule.ALL_SUCCESS"}, task_id
