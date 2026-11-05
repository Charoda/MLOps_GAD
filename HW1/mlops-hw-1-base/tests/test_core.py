import copy
import json
from pathlib import Path

import pendulum
import pytest

import batch_common as core

DATA = Path(__file__).resolve().parents[1] / "data"
DAY = "2026-09-01"


@pytest.fixture
def rows():
    return json.loads((DATA / "basic" / f"{DAY}.json").read_text())["rows"]


@pytest.fixture
def model():
    return json.loads((DATA / "models/v1.json").read_text())


def test_batches_really_call_model_per_chunk(rows, model, monkeypatch):
    calls = []
    original = core.predict
    def observed(batch, loaded):
        calls.append(len(batch))
        assert loaded is model
        return original(batch, loaded)
    monkeypatch.setattr(core, "predict", observed)
    actual = core.infer_batches(rows, model, 32)
    assert calls == [32, 32, 32, 29]
    assert len(actual) == 125
    assert actual == original(rows, model)


@pytest.mark.parametrize("size", [1, 16, 32, 1000])
def test_batch_size_does_not_change_scores(rows, model, size):
    assert core.infer_batches(rows, model, size) == core.predict(rows, model)


@pytest.mark.parametrize("size", [0, -1, True, 2.5, 1001])
def test_bad_batch_size(rows, model, size):
    with pytest.raises(ValueError):
        core.infer_batches(rows, model, size)


def test_valid_rows(rows):
    assert core.validate_rows(rows, DAY) == rows


@pytest.mark.parametrize("case", ["empty", "duplicate", "date", "nan", "string", "bool", "range"])
def test_invalid_rows(rows, case):
    if case == "empty": rows = []
    if case == "duplicate": rows.append(copy.deepcopy(rows[0]))
    if case == "date": rows[0]["business_date"] = "2026-09-02"
    if case == "nan": rows[0]["age"] = float("nan")
    if case == "string": rows[0]["age"] = "42"
    if case == "bool": rows[0]["age"] = True
    if case == "range": rows[0]["age"] = 200
    with pytest.raises(ValueError): core.validate_rows(rows, DAY)


def test_feature_order(model, rows):
    model["feature_names"].reverse()
    with pytest.raises(ValueError): core.predict(rows, model)


def test_probability_contract(rows, model):
    actual = core.predict(rows, model)
    core.validate_predictions(actual, [r["customer_id"] for r in rows])
    actual[0]["score"] = 1.5
    with pytest.raises(ValueError): core.validate_predictions(actual, [r["customer_id"] for r in rows])


def test_artifact_integrity(tmp_path, monkeypatch):
    monkeypatch.setattr(core, "ARTIFACTS", tmp_path)
    ref = core.save({"a": 1})
    assert core.read(ref) == {"a": 1}
    (tmp_path / (ref["sha256"] + ".json")).write_text('{}')
    with pytest.raises(ValueError): core.read(ref)
    with pytest.raises(ValueError): core.read({"sha256": "../secret"})


def test_moscow_date():
    assert core.business_date(None, pendulum.datetime(2026, 8, 31, 21, tz="UTC")) == DAY
    assert core.business_date("2026-09-02", None) == "2026-09-02"
    with pytest.raises(ValueError): core.business_date(None, None)


def test_known_model_predictions(rows, model):
    actual = core.predict(rows, model)
    assert [r["score"] for r in actual[:3]] == [0.20572767, 0.46234233, 0.24357032]
    assert sum(r["label"] for r in actual) == 44
