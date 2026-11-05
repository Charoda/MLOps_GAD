import psycopg
import pytest

from batch_common import replace_day


@pytest.fixture
def conn():
    conn = psycopg.connect("postgresql://etl:etl@warehouse/warehouse")
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM predictions WHERE business_date='2099-01-01'")
    yield conn
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("DELETE FROM predictions WHERE business_date='2099-01-01'")
    conn.close()


def count():
    with psycopg.connect("postgresql://etl:etl@warehouse/warehouse", autocommit=True) as reader, reader.cursor() as cur:
        cur.execute("SELECT count(*) FROM predictions WHERE business_date='2099-01-01'")
        return cur.fetchone()[0]


def test_repeat_and_removed_customer(conn):
    rows = [{"customer_id": "a", "score": 0.2, "label": 0}, {"customer_id": "b", "score": 0.8, "label": 1}]
    replace_day(conn, "2099-01-01", "v1", rows)
    replace_day(conn, "2099-01-01", "v1", rows)
    assert count() == 2
    replace_day(conn, "2099-01-01", "v1", rows[:1])
    assert count() == 1


def test_database_error_rolls_back_delete(conn):
    rows = [{"customer_id": "a", "score": 0.2, "label": 0}]
    replace_day(conn, "2099-01-01", "v1", rows)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute("ALTER TABLE predictions ADD CONSTRAINT homework_test_failure CHECK (customer_id <> 'reject-me')")
    try:
        with pytest.raises(psycopg.errors.CheckViolation):
            replace_day(conn, "2099-01-01", "v1", [{"customer_id": "reject-me", "score": 0.2, "label": 0}])
        assert count() == 1
    finally:
        with conn.transaction(), conn.cursor() as cur:
            cur.execute("ALTER TABLE predictions DROP CONSTRAINT homework_test_failure")


def test_empty_publication_preserves_previous_day(conn):
    replace_day(conn, "2099-01-01", "v1", [{"customer_id": "a", "score": 0.2, "label": 0}])
    with pytest.raises(ValueError, match="Empty"):
        replace_day(conn, "2099-01-01", "v1", [])
    assert count() == 1
