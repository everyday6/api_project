import pytest

from src.common import db
from tests.conftest import reset_table

pytestmark = pytest.mark.usefixtures("require_postgres")

TABLE_NAME = "test_segment_metrics"
KEY_COLUMNS = ("entity_id", "record_type", "bucket")
COLUMNS = {
    "travel_seconds": "NUMERIC",
    "sample_count": "INTEGER",
    "collected_date": "DATE",
}


def _create_test_table():
    reset_table(TABLE_NAME, COLUMNS, KEY_COLUMNS)


def _key(entity_id: str, record_type: str = "BUCKET", bucket: str = "1200"):
    return {"entity_id": entity_id, "record_type": record_type, "bucket": bucket}


def test_put_item_then_batch_get_returns_it():
    _create_test_table()
    db.put_item(
        TABLE_NAME,
        _key("1") | {"travel_seconds": 30},
        key_columns=KEY_COLUMNS,
    )

    result = db.batch_get_items(TABLE_NAME, [_key("1")])

    assert result[("1", "BUCKET", "1200")]["travel_seconds"] == 30


def test_batch_get_missing_key_is_absent_from_result():
    _create_test_table()
    db.put_item(
        TABLE_NAME,
        _key("1") | {"travel_seconds": 30},
        key_columns=KEY_COLUMNS,
    )

    result = db.batch_get_items(TABLE_NAME, [_key("1"), _key("999")])

    assert ("1", "BUCKET", "1200") in result
    assert ("999", "BUCKET", "1200") not in result


def test_batch_get_handles_many_keys_via_chunking():
    _create_test_table()
    count = 1200
    items = [_key(str(i)) | {"travel_seconds": i} for i in range(count)]
    db.batch_write_items(TABLE_NAME, items, key_columns=KEY_COLUMNS)

    result = db.batch_get_items(TABLE_NAME, [_key(str(i)) for i in range(count)])

    assert len(result) == count
    assert result[(str(count - 1), "BUCKET", "1200")]["travel_seconds"] == count - 1


def test_batch_get_supports_single_column_primary_key():
    table = "test_segment_lengths"
    reset_table(table, {"length_ft": "NUMERIC"}, ("segment_id",))
    items = [{"segment_id": str(i), "length_ft": i * 10} for i in range(30)]
    db.batch_write_items(table, items, key_columns=("segment_id",))

    result = db.batch_get_items(table, [{"segment_id": str(i)} for i in range(30)])

    assert len(result) == 30
    assert result[("29",)]["length_ft"] == 290


def test_batch_get_empty_keys_returns_empty_dict():
    assert db.batch_get_items(TABLE_NAME, []) == {}


def test_batch_get_items_logs_query_duration(caplog):
    # Grafana의 RDS 응답시간 p50/p95/p99 패널(CloudWatch Logs Insights)이
    # 이 로그를 집계한다 - 빈 키 호출(쿼리 자체가 안 나감)은 로그를 안
    # 남기는지, 실제 조회는 테이블명과 함께 남기는지 확인한다.
    _create_test_table()
    db.put_item(TABLE_NAME, _key("1") | {"travel_seconds": 30}, key_columns=KEY_COLUMNS)

    with caplog.at_level("INFO", logger="src.common.db"):
        db.batch_get_items(TABLE_NAME, [_key("1")])

    duration_logs = [r.message for r in caplog.records if "[rds_query_duration]" in r.message]
    assert len(duration_logs) == 1
    assert f"table={TABLE_NAME}" in duration_logs[0]


def test_batch_write_handles_different_optional_fields():
    _create_test_table()
    db.batch_write_items(
        TABLE_NAME,
        [
            _key("1") | {"travel_seconds": 30, "collected_date": "2026-08-21"},
            _key("1", "AVG", "") | {"travel_seconds": 29, "sample_count": 5},
        ],
        key_columns=KEY_COLUMNS,
    )

    result = db.batch_get_items(TABLE_NAME, [_key("1"), _key("1", "AVG", "")])
    bucket = result[("1", "BUCKET", "1200")]
    avg = result[("1", "AVG", "")]
    assert bucket["collected_date"] == "2026-08-21"
    assert "sample_count" not in bucket
    assert avg["sample_count"] == 5
    assert "collected_date" not in avg


def test_get_value_uses_semantic_value_column_and_default():
    table = "test_toll_amounts"
    reset_table(table, {"toll_amount": "NUMERIC"}, ("segment_id",))
    db.put_item(
        table,
        {"segment_id": "S1", "toll_amount": 0.75},
        key_columns=("segment_id",),
    )

    assert db.get_value(table, {"segment_id": "S1"}, "toll_amount") == 0.75
    assert db.get_value(table, {"segment_id": "missing"}, "toll_amount", default=0) == 0


def test_get_value_returns_default_when_table_does_not_exist():
    conn = db._get_connection()
    with conn.cursor() as cur:
        cur.execute('DROP TABLE IF EXISTS "table_that_does_not_exist"')

    result = db.get_value(
        "table_that_does_not_exist",
        {"segment_id": "S1"},
        "toll_amount",
        default=0,
    )
    assert result == 0


def test_ensure_table_is_idempotent_with_explicit_primary_key():
    conn = db._get_connection()
    with conn.cursor() as cur:
        cur.execute(f'DROP TABLE IF EXISTS "{TABLE_NAME}"')

    db.ensure_table(TABLE_NAME, COLUMNS, KEY_COLUMNS)
    db.ensure_table(TABLE_NAME, COLUMNS, KEY_COLUMNS)

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (TABLE_NAME,))
        assert cur.fetchone()[0] == TABLE_NAME
