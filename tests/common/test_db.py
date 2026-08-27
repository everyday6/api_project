import pytest
from psycopg2 import sql

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


# replace_table_snapshot: Type2/Type4처럼 매번 전체를 다시 계산하는
# 파이프라인이 쓰는 staging+swap 전체 교체.

SNAPSHOT_TABLE = "test_snapshot_swap"


def _create_snapshot_table():
    reset_table(SNAPSHOT_TABLE, {"value": "NUMERIC"}, ("segment_id",))


def test_replace_table_snapshot_removes_rows_absent_from_new_items():
    _create_snapshot_table()
    db.batch_write_items(
        SNAPSHOT_TABLE,
        [{"segment_id": "OLD", "value": 1}, {"segment_id": "KEEP", "value": 1}],
        key_columns=("segment_id",),
    )

    db.replace_table_snapshot(
        SNAPSHOT_TABLE,
        [{"segment_id": "KEEP", "value": 2}, {"segment_id": "NEW", "value": 3}],
        key_columns=("segment_id",),
    )

    result = db.batch_get_items(
        SNAPSHOT_TABLE, [{"segment_id": "OLD"}, {"segment_id": "KEEP"}, {"segment_id": "NEW"}]
    )
    assert ("OLD",) not in result
    assert result[("KEEP",)]["value"] == 2
    assert result[("NEW",)]["value"] == 3


def test_replace_table_snapshot_skips_swap_when_items_empty():
    _create_snapshot_table()
    db.batch_write_items(
        SNAPSHOT_TABLE, [{"segment_id": "KEEP", "value": 1}], key_columns=("segment_id",)
    )

    result = db.replace_table_snapshot(SNAPSHOT_TABLE, [], key_columns=("segment_id",))

    assert result == 0
    assert db.batch_get_items(SNAPSHOT_TABLE, [{"segment_id": "KEEP"}])


def test_replace_table_snapshot_preserves_primary_key_constraint():
    _create_snapshot_table()

    db.replace_table_snapshot(
        SNAPSHOT_TABLE, [{"segment_id": "A", "value": 1}], key_columns=("segment_id",)
    )

    # LIKE ... INCLUDING ALL로 만든 새 테이블도 원래 PK 제약을 그대로
    # 가져야 한다 - 이미 있는 키("A")를 또 넣으면 여전히 막혀야 한다.
    conn = db._get_connection()
    with conn.cursor() as cur:
        with pytest.raises(Exception):
            cur.execute(
                sql.SQL("INSERT INTO {table} (segment_id, value) VALUES (%s, %s)").format(
                    table=sql.Identifier(SNAPSHOT_TABLE)
                ),
                ("A", 2),
            )
    conn.rollback()
    conn.autocommit = True


# cleanup_keys_not_in: Type1처럼 증분 upsert만 하는 테이블에서, LION 갱신
# 시점에 더 이상 유효하지 않은 세그먼트만 targeted delete로 정리하면서
# 지표(valid_count/stale_keys/deleted_rows)까지 한 번에 얻는다.

CLEANUP_TABLE = "test_stale_cleanup"


def _create_cleanup_table():
    reset_table(CLEANUP_TABLE, {"value": "NUMERIC"}, ("segment_id", "time"))


def test_cleanup_keys_not_in_removes_only_stale_rows_and_returns_metrics():
    _create_cleanup_table()
    db.batch_write_items(
        CLEANUP_TABLE,
        [
            {"segment_id": "STALE", "time": "0000", "value": 1},
            {"segment_id": "STALE", "time": "0030", "value": 1},
            {"segment_id": "VALID", "time": "0000", "value": 1},
        ],
        key_columns=("segment_id", "time"),
    )

    result = db.cleanup_keys_not_in(CLEANUP_TABLE, ["VALID"], key_column="segment_id")

    # STALE의 time 슬롯 2개가 다 지워져도 stale_keys는 세그먼트 단위로
    # 중복 제거된다("몇 행 지워졌는지"가 아니라 "어떤 세그먼트가
    # 지워졌는지"가 로그/알림에 필요한 정보라서). deleted_rows는 실제
    # 지워진 행 수(2)를 그대로 보여준다.
    assert result == {"valid_count": 1, "stale_keys": ["STALE"], "deleted_rows": 2}
    remaining = db.batch_get_items(
        CLEANUP_TABLE,
        [
            {"segment_id": "STALE", "time": "0000"},
            {"segment_id": "STALE", "time": "0030"},
            {"segment_id": "VALID", "time": "0000"},
        ],
    )
    assert ("STALE", "0000") not in remaining
    assert ("STALE", "0030") not in remaining
    assert ("VALID", "0000") in remaining


def test_cleanup_keys_not_in_dedupes_valid_values_without_pk_violation():
    _create_cleanup_table()
    db.batch_write_items(
        CLEANUP_TABLE,
        [{"segment_id": "VALID", "time": "0000", "value": 1}],
        key_columns=("segment_id", "time"),
    )

    result = db.cleanup_keys_not_in(
        CLEANUP_TABLE, ["VALID", "VALID"], key_column="segment_id"
    )

    assert result == {"valid_count": 1, "stale_keys": [], "deleted_rows": 0}


def test_cleanup_keys_not_in_raises_on_empty_valid_values():
    _create_cleanup_table()
    db.batch_write_items(
        CLEANUP_TABLE,
        [{"segment_id": "VALID", "time": "0000", "value": 1}],
        key_columns=("segment_id", "time"),
    )

    with pytest.raises(ValueError):
        db.cleanup_keys_not_in(CLEANUP_TABLE, [], key_column="segment_id")

    assert db.batch_get_items(CLEANUP_TABLE, [{"segment_id": "VALID", "time": "0000"}])
