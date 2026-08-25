from datetime import date, datetime, timedelta
from unittest.mock import patch

import pytest

from src.serving import nav_lookup

# 순수 로직 테스트(배치 조회를 monkeypatch로 대체)는 RDS가 없어도 돈다 -
# 실제 테이블에 값을 심어두고 조회하는 테스트에만 개별로
# @requires_postgres를 붙인다.
requires_postgres = pytest.mark.usefixtures("require_postgres")

# _is_fresh()가 date.today()를 직접 부르므로, date 자체를 mock하는 대신
# 실제 "오늘"을 기준으로 어제를 계산한다 - date를 MagicMock으로 바꿔치기하면
# isinstance(last_sample_at, datetime) 검사가 깨진다(datetime이 더 이상
# 타입이 아니게 됨).
TODAY = datetime.combine(date.today(), datetime.min.time())
YESTERDAY = TODAY - timedelta(days=1)


@pytest.fixture(autouse=True)
def _clear_caches():
    """메모리 캐시/S3 스냅샷 로드 상태가 테스트 간에 새지 않도록 초기화한다."""
    nav_lookup._memory_cache.clear()
    nav_lookup._s3_snapshot_loaded = False
    nav_lookup._s3_snapshot = {}
    yield
    nav_lookup._memory_cache.clear()
    nav_lookup._s3_snapshot_loaded = False
    nav_lookup._s3_snapshot = {}


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.SERVING_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.SERVING_TABLE_TYPE2


def test_add_seconds_advances_within_same_hour():
    assert nav_lookup._add_seconds("12:00", 600) == "12:10"


def test_add_seconds_wraps_past_midnight():
    assert nav_lookup._add_seconds("23:50", 900) == "00:05"


# ---------------------------------------------------------------------------
# _is_fresh / _resolve_from_row — 한 행 안에서 Fresh Exact -> Historical AVG
# -> SPEC Estimate 순서를 고르는 순수 로직.
# ---------------------------------------------------------------------------

def test_is_fresh_true_only_for_todays_date():
    assert nav_lookup._is_fresh(TODAY) is True
    assert nav_lookup._is_fresh(YESTERDAY) is False


def test_is_fresh_false_for_missing_or_malformed_value():
    assert nav_lookup._is_fresh(None) is False
    assert nav_lookup._is_fresh("2026-08-21") is False  # 문자열은 datetime 인스턴스가 아님


def test_resolve_from_row_prefers_fresh_exact():
    row = {"value": 30, "avg": 40, "last_sample_at": TODAY}
    assert nav_lookup._resolve_from_row(row) == (30, "fresh")


def test_resolve_from_row_falls_back_to_avg_when_value_is_stale():
    row = {"value": 30, "avg": 40, "last_sample_at": YESTERDAY}
    assert nav_lookup._resolve_from_row(row) == (40, "avg")



def test_resolve_from_row_returns_none_when_row_is_none_or_empty():
    assert nav_lookup._resolve_from_row(None) == (None, None)
    assert nav_lookup._resolve_from_row({"value": None, "avg": None}) == (None, None)


# ---------------------------------------------------------------------------
# Type1 — RDS 정상 응답
# ---------------------------------------------------------------------------

def test_resolve_uses_fresh_exact_when_collected_today():
    rows = {"1": {"1200": {"value": 30, "avg": 999, "last_sample_at": TODAY}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]


def test_resolve_falls_back_to_avg_when_value_is_stale():
    rows = {"1": {"1200": {"value": 30, "avg": 40, "last_sample_at": YESTERDAY}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_falls_back_to_hardcoded_constant_when_slot_missing_but_rds_up():
    # RDS는 정상 응답했지만 이 세그먼트/슬롯 자체가 없는 경우 - 메모리/S3
    # 폴백으로 내려가지 않고 곧장 코드 상수로 간다.
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value={}):
        result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_type2_has_no_avg_tier_goes_straight_to_default():
    # RDS 연결 자체는 되고(fast-fail 커넥션 획득 성공) 조회 결과가 비어있는
    # 경우 - _get_fast_rds_connection도 같이 mock해야 batch_get_items가
    # 실제로 호출되는 지점까지 도달한다.
    with patch.object(nav_lookup, "_get_fast_rds_connection", return_value=None), \
         patch.object(nav_lookup, "batch_get_items", return_value={}) as mock_batch:
        result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[2]]
    mock_batch.assert_called()


# ---------------------------------------------------------------------------
# Type1 — RDS 자체가 응답 불가능한 경우: 메모리 캐시 -> S3 스냅샷 -> 코드 상수
# ---------------------------------------------------------------------------

def test_resolve_falls_back_to_s3_snapshot_when_rds_unreachable():
    snapshot = {"1": {"1200": {"value": 77, "last_sample_at": TODAY.isoformat()}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value=snapshot):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    # 스냅샷의 last_sample_at은 문자열(JSON 왕복)이라 datetime 인스턴스가 아니므로
    # _is_fresh가 False를 주고, 대신 "value"가 없으면 None -> 코드 상수로
    # 떨어진다는 점까지 같이 확인한다(스냅샷 값은 신선도 판단 없이 그대로
    # 못 씀 - 이 케이스는 avg가 없어 코드 상수로 떨어지는 게 맞다).
    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_uses_s3_snapshot_avg_when_rds_unreachable():
    snapshot = {"1": {"1200": {"avg": 55}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value=snapshot):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [55]


def test_resolve_uses_memory_cache_before_reloading_s3_snapshot():
    # 1번째 요청(RDS 정상)에서 성공적으로 읽은 값이 메모리 캐시에 남는다.
    rows = {"1": {"1200": {"value": 30, "avg": None, "last_sample_at": TODAY}}}
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    # 2번째 요청은 RDS가 죽었다고 가정 - S3를 한 번도 안 불러도 메모리
    # 캐시로 응답할 수 있어야 한다.
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot") as mock_read_snapshot:
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]
    mock_read_snapshot.assert_not_called()


def test_resolve_falls_back_to_hardcoded_constant_when_everything_fails():
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", side_effect=RuntimeError("down")), \
         patch("src.common.gold_snapshot.read_snapshot", return_value={}):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 2


# ---------------------------------------------------------------------------
# 누적 경로 시각 계산 (segment_ids를 경로 순서로 취급)
# ---------------------------------------------------------------------------

def test_resolve_time_values_uses_cumulative_elapsed_time_per_segment():
    rows = {
        "1": {"1200": {"value": 1800, "avg": None, "last_sample_at": TODAY}},
        "2": {
            "1200": {"value": 111, "avg": None, "last_sample_at": TODAY},
            "1230": {"value": 999, "avg": None, "last_sample_at": TODAY},
        },
    }
    # 세그먼트 1: 12:00 슬롯에 1800초(30분) 소요 -> 세그먼트 2는 12:30에 도착.
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [1800, 999]


def test_resolve_time_values_logs_fallback_tier_summary(caplog):
    # Grafana의 fallback 히트율 대시보드(CloudWatch Logs Insights)가 이
    # 요약 로그 한 줄을 집계한다 - 세그먼트마다 로그를 안 남기고 요청당
    # 한 번만 남기는지, tier별 개수가 맞는지 확인한다.
    rows = {
        "fresh_seg": {"1200": {"value": 10, "avg": None, "last_sample_at": TODAY}},
        "avg_seg": {"1200": {"value": None, "avg": 20, "last_sample_at": None}},
    }
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows), \
         caplog.at_level("INFO", logger="src.serving.nav_lookup"):
        nav_lookup.resolve_segment_values(["fresh_seg", "avg_seg", "missing_seg"], 1, "12:00")

    summary_logs = [r.message for r in caplog.records if "[fallback_tier_summary]" in r.message]
    assert len(summary_logs) == 1
    assert "fresh=1" in summary_logs[0]
    assert "avg=1" in summary_logs[0]
    assert "hardcoded=1" in summary_logs[0]
    assert "total=3" in summary_logs[0]


def test_resolve_time_values_same_segment_twice_uses_different_buckets():
    rows = {
        "loop": {
            "1200": {"value": 1800, "avg": None, "last_sample_at": TODAY},
            "1230": {"value": 77, "avg": None, "last_sample_at": TODAY},
        },
    }
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value=rows):
        result = nav_lookup.resolve_segment_values(["loop", "loop"], 1, "12:00")

    assert result == [1800, 77]


def test_resolve_never_raises_on_invalid_type():
    result = nav_lookup.resolve_segment_values(["1", "2"], 3, "12:00")

    assert len(result) == 2
    assert all(isinstance(v, int) for v in result)


def test_resolve_never_raises_on_malformed_time():
    with patch.object(nav_lookup, "_batch_fetch_type1_rows", return_value={}):
        result = nav_lookup.resolve_segment_values(["1"], 1, "not-a-time")

    assert len(result) == 1
    assert isinstance(result[0], int)


# ---------------------------------------------------------------------------
# Type2 (길이) — 시간 무관, GLOBAL 기본값 폴백. 실제 RDS 왕복은 통합 테스트로.
# ---------------------------------------------------------------------------

@requires_postgres
def test_resolve_type2_reads_written_value():
    from src.common.config import SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS
    from src.common.db import put_item
    from tests.conftest import reset_table

    table = nav_lookup.SERVING_TABLE_TYPE2
    reset_table(table, SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS)
    put_item(table, {"segment_id": "1", "value": 500}, key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS)

    result = nav_lookup.resolve_segment_values(["1", "1"], 2, "09:00")

    assert result == [500, 500]


@requires_postgres
def test_resolve_type2_falls_back_to_global_default_when_missing():
    from src.common.config import (
        GLOBAL_PARTITION_KEY,
        SERVING_TABLE_TYPE2_COLUMNS,
        SERVING_TABLE_TYPE2_KEY_COLUMNS,
    )
    from src.common.db import put_item
    from tests.conftest import reset_table

    table = nav_lookup.SERVING_TABLE_TYPE2
    reset_table(table, SERVING_TABLE_TYPE2_COLUMNS, SERVING_TABLE_TYPE2_KEY_COLUMNS)
    put_item(
        table,
        {"segment_id": GLOBAL_PARTITION_KEY, "value": 300},
        key_columns=SERVING_TABLE_TYPE2_KEY_COLUMNS,
    )

    result = nav_lookup.resolve_segment_values(["missing-segment"], 2, "09:00")

    assert result == [300]


def test_resolve_type2_falls_back_to_hardcoded_constant_when_rds_unreachable():
    with patch.object(nav_lookup, "batch_get_items", side_effect=RuntimeError("network down")):
        result = nav_lookup.resolve_segment_values(["1", "2"], 2, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[2]] * 2
