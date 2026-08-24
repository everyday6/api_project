import time
from unittest.mock import patch

import pytest

from src.common import gold_snapshot, rds
from src.common.config import NAV_GOLD_RDS_LOCAL_DSN
from src.serving import nav_lookup

RDS_TEST_TABLE = "test_segment_metrics_type1_lookup"
RDS_TYPE2_TEST_TABLE = "test_segment_metrics_type2_lookup"


@pytest.fixture(autouse=True)
def _reset_nav_lookup_fallback_state():
    """RDS 폴백용 모듈 전역 상태(메모리 캐시/S3 스냅샷 로드 여부)가 테스트
    간에 새지 않도록 매 테스트 전에 초기화한다."""
    nav_lookup._memory_cache.clear()
    nav_lookup._s3_snapshot_loaded = False
    nav_lookup._s3_snapshot = {}
    yield


@pytest.fixture
def rds_table(monkeypatch):
    """type1 테스트용 실제 로컬 Postgres 테이블(docker-compose의
    nav-gold-postgres 컨테이너, 미리 `docker compose up -d nav-gold-postgres`로
    띄워둬야 한다) - RDS는 DynamoDB의 moto 같은 인메모리 mock 수단이 없어
    실제 로컬 인스턴스로 검증한다."""
    monkeypatch.setattr(rds, "get_rds_dsn", lambda: NAV_GOLD_RDS_LOCAL_DSN)
    monkeypatch.setattr(nav_lookup, "RDS_TABLE_TYPE1", RDS_TEST_TABLE)
    rds._connection = None
    rds.ensure_table(RDS_TEST_TABLE)
    conn = rds.get_connection()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {RDS_TEST_TABLE}")
    yield RDS_TEST_TABLE
    rds._connection = None


def _put_row(table_name, segment_id, sk, value, observed_at=None):
    rds.upsert_items(
        [{
            "segment_id": segment_id,
            "sk": sk,
            "value": value,
            "observed_at": observed_at,
            "collected_date": None,
            "count": None,
        }],
        table_name,
    )


@pytest.fixture
def rds_type2_table(monkeypatch):
    """type2 테스트용 실제 로컬 Postgres 테이블. type1과 스키마가 달라
    (segment_id 하나만 PK) 별도 테이블/헬퍼를 쓴다."""
    monkeypatch.setattr(rds, "get_rds_dsn", lambda: NAV_GOLD_RDS_LOCAL_DSN)
    monkeypatch.setattr(nav_lookup, "RDS_TABLE_TYPE2", RDS_TYPE2_TEST_TABLE)
    rds._connection = None
    rds.ensure_static_table(RDS_TYPE2_TEST_TABLE)
    conn = rds.get_connection()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {RDS_TYPE2_TEST_TABLE}")
    yield RDS_TYPE2_TEST_TABLE
    rds._connection = None


def _put_static_row(table_name, segment_id, value):
    rds.upsert_static_items([{"segment_id": segment_id, "value": value}], table_name)


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.RDS_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.RDS_TABLE_TYPE2


# ---------------------------------------------------------------------------
# Type1 (RDS 기반) — Fresh Exact -> Historical AVG -> SPEC Estimate -> 코드 상수
# ---------------------------------------------------------------------------

def test_resolve_uses_fresh_exact_bucket_value_when_present(rds_table):
    _put_row(rds_table, "1", "1200", 30, observed_at=time.time())

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [30]


def test_resolve_falls_back_to_avg_when_exact_is_stale(rds_table):
    # observed_at이 freshness 기준(1시간)보다 훨씬 오래됨 -> 그 exact 값은
    # 못 쓰고 AVG로 내려가야 한다.
    _put_row(rds_table, "1", "1200", 30, observed_at=time.time() - 999_999)
    _put_row(rds_table, "1", "AVG", 40)

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_falls_back_to_avg_when_bucket_missing(rds_table):
    _put_row(rds_table, "1", "AVG", 40)

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_falls_back_to_spec_when_exact_and_avg_missing(rds_table):
    _put_row(rds_table, "1", "SPEC", 55)

    result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [55]


def test_resolve_falls_back_to_hardcoded_constant_when_nothing_for_segment(rds_table):
    result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_falls_back_to_hardcoded_constant_when_rds_unreachable():
    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("network down")):
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 2


def test_resolve_time_values_uses_cumulative_elapsed_time_per_segment(rds_table):
    now = time.time()
    # 세그먼트 1: 12:00 버킷에 1800초(30분) 소요.
    _put_row(rds_table, "1", "1200", 1800, observed_at=now)
    # 세그먼트 2: 12:00 버킷과 12:30 버킷에 서로 다른 값 -> 누적 시각이
    # 제대로 반영되면 12:30 버킷 값(999)을 써야 한다.
    _put_row(rds_table, "2", "1200", 111, observed_at=now)
    _put_row(rds_table, "2", "1230", 999, observed_at=now)

    result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [1800, 999]


def test_resolve_time_values_same_segment_twice_uses_different_buckets(rds_table):
    now = time.time()
    _put_row(rds_table, "loop", "1200", 1800, observed_at=now)
    _put_row(rds_table, "loop", "1230", 77, observed_at=now)

    # 같은 세그먼트가 경로에 두 번 등장 - 두 번째 등장은 첫 번째 소요시간만큼
    # 시각이 밀려 다른 버킷(1230)을 봐야 하므로 값도 달라야 한다.
    result = nav_lookup.resolve_segment_values(["loop", "loop"], 1, "12:00")

    assert result == [1800, 77]


def test_resolve_time_values_remembers_successful_reads_in_memory_cache(rds_table):
    _put_row(rds_table, "1", "AVG", 40)

    nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert nav_lookup._memory_cache["1"]["avg"] == 40.0


def test_resolve_time_values_makes_exactly_one_rds_call_regardless_of_segment_count():
    # RDS 조회는 요청당 한 번(batch_resolve_type1_rows)뿐이어야 한다 -
    # 세그먼트 수만큼 순차 왕복이 쌓이던 예전 구조가 아니라서, 실패해도
    # 딱 1번만 시도하고 바로 전체 폴백으로 넘어가야 한다(재시도 없음).
    segment_ids = [str(i) for i in range(10)]

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("network down")) as mock_resolve:
        result = nav_lookup.resolve_segment_values(segment_ids, 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 10
    mock_resolve.assert_called_once_with(segment_ids, nav_lookup.RDS_TABLE_TYPE1)


def test_resolve_time_values_batch_fetches_once_then_resolves_locally():
    # 배치로 미리 가져온 결과만으로 세그먼트별 누적시각/버킷 계산이 전부
    # 로컬에서 끝나야 한다 - RDS는 최초 배치 호출 한 번만 나가야 한다.
    def fake_batch_resolve_type1_rows(segment_ids, table_name):
        now = time.time()
        return {
            "1": {"1200": {"value": 111, "observed_at": now}},
            "2": {"1200": {"value": 222, "observed_at": now}},
        }

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=fake_batch_resolve_type1_rows) as mock_resolve:
        result = nav_lookup.resolve_segment_values(["1", "2", "3", "4"], 1, "12:00")

    assert result == [111, 222, nav_lookup._HARDCODED_DEFAULTS[1], nav_lookup._HARDCODED_DEFAULTS[1]]
    mock_resolve.assert_called_once()


# ---------------------------------------------------------------------------
# Type1 — RDS 자체가 응답 불가능할 때: 메모리 캐시 -> S3 Gold 스냅샷 -> 코드 상수
# ---------------------------------------------------------------------------

def test_resolve_uses_memory_cache_when_rds_down(rds_table):
    _put_row(rds_table, "1", "AVG", 40)
    nav_lookup.resolve_segment_values(["1"], 1, "12:00")  # 메모리 캐시 예열

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_uses_s3_snapshot_when_rds_down_and_memory_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {
        "1": {"avg": 71.0, "spec": 66.0, "exact_value": None, "exact_observed_at": None},
    })

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [71]


def test_resolve_s3_snapshot_prefers_fresh_exact_over_avg(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {
        "1": {"avg": 71.0, "spec": 66.0, "exact_value": 20.0, "exact_observed_at": time.time()},
    })

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [20]


def test_resolve_s3_snapshot_reapplies_freshness_to_exact_value(monkeypatch, tmp_path):
    # 스냅샷 안의 exact 값도 라이브 RDS 조회와 똑같은 신선도 기준을 다시
    # 적용해야 한다 - 오래된 값이면 AVG로 내려가야 함.
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {
        "1": {"avg": 71.0, "spec": 66.0, "exact_value": 20.0, "exact_observed_at": time.time() - 999_999},
    })

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [71]


def test_resolve_s3_snapshot_falls_back_to_spec_when_avg_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {"1": {"spec": 66.0}})

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [66]


def test_resolve_falls_back_to_hardcoded_when_rds_down_and_no_cache_or_snapshot(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_s3_snapshot_loads_only_once_per_process(monkeypatch, tmp_path):
    # 세그먼트마다 S3를 매번 부르면 왕복이 세그먼트 수만큼 쌓이는 문제가
    # 재발한다 - 프로세스당 딱 한 번만 읽어야 한다.
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {"1": {"avg": 71.0}, "2": {"avg": 82.0}})

    with patch.object(nav_lookup.rds, "batch_resolve_type1_rows", side_effect=RuntimeError("rds down")), \
         patch.object(nav_lookup.gold_snapshot, "read_snapshot", wraps=nav_lookup.gold_snapshot.read_snapshot) as mock_read:
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [71, 82]
    mock_read.assert_called_once()


# ---------------------------------------------------------------------------
# Type2 (RDS, 시간 무관 정적값) — 정확한 segment_id 값 -> 코드 상수
# (GLOBAL#DEFAULT 같은 전역 폴백 행은 안 둔다 - 정성적 초안값이라 DB에
# 저장할 이유가 약함)
# ---------------------------------------------------------------------------

def test_resolve_type2_returns_exact_value_when_present(rds_type2_table):
    _put_static_row(rds_type2_table, "1", 300)

    result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [300]


def test_resolve_type2_falls_back_to_hardcoded_when_missing(rds_type2_table):
    result = nav_lookup.resolve_segment_values(["999"], 2, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[2]]


def test_resolve_type2_falls_back_to_hardcoded_when_rds_unreachable():
    with patch.object(nav_lookup.rds, "batch_get_static_values", side_effect=RuntimeError("network down")):
        result = nav_lookup.resolve_segment_values(["1", "2"], 2, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[2]] * 2


def test_resolve_preserves_order_and_duplicates(rds_type2_table):
    _put_static_row(rds_type2_table, "1", 100)
    _put_static_row(rds_type2_table, "2", 200)

    result = nav_lookup.resolve_segment_values(["2", "1", "2"], 2, "12:00")

    assert result == [200, 100, 200]


def test_resolve_type2_still_dedupes_since_time_independent(rds_type2_table):
    _put_static_row(rds_type2_table, "1", 500)

    with patch.object(nav_lookup.rds, "batch_get_static_values", wraps=rds.batch_get_static_values) as mock_batch:
        result = nav_lookup.resolve_segment_values(["1", "1", "1"], 2, "09:00")

    assert result == [500, 500, 500]
    called_segment_ids = mock_batch.call_args.args[1]
    assert called_segment_ids == ["1"]


# ---------------------------------------------------------------------------
# 공통 (type/시각 무관)
# ---------------------------------------------------------------------------

def test_resolve_never_raises_on_invalid_type():
    result = nav_lookup.resolve_segment_values(["1", "2"], 3, "12:00")

    assert len(result) == 2
    assert all(isinstance(v, int) for v in result)


def test_resolve_never_raises_on_malformed_time():
    result = nav_lookup.resolve_segment_values(["1"], 1, "not-a-time")

    assert len(result) == 1
    assert isinstance(result[0], int)


def test_add_seconds_advances_within_same_hour():
    assert nav_lookup._add_seconds("12:00", 600) == "12:10"


def test_add_seconds_wraps_past_midnight():
    assert nav_lookup._add_seconds("23:50", 900) == "00:05"
