import time
from unittest.mock import patch

import boto3
import pytest
from moto import mock_aws

from src.common import gold_snapshot, rds
from src.common.config import AWS_REGION, NAV_GOLD_RDS_LOCAL_DSN
from src.serving import nav_lookup

RDS_TEST_TABLE = "test_segment_metrics_type1_lookup"


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


def test_time_to_bucket_rounds_down_to_30_minutes():
    assert nav_lookup.time_to_bucket("12:03") == "1200"
    assert nav_lookup.time_to_bucket("12:47") == "1230"
    assert nav_lookup.time_to_bucket("00:00") == "0000"


def test_table_for_type():
    assert nav_lookup.table_for_type(1) == nav_lookup.RDS_TABLE_TYPE1
    assert nav_lookup.table_for_type(2) == nav_lookup.DYNAMODB_TABLE_TYPE2


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
    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("network down")):
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


def test_resolve_time_values_circuit_breaker_bounds_rds_calls():
    # RDS가 완전히 죽었을 때, 세그먼트 수만큼 순차로 느린 실패가 쌓이면
    # 안 된다 - 연속 실패가 임계치를 넘으면 남은 세그먼트는 RDS를 더 안
    # 건드리고 메모리/S3 폴백(둘 다 비어있으니 코드 상수)으로 바로 채워야 한다.
    segment_ids = [str(i) for i in range(10)]

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("network down")) as mock_resolve:
        result = nav_lookup.resolve_segment_values(segment_ids, 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]] * 10
    assert mock_resolve.call_count == nav_lookup._CIRCUIT_BREAKER_THRESHOLD


def test_resolve_time_values_opens_circuit_when_time_budget_exceeded():
    # 호출이 전부 성공해도(장애 아님) 세그먼트가 많아 순차 호출이 쌓이면
    # 응답이 Lambda 타임아웃을 넘길 수 있다(실측 확인됨). 남은 시간이
    # 얼마 안 되면 성공/실패와 무관하게 회로를 열어 남은 세그먼트는
    # RDS를 더 안 건드리고 폴백으로 채워야 한다.
    def fake_resolve_type1_tiers(segment_id, bucket_sk, table_name):
        if segment_id in ("1", "2") and bucket_sk == "1200":
            return {"1200": {"value": 111, "observed_at": time.time()}}
        return {}

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=fake_resolve_type1_tiers) as mock_resolve, \
         patch.object(nav_lookup.time, "monotonic", side_effect=[0.0, 0.0, 0.0, 100.0]):
        result = nav_lookup.resolve_segment_values(["1", "2", "3", "4"], 1, "12:00")

    assert result == [111, 111, nav_lookup._HARDCODED_DEFAULTS[1], nav_lookup._HARDCODED_DEFAULTS[1]]
    # 세그먼트 "3"에서 예산 초과가 감지된 시점 이후로는(그 세그먼트 포함)
    # RDS를 더 안 건드려야 한다 - "1", "2"만 실제 조회됨.
    assert mock_resolve.call_count == 2


# ---------------------------------------------------------------------------
# Type1 — RDS 자체가 응답 불가능할 때: 메모리 캐시 -> S3 Gold 스냅샷 -> 코드 상수
# ---------------------------------------------------------------------------

def test_resolve_uses_memory_cache_when_rds_down(rds_table):
    _put_row(rds_table, "1", "AVG", 40)
    nav_lookup.resolve_segment_values(["1"], 1, "12:00")  # 메모리 캐시 예열

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [40]


def test_resolve_uses_s3_snapshot_when_rds_down_and_memory_empty(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {
        "1": {"avg": 71.0, "spec": 66.0, "exact_value": None, "exact_observed_at": None},
    })

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [71]


def test_resolve_s3_snapshot_prefers_fresh_exact_over_avg(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {
        "1": {"avg": 71.0, "spec": 66.0, "exact_value": 20.0, "exact_observed_at": time.time()},
    })

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [20]


def test_resolve_s3_snapshot_reapplies_freshness_to_exact_value(monkeypatch, tmp_path):
    # 스냅샷 안의 exact 값도 라이브 RDS 조회와 똑같은 신선도 기준을 다시
    # 적용해야 한다 - 오래된 값이면 AVG로 내려가야 함.
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {
        "1": {"avg": 71.0, "spec": 66.0, "exact_value": 20.0, "exact_observed_at": time.time() - 999_999},
    })

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [71]


def test_resolve_s3_snapshot_falls_back_to_spec_when_avg_missing(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {"1": {"spec": 66.0}})

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["1"], 1, "12:00")

    assert result == [66]


def test_resolve_falls_back_to_hardcoded_when_rds_down_and_no_cache_or_snapshot(monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")):
        result = nav_lookup.resolve_segment_values(["999"], 1, "12:00")

    assert result == [nav_lookup._HARDCODED_DEFAULTS[1]]


def test_resolve_s3_snapshot_loads_only_once_per_process(monkeypatch, tmp_path):
    # 세그먼트마다 S3를 매번 부르면 RDS 순차 조회가 느려서 겪었던 것과 같은
    # 문제(_TIME_BUDGET_SECONDS)가 재발한다 - 프로세스당 딱 한 번만 읽어야 한다.
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)
    gold_snapshot.write_snapshot("type1", {"1": {"avg": 71.0}, "2": {"avg": 82.0}})

    with patch.object(nav_lookup.rds, "resolve_type1_tiers", side_effect=RuntimeError("rds down")), \
         patch.object(nav_lookup.gold_snapshot, "read_snapshot", wraps=nav_lookup.gold_snapshot.read_snapshot) as mock_read:
        result = nav_lookup.resolve_segment_values(["1", "2"], 1, "12:00")

    assert result == [71, 82]
    mock_read.assert_called_once()


# ---------------------------------------------------------------------------
# Type2 (여전히 DynamoDB) — 정확한 (segment_id, LENGTH) -> GLOBAL#DEFAULT -> 코드 상수
# ---------------------------------------------------------------------------

def _create_table(table_name, region=AWS_REGION):
    client = boto3.client("dynamodb", region_name=region)
    client.create_table(
        TableName=table_name,
        KeySchema=[
            {"AttributeName": "segment_id", "KeyType": "HASH"},
            {"AttributeName": "sk", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "segment_id", "AttributeType": "S"},
            {"AttributeName": "sk", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )


@mock_aws
def test_resolve_type2_has_no_avg_tier_goes_straight_to_default():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(
        nav_lookup.DYNAMODB_TABLE_TYPE2,
        {"segment_id": nav_lookup.GLOBAL_PARTITION_KEY, "sk": nav_lookup.DEFAULT_SORT_KEY, "value": 300},
    )
    # type2는 sk가 항상 "LENGTH"라, "AVG" 항목이 있어도 안 쓰여야 한다.
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "AVG", "value": 999})

    result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [300]


@mock_aws
def test_resolve_preserves_order_and_duplicates():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "LENGTH", "value": 100})
    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "2", "sk": "LENGTH", "value": 200})

    result = nav_lookup.resolve_segment_values(["2", "1", "2"], 2, "12:00")

    assert result == [200, 100, 200]


@mock_aws
def test_resolve_type2_still_dedupes_since_time_independent():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import put_item

    put_item(nav_lookup.DYNAMODB_TABLE_TYPE2, {"segment_id": "1", "sk": "LENGTH", "value": 500})

    result = nav_lookup.resolve_segment_values(["1", "1", "1"], 2, "09:00")

    assert result == [500, 500, 500]


@mock_aws
def test_resolve_skips_malformed_item_and_falls_through():
    _create_table(nav_lookup.DYNAMODB_TABLE_TYPE2)
    from src.common.dynamodb import get_table, put_item

    # "value" 필드가 없는 깨진 항목을 직접 DynamoDB에 넣음(put_item 헬퍼로는 못 만드니 저수준으로)
    get_table(nav_lookup.DYNAMODB_TABLE_TYPE2).put_item(Item={"segment_id": "1", "sk": "LENGTH"})
    put_item(
        nav_lookup.DYNAMODB_TABLE_TYPE2,
        {"segment_id": nav_lookup.GLOBAL_PARTITION_KEY, "sk": nav_lookup.DEFAULT_SORT_KEY, "value": 300},
    )

    result = nav_lookup.resolve_segment_values(["1"], 2, "12:00")

    assert result == [300]


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
