from datetime import date, datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.common.config import SERVING_TABLE_TYPE1_COLUMNS, SERVING_TABLE_TYPE1_KEY_COLUMNS
from src.nav_time import gold2
from tests.conftest import reset_table

# compute_time_seconds 계열은 순수 Spark 연산이라 RDS가 없어도 도는데,
# 파일 전체에 pytestmark를 걸면 그 테스트들까지 불필요하게 스킵된다 -
# RDS를 실제로 쓰는 테스트(to_serving_items/write_to_rds 왕복)에만
# @pytest.mark.usefixtures("require_postgres")를 개별로 붙인다.
requires_postgres = pytest.mark.usefixtures("require_postgres")

TABLE_NAME = "test_segment_metrics_type1"
TODAY = date(2026, 8, 21)


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold2_test").getOrCreate()
    yield session
    session.stop()


def _create_test_table():
    reset_table(TABLE_NAME, SERVING_TABLE_TYPE1_COLUMNS, SERVING_TABLE_TYPE1_KEY_COLUMNS)


def _items_by_key(items):
    return {(item["segment_id"], item["time"]): item for item in items}


def test_compute_time_seconds_uses_length_and_speed(spark):
    # 길이 5280ft(1마일)를 30mph로 -> 1/30시간 = 120초
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["bucket"] == "1200"
    assert abs(result[0]["time_seconds"] - 120.0) < 0.01


def test_compute_time_seconds_buckets_to_30_minutes(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 47)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["bucket"] == "1230"


def test_compute_time_seconds_uses_increasing_weighted_average(spark):
    # 같은 세그먼트/버킷에 속도 10, 30이 시간순으로 두 번 들어옴.
    # 단순평균이면 20이지만, 1:2 가중평균이면 10*(1/3)+30*(2/3) = 23.333...
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 10.0, "observed_at": datetime(2026, 8, 21, 12, 0)},
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    weighted_avg_speed = 10.0 * 1 / 3 + 30.0 * 2 / 3  # 23.333...
    expected_time_seconds = (5280.0 / 5280.0) / weighted_avg_speed * 3600.0
    naive_avg_time_seconds = (5280.0 / 5280.0) / 20.0 * 3600.0  # 단순평균(20)이었다면 나올 값

    assert len(result) == 1
    assert abs(result[0]["time_seconds"] - expected_time_seconds) < 0.01
    assert abs(result[0]["time_seconds"] - naive_avg_time_seconds) > 1.0


def test_compute_time_seconds_excludes_zero_speed_segment(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
        {"segment_id": "2", "speed": 0.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 5280.0},
        {"segment_id": "2", "length_ft": 5280.0},
    ])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "1"


def test_compute_time_seconds_includes_collected_date(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["collected_date"] == date(2026, 8, 21)


def test_compute_time_seconds_collected_date_uses_latest_observed_at_when_dates_mixed(spark):
    # 같은 세그먼트/버킷(0000)에 서로 다른 날짜의 판독값이 섞여 들어오는 경우
    # (자정 경계 등) -> 가장 최근 observed_at의 날짜를 collected_date로 쓴다.
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 20.0, "observed_at": datetime(2026, 8, 21, 0, 5)},
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 22, 0, 10)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["bucket"] == "0000"
    assert result[0]["collected_date"] == date(2026, 8, 22)


@requires_postgres
def test_to_serving_items_incrementally_updates_avg_per_slot(spark):
    # avg는 세그먼트 전체가 아니라 슬롯(segment_id, time) 단위다 - 같은
    # 세그먼트라도 슬롯이 다르면 서로 다른 avg를 갖는다.
    _create_test_table()

    # 1) 빈 테이블 -> 슬롯(1, 1200)에 30 upsert -> avg=30, count=1
    df1 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": TODAY},
    ])
    items1 = gold2.to_serving_items(df1, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items1, TABLE_NAME)

    by_key1 = _items_by_key(items1)
    assert by_key1[("1", "1200")]["value"] == 30
    assert by_key1[("1", "1200")]["avg"] == 30
    assert by_key1[("1", "1200")]["count"] == 1

    # 2) 다른 슬롯(1, 1230)에 50 upsert -> 슬롯 1200과 무관하게 avg=50, count=1
    df2 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "collected_date": TODAY},
    ])
    items2 = gold2.to_serving_items(df2, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items2, TABLE_NAME)

    by_key2 = _items_by_key(items2)
    assert by_key2[("1", "1230")]["value"] == 50
    assert by_key2[("1", "1230")]["avg"] == 50
    assert by_key2[("1", "1230")]["count"] == 1

    # 3) 슬롯 1200을 60으로 교체 -> avg=(30+60)/2=45, count=2 (슬롯 1230엔 영향 없음)
    df3 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 60.0, "collected_date": TODAY},
    ])
    items3 = gold2.to_serving_items(df3, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items3, TABLE_NAME)

    by_key3 = _items_by_key(items3)
    assert by_key3[("1", "1200")]["value"] == 60
    assert by_key3[("1", "1200")]["avg"] == 45
    assert by_key3[("1", "1200")]["count"] == 2


@requires_postgres
def test_to_serving_items_handles_legacy_row_without_count(spark):
    # count 필드 없이 저장된 옛 버전 데이터를 시뮬레이션.
    _create_test_table()
    gold2.write_to_rds(
        [{"segment_id": "1", "time": "1200", "value": 42, "updated_date": TODAY.isoformat()}],
        TABLE_NAME,
    )

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": TODAY},
    ])

    # KeyError 없이 정상 동작해야 한다.
    items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    by_key = _items_by_key(items)
    assert by_key[("1", "1200")]["value"] == 30
    # count 없던 레거시 행은 old_count=0으로 취급 -> new_count=1, 리셋
    assert by_key[("1", "1200")]["count"] == 1
    assert by_key[("1", "1200")]["avg"] == 30


@requires_postgres
def test_to_serving_items_folds_multiple_slots_of_same_segment_independently(spark):
    # 한 번의 호출에 같은 세그먼트의 슬롯이 2개 들어와도, 슬롯별로 각자의
    # avg/count를 갖는다(서로 섞이지 않는다).
    _create_test_table()

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": TODAY},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "collected_date": TODAY},
    ])

    items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    by_key = _items_by_key(items)
    assert by_key[("1", "1200")]["value"] == 30
    assert by_key[("1", "1200")]["avg"] == 30
    assert by_key[("1", "1230")]["value"] == 50
    assert by_key[("1", "1230")]["avg"] == 50


def test_to_serving_items_includes_collected_date_and_updated_date(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": TODAY},
    ])

    with patch.object(gold2, "batch_get_items", return_value={}):
        items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    by_key = _items_by_key(items)
    assert by_key[("1", "1200")]["collected_date"] == "2026-08-21"
    assert by_key[("1", "1200")]["updated_date"] == "2026-08-21"


def test_write_to_rds_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "time": "1200", "value": 30, "avg": 30, "count": 1}]

    with patch.object(gold2, "batch_write_items") as mock_write, \
         patch.object(gold2, "_export_snapshot", return_value={}), \
         patch.object(gold2.gold_snapshot, "write_snapshot"):
        count = gold2.write_to_rds(items, "SegmentMetricsType1")

    mock_write.assert_called_once_with(
        "SegmentMetricsType1",
        items,
        key_columns=SERVING_TABLE_TYPE1_KEY_COLUMNS,
    )
    assert count == 1


def test_write_to_rds_survives_snapshot_export_failure():
    # 스냅샷 갱신이 실패해도 RDS 쓰기 자체는 이미 끝났으므로 예외를 전파하면
    # 안 된다 - 다음 정상 실행 때 다시 시도되면 충분하다.
    items = [{"segment_id": "1", "time": "1200", "value": 30}]

    with patch.object(gold2, "batch_write_items"), \
         patch.object(gold2, "_export_snapshot", side_effect=RuntimeError("S3 down")):
        count = gold2.write_to_rds(items, "SegmentMetricsType1")

    assert count == 1
