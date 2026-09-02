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


def test_compute_time_seconds_includes_last_observed_at(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["last_observed_at"] == datetime(2026, 8, 21, 12, 5)


def test_compute_time_seconds_last_observed_at_uses_latest_when_dates_mixed(spark):
    # 같은 세그먼트/버킷(0000)에 서로 다른 날짜의 판독값이 섞여 들어오는 경우
    # (자정 경계 등) -> 가장 최근 observed_at을 last_observed_at으로 쓴다.
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 20.0, "observed_at": datetime(2026, 8, 21, 0, 5)},
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 22, 0, 10)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["bucket"] == "0000"
    assert result[0]["last_observed_at"] == datetime(2026, 8, 22, 0, 10)


def test_compute_time_seconds_zero_length_produces_zero_time(spark):
    # length_ft<=0인 세그먼트가 Gold1(속도만 거름)을 통과해 여기까지 오면
    # time_seconds=0이 그대로 계산된다 - validate_bucket_time_seconds가
    # 이걸 잡아야 하는 이유를 보여주는 케이스.
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 0.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert len(result) == 1
    assert result[0]["time_seconds"] == 0.0


def test_validate_bucket_time_seconds_passes_through_valid_rows(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 120.0, "last_observed_at": datetime(2026, 8, 21, 12, 0)},
    ])

    result = gold2.validate_bucket_time_seconds(df)

    assert result is df


def test_validate_bucket_time_seconds_rejects_zero_or_negative(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 5)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 0.0}])
    bucket_df = gold2.compute_time_seconds(df, dim_segment_length_df)

    with pytest.raises(ValueError, match="0 이하"):
        gold2.validate_bucket_time_seconds(bucket_df)


@requires_postgres
def test_to_serving_items_incrementally_updates_avg_per_slot(spark):
    # avg는 세그먼트 전체가 아니라 슬롯(segment_id, time) 단위다 - 같은
    # 세그먼트라도 슬롯이 다르면 서로 다른 avg를 갖는다.
    _create_test_table()

    # 1) 빈 테이블 -> 슬롯(1, 1200)에 30 upsert(배치 t1) -> avg=30, count=1
    t1 = datetime(2026, 8, 21, 12, 0)
    df1 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "last_observed_at": t1},
    ])
    items1 = gold2.to_serving_items(df1, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items1, TABLE_NAME)

    by_key1 = _items_by_key(items1)
    assert by_key1[("1", "1200")]["value"] == 30
    assert by_key1[("1", "1200")]["avg"] == 30
    assert by_key1[("1", "1200")]["count"] == 1

    # 2) 다른 슬롯(1, 1230)에 50 upsert -> 슬롯 1200과 무관하게 avg=50, count=1
    df2 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "last_observed_at": datetime(2026, 8, 21, 12, 30)},
    ])
    items2 = gold2.to_serving_items(df2, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items2, TABLE_NAME)

    by_key2 = _items_by_key(items2)
    assert by_key2[("1", "1230")]["value"] == 50
    assert by_key2[("1", "1230")]["avg"] == 50
    assert by_key2[("1", "1230")]["count"] == 1

    # 3) 슬롯 1200에 진짜 새 배치(t2)가 60으로 들어옴 -> avg=(30+60)/2=45, count=2
    t2 = datetime(2026, 8, 21, 12, 30)
    df3 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 60.0, "last_observed_at": t2},
    ])
    items3 = gold2.to_serving_items(df3, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items3, TABLE_NAME)

    by_key3 = _items_by_key(items3)
    assert by_key3[("1", "1200")]["value"] == 60
    assert by_key3[("1", "1200")]["avg"] == 45
    assert by_key3[("1", "1200")]["count"] == 2

    # 4) 같은 배치(t2)가 Airflow 재시도 등으로 다시 들어옴 -> avg/count가
    # 그대로 승계돼야 한다(이미 반영한 배치를 또 반영해 중복 카운트되면 안 됨).
    df4 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 60.0, "last_observed_at": t2},
    ])
    items4 = gold2.to_serving_items(df4, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items4, TABLE_NAME)

    by_key4 = _items_by_key(items4)
    assert by_key4[("1", "1200")]["avg"] == 45
    assert by_key4[("1", "1200")]["count"] == 2

    # 5) 진짜 다음 배치(t3)가 90으로 들어옴 -> avg=(30+60+90)/3=60, count=3
    t3 = datetime(2026, 8, 21, 13, 0)
    df5 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 90.0, "last_observed_at": t3},
    ])
    items5 = gold2.to_serving_items(df5, TABLE_NAME, today=TODAY)
    gold2.write_to_rds(items5, TABLE_NAME)

    by_key5 = _items_by_key(items5)
    assert by_key5[("1", "1200")]["avg"] == 60
    assert by_key5[("1", "1200")]["count"] == 3


@requires_postgres
def test_to_serving_items_handles_legacy_row_without_count(spark):
    # count 필드 없이 저장된 옛 버전 데이터를 시뮬레이션.
    _create_test_table()
    gold2.write_to_rds(
        [{"segment_id": "1", "time": "1200", "value": 42, "updated_date": TODAY.isoformat()}],
        TABLE_NAME,
    )

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "last_observed_at": datetime(2026, 8, 21, 12, 0)},
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
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "last_observed_at": datetime(2026, 8, 21, 12, 0)},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "last_observed_at": datetime(2026, 8, 21, 12, 30)},
    ])

    items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    by_key = _items_by_key(items)
    assert by_key[("1", "1200")]["value"] == 30
    assert by_key[("1", "1200")]["avg"] == 30
    assert by_key[("1", "1230")]["value"] == 50
    assert by_key[("1", "1230")]["avg"] == 50


def test_to_serving_items_caps_avg_smoothing_but_not_stored_count(spark):
    # count는 10을 넘어도 계속(정직하게) 늘어나지만, avg 계산에서 나누는
    # 수는 AVG_SMOOTHING_WINDOW(10)에서 멈춘다 - 그래야 배치가 아무리
    # 많이 쌓여도 새 값의 영향력이 최소 1/10로 유지된다.
    old_row = {"value": 60, "avg": 60, "count": 10, "last_sample_at": "2026-08-01T00:00:00"}
    df = spark.createDataFrame([
        {
            "segment_id": "1", "bucket": "1200", "time_seconds": 200.0,
            "last_observed_at": datetime(2026, 8, 21, 12, 0),
        },
    ])

    with patch.object(gold2, "batch_get_items", return_value={("1", "1200"): old_row}):
        items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    by_key = _items_by_key(items)
    # count는 정직하게 11로 증가.
    assert by_key[("1", "1200")]["count"] == 11
    # 근데 나누는 수는 11이 아니라 10으로 묶여서: 60 + (200-60)/10 = 74.
    assert by_key[("1", "1200")]["avg"] == 74


def test_to_serving_items_includes_last_sample_at_and_updated_date(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "last_observed_at": datetime(2026, 8, 21, 12, 0)},
    ])

    with patch.object(gold2, "batch_get_items", return_value={}):
        items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    by_key = _items_by_key(items)
    assert by_key[("1", "1200")]["last_sample_at"] == "2026-08-21T12:00:00"
    assert by_key[("1", "1200")]["updated_date"] == "2026-08-21"


def test_to_serving_items_stamps_avg_formula_version(spark):
    # 저장되는 모든 행에 avg를 계산한 공식 버전이 새겨져야 한다(lineage).
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "last_observed_at": datetime(2026, 8, 21, 12, 0)},
    ])

    with patch.object(gold2, "batch_get_items", return_value={}):
        items = gold2.to_serving_items(df, TABLE_NAME, today=TODAY)

    assert items[0]["avg_formula_version"] == gold2.AVG_FORMULA_VERSION
    assert gold2.AVG_FORMULA_VERSION.startswith("v1+")


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
