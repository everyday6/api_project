from datetime import date, datetime
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
from moto import mock_aws
from pyspark.sql import SparkSession

from src.common.config import AWS_REGION
from src.nav_time import gold2

TABLE_NAME = "TestSegmentMetricsType1"


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold2_test").getOrCreate()
    yield session
    session.stop()


def _create_test_table():
    client = boto3.client("dynamodb", region_name=AWS_REGION)
    client.create_table(
        TableName=TABLE_NAME,
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
    return client


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


@mock_aws
def test_to_dynamodb_items_incrementally_updates_avg(spark):
    _create_test_table()

    # 1) 빈 테이블 -> 세그먼트 1의 버킷 1200에 30 upsert -> AVG=30, count=1
    df1 = spark.createDataFrame([{"segment_id": "1", "bucket": "1200", "time_seconds": 30.0}])
    items1 = gold2.to_dynamodb_items(df1, TABLE_NAME)
    gold2.write_to_dynamodb(items1, TABLE_NAME)

    by_sk1 = {(i["segment_id"], i["sk"]): i for i in items1}
    assert by_sk1[("1", "1200")]["value"] == 30
    assert by_sk1[("1", "AVG")]["value"] == 30
    assert by_sk1[("1", "AVG")]["count"] == 1

    # 2) 새 버킷 1230에 50 upsert -> AVG=(30+50)/2=40, count=2
    df2 = spark.createDataFrame([{"segment_id": "1", "bucket": "1230", "time_seconds": 50.0}])
    items2 = gold2.to_dynamodb_items(df2, TABLE_NAME)
    gold2.write_to_dynamodb(items2, TABLE_NAME)

    by_sk2 = {(i["segment_id"], i["sk"]): i for i in items2}
    assert by_sk2[("1", "1230")]["value"] == 50
    assert by_sk2[("1", "AVG")]["value"] == 40
    assert by_sk2[("1", "AVG")]["count"] == 2

    # 3) 기존 버킷 1200을 60으로 교체 -> AVG=(60+50)/2=55, count는 그대로 2
    df3 = spark.createDataFrame([{"segment_id": "1", "bucket": "1200", "time_seconds": 60.0}])
    items3 = gold2.to_dynamodb_items(df3, TABLE_NAME)
    gold2.write_to_dynamodb(items3, TABLE_NAME)

    by_sk3 = {(i["segment_id"], i["sk"]): i for i in items3}
    assert by_sk3[("1", "1200")]["value"] == 60
    assert by_sk3[("1", "AVG")]["value"] == 55
    assert by_sk3[("1", "AVG")]["count"] == 2


@mock_aws
def test_to_dynamodb_items_handles_legacy_avg_item_without_count(spark):
    # 레거시 AVG 레코드: count 필드 없이 저장된 옛 버전 데이터를 시뮬레이션.
    client = _create_test_table()
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "segment_id": {"S": "1"},
            "sk": {"S": "AVG"},
            "value": {"N": "42"},
        },
    )

    df = spark.createDataFrame([{"segment_id": "1", "bucket": "1200", "time_seconds": 30.0}])

    # KeyError 없이 정상 동작해야 한다.
    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    # count 없던 레거시 레코드는 old_count=0으로 취급 -> new_count=1
    assert by_sk[("1", "AVG")]["count"] == 1
    assert by_sk[("1", "AVG")]["value"] == round(42.0 + (30.0 - 42.0) / 1)


@mock_aws
def test_to_dynamodb_items_resets_legacy_avg_when_bucket_already_exists(spark):
    # 레거시 AVG(count 없음) + 이미 존재하는 버킷 값이 같이 있는 상태.
    # old_count=0을 "1개짜리 평균"으로 착각해 델타를 통째로 반영하면 평균이
    # 무한정 발산한다(회귀 재현 시 42 -> -130 -> -275 -> ... 로 계속 떨어짐).
    # count를 모르면 old_avg를 버리고 리셋해야 한다.
    client = _create_test_table()
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "segment_id": {"S": "1"},
            "sk": {"S": "AVG"},
            "value": {"N": "42"},
        },
    )
    client.put_item(
        TableName=TABLE_NAME,
        Item={
            "segment_id": {"S": "1"},
            "sk": {"S": "1200"},
            "value": {"N": "100"},
        },
    )

    df = spark.createDataFrame([{"segment_id": "1", "bucket": "1200", "time_seconds": 30.0}])
    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    assert by_sk[("1", "AVG")]["count"] == 1
    assert by_sk[("1", "AVG")]["value"] == 30


@mock_aws
def test_to_dynamodb_items_folds_multiple_buckets_of_same_segment_sequentially(spark):
    # 한 번의 호출에 같은 세그먼트의 버킷이 2개(수집 구간 경계 겹침 등으로) 동시에
    # 들어와도, 순차적으로 접어(fold) 반영해서 AVG가 정확히 계산되고 세그먼트당
    # AVG 항목이 딱 1개만 나와야 한다.
    _create_test_table()

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0},
    ])

    items = gold2.to_dynamodb_items(df, TABLE_NAME)

    avg_items = [i for i in items if i["segment_id"] == "1" and i["sk"] == "AVG"]
    assert len(avg_items) == 1
    assert avg_items[0]["value"] == 40  # (30+50)/2
    assert avg_items[0]["count"] == 2

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    assert by_sk[("1", "1230")]["value"] == 50


def test_write_to_dynamodb_calls_batch_write_and_returns_count():
    items = [{"segment_id": "1", "sk": "1200", "value": 30}]

    with patch.object(gold2, "batch_write_items") as mock_write:
        count = gold2.write_to_dynamodb(items, "SegmentMetricsType1")

    mock_write.assert_called_once_with("SegmentMetricsType1", items)
    assert count == 1
