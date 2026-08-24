from datetime import date, datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.common import gold_snapshot, rds
from src.common.config import NAV_GOLD_RDS_LOCAL_DSN
from src.nav_time import gold2

TABLE_NAME = "test_segment_metrics_type1_gold2"


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_time_gold2_test").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def rds_table(monkeypatch):
    """실제 로컬 Postgres(docker-compose의 nav-gold-postgres 컨테이너,
    미리 `docker compose up -d nav-gold-postgres`로 띄워둬야 한다)로
    검증한다 — RDS는 DynamoDB의 moto 같은 인메모리 mock 수단이 없다."""
    monkeypatch.setattr(rds, "get_rds_dsn", lambda: NAV_GOLD_RDS_LOCAL_DSN)
    rds._connection = None
    rds.ensure_table(TABLE_NAME)
    conn = rds.get_connection()
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE TABLE {TABLE_NAME}")
    yield TABLE_NAME
    rds._connection = None


def _put_row(table_name, segment_id, sk, value, count=None):
    rds.upsert_items(
        [{
            "segment_id": segment_id,
            "sk": sk,
            "value": value,
            "observed_at": None,
            "collected_date": None,
            "count": count,
        }],
        table_name,
    )


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


def test_compute_time_seconds_observed_at_is_the_bucket_max_timestamp(spark):
    # observed_at은 collected_date(날짜만)와 별개로, freshness 판단에 쓸
    # epoch 타임스탬프 원본이 그대로 나와야 한다(src.serving.nav_lookup._is_fresh).
    df = spark.createDataFrame([
        {"segment_id": "1", "speed": 20.0, "observed_at": datetime(2026, 8, 21, 12, 0)},
        {"segment_id": "1", "speed": 30.0, "observed_at": datetime(2026, 8, 21, 12, 10)},
    ])
    dim_segment_length_df = pd.DataFrame([{"segment_id": "1", "length_ft": 5280.0}])

    result = gold2.compute_time_seconds(df, dim_segment_length_df).collect()

    assert result[0]["observed_at"] == datetime(2026, 8, 21, 12, 10)


def test_to_type1_items_incrementally_updates_avg(spark, rds_table):
    # 1) 빈 테이블 -> 세그먼트 1의 버킷 1200에 30 upsert -> AVG=30, count=1
    df1 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])
    items1 = gold2.to_type1_items(df1, rds_table)
    gold2.write_to_rds(items1, rds_table)

    by_sk1 = {(i["segment_id"], i["sk"]): i for i in items1}
    assert by_sk1[("1", "1200")]["value"] == 30
    assert by_sk1[("1", "AVG")]["value"] == 30
    assert by_sk1[("1", "AVG")]["count"] == 1

    # 2) 새 버킷 1230에 50 upsert -> AVG=(30+50)/2=40, count=2
    df2 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])
    items2 = gold2.to_type1_items(df2, rds_table)
    gold2.write_to_rds(items2, rds_table)

    by_sk2 = {(i["segment_id"], i["sk"]): i for i in items2}
    assert by_sk2[("1", "1230")]["value"] == 50
    assert by_sk2[("1", "AVG")]["value"] == 40
    assert by_sk2[("1", "AVG")]["count"] == 2

    # 3) 기존 버킷 1200을 60으로 교체 -> AVG=(60+50)/2=55, count는 그대로 2
    df3 = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 60.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])
    items3 = gold2.to_type1_items(df3, rds_table)
    gold2.write_to_rds(items3, rds_table)

    by_sk3 = {(i["segment_id"], i["sk"]): i for i in items3}
    assert by_sk3[("1", "1200")]["value"] == 60
    assert by_sk3[("1", "AVG")]["value"] == 55
    assert by_sk3[("1", "AVG")]["count"] == 2


def test_to_type1_items_handles_legacy_avg_item_without_count(spark, rds_table):
    # 레거시 AVG 레코드: count 없이(RDS에선 NULL로) 저장된 옛 버전 데이터를 시뮬레이션.
    _put_row(rds_table, "1", "AVG", 42, count=None)

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])

    # TypeError 없이 정상 동작해야 한다(count가 NULL인 컬럼 값 -> int(None) 방지).
    items = gold2.to_type1_items(df, rds_table)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    # count 없던 레거시 레코드는 old_count=0으로 취급 -> new_count=1
    assert by_sk[("1", "AVG")]["count"] == 1
    assert by_sk[("1", "AVG")]["value"] == round(42.0 + (30.0 - 42.0) / 1)


def test_to_type1_items_resets_legacy_avg_when_bucket_already_exists(spark, rds_table):
    # 레거시 AVG(count NULL) + 이미 존재하는 버킷 값이 같이 있는 상태.
    # old_count=0을 "1개짜리 평균"으로 착각해 델타를 통째로 반영하면 평균이
    # 무한정 발산한다(회귀 재현 시 42 -> -130 -> -275 -> ... 로 계속 떨어짐).
    # count를 모르면 old_avg를 버리고 리셋해야 한다.
    _put_row(rds_table, "1", "AVG", 42, count=None)
    _put_row(rds_table, "1", "1200", 100)

    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])
    items = gold2.to_type1_items(df, rds_table)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    assert by_sk[("1", "AVG")]["count"] == 1
    assert by_sk[("1", "AVG")]["value"] == 30


def test_to_type1_items_folds_multiple_buckets_of_same_segment_sequentially(spark, rds_table):
    # 한 번의 호출에 같은 세그먼트의 버킷이 2개(수집 구간 경계 겹침 등으로) 동시에
    # 들어와도, 순차적으로 접어(fold) 반영해서 AVG가 정확히 계산되고 세그먼트당
    # AVG 항목이 딱 1개만 나와야 한다.
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
        {"segment_id": "1", "bucket": "1230", "time_seconds": 50.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])

    items = gold2.to_type1_items(df, rds_table)

    avg_items = [i for i in items if i["segment_id"] == "1" and i["sk"] == "AVG"]
    assert len(avg_items) == 1
    assert avg_items[0]["value"] == 40  # (30+50)/2
    assert avg_items[0]["count"] == 2

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["value"] == 30
    assert by_sk[("1", "1230")]["value"] == 50


def test_to_type1_items_includes_collected_date_and_observed_at_in_bucket_items(spark, rds_table):
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])

    items = gold2.to_type1_items(df, rds_table)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert by_sk[("1", "1200")]["collected_date"] == "2026-08-21"
    assert by_sk[("1", "1200")]["observed_at"] == datetime(2026, 8, 21, 12, 0, 0).timestamp()


def test_to_type1_items_avg_item_has_no_collected_date_or_observed_at(spark, rds_table):
    df = spark.createDataFrame([
        {"segment_id": "1", "bucket": "1200", "time_seconds": 30.0, "collected_date": date(2026, 8, 21), "observed_at": datetime(2026, 8, 21, 12, 0, 0)},
    ])

    items = gold2.to_type1_items(df, rds_table)

    by_sk = {(i["segment_id"], i["sk"]): i for i in items}
    assert "collected_date" not in by_sk[("1", "AVG")]
    assert "observed_at" not in by_sk[("1", "AVG")]


def test_write_to_rds_upserts_and_returns_count(rds_table):
    items = [{"segment_id": "1", "sk": "1200", "value": 30}]

    count = gold2.write_to_rds(items, rds_table)

    assert count == 1
    existing = rds.batch_get_rows(rds_table, [("1", "1200")])
    assert existing[("1", "1200")]["value"] == 30


def test_write_to_rds_refreshes_s3_snapshot(rds_table, monkeypatch, tmp_path):
    monkeypatch.setattr(gold_snapshot, "GOLD_CACHE_DIR", tmp_path)

    items = [{"segment_id": "1", "sk": "AVG", "value": 25, "count": 2}]
    gold2.write_to_rds(items, rds_table)

    snapshot = gold_snapshot.read_snapshot("type1")
    assert snapshot["1"]["avg"] == 25.0


def test_compute_spec_travel_seconds_uses_length_and_speed_limit():
    # 500ft / 25mph -> (500/5280) / 25 * 3600 = 13.6363...초
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 500.0, "speed_limit_mph": 25.0},
    ])

    result = gold2.compute_spec_travel_seconds(df)

    assert result.iloc[0]["segment_id"] == "1"
    assert round(result.iloc[0]["spec_travel_time_sec"], 2) == 13.64


def test_compute_spec_travel_seconds_excludes_missing_speed_limit():
    # 제한속도 미표기(NaN) segment는 추정 자체가 불가능해 결과에서 빠져야 한다.
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 500.0, "speed_limit_mph": 25.0},
        {"segment_id": "2", "length_ft": 300.0, "speed_limit_mph": float("nan")},
    ])

    result = gold2.compute_spec_travel_seconds(df)

    assert list(result["segment_id"]) == ["1"]


def test_compute_spec_travel_seconds_excludes_zero_or_missing_length():
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 0.0, "speed_limit_mph": 25.0},
        {"segment_id": "2", "length_ft": float("nan"), "speed_limit_mph": 25.0},
        {"segment_id": "3", "length_ft": 100.0, "speed_limit_mph": 0.0},
    ])

    result = gold2.compute_spec_travel_seconds(df)

    assert result.empty


def test_spec_estimate_items_formats_sort_key_and_rounds_value():
    spec_df = pd.DataFrame([
        {"segment_id": "1", "spec_travel_time_sec": 13.64},
    ])

    items = gold2.spec_estimate_items(spec_df)

    assert items == [{"segment_id": "1", "sk": "SPEC", "value": 14}]
