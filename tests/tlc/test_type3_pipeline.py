from datetime import date, datetime, timedelta
from decimal import Decimal
from unittest.mock import patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from src.common.config import TAXI_TYPES
from src.tlc.gold2 import (
    DOW_NAMES,
    TIME_SLOTS,
    _write_type3_partition,
    build_daily_zone_frame,
    build_weekday_rolling_frame,
    expand_zone_values_to_segments,
    validate_daily_zone_month,
    validate_segment_values,
    write_type3_rolling_to_dynamodb,
)
from src.tlc.type3_pipeline import (
    _complete_silver_paths_for_month,
    _find_pending_type3_months,
    _month_success_marker,
    _staging_run_path,
    _type3_metadata_is_current,
    _type3_reference_exists,
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("tlc_type3_pipeline_test")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def _rolling_daily_frame(spark, days=98):
    start = date(2026, 7, 6)  # Monday
    return spark.createDataFrame([
        {
            "zone_id": 1,
            "type": 3,
            "date": start + timedelta(days=offset),
            "time": "1200",
            "value": float(offset),
        }
        for offset in range(days)
    ])


class _FakeBatchWriter:
    def __init__(self, table):
        self.table = table

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def put_item(self, Item):
        if self.table.fail_batch:
            raise RuntimeError("DynamoDB batch failure")
        self.table.items.append(Item)


class _FakeTable:
    """batch_writer로 실제 항목이 쓰이는지 확인하는 가짜 테이블.

    _write_type3_partition은 executor 프로세스 안에서 실행되므로,
    write_type3_rolling_to_dynamodb를 통째로 호출하는 테스트에는 못 쓴다
    (mock은 프로세스 경계를 못 넘음) — _write_type3_partition이 반환하는
    함수를 같은 프로세스에서 직접 호출하는 테스트에만 쓴다.
    """

    def __init__(self, fail_batch=False):
        self.fail_batch = fail_batch
        self.items = []
        self.overwrite_by_pkeys = None

    def batch_writer(self, overwrite_by_pkeys):
        self.overwrite_by_pkeys = overwrite_by_pkeys
        return _FakeBatchWriter(self)


class _FakeMetaTable:
    """write_type3_rolling_to_dynamodb의 완료 메타데이터 기록만 확인하는
    가짜 테이블. driver에서 직접 호출되므로 monkeypatch로 주입해도 된다."""

    def __init__(self):
        self.metadata = []

    def put_item(self, Item):
        self.metadata.append(Item)


def test_complete_silver_paths_require_all_taxi_types(tmp_path):
    for taxi_type in TAXI_TYPES[:-1]:
        output = tmp_path / f"{taxi_type}_tripdata_2026-05"
        output.mkdir()
        (output / "_SUCCESS").touch()

    paths, missing_types = _complete_silver_paths_for_month(
        "2026-05",
        silver_root=tmp_path,
    )

    assert len(paths) == len(TAXI_TYPES) - 1
    assert missing_types == [TAXI_TYPES[-1]]

    final_output = tmp_path / f"{TAXI_TYPES[-1]}_tripdata_2026-05"
    final_output.mkdir()
    (final_output / "_SUCCESS").touch()

    paths, missing_types = _complete_silver_paths_for_month(
        "2026-05",
        silver_root=tmp_path,
    )

    assert len(paths) == len(TAXI_TYPES)
    assert missing_types == []


def test_find_pending_type3_months_uses_month_success_marker(tmp_path):
    silver_root = tmp_path / "silver"
    marker_root = tmp_path / "markers"
    for taxi_type in TAXI_TYPES:
        output = silver_root / f"{taxi_type}_tripdata_2026-05"
        output.mkdir(parents=True)
        (output / "_SUCCESS").touch()

    assert _find_pending_type3_months(
        ["2026-05"],
        silver_root=silver_root,
        marker_root=marker_root,
    ) == ["2026-05"]

    marker = _month_success_marker("2026-05", marker_root)
    marker.parent.mkdir(parents=True)
    marker.touch()

    assert _find_pending_type3_months(
        ["2026-05"],
        silver_root=silver_root,
        marker_root=marker_root,
    ) == []


def test_type3_metadata_is_current_only_after_completed_publish():
    window_start = datetime(2026, 5, 4).date()
    window_end = datetime(2026, 5, 31).date()
    metadata = {
        "status": "COMPLETED",
        "window_start": "2026-05-04",
        "window_end": "2026-05-31",
        "mapping_version": "map-v1",
    }

    assert _type3_metadata_is_current(metadata, window_start, window_end, "map-v1")
    assert not _type3_metadata_is_current(
        {**metadata, "status": "WRITING"},
        window_start,
        window_end,
        "map-v1",
    )
    assert not _type3_metadata_is_current(
        {**metadata, "window_end": "2026-04-30"},
        window_start,
        window_end,
        "map-v1",
    )
    assert not _type3_metadata_is_current(
        metadata,
        window_start,
        window_end,
        "map-v2",
    ), "zone-segment 매핑이 바뀌면(mapping_version 불일치) 최신으로 보면 안 된다"


def test_type3_reference_exists_is_false_until_mapping_is_published(tmp_path):
    """zone_segment_pipeline이 아직 안 돌았을 때(최초 배포/재부트스트랩)
    DAG를 실패시키는 대신 조용히 False를 반환해야 한다."""

    mapping_path = tmp_path / "map_zone_segment.parquet"

    assert _type3_reference_exists(mapping_path) is False

    mapping_path.touch()

    assert _type3_reference_exists(mapping_path) is True


def test_build_weekday_rolling_frame_uses_latest_12_weeks(spark):
    rolling, window_start, window_end = build_weekday_rolling_frame(
        _rolling_daily_frame(spark),
        rolling_weeks=12,
    )

    monday = rolling.filter("dow = 'MON'").first()
    assert window_start == date(2026, 7, 20)
    assert window_end == date(2026, 10, 11)
    assert monday["value"] == 52.5
    assert monday["sample_count"] == 12
    assert rolling.select("dow").distinct().count() == 7


def test_build_weekday_rolling_frame_includes_zero_in_average(spark):
    daily = _rolling_daily_frame(spark, days=14).withColumn(
        "value",
        when(
            col("date") == date(2026, 7, 13),
            28.0,
        ).otherwise(0.0),
    )

    rolling, _, _ = build_weekday_rolling_frame(daily, rolling_weeks=2)

    assert rolling.filter("dow = 'MON'").first()["value"] == 14.0


def test_build_weekday_rolling_frame_rejects_missing_date(spark):
    daily = _rolling_daily_frame(spark, days=84).filter(
        "date != DATE '2026-07-10'"
    )

    with pytest.raises(ValueError, match="연속 데이터"):
        build_weekday_rolling_frame(daily, rolling_weeks=12)


def test_write_type3_partition_builds_expected_sk_and_decimal_value():
    """executor에서 실제로 도는 부분만 떼어내 같은 프로세스에서 직접 검증한다."""

    table = _FakeTable()

    with patch("src.tlc.gold2.get_table", return_value=table):
        write_partition = _write_type3_partition("nav-segment-metrics")
        write_partition([
            {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
            {"segment_id": "0000002", "dow": "MON", "time": "1230", "value": 2.5},
        ])

    assert table.overwrite_by_pkeys == ["segment_id", "sk"]
    assert table.items == [
        {"segment_id": "0000001", "sk": "3#FRI#1200", "value": Decimal("1.5")},
        {"segment_id": "0000002", "sk": "3#MON#1230", "value": Decimal("2.5")},
    ]


def test_write_type3_partition_propagates_batch_failure():
    table = _FakeTable(fail_batch=True)

    with patch("src.tlc.gold2.get_table", return_value=table):
        write_partition = _write_type3_partition("nav-segment-metrics")
        with pytest.raises(RuntimeError, match="batch failure"):
            write_partition([
                {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
            ])


def test_write_type3_rolling_to_dynamodb_writes_metadata_after_success(spark, monkeypatch):
    """실제 파티션 쓰기는 no-op으로 바꿔치기하고, 오케스트레이션(개수 집계 +
    완료 메타데이터 기록)만 검증한다."""

    rolling = spark.createDataFrame([
        {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
        {"segment_id": "0000002", "dow": "MON", "time": "1230", "value": 2.5},
    ])
    table = _FakeMetaTable()
    monkeypatch.setattr("src.tlc.gold2.get_table", lambda _name: table)
    monkeypatch.setattr(
        "src.tlc.gold2._write_type3_partition",
        lambda _table_name: (lambda rows: None),
    )

    written = write_type3_rolling_to_dynamodb(
        "nav-segment-metrics",
        rolling,
        date(2026, 5, 4),
        date(2026, 5, 31),
        12,
        "map-v1",
    )

    assert written == 2
    assert len(table.metadata) == 1
    assert table.metadata[0] | {"updated_at": "ignored"} == {
        "segment_id": "__META__",
        "sk": "TYPE#3",
        "status": "COMPLETED",
        "window_start": "2026-05-04",
        "window_end": "2026-05-31",
        "rolling_weeks": 12,
        "mapping_version": "map-v1",
        "updated_at": "ignored",
    }


def test_write_type3_rolling_to_dynamodb_does_not_complete_after_partition_failure(
    spark, monkeypatch,
):
    rolling = spark.createDataFrame([
        {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
    ])
    table = _FakeMetaTable()

    def _always_fail(_table_name):
        def _write(_rows):
            raise RuntimeError("DynamoDB batch failure")
        return _write

    monkeypatch.setattr("src.tlc.gold2.get_table", lambda _name: table)
    monkeypatch.setattr("src.tlc.gold2._write_type3_partition", _always_fail)

    # foreachPartition은 executor 예외를 Spark 자체 예외 타입으로 감싸서
    # driver에 전파하므로, 원래 예외 타입이 아니라 메시지만으로 확인한다.
    with pytest.raises(Exception, match="batch failure"):
        write_type3_rolling_to_dynamodb(
            "nav-segment-metrics",
            rolling,
            date(2026, 5, 4),
            date(2026, 5, 31),
            12,
        )

    assert table.metadata == []


def _write_staged_month(spark, stage_path, dates, zone_ids=(1, 2)):
    rows = [
        {
            "zone_id": zone_id,
            "type": 3,
            "date": service_date,
            "time": f"{hour_value:02d}{minute_value:02d}",
            "value": 0.0,
        }
        for zone_id in zone_ids
        for service_date in dates
        for hour_value in range(24)
        for minute_value in (0, 30)
    ]
    spark.createDataFrame(rows).write.partitionBy("date").parquet(str(stage_path))


def test_validate_daily_zone_month_accepts_complete_calendar_month(tmp_path, spark):
    stage_path = tmp_path / "stage"
    dates = [date(2026, 2, 1) + timedelta(days=offset) for offset in range(28)]
    _write_staged_month(spark, stage_path, dates)

    result = validate_daily_zone_month(
        spark,
        stage_path,
        "2026-02",
        expected_zone_ids=(1, 2),
    )

    assert result == {
        "month": "2026-02",
        "rows": 2 * 28 * 48,
        "zones": 2,
        "dates": 28,
    }


def test_validate_daily_zone_month_rejects_missing_date(tmp_path, spark):
    stage_path = tmp_path / "stage"
    dates = [date(2026, 2, 1) + timedelta(days=offset) for offset in range(27)]
    _write_staged_month(spark, stage_path, dates)

    with pytest.raises(ValueError, match="날짜 불일치"):
        validate_daily_zone_month(
            spark,
            stage_path,
            "2026-02",
            expected_zone_ids=(1, 2),
        )


def test_staging_run_path_rejects_untrusted_path_component(tmp_path):
    with pytest.raises(ValueError, match="잘못된"):
        _staging_run_path("../../", staging_root=tmp_path)


def test_build_daily_zone_frame_fills_complete_zone_grid(
    tmp_path,
    spark,
):
    silver_path = tmp_path / "yellow_tripdata_2026-07"

    spark.createDataFrame([
        {"pickup_datetime": datetime(2026, 7, 1, 12, 5), "pickup_location_id": 1},
        {"pickup_datetime": datetime(2026, 7, 1, 12, 10), "pickup_location_id": 1},
        {"pickup_datetime": datetime(2026, 7, 1, 12, 35), "pickup_location_id": 2},
        # 서비스 월과 공식 NYC Taxi Zone 범위 밖의 행은 사용하지 않는다.
        {"pickup_datetime": datetime(2026, 8, 1, 12, 0), "pickup_location_id": 1},
        {"pickup_datetime": datetime(2026, 7, 1, 12, 0), "pickup_location_id": 264},
    ]).write.parquet(str(silver_path))

    result = build_daily_zone_frame(
        spark,
        [str(silver_path)],
        service_month="2026-07",
        zone_ids=(1, 2),
    )

    assert result.count() == 2 * 48
    assert result.columns == ["zone_id", "type", "date", "time", "value"]

    values = {
        (row.zone_id, row.time): row.value
        for row in result.filter("time IN ('1200', '1230')").collect()
    }
    assert values[(1, "1200")] == 2.0
    assert values[(2, "1200")] == 0.0
    assert values[(1, "1230")] == 0.0
    assert values[(2, "1230")] == 1.0


def test_expand_zone_values_to_segments_joins_only_after_rolling(spark):
    rolling = spark.createDataFrame([
        {"zone_id": 1, "type": 3, "dow": "FRI", "time": "1200", "value": 2.0},
        {"zone_id": 2, "type": 3, "dow": "FRI", "time": "1200", "value": 5.0},
    ])
    mapping = spark.createDataFrame([
        {"segment_id": "0000001", "zone_id": 1},
        {"segment_id": "0000002", "zone_id": 1},
        {"segment_id": "0000003", "zone_id": 2},
    ])

    result = expand_zone_values_to_segments(rolling, mapping)
    values = {
        row.segment_id: row.value
        for row in result.collect()
    }

    assert values == {
        "0000001": 2.0,
        "0000002": 2.0,
        "0000003": 5.0,
    }


def test_validate_segment_values_requires_all_segments_and_slots(spark):
    mapping = spark.createDataFrame([
        {"segment_id": "0000001", "zone_id": 1},
        {"segment_id": "0000002", "zone_id": 1},
        {"segment_id": "0000003", "zone_id": 2},
    ])
    rolling = spark.createDataFrame([
        {
            "zone_id": zone_id,
            "type": 3,
            "dow": dow,
            "time": time,
            "value": float(zone_id),
        }
        for zone_id in (1, 2)
        for dow in DOW_NAMES
        for time in TIME_SLOTS
    ])
    segment_values = expand_zone_values_to_segments(rolling, mapping)

    assert validate_segment_values(segment_values, mapping) == {
        "segments": 3,
        "rows": 3 * 7 * 48,
    }
