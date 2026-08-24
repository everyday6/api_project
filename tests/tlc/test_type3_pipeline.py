from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

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
    write_type3_rolling_to_rds,
)
from src.tlc.type3_pipeline import (
    _complete_silver_paths_for_month,
    _find_pending_type3_months,
    _month_success_marker,
    _read_type3_publish_state,
    _staging_run_path,
    _type3_metadata_is_current,
    _type3_reference_exists,
    _write_type3_publish_state,
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


class _FakeBatchWriteItems:
    """db.batch_write_items를 대체해 실제로 넘어온 items를 기록한다.

    _write_type3_partition은 스레드 여러 개가 동시에 호출하므로(성능을 위해),
    list.append는 GIL 덕에 개별 호출은 원자적이라 별도 락 없이도 안전하다."""

    def __init__(self, fail=False):
        self.fail = fail
        self.items = []
        self.table_name = None

    def __call__(self, table_name, items, key_columns=None, conn=None):
        if self.fail:
            raise RuntimeError("RDS batch failure")
        self.table_name = table_name
        self.items.extend(items)


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


def test_type3_publish_state_round_trip(tmp_path):
    state_path = tmp_path / "_rds_publish_state.json"
    metadata = {
        "status": "COMPLETED",
        "window_start": "2026-05-04",
        "window_end": "2026-05-31",
        "rolling_weeks": 12,
        "mapping_version": "map-v1",
    }

    assert _read_type3_publish_state(state_path) == {}
    _write_type3_publish_state(metadata, state_path)
    assert _read_type3_publish_state(state_path) == metadata


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


class _FixedDate(date):
    """date.today()를 고정값으로 바꿔치기하기 위한 서브클래스(updated_date
    검증용) - src.tlc.gold2._write_rows_chunk가 date.today()를 직접 부른다."""

    @classmethod
    def today(cls):
        return date(2026, 8, 24)


def test_write_type3_partition_builds_flat_rows_per_slot(monkeypatch):
    """executor에서 실제로 도는 부분만 떼어내 같은 프로세스에서 직접 검증한다.

    row 하나가 (segment_id, dow, time) 슬롯 하나다(flat 스키마 - 2026-08-24
    개편). 파티션 안에서 여러 스레드가 청크를 나눠 동시에 쓰므로(성능을
    위해), items가 입력 순서 그대로 쌓인다는 보장은 없다 — segment_id/dow/
    time으로 정렬해 내용만 비교한다. db.new_connection()/
    db.batch_write_items()를 대체해서 실제 RDS 없이 검증한다(스레드마다 새
    커넥션을 여는 구조라 conn.close()가 호출되므로 fake 커넥션도 그
    메서드가 있어야 한다)."""

    monkeypatch.setattr("src.tlc.gold2.date", _FixedDate)
    fake_write = _FakeBatchWriteItems()

    with patch("src.tlc.gold2.db.new_connection", return_value=MagicMock()), \
         patch("src.tlc.gold2.db.batch_write_items", fake_write):
        write_partition = _write_type3_partition("nav-segment-metrics", date(2026, 8, 20))
        write_partition([
            {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
            {"segment_id": "0000001", "dow": "MON", "time": "0900", "value": 3.0},
            {"segment_id": "0000002", "dow": "MON", "time": "1230", "value": 2.5},
        ])

    assert fake_write.table_name == "nav-segment-metrics"
    assert sorted(
        fake_write.items, key=lambda item: (item["segment_id"], item["dow"], item["time"])
    ) == [
        {
            "segment_id": "0000001",
            "dow": "FRI",
            "time": "1200",
            "value": 1.5,
            "collected_date": "2026-08-20",
            "updated_date": "2026-08-24",
        },
        {
            "segment_id": "0000001",
            "dow": "MON",
            "time": "0900",
            "value": 3.0,
            "collected_date": "2026-08-20",
            "updated_date": "2026-08-24",
        },
        {
            "segment_id": "0000002",
            "dow": "MON",
            "time": "1230",
            "value": 2.5,
            "collected_date": "2026-08-20",
            "updated_date": "2026-08-24",
        },
    ]


def test_write_type3_partition_propagates_batch_failure():
    fake_write = _FakeBatchWriteItems(fail=True)

    with patch("src.tlc.gold2.db.new_connection", return_value=MagicMock()), \
         patch("src.tlc.gold2.db.batch_write_items", fake_write):
        write_partition = _write_type3_partition("nav-segment-metrics", date(2026, 8, 20))
        with pytest.raises(RuntimeError, match="batch failure"):
            write_partition([
                {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
            ])


def test_write_type3_rolling_to_rds_returns_segment_count(spark, monkeypatch):
    """실제 파티션 쓰기는 no-op으로 바꿔치기하고 적재 개수 집계만 검증한다."""

    rolling = spark.createDataFrame([
        {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
        {"segment_id": "0000002", "dow": "MON", "time": "1230", "value": 2.5},
    ])
    monkeypatch.setattr(
        "src.tlc.gold2._write_type3_partition",
        lambda _table_name, _collected_date: (lambda rows: None),
    )

    written = write_type3_rolling_to_rds(
        "nav-segment-metrics",
        rolling,
        date(2026, 8, 20),
    )

    assert written == 2


def test_write_type3_rolling_to_rds_propagates_partition_failure(
    spark, monkeypatch,
):
    rolling = spark.createDataFrame([
        {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
    ])
    def _always_fail(_table_name, _collected_date):
        def _write(_rows):
            raise RuntimeError("RDS batch failure")
        return _write

    monkeypatch.setattr("src.tlc.gold2._write_type3_partition", _always_fail)

    # foreachPartition은 executor 예외를 Spark 자체 예외 타입으로 감싸서
    # driver에 전파하므로, 원래 예외 타입이 아니라 메시지만으로 확인한다.
    with pytest.raises(Exception, match="batch failure"):
        write_type3_rolling_to_rds(
            "nav-segment-metrics",
            rolling,
            date(2026, 8, 20),
        )


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
