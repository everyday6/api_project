from datetime import date, datetime, timedelta
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

from src.common.config import TAXI_TYPES
from src.tlc.gold2 import (
    DOW_NAMES,
    TIME_SLOTS,
    _copy_type3_partition,
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
    검증용) - src.tlc.gold2._copy_type3_partition이 date.today()를 직접
    부른다."""

    @classmethod
    def today(cls):
        return date(2026, 8, 24)


def _fake_connection():
    """with conn.cursor() as cur: 패턴을 지원하는 fake connection/cursor 쌍.

    conn.cursor()를 몇 번을 부르든 항상 같은 cur를 돌려주므로, 여러
    with 블록에 걸친 cur.execute 호출을 call_args_list 하나로 순서대로
    모아 검증할 수 있다."""

    cur = MagicMock()
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = False
    conn = MagicMock()
    conn.cursor.return_value = cur
    return conn, cur


def test_copy_type3_partition_streams_csv_rows_to_staging_table(monkeypatch):
    """executor에서 실제로 도는 부분만 떼어내 같은 프로세스에서 직접 검증한다.

    row 하나가 (segment_id, dow, time) 슬롯 하나다(flat 스키마 - 2026-08-24
    개편). COPY는 upsert와 달리 파티션 전체를 커넥션 하나로 한 번에
    보내므로(스레드 분할 없음), copy_expert가 정확히 한 번 호출되고 그
    안의 CSV 내용이 입력 행 순서 그대로인지 검증한다."""

    monkeypatch.setattr("src.tlc.gold2.date", _FixedDate)
    conn, cur = _fake_connection()

    with patch("src.tlc.gold2.db.new_connection", return_value=conn):
        write_partition = _copy_type3_partition("segment_metrics_type3_staging_abc", date(2026, 8, 20))
        write_partition([
            {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
            {"segment_id": "0000002", "dow": "MON", "time": "1230", "value": 2.5},
        ])

    cur.copy_expert.assert_called_once()
    copy_sql, buffer = cur.copy_expert.call_args[0]
    assert "segment_metrics_type3_staging_abc" in str(copy_sql)
    assert buffer.read() == (
        "0000001,FRI,1200,1.5,2026-08-20,2026-08-24\r\n"
        "0000002,MON,1230,2.5,2026-08-20,2026-08-24\r\n"
    )
    conn.close.assert_called_once()


def test_copy_type3_partition_skips_empty_partition():
    conn, cur = _fake_connection()

    with patch("src.tlc.gold2.db.new_connection", return_value=conn):
        write_partition = _copy_type3_partition("segment_metrics_type3_staging_abc", date(2026, 8, 20))
        write_partition([])

    cur.copy_expert.assert_not_called()
    conn.close.assert_not_called()


def test_copy_type3_partition_propagates_copy_failure():
    conn, cur = _fake_connection()
    cur.copy_expert.side_effect = RuntimeError("COPY failure")

    with patch("src.tlc.gold2.db.new_connection", return_value=conn):
        write_partition = _copy_type3_partition("segment_metrics_type3_staging_abc", date(2026, 8, 20))
        with pytest.raises(RuntimeError, match="COPY failure"):
            write_partition([
                {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
            ])

    # 실패해도 커넥션은 반드시 정리된다(finally).
    conn.close.assert_called_once()


def test_write_type3_rolling_to_rds_returns_segment_count(spark, monkeypatch):
    """스테이징 테이블 생성/병합/정리 SQL과 파티션 COPY 호출을 검증한다.

    실제 파티션 쓰기(_copy_type3_partition)는 no-op으로 바꿔치기하고,
    driver 쪽에서 직접 여는 db.new_connection()만 fake로 검증한다."""

    rolling = spark.createDataFrame([
        {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
        {"segment_id": "0000002", "dow": "MON", "time": "1230", "value": 2.5},
    ])
    conn, cur = _fake_connection()
    copy_calls = []

    def _fake_copy(staging_table, collected_date):
        copy_calls.append((staging_table, collected_date))
        return lambda rows: None

    with patch("src.tlc.gold2.db.new_connection", return_value=conn), \
         patch("src.tlc.gold2._copy_type3_partition", _fake_copy):
        written = write_type3_rolling_to_rds(
            "nav-segment-metrics",
            rolling,
            date(2026, 8, 20),
        )

    assert written == 2

    # CREATE TABLE, ALTER TABLE, 병합 upsert, DROP TABLE = execute 4번.
    assert cur.execute.call_count == 4
    executed_sql = [str(call.args[0]) for call in cur.execute.call_args_list]
    assert "CREATE TABLE" in executed_sql[0]
    assert "SET UNLOGGED" in executed_sql[1]
    assert "ON CONFLICT" in executed_sql[2]
    assert "DROP TABLE" in executed_sql[3]

    # 스테이징 테이블명이 CREATE/병합/DROP 전부에서 같은 값으로 쓰였고,
    # _copy_type3_partition에도 그대로 전달됐는지 확인한다.
    assert len(copy_calls) == 1
    staging_table, collected_date = copy_calls[0]
    assert staging_table.startswith("nav-segment-metrics_staging_")
    assert collected_date == date(2026, 8, 20)
    assert staging_table in executed_sql[0]
    assert staging_table in executed_sql[3]

    conn.close.assert_called_once()


def test_write_type3_rolling_to_rds_skips_empty_input(spark):
    """빈 DataFrame이면 스테이징 테이블도 안 만들고 바로 0을 반환한다."""

    rolling = spark.createDataFrame(
        [], "segment_id string, dow string, time string, value double"
    )
    conn, cur = _fake_connection()

    with patch("src.tlc.gold2.db.new_connection", return_value=conn):
        written = write_type3_rolling_to_rds(
            "nav-segment-metrics",
            rolling,
            date(2026, 8, 20),
        )

    assert written == 0
    cur.execute.assert_not_called()
    conn.close.assert_not_called()


def test_write_type3_rolling_to_rds_cleans_up_staging_table_on_partition_failure(
    spark,
):
    """COPY 단계에서 실패해도 스테이징 테이블 DROP은 반드시 실행된다."""

    rolling = spark.createDataFrame([
        {"segment_id": "0000001", "dow": "FRI", "time": "1200", "value": 1.5},
    ])
    conn, cur = _fake_connection()

    def _always_fail(_staging_table, _collected_date):
        def _write(_rows):
            raise RuntimeError("COPY failure")
        return _write

    with patch("src.tlc.gold2.db.new_connection", return_value=conn), \
         patch("src.tlc.gold2._copy_type3_partition", _always_fail):
        # foreachPartition은 executor 예외를 Spark 자체 예외 타입으로
        # 감싸서 driver에 전파하므로, 원래 예외 타입이 아니라 메시지만으로
        # 확인한다.
        with pytest.raises(Exception, match="COPY failure"):
            write_type3_rolling_to_rds(
                "nav-segment-metrics",
                rolling,
                date(2026, 8, 20),
            )

    # CREATE TABLE, ALTER TABLE까지만 성공하고 병합은 못 갔지만, DROP은
    # finally에서 실행돼야 한다.
    executed_sql = [str(call.args[0]) for call in cur.execute.call_args_list]
    assert len(executed_sql) == 3
    assert "CREATE TABLE" in executed_sql[0]
    assert "SET UNLOGGED" in executed_sql[1]
    assert "DROP TABLE" in executed_sql[2]
    conn.close.assert_called_once()


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
