from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from spark_jobs import tlc_pipeline_job
from spark_jobs.tlc_pipeline_job import _export_type3_snapshot, _validate_bronze


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("tlc_pipeline_job_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def _write_bronze_fixture(tmp_path, name, rows):
    path = tmp_path / name
    pd.DataFrame(rows).to_parquet(
        path, index=False, coerce_timestamps="us", allow_truncated_timestamps=True,
    )
    return str(path)


def test_validate_bronze_passes_clean_yellow_file(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "good.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    result = _validate_bronze(spark, {"bronze_chunk": [
        {"filename": "good.parquet", "taxi_type": "yellow", "bronze_path": path},
    ]})

    assert [item["filename"] for item in result["passed"]] == ["good.parquet"]
    assert result["excluded"] == []


def test_validate_bronze_excludes_file_missing_critical_column(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "critical.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        # tpep_dropoff_datetime 컬럼 자체가 없음
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    result = _validate_bronze(spark, {"bronze_chunk": [
        {"filename": "critical.parquet", "taxi_type": "yellow", "bronze_path": path},
    ]})

    assert result["passed"] == []
    assert len(result["excluded"]) == 1
    assert result["excluded"][0]["filename"] == "critical.parquet"
    assert "tpep_dropoff_datetime" in result["excluded"][0]["reason"]


def test_validate_bronze_passes_but_logs_when_location_out_of_range(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "out_of_range.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 999,  # 유효 범위(1~265) 밖
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    result = _validate_bronze(spark, {"bronze_chunk": [
        {"filename": "out_of_range.parquet", "taxi_type": "yellow", "bronze_path": path},
    ]})

    # log-only 실패는 파일을 제외하지 않는다 - 통과 목록에 그대로 남아야 한다.
    assert [item["filename"] for item in result["passed"]] == ["out_of_range.parquet"]
    assert result["excluded"] == []


def test_validate_bronze_fhv_skips_passenger_count_check(tmp_path, spark):
    # FHV는 passenger_count/trip_distance 컬럼 자체가 없는 게 정상이다.
    path = _write_bronze_fixture(tmp_path, "fhv.parquet", [{
        "pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "dropOff_datetime": datetime(2024, 1, 1, 8, 30),
        "PUlocationID": 10,
        "DOlocationID": 20,
    }])

    result = _validate_bronze(spark, {"bronze_chunk": [
        {"filename": "fhv.parquet", "taxi_type": "fhv", "bronze_path": path},
    ]})

    assert [item["filename"] for item in result["passed"]] == ["fhv.parquet"]
    assert result["excluded"] == []


def test_validate_bronze_continues_after_middle_file_fails(tmp_path, spark):
    first_path = _write_bronze_fixture(tmp_path, "first.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])
    critical_path = _write_bronze_fixture(tmp_path, "critical3.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])
    third_path = _write_bronze_fixture(tmp_path, "third.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 9, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 9, 30),
        "PULocationID": 30, "DOLocationID": 40,
        "passenger_count": 2, "trip_distance": 3.0,
    }])

    chunk = [
        {"filename": "first.parquet", "taxi_type": "yellow", "bronze_path": first_path},
        {"filename": "critical3.parquet", "taxi_type": "yellow", "bronze_path": critical_path},
        {"filename": "third.parquet", "taxi_type": "yellow", "bronze_path": third_path},
    ]

    result = _validate_bronze(spark, {"bronze_chunk": chunk})

    # first.parquet과 third.parquet 둘 다 살아남아야 한다 - 중간 파일 실패로
    # 루프 자체가 멈추지 않고 계속 순회한다는 것을 증명한다.
    assert [item["filename"] for item in result["passed"]] == ["first.parquet", "third.parquet"]
    assert [item["filename"] for item in result["excluded"]] == ["critical3.parquet"]


def test_validate_bronze_aggregates_multiple_critical_failures(tmp_path, spark):
    good_path = _write_bronze_fixture(tmp_path, "good_multi.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])
    critical_path_1 = _write_bronze_fixture(tmp_path, "critical_multi_1.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])
    critical_path_2 = _write_bronze_fixture(tmp_path, "critical_multi_2.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])

    chunk = [
        {"filename": "good_multi.parquet", "taxi_type": "yellow", "bronze_path": good_path},
        {
            "filename": "critical_multi_1.parquet",
            "taxi_type": "yellow",
            "bronze_path": critical_path_1,
        },
        {
            "filename": "critical_multi_2.parquet",
            "taxi_type": "yellow",
            "bronze_path": critical_path_2,
        },
    ]

    result = _validate_bronze(spark, {"bronze_chunk": chunk})

    assert [item["filename"] for item in result["passed"]] == ["good_multi.parquet"]
    assert [item["filename"] for item in result["excluded"]] == [
        "critical_multi_1.parquet", "critical_multi_2.parquet",
    ]


def test_export_type3_snapshot_writes_zone_and_mapping_snapshots(spark):
    zone_rolling = spark.createDataFrame([
        {"zone_id": 42, "dow": "FRI", "time": "1200", "value": 33.0},
        {"zone_id": 7, "dow": "MON", "time": "0000", "value": 12.5},
    ])
    mapping = spark.createDataFrame([
        {"segment_id": "S1", "zone_id": 42},
        {"segment_id": "S2", "zone_id": 42},
        {"segment_id": "S3", "zone_id": 7},
    ])

    with patch.object(tlc_pipeline_job.gold_snapshot, "write_snapshot") as mock_write:
        _export_type3_snapshot(zone_rolling, mapping)

    calls = {call.args[0]: call.args[1] for call in mock_write.call_args_list}
    assert calls["type3_zone"] == {"42#FRI#1200": 33.0, "7#MON#0000": 12.5}
    assert calls["type3_mapping"] == {"S1": 42, "S2": 42, "S3": 7}


def test_export_type3_snapshot_survives_write_failure(spark):
    zone_rolling = spark.createDataFrame([
        {"zone_id": 42, "dow": "FRI", "time": "1200", "value": 33.0},
    ])
    mapping = spark.createDataFrame([{"segment_id": "S1", "zone_id": 42}])

    with patch.object(
        tlc_pipeline_job.gold_snapshot, "write_snapshot", side_effect=RuntimeError("S3 down"),
    ):
        _export_type3_snapshot(zone_rolling, mapping)  # 예외 없이 정상 종료돼야 한다.
