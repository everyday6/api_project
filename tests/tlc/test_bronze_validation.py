from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.tlc import bronze_validation
from src.tlc.bronze_validation import CriticalValidationError, validate_bronze_file


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("bronze_validation_test")
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


def test_validate_bronze_file_passes_clean_yellow_file(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "good.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    failed_checks = validate_bronze_file(spark, path, "yellow")

    assert failed_checks == []


def test_validate_bronze_file_raises_when_dropoff_column_missing(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "critical.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        # tpep_dropoff_datetime 컬럼 자체가 없음
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    with pytest.raises(CriticalValidationError, match="tpep_dropoff_datetime"):
        validate_bronze_file(spark, path, "yellow")


def test_validate_bronze_file_logs_but_passes_when_location_out_of_range(tmp_path, spark):
    path = _write_bronze_fixture(tmp_path, "out_of_range.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 999,  # 유효 범위(1~263) 밖
        "DOLocationID": 20,
        "passenger_count": 1,
        "trip_distance": 5.0,
    }])

    failed_checks = validate_bronze_file(spark, path, "yellow")

    assert len(failed_checks) == 1
    assert failed_checks[0]["kwargs"]["column"] == "PULocationID"


def test_validate_bronze_file_fhv_skips_passenger_count_check(tmp_path, spark):
    # FHV는 passenger_count/trip_distance 컬럼 자체가 없는 게 정상이다.
    path = _write_bronze_fixture(tmp_path, "fhv.parquet", [{
        "pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "dropOff_datetime": datetime(2024, 1, 1, 8, 30),
        "PUlocationID": 10,
        "DOlocationID": 20,
    }])

    failed_checks = validate_bronze_file(spark, path, "fhv")

    assert failed_checks == []


def test_validate_chunk_files_excludes_only_critical_failure(tmp_path, spark):
    good_path = _write_bronze_fixture(tmp_path, "good.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "tpep_dropoff_datetime": datetime(2024, 1, 1, 8, 30),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])
    critical_path = _write_bronze_fixture(tmp_path, "critical2.parquet", [{
        "tpep_pickup_datetime": datetime(2024, 1, 1, 8, 0),
        "PULocationID": 10, "DOLocationID": 20,
        "passenger_count": 1, "trip_distance": 5.0,
    }])

    chunk = [
        {"filename": "good.parquet", "taxi_type": "yellow", "bronze_path": good_path},
        {"filename": "critical2.parquet", "taxi_type": "yellow", "bronze_path": critical_path},
    ]

    with patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        passed = bronze_validation._validate_chunk_files(spark, chunk)

    assert [f["filename"] for f in passed] == ["good.parquet"]
    mock_notify.assert_called_once()
    assert "critical2.parquet" in mock_notify.call_args.args[0]


def test_validate_chunk_files_empty_chunk_returns_empty(spark):
    assert bronze_validation._validate_chunk_files(spark, []) == []


def test_validate_chunk_files_continues_after_middle_file_fails(tmp_path, spark):
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

    with patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        passed = bronze_validation._validate_chunk_files(spark, chunk)

    # first.parquet and third.parquet must BOTH survive — proving the loop
    # continues past the middle failure, not just that the failure is excluded.
    assert [f["filename"] for f in passed] == ["first.parquet", "third.parquet"]
    mock_notify.assert_called_once()
