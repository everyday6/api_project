from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

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
