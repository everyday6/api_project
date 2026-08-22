from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

from src.tlc.silver1_transform import SILVER_COLUMNS, transform


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("tlc_silver1_transform_test")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_transform_yellow_standardizes_schema_and_prunes_extra_columns(spark):
    source = spark.createDataFrame([{
        "tpep_pickup_datetime": datetime(2026, 8, 21, 12, 0),
        "tpep_dropoff_datetime": datetime(2026, 8, 21, 12, 20),
        "PULocationID": 10,
        "DOLocationID": 20,
        "passenger_count": 2,
        "trip_distance": 3.5,
        "fare_amount": 18.0,
    }])

    result = transform(source, "yellow")

    assert result.columns == SILVER_COLUMNS
    assert isinstance(result.schema["pickup_datetime"].dataType, TimestampType)
    assert isinstance(result.schema["dropoff_datetime"].dataType, TimestampType)
    assert isinstance(result.schema["pickup_location_id"].dataType, IntegerType)
    assert isinstance(result.schema["dropoff_location_id"].dataType, IntegerType)
    assert isinstance(result.schema["passenger_count"].dataType, IntegerType)
    assert isinstance(result.schema["trip_distance"].dataType, DoubleType)

    row = result.first()
    assert row.pickup_location_id == 10
    assert row.dropoff_location_id == 20
    assert row.passenger_count == 2
    assert row.trip_distance == 3.5


def test_transform_green_uses_green_datetime_column_names(spark):
    source = spark.createDataFrame([{
        "lpep_pickup_datetime": datetime(2026, 8, 21, 8, 0),
        "lpep_dropoff_datetime": datetime(2026, 8, 21, 8, 10),
        "PULocationID": 30,
        "DOLocationID": 40,
        "passenger_count": 1,
        "trip_distance": 1.25,
    }])

    row = transform(source, "green").first()

    assert row.pickup_location_id == 30
    assert row.dropoff_location_id == 40


def test_transform_fhv_adds_source_missing_optional_columns_as_null(spark):
    source = spark.createDataFrame([{
        "pickup_datetime": datetime(2026, 8, 21, 9, 0),
        "dropOff_datetime": datetime(2026, 8, 21, 9, 15),
        "PUlocationID": 50,
        "DOlocationID": 60,
    }])

    row = transform(source, "fhv").first()

    assert row.pickup_location_id == 50
    assert row.dropoff_location_id == 60
    assert row.passenger_count is None
    assert row.trip_distance is None


def test_transform_fhvhv_maps_trip_miles_and_adds_passenger_count(spark):
    source = spark.createDataFrame([{
        "pickup_datetime": datetime(2026, 8, 21, 10, 0),
        "dropoff_datetime": datetime(2026, 8, 21, 10, 30),
        "PULocationID": 70,
        "DOLocationID": 80,
        "trip_miles": 7.75,
    }])

    row = transform(source, "fhvhv").first()

    assert row.passenger_count is None
    assert row.trip_distance == 7.75


def test_transform_rejects_unsupported_taxi_type(spark):
    source = spark.createDataFrame([{"pickup_datetime": datetime(2026, 8, 21, 11, 0)}])

    with pytest.raises(ValueError, match="지원하지 않는 택시 종류"):
        transform(source, "invalid")
