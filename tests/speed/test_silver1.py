import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from src.speed.silver1 import clean_speed_silver1


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("speed_silver1_test").getOrCreate()
    yield session
    session.stop()


def test_clean_renames_and_casts(spark):
    df = spark.createDataFrame([
        {
            "link_id": "123",
            "speed": "35.5",
            "link_points": "40.7,-74.0 40.71,-74.01",
            "data_as_of": "2026-08-21T12:05:00.000",
        }
    ])

    result = clean_speed_silver1(df).collect()

    assert len(result) == 1
    assert result[0]["link_id"] == "123"
    assert result[0]["speed"] == 35.5
    assert result[0]["link_points"] == "40.7,-74.0 40.71,-74.01"


def test_clean_drops_rows_with_missing_speed(spark):
    df = spark.createDataFrame([
        {"link_id": "1", "speed": None, "link_points": "40.7,-74.0 40.71,-74.01", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "2", "speed": "20.0", "link_points": "40.7,-74.0 40.71,-74.01", "data_as_of": "2026-08-21T12:05:00.000"},
    ])

    result = clean_speed_silver1(df).collect()

    assert len(result) == 1
    assert result[0]["link_id"] == "2"


def test_clean_drops_rows_with_missing_link_points(spark):
    schema = StructType([
        StructField("link_id", StringType()),
        StructField("speed", StringType()),
        StructField("link_points", StringType()),
        StructField("data_as_of", StringType()),
    ])
    df = spark.createDataFrame([
        ("1", "20.0", None, "2026-08-21T12:05:00.000"),
    ], schema=schema)

    result = clean_speed_silver1(df).collect()

    assert len(result) == 0
