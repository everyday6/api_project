import pytest
from pyspark.sql import SparkSession

from src.nav_length.gold1 import filter_routable_segments


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("nav_length_gold1_test").getOrCreate()
    yield session
    session.stop()


def test_filter_keeps_routable_positive_length(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 120.5, "is_routable": True},
        {"segment_id": "2", "length_ft": 0.0, "is_routable": True},
        {"segment_id": "3", "length_ft": 80.0, "is_routable": False},
    ])

    result = filter_routable_segments(df).collect()

    assert len(result) == 1
    assert result[0]["segment_id"] == "1"


def test_filter_output_has_only_segment_id_and_length_ft(spark):
    df = spark.createDataFrame([
        {"segment_id": "1", "length_ft": 120.5, "is_routable": True, "street_name": "X"},
    ])

    result = filter_routable_segments(df)

    assert sorted(result.columns) == ["length_ft", "segment_id"]
