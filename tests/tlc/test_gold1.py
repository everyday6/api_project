from datetime import datetime

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.tlc.gold1 import drop_null_zone, filter_weekday


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("tlc_gold1_test").getOrCreate()
    yield session
    session.stop()


def test_filter_weekday_keeps_weekday_drops_weekend(spark):
    # 2024-01-01(월)은 남고 2024-01-06(토)은 제외되어야 한다.
    df = spark.createDataFrame([
        {"dropoff_datetime": datetime(2024, 1, 1, 8, 0), "dropoff_location_id": 5},
        {"dropoff_datetime": datetime(2024, 1, 6, 8, 0), "dropoff_location_id": 5},
    ])

    result = filter_weekday(df).collect()

    assert len(result) == 1
    assert result[0]["dropoff_datetime"] == datetime(2024, 1, 1, 8, 0)


def test_drop_null_zone_removes_missing_and_logs(caplog):
    df = pd.DataFrame({
        "zone_id": [5, None, None],
        "hour": [8, 9, 9],
        "dropoff_count": [2, 1, 1],
    })

    with caplog.at_level("WARNING"):
        result = drop_null_zone(df)

    assert len(result) == 1
    assert result.iloc[0]["zone_id"] == 5
    assert any("결측" in rec.message and "2건" in rec.message for rec in caplog.records)


def test_drop_null_zone_noop_when_nothing_missing():
    df = pd.DataFrame({"zone_id": [5, 7], "hour": [8, 9], "dropoff_count": [2, 1]})

    result = drop_null_zone(df)

    assert len(result) == 2
