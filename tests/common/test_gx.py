import great_expectations as gx
import pandas as pd
import pytest
from pyspark.sql import SparkSession

from src.common.gx import validate_pandas_dataframe, validate_spark_dataframe


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[1]").appName("gx_test").getOrCreate()
    yield session
    session.stop()


def test_validate_spark_dataframe_detects_null(spark):
    df = spark.createDataFrame(
        [(1, "yellow"), (2, "green"), (3, None)],
        ["trip_id", "taxi_type"],
    )

    results = validate_spark_dataframe(
        df,
        [gx.expectations.ExpectColumnValuesToNotBeNull(column="taxi_type")],
        datasource_name="test_ds",
        asset_name="test_asset",
    )

    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["expectation_type"] == "expect_column_values_to_not_be_null"
    assert results[0]["result"]["unexpected_count"] == 1
    assert "exception_info" in results[0]
    assert isinstance(results[0]["exception_info"], dict)


def test_validate_spark_dataframe_all_pass(spark):
    df = spark.createDataFrame([(1, "yellow"), (2, "green")], ["trip_id", "taxi_type"])

    results = validate_spark_dataframe(
        df,
        [gx.expectations.ExpectColumnValuesToNotBeNull(column="taxi_type")],
        datasource_name="test_ds2",
        asset_name="test_asset2",
    )

    assert results[0]["success"] is True


def test_validate_spark_dataframe_runs_multiple_expectations_in_order(spark):
    df = spark.createDataFrame([(1,)], ["id"])

    results = validate_spark_dataframe(
        df,
        [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
            gx.expectations.ExpectColumnToExist(column="does_not_exist"),
        ],
        datasource_name="test_ds3",
        asset_name="test_asset3",
    )

    assert len(results) == 2
    assert results[0]["expectation_type"] == "expect_table_row_count_to_be_between"
    assert results[0]["success"] is True
    assert results[1]["expectation_type"] == "expect_column_to_exist"
    assert results[1]["success"] is False

    # exception_info는 GX가 내부적으로 메트릭 계산 예외를 잡았을 때 실제
    # 원인을 담는 필드다 — 결과 dict에 항상 존재해야 나중에 구조적 실패의
    # 진짜 이유를 로그로 확인할 수 있다.
    for result in results:
        assert "exception_info" in result
        assert isinstance(result["exception_info"], dict)


def test_validate_pandas_dataframe_detects_null():
    df = pd.DataFrame({"trip_id": [1, 2, 3], "taxi_type": ["yellow", "green", None]})

    results = validate_pandas_dataframe(
        df,
        [gx.expectations.ExpectColumnValuesToNotBeNull(column="taxi_type")],
        datasource_name="test_pd_ds",
        asset_name="test_pd_asset",
    )

    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["expectation_type"] == "expect_column_values_to_not_be_null"
    assert results[0]["result"]["unexpected_count"] == 1
    assert "exception_info" in results[0]


def test_validate_pandas_dataframe_all_pass():
    df = pd.DataFrame({"trip_id": [1, 2], "taxi_type": ["yellow", "green"]})

    results = validate_pandas_dataframe(
        df,
        [gx.expectations.ExpectColumnValuesToNotBeNull(column="taxi_type")],
        datasource_name="test_pd_ds2",
        asset_name="test_pd_asset2",
    )

    assert results[0]["success"] is True


def test_validate_pandas_dataframe_runs_multiple_expectations_in_order():
    df = pd.DataFrame({"id": [1, 2]})

    results = validate_pandas_dataframe(
        df,
        [
            gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
            gx.expectations.ExpectColumnToExist(column="does_not_exist"),
        ],
        datasource_name="test_pd_ds3",
        asset_name="test_pd_asset3",
    )

    assert len(results) == 2
    assert results[0]["expectation_type"] == "expect_table_row_count_to_be_between"
    assert results[0]["success"] is True
    assert results[1]["expectation_type"] == "expect_column_to_exist"
    assert results[1]["success"] is False
