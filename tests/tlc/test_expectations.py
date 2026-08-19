import great_expectations as gx
import pytest

from src.tlc.expectations import critical_expectations, log_only_expectations


def test_critical_expectations_yellow_checks_all_columns():
    expectations = critical_expectations("yellow")

    columns = {e.column for e in expectations}
    assert columns == {
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
    }
    assert all(isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations)


def test_critical_expectations_green_checks_all_columns():
    expectations = critical_expectations("green")

    columns = {e.column for e in expectations}
    assert columns == {
        "lpep_pickup_datetime",
        "lpep_dropoff_datetime",
        "PULocationID",
        "DOLocationID",
        "passenger_count",
        "trip_distance",
    }
    assert all(isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations)


def test_critical_expectations_fhv_uses_fhv_column_names():
    expectations = critical_expectations("fhv")

    columns = {e.column for e in expectations}
    assert columns == {
        "pickup_datetime",
        "dropOff_datetime",
        "PUlocationID",
        "DOlocationID",
    }


def test_log_only_expectations_yellow_includes_passenger_and_distance_checks():
    expectations = log_only_expectations("yellow")

    types_and_columns = {
        (type(e).__name__, getattr(e, "column", None)) for e in expectations
    }
    assert ("ExpectColumnValuesToBeBetween", "passenger_count") in types_and_columns
    assert ("ExpectColumnValuesToBeBetween", "trip_distance") in types_and_columns


def test_log_only_expectations_fhv_excludes_passenger_and_distance_checks():
    expectations = log_only_expectations("fhv")

    columns = {getattr(e, "column", None) for e in expectations}
    assert "passenger_count" not in columns
    assert "trip_distance" not in columns


def test_log_only_expectations_fhvhv_checks_trip_miles_as_trip_distance():
    expectations = log_only_expectations("fhvhv")

    columns = {getattr(e, "column", None) for e in expectations}
    assert "trip_miles" in columns
    assert "passenger_count" not in columns


def test_log_only_expectations_includes_row_count_check():
    expectations = log_only_expectations("yellow")

    assert any(
        isinstance(e, gx.expectations.ExpectTableRowCountToBeBetween)
        for e in expectations
    )


def test_log_only_expectations_has_no_column_existence_checks():
    expectations = log_only_expectations("yellow")

    assert not any(
        isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations
    )


def test_log_only_expectations_checks_location_ids_not_null():
    expectations = log_only_expectations("yellow")

    not_null_columns = {
        e.column
        for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnValuesToNotBeNull)
    }
    assert "PULocationID" in not_null_columns
    assert "DOLocationID" in not_null_columns


def test_log_only_expectations_location_id_range_allows_up_to_265():
    expectations = log_only_expectations("yellow")

    range_checks = {
        e.column: e
        for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnValuesToBeBetween)
        and e.column in {"PULocationID", "DOLocationID"}
    }
    assert range_checks["PULocationID"].max_value == 265
    assert range_checks["DOLocationID"].max_value == 265


def test_raw_columns_invalid_taxi_type_raises_descriptive_value_error():
    with pytest.raises(ValueError, match="지원하지 않는"):
        critical_expectations("invalid_type")
