from datetime import datetime, timedelta

import great_expectations as gx

from src.speed.expectations import critical_expectations, log_only_expectations


def test_critical_expectations_checks_downstream_columns():
    expectations = critical_expectations()

    columns = {e.column for e in expectations}
    assert columns == {"speed", "link_points", "data_as_of", "link_id"}
    assert all(isinstance(e, gx.expectations.ExpectColumnToExist) for e in expectations)


def test_log_only_expectations_includes_row_count_check():
    expectations = log_only_expectations()

    types = [type(e).__name__ for e in expectations]
    assert "ExpectTableRowCountToBeBetween" in types


def test_log_only_expectations_null_checks_use_ten_percent_tolerance():
    expectations = log_only_expectations()

    null_checks = {
        e.column: e.mostly
        for e in expectations
        if type(e).__name__ == "ExpectColumnValuesToNotBeNull"
    }
    assert null_checks == {
        "speed": 0.90,
        "link_points": 0.90,
        "data_as_of": 0.90,
        "link_id": 0.90,
    }


def test_log_only_expectations_speed_range_is_zero_to_150():
    expectations = log_only_expectations()

    speed_range = next(
        e for e in expectations
        if type(e).__name__ == "ExpectColumnValuesToBeBetween" and e.column == "speed"
    )
    assert speed_range.min_value == 0
    assert speed_range.max_value == 150


def test_log_only_expectations_requires_at_least_100k_unique_segments():
    # collect_speed_data()가 검증하는 df는 synthetic 보강분까지 합친
    # 것이라 LION 세그먼트 총 개수(약 10만 개)에 근접해야 한다.
    expectations = log_only_expectations()

    segment_count_check = next(
        e for e in expectations
        if type(e).__name__ == "ExpectColumnUniqueValueCountToBeBetween"
    )
    assert segment_count_check.column == "link_id"
    assert segment_count_check.min_value == 100_000
    assert segment_count_check.max_value is None


def test_log_only_expectations_data_as_of_range_starts_2017_and_ends_near_now():
    expectations = log_only_expectations()

    date_range = next(
        e for e in expectations
        if type(e).__name__ == "ExpectColumnValuesToBeBetween" and e.column == "data_as_of"
    )
    assert date_range.min_value == datetime(2017, 1, 1)

    # max_value는 호출 시점 기준으로 동적 계산되므로 정확한 값이 아니라
    # "오늘+1일 근방"인지만 확인한다.
    expected_max = datetime.now() + timedelta(days=1)
    assert abs((date_range.max_value - expected_max).total_seconds()) < 60
