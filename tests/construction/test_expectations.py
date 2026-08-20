import great_expectations as gx

from src.construction.expectations import critical_expectations, log_only_expectations
from src.construction.silver import READ_COLS


def test_critical_expectations_includes_row_count_check():
    expectations = critical_expectations()

    assert any(
        isinstance(e, gx.expectations.ExpectTableRowCountToBeBetween)
        for e in expectations
    )


def test_critical_expectations_checks_every_read_column_exists():
    expectations = critical_expectations()

    existence_checks = [
        e for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnToExist)
    ]
    checked_columns = {e.column for e in existence_checks}

    assert checked_columns == set(READ_COLS)


def test_critical_expectations_checks_permitnumber_not_null_and_unique():
    expectations = critical_expectations()

    not_null_checks = [
        e for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnValuesToNotBeNull)
    ]
    unique_checks = [
        e for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnValuesToBeUnique)
    ]

    assert any(e.column == "permitnumber" for e in not_null_checks)
    assert any(e.column == "permitnumber" for e in unique_checks)


def test_log_only_expectations_checks_date_columns_parseable():
    expectations = log_only_expectations()

    parseable_columns = {
        e.column for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnValuesToBeDateutilParseable)
    }

    assert parseable_columns == {"issuedworkstartdate", "issuedworkenddate"}


def test_log_only_expectations_checks_permitlinearfeet_not_null():
    expectations = log_only_expectations()

    not_null_columns = {
        e.column for e in expectations
        if isinstance(e, gx.expectations.ExpectColumnValuesToNotBeNull)
    }

    assert "permitlinearfeet" in not_null_columns


def test_log_only_expectations_has_no_existence_or_uniqueness_checks():
    # 존재/고유성 검증은 전부 critical 쪽에 있어야 한다 — log-only에 섞이면
    # "컬럼 없음"이 조용히 로그만 남기고 통과해버리는 회귀가 생긴다.
    expectations = log_only_expectations()

    assert not any(
        isinstance(e, (gx.expectations.ExpectColumnToExist, gx.expectations.ExpectColumnValuesToBeUnique))
        for e in expectations
    )
