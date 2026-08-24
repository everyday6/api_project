from unittest.mock import patch

import pandas as pd
import pytest

from src.speed import bronze_validation
from src.speed.bronze_validation import CriticalValidationError, validate_bronze_file


def _write_bronze_fixture(tmp_path, name, rows):
    path = tmp_path / name
    pd.DataFrame(rows).to_parquet(path, index=False)
    return str(path)


def _good_row(**overrides):
    row = {
        "id": "1", "speed": "29.82", "travel_time": "90", "status": "0",
        "data_as_of": "2026-08-23T02:55:08.000", "link_id": "4620332",
        "link_points": "40.0,-73.0", "encoded_poly_line": "", "encoded_poly_line_lvls": "",
        "owner": "NYC-DOT", "transcom_id": "4620332", "borough": "Manhattan",
        "link_name": "TEST ST",
    }
    row.update(overrides)
    return row


def test_validate_bronze_file_passes_clean_file(tmp_path):
    path = _write_bronze_fixture(tmp_path, "good.parquet", [_good_row()])

    failed_checks = validate_bronze_file(path)

    assert failed_checks == []


def test_validate_bronze_file_raises_when_speed_column_missing(tmp_path):
    row = _good_row()
    del row["speed"]
    path = _write_bronze_fixture(tmp_path, "critical.parquet", [row])

    with pytest.raises(CriticalValidationError, match="speed"):
        validate_bronze_file(path)


def test_validate_bronze_file_logs_but_passes_when_speed_out_of_range(tmp_path):
    path = _write_bronze_fixture(
        tmp_path, "out_of_range.parquet", [_good_row(speed="-5.0")]
    )

    failed_checks = validate_bronze_file(path)

    assert len(failed_checks) == 1
    assert failed_checks[0]["kwargs"]["column"] == "speed"


def test_validate_bronze_file_flags_ancient_data_as_of(tmp_path):
    # 실제로 라이브 API에서 발견했던 1930년 이상치 재현.
    path = _write_bronze_fixture(
        tmp_path, "ancient.parquet", [_good_row(data_as_of="1930-12-09T14:40:47.000")]
    )

    failed_checks = validate_bronze_file(path)

    assert any(check["kwargs"].get("column") == "data_as_of" for check in failed_checks)


def test_validate_bronze_file_does_not_mutate_original_dtypes(tmp_path):
    # speed/data_as_of 캐스팅은 검증용 복사본에서만 해야 한다 - 원본
    # Bronze 파일 자체가 바뀌면 안 된다(Bronze 원칙: 변환 없음).
    path = _write_bronze_fixture(tmp_path, "good.parquet", [_good_row()])
    before = pd.read_parquet(path)

    validate_bronze_file(path)

    after = pd.read_parquet(path)
    assert before["speed"].dtype == after["speed"].dtype == object


def test_validate_bronze_file_null_within_tolerance_does_not_fail(tmp_path):
    # 10개 중 1개(10%)만 비어있으면 mostly=0.90 허용치 이내라 안 걸려야 한다.
    rows = [_good_row(id=str(i)) for i in range(9)]
    rows.append(_good_row(id="9", speed=None))
    path = _write_bronze_fixture(tmp_path, "mostly_ok.parquet", rows)

    failed_checks = validate_bronze_file(path)

    assert not any(
        c["kwargs"].get("column") == "speed" and c["expectation_type"] == "expect_column_values_to_not_be_null"
        for c in failed_checks
    )


def test_validate_bronze_file_null_over_tolerance_fails(tmp_path):
    # 10개 중 2개(20%)가 비면 mostly=0.90 허용치를 넘어서 걸려야 한다.
    rows = [_good_row(id=str(i)) for i in range(8)]
    rows.append(_good_row(id="8", speed=None))
    rows.append(_good_row(id="9", speed=None))
    path = _write_bronze_fixture(tmp_path, "over_tolerance.parquet", rows)

    failed_checks = validate_bronze_file(path)

    assert any(
        c["kwargs"].get("column") == "speed" and c["expectation_type"] == "expect_column_values_to_not_be_null"
        for c in failed_checks
    )


def test_validate_and_decide_returns_true_when_bronze_path_empty():
    # collect_bronze()가 빈 문자열을 반환하는 경우(신규 데이터 없음) -
    # check_new_data가 이미 이전 단계에서 걸렀어야 하지만 방어적으로도
    # 통과시킨다.
    with patch.object(bronze_validation, "validate_bronze_file") as mock_validate:
        result = bronze_validation._validate_and_decide("")

    assert result is True
    mock_validate.assert_not_called()


def test_validate_and_decide_returns_false_and_alerts_on_critical_failure():
    with patch.object(
        bronze_validation, "validate_bronze_file",
        side_effect=CriticalValidationError("필수 컬럼 없음: ['speed']"),
    ), patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation._validate_and_decide("s3://bucket/bronze.parquet")

    assert result is False
    mock_notify.assert_called_once()
    message = mock_notify.call_args.args[0]
    assert "speed" in message
    # 어느 파일/사이클인지 Airflow 로그를 따로 뒤지지 않도록 경로가 포함돼야 한다.
    assert "s3://bucket/bronze.parquet" in message


def test_validate_and_decide_returns_true_and_alerts_on_log_only_failure(caplog):
    failed = [{
        "expectation_type": "expect_column_values_to_be_between",
        "kwargs": {"column": "speed"},
        "result": {"unexpected_count": 3},
        "exception_info": {"exception_message": "dtype mismatch: expected numeric"},
    }]
    with patch.object(bronze_validation, "validate_bronze_file", return_value=failed), \
         patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        with caplog.at_level("WARNING"):
            result = bronze_validation._validate_and_decide("s3://bucket/bronze.parquet")

    assert result is True
    mock_notify.assert_called_once()
    message = mock_notify.call_args.args[0]
    assert "1건" in message
    # 알림 받은 사람이 Airflow 로그를 따로 뒤지지 않도록 파일 경로와 실패
    # 컬럼 이름이 Slack 메시지에 바로 담겨 있어야 한다.
    assert "s3://bucket/bronze.parquet" in message
    assert "speed" in message
    # GX가 메트릭 계산 중 내부적으로 예외를 삼킨 경우(success=False,
    # result={}) 실제 원인은 exception_info에만 담기므로 로그에도 남겨야 한다.
    assert any(
        "exception_info" in record.message and "dtype mismatch" in record.message
        for record in caplog.records
    )


def test_validate_and_decide_returns_true_without_alert_when_all_pass():
    with patch.object(bronze_validation, "validate_bronze_file", return_value=[]), \
         patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation._validate_and_decide("s3://bucket/bronze.parquet")

    assert result is True
    mock_notify.assert_not_called()
