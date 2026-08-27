from unittest.mock import patch

from src.tlc import bronze_validation


def test_chunk_bronze_files_groups_by_taxi_type_in_priority_order():
    bronze_files = [
        {"taxi_type": "yellow", "filename": "y1.parquet"},
        {"taxi_type": "fhvhv", "filename": "hv1.parquet"},
        {"taxi_type": "yellow", "filename": "y2.parquet"},
    ]

    chunks = bronze_validation.chunk_bronze_files.function(bronze_files)

    # fhvhv가 가장 오래 걸려 맨 앞. green/fhv는 파일이 없어 청크 자체가 안 생긴다.
    assert [chunk[0]["taxi_type"] for chunk in chunks] == ["fhvhv", "yellow"]
    assert [item["filename"] for item in chunks[1]] == ["y1.parquet", "y2.parquet"]


def test_chunk_bronze_files_empty_input_returns_empty():
    assert bronze_validation.chunk_bronze_files.function([]) == []


def test_build_excluded_files_message_lists_all_when_under_limit():
    excluded = [{"filename": "a.parquet", "reason": "필수 컬럼 없음"}]

    message = bronze_validation._build_excluded_files_message(excluded)

    assert "제외된 파일 수*: 1건" in message
    assert "`a.parquet`: 필수 컬럼 없음" in message
    assert "...외" not in message


def test_build_excluded_files_message_truncates_when_over_limit():
    excluded = [
        {"filename": f"file_{i}.parquet", "reason": "필수 컬럼 없음"}
        for i in range(bronze_validation.MAX_EXCLUDED_FILES_IN_MESSAGE + 5)
    ]

    message = bronze_validation._build_excluded_files_message(excluded)

    assert "...외 5건" in message
    assert "file_0.parquet" in message
    assert f"file_{bronze_validation.MAX_EXCLUDED_FILES_IN_MESSAGE}.parquet" not in message


def test_validate_bronze_quality_returns_empty_for_empty_chunk():
    with patch.object(bronze_validation, "run_tlc_emr_operation") as mock_run:
        result = bronze_validation.validate_bronze_quality.function([])

    mock_run.assert_not_called()
    assert result == []


def test_validate_bronze_quality_delegates_to_emr_and_returns_passed():
    chunk = [{"filename": "a.parquet", "taxi_type": "yellow", "bronze_path": "s3://a"}]

    with patch.object(
        bronze_validation, "run_tlc_emr_operation",
        return_value={"passed": chunk, "excluded": []},
    ) as mock_run, patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation.validate_bronze_quality.function(chunk)

    mock_run.assert_called_once_with(
        "validate_bronze", {"bronze_chunk": chunk},
        max_executors=bronze_validation.EMR_MAX_EXECUTORS_TLC_INGEST,
    )
    mock_notify.assert_not_called()
    assert result == chunk


def test_validate_bronze_quality_notifies_slack_when_files_excluded():
    chunk = [{"filename": "a.parquet", "taxi_type": "yellow", "bronze_path": "s3://a"}]
    excluded = [{"filename": "a.parquet", "reason": "필수 컬럼 없음"}]

    with patch.object(
        bronze_validation, "run_tlc_emr_operation",
        return_value={"passed": [], "excluded": excluded},
    ), patch.object(bronze_validation, "notify_slack_message") as mock_notify:
        result = bronze_validation.validate_bronze_quality.function(chunk)

    mock_notify.assert_called_once()
    assert "a.parquet" in mock_notify.call_args.args[0]
    assert result == []
