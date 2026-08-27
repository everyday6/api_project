import pytest

from src.common.validator import validate_download


def test_validate_download_passes_through_result_for_valid_file(tmp_path):
    tmp_file = tmp_path / "file.parquet"
    tmp_file.write_bytes(b"content")
    download_result = {"filename": "file.parquet", "tmp_path": str(tmp_file)}

    result = validate_download.function(download_result)

    assert result == download_result


def test_validate_download_raises_when_file_missing(tmp_path):
    download_result = {"filename": "missing.parquet", "tmp_path": str(tmp_path / "missing.parquet")}

    with pytest.raises(FileNotFoundError):
        validate_download.function(download_result)


def test_validate_download_raises_when_file_empty(tmp_path):
    tmp_file = tmp_path / "empty.parquet"
    tmp_file.write_bytes(b"")
    download_result = {"filename": "empty.parquet", "tmp_path": str(tmp_file)}

    with pytest.raises(ValueError, match="빈 파일"):
        validate_download.function(download_result)
