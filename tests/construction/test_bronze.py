from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from airflow.sdk.exceptions import AirflowSkipException

from src.construction import bronze
from src.construction.silver import READ_COLS


def _write_bronze_snapshot(base_dir: Path, run_date: str, row: dict) -> Path:
    snapshot_dir = base_dir / f"dt={run_date}"
    snapshot_dir.mkdir(parents=True)
    path = snapshot_dir / "data.parquet"
    pd.DataFrame([row]).to_parquet(path, index=False)
    return path


def _valid_row() -> dict:
    row = {column: "x" for column in READ_COLS}
    row["permitnumber"] = "P001"
    row["issuedworkstartdate"] = "2026-01-01T00:00:00.000"
    row["issuedworkenddate"] = "2026-01-02T00:00:00.000"
    row["permitlinearfeet"] = "100"
    return row


# ---------- _find_recent_bronze_snapshot ----------


def test_find_recent_bronze_snapshot_within_threshold(tmp_path):
    _write_bronze_snapshot(tmp_path, "2026-08-18", _valid_row())

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found == str(tmp_path / "dt=2026-08-18" / "data.parquet")


def test_find_recent_bronze_snapshot_too_old_returns_none(tmp_path):
    _write_bronze_snapshot(tmp_path, "2026-08-17", _valid_row())

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_no_folders_returns_none(tmp_path):
    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_ignores_folder_without_data_file(tmp_path):
    (tmp_path / "dt=2026-08-19").mkdir(parents=True)  # data.parquet 없음

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_picks_the_latest_of_several(tmp_path):
    _write_bronze_snapshot(tmp_path, "2026-08-17", _valid_row())
    _write_bronze_snapshot(tmp_path, "2026-08-19", _valid_row())

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found == str(tmp_path / "dt=2026-08-19" / "data.parquet")


# ---------- validate_output ----------


def test_validate_output_passes_clean_file(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    path = _write_bronze_snapshot(tmp_path / "construction", "2026-08-20", _valid_row())

    result = bronze.validate_output(str(path))

    assert result == str(path)


def test_validate_output_skips_when_critical_fails_and_recent_backup_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    _write_bronze_snapshot(construction_dir, "2026-08-18", _valid_row())

    bad_row = _valid_row()
    del bad_row["wkt"]
    bad_path = _write_bronze_snapshot(construction_dir, "2026-08-20", bad_row)

    with patch.object(bronze, "notify_slack_message") as mock_notify:
        with pytest.raises(AirflowSkipException):
            bronze.validate_output(str(bad_path))

    mock_notify.assert_called_once()
    assert "wkt" in mock_notify.call_args.args[0]


def test_validate_output_raises_when_critical_fails_and_no_backup(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    bad_row = _valid_row()
    del bad_row["wkt"]
    bad_path = _write_bronze_snapshot(construction_dir, "2026-08-20", bad_row)

    with patch.object(bronze, "notify_slack_message") as mock_notify:
        with pytest.raises(ValueError):
            bronze.validate_output(str(bad_path))

    mock_notify.assert_not_called()


def test_validate_output_raises_when_backup_too_old(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    _write_bronze_snapshot(construction_dir, "2026-08-17", _valid_row())  # 3일 전

    bad_row = _valid_row()
    del bad_row["wkt"]
    bad_path = _write_bronze_snapshot(construction_dir, "2026-08-20", bad_row)

    with pytest.raises(ValueError):
        bronze.validate_output(str(bad_path))


def test_validate_output_logs_but_passes_when_only_log_only_issue(tmp_path, monkeypatch, caplog):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    row = _valid_row()
    row["issuedworkstartdate"] = "not-a-date"
    path = _write_bronze_snapshot(construction_dir, "2026-08-20", row)

    with caplog.at_level("WARNING"):
        result = bronze.validate_output(str(path))

    assert result == str(path)
    assert any("expect_column_values_to_be_dateutil_parseable" in rec.message for rec in caplog.records)
