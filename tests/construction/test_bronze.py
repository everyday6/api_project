from datetime import date
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest
from airflow.sdk.exceptions import AirflowSkipException

from src.construction import bronze
from src.construction.silver1 import READ_COLS


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


def _mark_validated(snapshot_dir: Path) -> None:
    (snapshot_dir / bronze.VALIDATED_MARKER_NAME).touch()


# ---------- _find_recent_bronze_snapshot ----------


def test_find_recent_bronze_snapshot_within_threshold(tmp_path):
    path = _write_bronze_snapshot(tmp_path, "2026-08-18", _valid_row())
    _mark_validated(path.parent)

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found == str(tmp_path / "dt=2026-08-18" / "data.parquet")


def test_find_recent_bronze_snapshot_too_old_returns_none(tmp_path):
    path = _write_bronze_snapshot(tmp_path, "2026-08-17", _valid_row())
    _mark_validated(path.parent)

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_no_folders_returns_none(tmp_path):
    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_ignores_folder_without_data_file(tmp_path):
    (tmp_path / "dt=2026-08-19").mkdir(parents=True)  # data.parquet 없음

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_ignores_unmarked_folder_even_with_data_file(tmp_path):
    # data.parquet exists (as it would for a day that failed critical
    # validation — build() always writes before validate_output() runs)
    # but no _VALIDATED marker, because that day's validation never passed.
    _write_bronze_snapshot(tmp_path, "2026-08-18", _valid_row())

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found is None


def test_find_recent_bronze_snapshot_skips_past_unmarked_day_to_older_valid_one(tmp_path):
    # 2026-08-19(하루 전, 그날 검증 실패라 마커 없음)이 더 최근이지만 후보가
    # 아니므로, 그보다 하루 더 전인 2026-08-18(검증 통과, 마커 있음)을
    # 찾아내야 한다 — "최신 것부터 훑다가 실패한 날은 건너뛰고 그 이전에
    # 통과한 날을 쓴다"는 이 fix의 핵심 동작을 직접 증명하는 테스트.
    failed_path = _write_bronze_snapshot(tmp_path, "2026-08-19", _valid_row())
    passed_path = _write_bronze_snapshot(tmp_path, "2026-08-18", _valid_row())
    _mark_validated(passed_path.parent)
    # failed_path의 dt=2026-08-19에는 의도적으로 마커를 남기지 않는다.
    assert not (failed_path.parent / bronze.VALIDATED_MARKER_NAME).exists()

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found == str(tmp_path / "dt=2026-08-18" / "data.parquet")


def test_find_recent_bronze_snapshot_picks_the_latest_of_several(tmp_path):
    path_17 = _write_bronze_snapshot(tmp_path, "2026-08-17", _valid_row())
    path_19 = _write_bronze_snapshot(tmp_path, "2026-08-19", _valid_row())
    _mark_validated(path_17.parent)
    _mark_validated(path_19.parent)

    found = bronze._find_recent_bronze_snapshot(tmp_path, "2026-08-20", max_age_days=2)

    assert found == str(tmp_path / "dt=2026-08-19" / "data.parquet")


# ---------- validate_output ----------


def test_validate_output_passes_clean_file(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    path = _write_bronze_snapshot(tmp_path / "construction", "2026-08-20", _valid_row())

    result = bronze.validate_output(str(path))

    assert result == str(path)
    assert (Path(path).parent / bronze.VALIDATED_MARKER_NAME).exists()


def test_validate_output_skips_when_critical_fails_and_recent_backup_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    good_path = _write_bronze_snapshot(construction_dir, "2026-08-18", _valid_row())
    _mark_validated(good_path.parent)

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

    old_path = _write_bronze_snapshot(construction_dir, "2026-08-17", _valid_row())  # 3일 전
    _mark_validated(old_path.parent)

    bad_row = _valid_row()
    del bad_row["wkt"]
    bad_path = _write_bronze_snapshot(construction_dir, "2026-08-20", bad_row)

    with pytest.raises(ValueError):
        bronze.validate_output(str(bad_path))


def test_validate_output_escalates_to_raise_after_repeated_critical_failures(tmp_path, monkeypatch):
    # 이 fix가 고치려던 바로 그 시나리오: critical 실패가 반복되면 실패한
    # 날의 파일이 다음 날의 "백업"이 되어 영원히 조용히 skip만 반복하면
    # 안 되고, MAX_FALLBACK_AGE_DAYS(2일)를 넘어가면 결국 raise로
    # 승격돼야 한다.
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    def _bad_row():
        row = _valid_row()
        del row["wkt"]
        return row

    good_path = _write_bronze_snapshot(construction_dir, "2026-08-17", _valid_row())
    _mark_validated(good_path.parent)

    day18 = _write_bronze_snapshot(construction_dir, "2026-08-18", _bad_row())
    day19 = _write_bronze_snapshot(construction_dir, "2026-08-19", _bad_row())
    day20 = _write_bronze_snapshot(construction_dir, "2026-08-20", _bad_row())

    with patch.object(bronze, "notify_slack_message") as mock_notify:
        # 1일 전 백업 존재 -> skip
        with pytest.raises(AirflowSkipException):
            bronze.validate_output(str(day18))
        assert not (day18.parent / bronze.VALIDATED_MARKER_NAME).exists()

        # 2일 전 백업이지만 아직 임계값(2일) 이내 -> skip
        with pytest.raises(AirflowSkipException):
            bronze.validate_output(str(day19))
        assert not (day19.parent / bronze.VALIDATED_MARKER_NAME).exists()

        assert mock_notify.call_count == 2

        # 3일 전 백업은 임계값 초과 -> 더 이상 skip 못 하고 raise로 승격
        with pytest.raises(ValueError):
            bronze.validate_output(str(day20))
        assert not (day20.parent / bronze.VALIDATED_MARKER_NAME).exists()

    # 마지막 raise 단계에서는 알릴 백업이 없으므로 Slack이 추가로 불리지 않는다.
    assert mock_notify.call_count == 2


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
    assert (Path(path).parent / bronze.VALIDATED_MARKER_NAME).exists()


def test_validate_output_missing_file_skips_when_recent_backup_exists(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    good_path = _write_bronze_snapshot(construction_dir, "2026-08-18", _valid_row())
    _mark_validated(good_path.parent)

    missing_path = construction_dir / "dt=2026-08-20" / "data.parquet"
    missing_path.parent.mkdir(parents=True)  # build() would have created the dt= dir even if fetch_all_streaming wrote nothing

    with patch.object(bronze, "notify_slack_message") as mock_notify:
        with pytest.raises(AirflowSkipException):
            bronze.validate_output(str(missing_path))

    mock_notify.assert_called_once()
    assert "bronze_file_missing" in mock_notify.call_args.args[0]


def test_validate_output_missing_file_raises_when_no_backup(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "BRONZE_DIR", tmp_path)
    construction_dir = tmp_path / "construction"

    missing_path = construction_dir / "dt=2026-08-20" / "data.parquet"
    missing_path.parent.mkdir(parents=True)

    with pytest.raises(ValueError, match="bronze_file_missing"):
        bronze.validate_output(str(missing_path))
