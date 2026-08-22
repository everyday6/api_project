from unittest.mock import MagicMock, patch

import pandas as pd

from src.speed import bronze


def test_has_new_speed_data_true_when_count_positive(tmp_path):
    with patch.object(bronze, "make_session", return_value=MagicMock()), \
         patch.object(bronze, "_get_count", return_value=42):
        result = bronze.has_new_speed_data(bronze_root=tmp_path)

    assert result is True


def test_has_new_speed_data_false_when_count_zero(tmp_path):
    with patch.object(bronze, "make_session", return_value=MagicMock()), \
         patch.object(bronze, "_get_count", return_value=0):
        result = bronze.has_new_speed_data(bronze_root=tmp_path)

    assert result is False


def test_has_new_speed_data_does_not_write_marker(tmp_path):
    # check 함수는 읽기 전용이어야 한다 - 마커를 여기서 갱신하면, 이후
    # 수집/처리 태스크가 실패해 재시도할 때 이미 처리한 구간으로 오인해
    # 조용히 건너뛰게 된다.
    with patch.object(bronze, "make_session", return_value=MagicMock()), \
         patch.object(bronze, "_get_count", return_value=42):
        bronze.has_new_speed_data(bronze_root=tmp_path)

    assert not bronze._marker_path(tmp_path).exists()


def test_has_new_speed_data_queries_since_marker(tmp_path):
    bronze._write_marker(tmp_path, "2026-08-21T12:00:00")
    mock_session = MagicMock()
    mock_session.get.return_value.json.return_value = [{"count": "5"}]

    with patch.object(bronze, "make_session", return_value=mock_session):
        bronze.has_new_speed_data(bronze_root=tmp_path)

    where = mock_session.get.call_args.kwargs["params"]["$where"]
    assert "2026-08-21T12:00:00" in where
    assert ">" in where


def test_collect_speed_data_saves_parquet_and_writes_marker(tmp_path):
    rows = [
        {"link_id": "1", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "2", "speed": "20.0", "data_as_of": "2026-08-21T12:10:00.000"},
    ]

    with patch.object(bronze, "fetch_all", return_value=rows):
        path = bronze.collect_speed_data(bronze_root=tmp_path)

    saved = pd.read_parquet(path)
    assert len(saved) == 2
    assert set(saved["link_id"]) == {"1", "2"}
    assert bronze._read_marker(tmp_path) == "2026-08-21T12:10:00.000"


def test_collect_speed_data_empty_result_returns_empty_string_and_no_marker(tmp_path):
    with patch.object(bronze, "fetch_all", return_value=[]):
        path = bronze.collect_speed_data(bronze_root=tmp_path)

    assert path == ""
    assert bronze._read_marker(tmp_path) is None


def test_collect_speed_data_uses_marker_as_lower_bound(tmp_path):
    bronze._write_marker(tmp_path, "2026-08-21T12:00:00")

    with patch.object(bronze, "fetch_all", return_value=[]) as mock_fetch:
        bronze.collect_speed_data(bronze_root=tmp_path)

    where = mock_fetch.call_args.kwargs["where"]
    assert "2026-08-21T12:00:00" in where


def test_collect_speed_data_without_marker_uses_epoch_sentinel(tmp_path):
    with patch.object(bronze, "fetch_all", return_value=[]) as mock_fetch:
        bronze.collect_speed_data(bronze_root=tmp_path)

    where = mock_fetch.call_args.kwargs["where"]
    assert bronze._EPOCH_SENTINEL in where


def test_collect_speed_data_does_not_advance_marker_past_last_written_value(tmp_path):
    rows = [{"link_id": "1", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"}]

    with patch.object(bronze, "fetch_all", return_value=rows):
        bronze.collect_speed_data(bronze_root=tmp_path)

    assert bronze._read_marker(tmp_path) == "2026-08-21T12:05:00.000"

    with patch.object(bronze, "fetch_all", return_value=[]):
        second_path = bronze.collect_speed_data(bronze_root=tmp_path)

    assert second_path == ""
    assert bronze._read_marker(tmp_path) == "2026-08-21T12:05:00.000"
