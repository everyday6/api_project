from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch

import pandas as pd

from src.speed import bronze, synthetic


def _synthetic_row(link_id="9001"):
    return {
        "id": link_id, "speed": "22.00", "travel_time": "10", "status": "0",
        "data_as_of": "2026-08-21T12:05:00.000", "link_id": link_id, "link_points": "40.0,-73.0",
        "encoded_poly_line": "", "encoded_poly_line_lvls": "", "owner": "NYC-DOT",
        "transcom_id": link_id, "borough": "Manhattan", "link_name": "TEST ST",
    }


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
        {"link_id": "1", "link_points": "40.0,-73.0", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "2", "link_points": "40.1,-73.1", "speed": "20.0", "data_as_of": "2026-08-21T12:10:00.000"},
    ]
    empty_synthetic = pd.DataFrame(columns=synthetic.SPEED_COLUMNS)

    with patch.object(bronze, "fetch_all", return_value=rows), \
         patch.object(bronze, "_synthesize_uncovered_segments", return_value=empty_synthetic):
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


def test_collect_speed_data_without_marker_uses_recent_bootstrap_not_epoch(tmp_path):
    # 마커가 없다고 진짜 1970년부터 요청하면 NYC DOT 피드 전체 역사(실측
    # 1억 행 이상)를 한 번에 끌어오려다 죽는다 - 최근 시점(부트스트랩
    # lookback 이내)이어야 한다.
    with patch.object(bronze, "fetch_all", return_value=[]) as mock_fetch:
        bronze.collect_speed_data(bronze_root=tmp_path)

    where = mock_fetch.call_args.kwargs["where"]
    assert "1970" not in where

    bound_str = where.split("'")[1]
    bound = datetime.fromisoformat(bound_str).replace(tzinfo=bronze._NY_TZ)
    now_ny = datetime.now(bronze._NY_TZ)
    assert timedelta(0) < now_ny - bound <= timedelta(hours=bronze._BOOTSTRAP_LOOKBACK_HOURS) + timedelta(minutes=1)


def test_collect_speed_data_does_not_advance_marker_past_last_written_value(tmp_path):
    rows = [{"link_id": "1", "link_points": "40.0,-73.0", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"}]
    empty_synthetic = pd.DataFrame(columns=synthetic.SPEED_COLUMNS)

    with patch.object(bronze, "fetch_all", return_value=rows), \
         patch.object(bronze, "_synthesize_uncovered_segments", return_value=empty_synthetic):
        bronze.collect_speed_data(bronze_root=tmp_path)

    assert bronze._read_marker(tmp_path) == "2026-08-21T12:05:00.000"

    with patch.object(bronze, "fetch_all", return_value=[]):
        second_path = bronze.collect_speed_data(bronze_root=tmp_path)

    assert second_path == ""
    assert bronze._read_marker(tmp_path) == "2026-08-21T12:05:00.000"


def test_collect_speed_data_appends_synthetic_rows(tmp_path):
    rows = [{"link_id": "1", "link_points": "40.0,-73.0", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"}]
    synthetic_df = pd.DataFrame([_synthetic_row()], columns=synthetic.SPEED_COLUMNS)

    with patch.object(bronze, "fetch_all", return_value=rows), \
         patch.object(bronze, "_synthesize_uncovered_segments", return_value=synthetic_df):
        path = bronze.collect_speed_data(bronze_root=tmp_path)

    saved = pd.read_parquet(path)
    assert len(saved) == 2
    assert "9001" in set(saved["link_id"])


def test_collect_speed_data_marker_unaffected_by_synthetic_rows(tmp_path):
    rows = [{"link_id": "1", "link_points": "40.0,-73.0", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"}]
    synthetic_df = pd.DataFrame([_synthetic_row()], columns=synthetic.SPEED_COLUMNS)

    with patch.object(bronze, "fetch_all", return_value=rows), \
         patch.object(bronze, "_synthesize_uncovered_segments", return_value=synthetic_df):
        bronze.collect_speed_data(bronze_root=tmp_path)

    assert bronze._read_marker(tmp_path) == "2026-08-21T12:05:00.000"


def test_collect_speed_data_handles_no_synthetic_rows(tmp_path):
    rows = [{"link_id": "1", "link_points": "40.0,-73.0", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"}]
    empty_synthetic = pd.DataFrame(columns=synthetic.SPEED_COLUMNS)

    with patch.object(bronze, "fetch_all", return_value=rows), \
         patch.object(bronze, "_synthesize_uncovered_segments", return_value=empty_synthetic):
        path = bronze.collect_speed_data(bronze_root=tmp_path)

    saved = pd.read_parquet(path)
    assert len(saved) == 1


def test_collect_speed_data_passes_distinct_links_to_synthesizer(tmp_path):
    rows = [
        {"link_id": "1", "link_points": "40.0,-73.0", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "1", "link_points": "40.0,-73.0", "speed": "36.0", "data_as_of": "2026-08-21T12:10:00.000"},
    ]
    empty_synthetic = pd.DataFrame(columns=synthetic.SPEED_COLUMNS)

    with patch.object(bronze, "fetch_all", return_value=rows), \
         patch.object(bronze, "_synthesize_uncovered_segments", return_value=empty_synthetic) as mock_synth:
        bronze.collect_speed_data(bronze_root=tmp_path)

    links_arg = mock_synth.call_args.args[0]
    assert len(links_arg) == 1
    assert mock_synth.call_args.args[1] == "2026-08-21T12:10:00.000"
