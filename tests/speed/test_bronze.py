from datetime import datetime
from unittest.mock import MagicMock, patch

import pandas as pd

from src.speed import bronze


def test_has_new_speed_data_true_when_count_positive():
    mock_session = MagicMock()

    with patch.object(bronze, "make_session", return_value=mock_session), \
         patch.object(bronze, "_get_count", return_value=42):
        result = bronze.has_new_speed_data(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30)
        )

    assert result is True


def test_has_new_speed_data_false_when_count_zero():
    with patch.object(bronze, "make_session", return_value=MagicMock()), \
         patch.object(bronze, "_get_count", return_value=0):
        result = bronze.has_new_speed_data(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30)
        )

    assert result is False


def test_collect_speed_window_saves_parquet(tmp_path):
    rows = [
        {"link_id": "1", "speed": "35.5", "data_as_of": "2026-08-21T12:05:00.000"},
        {"link_id": "2", "speed": "20.0", "data_as_of": "2026-08-21T12:10:00.000"},
    ]

    with patch.object(bronze, "fetch_all", return_value=rows):
        path = bronze.collect_speed_window(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30), bronze_root=tmp_path
        )

    saved = pd.read_parquet(path)
    assert len(saved) == 2
    assert set(saved["link_id"]) == {"1", "2"}


def test_collect_speed_window_empty_result_returns_empty_string(tmp_path):
    with patch.object(bronze, "fetch_all", return_value=[]):
        path = bronze.collect_speed_window(
            datetime(2026, 8, 21, 12, 0), datetime(2026, 8, 21, 12, 30), bronze_root=tmp_path
        )

    assert path == ""
