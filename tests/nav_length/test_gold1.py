import pandas as pd

from src.nav_length.gold1 import filter_valid_length_segments


def test_filter_keeps_positive_length_segments():
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 120.5},
        {"segment_id": "2", "length_ft": 0.0},
        {"segment_id": "3", "length_ft": -1.0},
    ])

    result = filter_valid_length_segments(df)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "1"


def test_filter_output_has_only_segment_id_and_length_ft():
    df = pd.DataFrame([
        {"segment_id": "1", "length_ft": 120.5, "street_name": "X"},
    ])

    result = filter_valid_length_segments(df)

    assert sorted(result.columns) == ["length_ft", "segment_id"]
