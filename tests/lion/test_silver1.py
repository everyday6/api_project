import pandas as pd
import pytest
from airflow.exceptions import AirflowSkipException

from src.lion.silver1 import _clean_lion_dataframe, build_dim_segment_staged


def _raw_row(**overrides):
    row = {
        "SegmentID": "1",
        "Street": "  WEST   19 STREET  ",
        "RW_TYPE": " 1 ",
        "TRUCK_ROUTE_TYPE": " 2 ",
        "TrafDir": "T",
        "FeatureTyp": "0",
        "Number_Travel_Lanes": " 2 ",
        "SHAPE_Length": "120.5",
        "LBoro": "1",
        "NodeIDFrom": "10",
        "NodeIDTo": "11",
        "SHAPE": "LINESTRING (0 0, 1 1)",
        "POSTED_SPEED": "25",
    }
    row.update(overrides)
    return row


def test_clean_lion_dataframe_renames_and_casts_columns():
    df = pd.DataFrame([_raw_row()])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["segment_id"] == "1"
    assert result.iloc[0]["length_ft"] == 120.5
    assert result.iloc[0]["lanes_total"] == 2
    assert result.iloc[0]["borough_code"] == "1"
    assert result.iloc[0]["geometry"] == "LINESTRING (0 0, 1 1)"


def test_clean_lion_dataframe_strips_street_whitespace():
    df = pd.DataFrame([_raw_row(Street="  WEST   19 STREET  ")])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["street_name"] == "WEST 19 STREET"


def test_clean_lion_dataframe_dedupes_by_segment_id():
    df = pd.DataFrame([_raw_row(SegmentID="1"), _raw_row(SegmentID="1")])

    result = _clean_lion_dataframe(df)

    assert len(result) == 1


def test_clean_lion_dataframe_keeps_rw_type_columns_for_gold2():
    df = pd.DataFrame([_raw_row()])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["RW_TYPE"] == "1"
    assert result.iloc[0]["FeatureTyp"] == "0"


def test_build_dim_segment_staged_skips_when_bronze_unchanged(tmp_path):
    # ingest_lion이 changed=False를 주면(원본 그대로), .gdb를 찾거나
    # ogr2ogr을 돌릴 필요 없이 바로 스킵해야 한다 - 그래야 downstream
    # (validate/publish/cleanup, Asset emit)까지 all_success 전파로
    # 조용히 다 같이 스킵된다.
    with pytest.raises(AirflowSkipException):
        build_dim_segment_staged(
            {"path": None, "changed": False},
            staging_root=tmp_path / "_staging" / "dim_segment",
        )
