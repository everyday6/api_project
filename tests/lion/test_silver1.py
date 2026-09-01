import pandas as pd
import pytest
from airflow.exceptions import AirflowSkipException

from src.lion import silver1
from src.lion.silver1 import _clean_lion_dataframe, build_dim_segment_staged, validate_dim_segment_base


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


def test_clean_lion_dataframe_marks_clean_row_not_suspect():
    df = pd.DataFrame([_raw_row()])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["is_suspect"] == False  # noqa: E712


def test_clean_lion_dataframe_marks_negative_length_as_suspect():
    df = pd.DataFrame([_raw_row(SHAPE_Length="-5.0")])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["is_suspect"] == True  # noqa: E712


def test_clean_lion_dataframe_marks_out_of_range_speed_limit_as_suspect():
    df = pd.DataFrame([_raw_row(POSTED_SPEED="200")])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["is_suspect"] == True  # noqa: E712


def test_clean_lion_dataframe_missing_speed_limit_is_not_suspect():
    # POSTED_SPEED 미표기는 실측 기준 약 32%로 흔한 정상 상태다 - null
    # 자체를 suspect로 잡으면 안 된다.
    df = pd.DataFrame([_raw_row(POSTED_SPEED="")])

    result = _clean_lion_dataframe(df)

    assert pd.isna(result.iloc[0]["speed_limit_mph"])
    assert result.iloc[0]["is_suspect"] == False  # noqa: E712


def test_clean_lion_dataframe_marks_missing_node_as_suspect():
    df = pd.DataFrame([_raw_row(NodeIDFrom="")])

    result = _clean_lion_dataframe(df)

    assert result.iloc[0]["is_suspect"] == True  # noqa: E712


def _dim_segment_df(n_rows: int, n_suspect: int = 0) -> pd.DataFrame:
    return pd.DataFrame({
        "segment_id": [str(i) for i in range(n_rows)],
        "borough_code": ["1"] * n_rows,
        "is_suspect": [i < n_suspect for i in range(n_rows)],
    })


def test_validate_dim_segment_base_passes_when_suspect_ratio_within_threshold(tmp_path, monkeypatch):
    monkeypatch.setattr(silver1, "MIN_EXPECTED_ROWS", 1)
    monkeypatch.setattr(silver1, "MAX_EXPECTED_ROWS", 1000)
    monkeypatch.setattr(silver1, "MAX_SUSPECT_RATIO", 0.05)
    path = tmp_path / "dim_segment.parquet"
    _dim_segment_df(100, n_suspect=3).to_parquet(path)  # 3% <= 5%

    assert validate_dim_segment_base(str(path)) == str(path)


def test_validate_dim_segment_base_blocks_when_suspect_ratio_exceeds_threshold(tmp_path, monkeypatch):
    monkeypatch.setattr(silver1, "MIN_EXPECTED_ROWS", 1)
    monkeypatch.setattr(silver1, "MAX_EXPECTED_ROWS", 1000)
    monkeypatch.setattr(silver1, "MAX_SUSPECT_RATIO", 0.05)
    path = tmp_path / "dim_segment.parquet"
    _dim_segment_df(100, n_suspect=10).to_parquet(path)  # 10% > 5%

    with pytest.raises(AssertionError, match="의심 행"):
        validate_dim_segment_base(str(path))


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
