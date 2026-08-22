import pandas as pd
import pytest

from src.lion.silver1 import (
    SILVER_COLUMNS,
    _staging_run_path,
    _transform_lion_frame,
    cleanup_dim_segment_staging,
    publish_dim_segment,
    validate_dim_segment,
    validate_staged_dim_segment,
)


def _raw_lion_frame():
    common = {
        "Street": "  west   19 street ",
        "RW_TYPE": " 1 ",
        "TRUCK_ROUTE_TYPE": " 2 ",
        "TrafDir": "T",
        "FeatureTyp": "0",
        "Number_Travel_Lanes": " 2 ",
        "Number_Total_Lanes": "2",
        "StreetWidth_Min": "20",
        "StreetWidth_Max": "30",
        "SHAPE_Length": "100.5",
        "LBoro": "1",
        "NodeIDFrom": "10",
        "NodeIDTo": "11",
        "WKT": "LINESTRING (0 0, 1 1)",
    }
    return pd.DataFrame([
        {**common, "SegmentID": "0000001"},
        {**common, "SegmentID": "0000001"},
        {**common, "SegmentID": "0000002"},
    ])


def test_transform_lion_frame_normalizes_and_deduplicates():
    result = _transform_lion_frame(_raw_lion_frame())

    assert result.columns.tolist() == SILVER_COLUMNS
    assert result["segment_id"].tolist() == ["0000001", "0000002"]
    assert result["street_name"].tolist() == ["WEST 19 STREET", "WEST 19 STREET"]
    assert result["length_ft"].tolist() == [100.5, 100.5]


def test_validate_and_publish_staged_dim_segment(tmp_path):
    staging_root = tmp_path / "staging"
    output_path = tmp_path / "silver1" / "lion" / "dim_segment.parquet"
    run_id = "a" * 32
    stage_path = _staging_run_path(run_id, staging_root) / "dim_segment.parquet"
    stage_path.parent.mkdir(parents=True)
    _transform_lion_frame(_raw_lion_frame()).to_parquet(stage_path, index=False)
    stage_result = {
        "run_id": run_id,
        "stage_path": str(stage_path),
        "source_version": "version_date=2026-08-22",
    }

    validate_dim_segment(stage_path, min_rows=1, max_rows=10)
    validated = validate_staged_dim_segment(
        stage_result,
        staging_root=staging_root,
        min_rows=1,
        max_rows=10,
    )
    published = publish_dim_segment(
        validated,
        output_path=output_path,
        staging_root=staging_root,
    )

    assert output_path.exists()
    assert published["output_path"] == str(output_path)

    cleanup_dim_segment_staging(published, staging_root=staging_root)
    assert not _staging_run_path(run_id, staging_root).exists()
def test_validate_dim_segment_rejects_duplicate_segment_id(tmp_path):
    path = tmp_path / "dim_segment.parquet"
    frame = _transform_lion_frame(_raw_lion_frame())
    pd.concat([frame, frame.iloc[[0]]], ignore_index=True).to_parquet(path, index=False)

    with pytest.raises(ValueError, match="중복"):
        validate_dim_segment(path, min_rows=1, max_rows=10)


def test_staging_run_path_rejects_untrusted_component(tmp_path):
    with pytest.raises(ValueError, match="잘못된"):
        _staging_run_path("../../", staging_root=tmp_path)
