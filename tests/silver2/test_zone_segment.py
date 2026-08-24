import pandas as pd
import pytest
from shapely.geometry import LineString, Polygon

from src.silver2.zone_segment import (
    _content_hash,
    _map_segments_to_zones,
    current_mapping_version,
    publish_map_zone_segment,
    validate_reference_inputs,
    validate_staged_map_zone_segment,
)


def test_map_segments_uses_midpoint_and_returns_one_zone_per_segment():
    segments = pd.DataFrame({
        "segment_id": ["0077356", "0000002"],
        "geometry": [
            LineString([(1, 1), (2, 2)]),
            LineString([(11, 1), (12, 2)]),
        ],
    })
    zones = pd.DataFrame({
        "zone_id": [1, 2],
        "borough": ["Manhattan", "Queens"],
        "geometry": [
            Polygon([(0, 0), (5, 0), (5, 5), (0, 5)]),
            Polygon([(10, 0), (15, 0), (15, 5), (10, 5)]),
        ],
    })

    result = _map_segments_to_zones(segments, zones)

    assert result[["segment_id", "zone_id"]].to_dict("records") == [
        {"segment_id": "0077356", "zone_id": 1},
        {"segment_id": "0000002", "zone_id": 2},
    ]
    assert result["mapping_method"].tolist() == ["contains", "contains"]


def test_map_segments_falls_back_to_nearest_zone_without_dropping_segment():
    segments = pd.DataFrame({
        "segment_id": ["0000003"],
        "geometry": [LineString([(8, 1), (9, 1)])],
    })
    zones = pd.DataFrame({
        "zone_id": [1, 2],
        "borough": ["Manhattan", "Queens"],
        "geometry": [
            Polygon([(0, 0), (5, 0), (5, 5), (0, 5)]),
            Polygon([(10, 0), (15, 0), (15, 5), (10, 5)]),
        ],
    })

    result = _map_segments_to_zones(segments, zones)

    assert len(result) == 1
    assert result.iloc[0]["zone_id"] == 2
    assert result.iloc[0]["mapping_method"] == "nearest"
    assert result.iloc[0]["distance_ft"] > 0


def test_map_segments_rejects_duplicate_segment_ids():
    segments = pd.DataFrame({
        "segment_id": ["same", "same"],
        "geometry": [LineString([(0, 0), (1, 1)]), LineString([(1, 1), (2, 2)])],
    })
    zones = pd.DataFrame({
        "zone_id": [1],
        "borough": ["Manhattan"],
        "geometry": [Polygon([(0, 0), (5, 0), (5, 5), (0, 5)])],
    })

    with pytest.raises(ValueError, match="segment_id 중복"):
        _map_segments_to_zones(segments, zones)


def test_validate_reference_inputs_requires_both_silver_inputs(tmp_path):
    lion_path = tmp_path / "dim_segment.parquet"
    zone_path = tmp_path / "taxi_zones.shp"

    with pytest.raises(FileNotFoundError, match="LION Silver1.*Taxi Zone Silver1"):
        validate_reference_inputs(lion_path, zone_path)

    lion_path.touch()
    with pytest.raises(FileNotFoundError, match="Taxi Zone Silver1"):
        validate_reference_inputs(lion_path, zone_path)

    zone_path.touch()
    assert validate_reference_inputs(lion_path, zone_path) == {
        "lion_segment_path": str(lion_path),
        "zone_shapefile_path": str(zone_path),
    }


def test_validated_staging_is_published_and_cleaned(tmp_path):
    run_id = "a" * 32
    staging_root = tmp_path / "staging"
    run_path = staging_root / f"run_id={run_id}"
    stage_path = run_path / "map_zone_segment.parquet"
    output_path = tmp_path / "silver2" / "map_zone_segment.parquet"
    lion_path = tmp_path / "silver1" / "dim_segment.parquet"
    run_path.mkdir(parents=True)
    lion_path.parent.mkdir(parents=True)

    pd.DataFrame({
        "segment_id": ["0000001", "0000002"],
        "geometry": ["line-1", "line-2"],
    }).to_parquet(lion_path, index=False)
    expected = pd.DataFrame({
        "segment_id": ["0000001", "0000002"],
        "zone_id": [1, 2],
        "borough": ["Manhattan", "Queens"],
        "mapping_method": ["contains", "nearest"],
        "distance_ft": [0.0, 1.0],
    })
    expected.to_parquet(stage_path, index=False)
    mapping_version = _content_hash(expected)
    stage_result = {
        "run_id": run_id,
        "stage_path": str(stage_path),
        "mapping_version": mapping_version,
    }

    validated = validate_staged_map_zone_segment(
        stage_result,
        lion_segment_path=lion_path,
        staging_root=staging_root,
    )
    version_path = tmp_path / "silver2" / "map_zone_segment_version.txt"
    result = publish_map_zone_segment(
        validated,
        output_path=output_path,
        staging_root=staging_root,
        version_path=version_path,
    )

    assert result == str(output_path)
    pd.testing.assert_frame_equal(pd.read_parquet(output_path), expected)
    assert not run_path.exists()
    assert current_mapping_version(version_path) == mapping_version


def test_current_mapping_version_returns_none_when_marker_missing(tmp_path):
    assert current_mapping_version(tmp_path / "map_zone_segment_version.txt") is None


def test_content_hash_is_stable_regardless_of_row_order():
    mapping_a = pd.DataFrame({
        "segment_id": ["0000001", "0000002"],
        "zone_id": [1, 2],
    })
    mapping_b = mapping_a.iloc[::-1].reset_index(drop=True)

    assert _content_hash(mapping_a) == _content_hash(mapping_b)


def test_content_hash_changes_when_values_change():
    mapping_a = pd.DataFrame({"segment_id": ["0000001"], "zone_id": [1]})
    mapping_b = pd.DataFrame({"segment_id": ["0000001"], "zone_id": [2]})

    assert _content_hash(mapping_a) != _content_hash(mapping_b)
