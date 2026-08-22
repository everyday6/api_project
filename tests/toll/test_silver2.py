import geopandas as gpd
import pandas as pd
import yaml
from shapely.geometry import LineString

from src.toll.silver2 import match_toll_facilities


def test_match_toll_facilities_matches_by_street_substring(tmp_path):
    segments = gpd.GeoDataFrame({
        "segment_id": ["S1", "S2", "S3"],
        "street": ["LINCOLN TUNNEL", "5 AVENUE", "QUEENS MIDTOWN TUNNEL APPROACH"],
        "geometry": [LineString([(0, 0), (1, 1)])] * 3,
    })

    facilities_path = tmp_path / "toll_facilities.yaml"
    facilities_path.write_text(yaml.dump({
        "lincoln_tunnel": {"street_contains": "LINCOLN TUNNEL"},
        "queens_midtown_tunnel": {"street_contains": "QUEENS MIDTOWN TUNNEL"},
    }))

    result = match_toll_facilities(segments, facilities_path)

    assert set(result["segment_id"]) == {"S1", "S3"}
    row_s1 = result[result["segment_id"] == "S1"].iloc[0]
    assert row_s1["facility_key"] == "lincoln_tunnel"
    row_s3 = result[result["segment_id"] == "S3"].iloc[0]
    assert row_s3["facility_key"] == "queens_midtown_tunnel"


def test_match_toll_facilities_excludes_non_matching_segments(tmp_path):
    segments = gpd.GeoDataFrame({
        "segment_id": ["S1"],
        "street": ["5 AVENUE"],
        "geometry": [LineString([(0, 0), (1, 1)])],
    })

    facilities_path = tmp_path / "toll_facilities.yaml"
    facilities_path.write_text(yaml.dump({"lincoln_tunnel": {"street_contains": "LINCOLN TUNNEL"}}))

    result = match_toll_facilities(segments, facilities_path)

    assert result.empty
