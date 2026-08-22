from pathlib import Path
from unittest.mock import patch

import geopandas as gpd
import pandas as pd
import yaml
from shapely.geometry import LineString, Polygon

from src.toll.silver2 import load_lion_segments, match_lion_cbd, match_lion_facilities


def test_match_lion_facilities_matches_by_street_substring(tmp_path):
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

    result = match_lion_facilities(segments, facilities_path)

    assert set(result["segment_id"]) == {"S1", "S3"}
    row_s1 = result[result["segment_id"] == "S1"].iloc[0]
    assert row_s1["facility_key"] == "lincoln_tunnel"
    row_s3 = result[result["segment_id"] == "S3"].iloc[0]
    assert row_s3["facility_key"] == "queens_midtown_tunnel"


def test_match_lion_facilities_excludes_non_matching_segments(tmp_path):
    segments = gpd.GeoDataFrame({
        "segment_id": ["S1"],
        "street": ["5 AVENUE"],
        "geometry": [LineString([(0, 0), (1, 1)])],
    })

    facilities_path = tmp_path / "toll_facilities.yaml"
    facilities_path.write_text(yaml.dump({"lincoln_tunnel": {"street_contains": "LINCOLN TUNNEL"}}))

    result = match_lion_facilities(segments, facilities_path)

    assert result.empty


def test_match_lion_cbd_keeps_segments_inside_polygon():
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]}
    )
    segments = gpd.GeoDataFrame({
        "segment_id": ["INSIDE", "OUTSIDE"],
        "geometry": [
            LineString([(2, 2), (3, 3)]),      # zone 안
            LineString([(100, 100), (101, 101)]),  # zone 밖
        ],
    })

    result = match_lion_cbd(segments, zone_polygon)

    assert list(result["segment_id"]) == ["INSIDE"]


def test_match_lion_cbd_keeps_segments_touching_boundary():
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]}
    )
    # 경계선에 걸치는 segment(zone 진입 지점)도 포함돼야 한다.
    segments = gpd.GeoDataFrame({
        "segment_id": ["BOUNDARY"],
        "geometry": [LineString([(10, 5), (15, 5)])],
    })

    result = match_lion_cbd(segments, zone_polygon)

    assert list(result["segment_id"]) == ["BOUNDARY"]


def test_match_lion_cbd_reprojects_when_crs_differs():
    # CBD Geofence는 위경도(EPSG:4326)로 오고 LION segment는 EPSG:2263(피트)다.
    # 좌표계가 다르면 좌표값 범위 자체가 완전히 달라서(-180~180 vs 수십만 단위)
    # 재투영 없이 조인하면 실제로 안 겹치는 걸로 나온다(실제로 겪은 버그).
    zone_polygon = gpd.GeoDataFrame(
        {"geometry": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])]},
        crs="EPSG:4326",
    )
    segments = gpd.GeoDataFrame(
        {
            "segment_id": ["INSIDE"],
            "geometry": [LineString([(2, 2), (3, 3)])],
        },
        crs="EPSG:4326",
    ).to_crs("EPSG:2263")

    result = match_lion_cbd(segments, zone_polygon)

    assert list(result["segment_id"]) == ["INSIDE"]


def test_load_lion_segments_drops_duplicate_segment_ids():
    # 실측: LION 원본은 같은 segment_id가 여러 행으로 중복돼 있다
    # (243,237행 중 고유 segment_id는 218,373개). 중복이 남아있으면
    # DynamoDB batch_write_item이 "duplicate keys" 에러를 낸다(실제로 겪음).
    raw = gpd.GeoDataFrame({
        "SegmentID": ["S1", "S1", "S2"],
        "Street": ["MAIN ST", "MAIN ST", "5 AVE"],
        "geometry": [LineString([(0, 0), (1, 1)])] * 3,
    })

    with patch("src.toll.silver2.gpd.read_file", return_value=raw):
        result = load_lion_segments(Path("dummy.gdb"))

    assert sorted(result["segment_id"]) == ["S1", "S2"]
