import geopandas as gpd
import pandas as pd

from src.common.config import LION_CRS, SPEED_CRS, SPEED_LION_MAX_DISTANCE_FT
from src.silver2.segment_speed_match import match_links_to_segments, parse_link_points


def test_parse_link_points_builds_linestring():
    line = parse_link_points("40.700,-74.000 40.701,-74.001")

    assert line is not None
    assert list(line.coords) == [(-74.000, 40.700), (-74.001, 40.701)]


def test_parse_link_points_returns_none_for_single_point():
    assert parse_link_points("40.700,-74.000") is None


def _reprojected_link_midpoint():
    """테스트에서 쓰는 링크(WGS84)를 LION_CRS(feet)로 변환한 실제 중심 좌표를 계산한다.

    하드코딩된 좌표를 추측하는 대신, parse_link_points가 실제로 만드는 geometry를
    직접 변환해서 얻는다 — 이러면 좌표계 변환 로직이 바뀌어도 테스트가 항상
    올바른 기준점을 쓴다.
    """
    line = parse_link_points("40.700,-74.000 40.7001,-74.0001")
    gdf = gpd.GeoDataFrame({"geometry": [line]}, crs=SPEED_CRS).to_crs(LION_CRS)
    midpoint = gdf.geometry.iloc[0].centroid
    return midpoint.x, midpoint.y


def test_match_links_to_segments_buffer_match():
    x, y = _reprojected_link_midpoint()
    # 링크 바로 옆(10ft)에 겹치는 세그먼트 -> buffer(50ft) 매칭
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-close", "geometry": f"LINESTRING ({x-10} {y}, {x+10} {y})", "is_routable": True},
    ])
    links_df = pd.DataFrame([
        {"link_id": "link-1", "link_points": "40.700,-74.000 40.7001,-74.0001"},
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "seg-close"
    assert result.iloc[0]["mapping_method"] == "buffer"


def test_match_links_to_segments_nearest_fallback_within_max_distance():
    x, y = _reprojected_link_midpoint()
    # buffer(50ft) 밖이지만 max_distance(1000ft) 안: 500ft 떨어진 세그먼트 -> nearest_fallback
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-far", "geometry": f"LINESTRING ({x+500} {y}, {x+520} {y})", "is_routable": True},
    ])
    links_df = pd.DataFrame([
        {"link_id": "link-1", "link_points": "40.700,-74.000 40.7001,-74.0001"},
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "seg-far"
    assert result.iloc[0]["mapping_method"] == "nearest_fallback"


def test_match_links_to_segments_excludes_beyond_max_distance():
    x, y = _reprojected_link_midpoint()
    too_far = SPEED_LION_MAX_DISTANCE_FT + 500
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-toofar", "geometry": f"LINESTRING ({x+too_far} {y}, {x+too_far+20} {y})", "is_routable": True},
    ])
    links_df = pd.DataFrame([
        {"link_id": "link-1", "link_points": "40.700,-74.000 40.7001,-74.0001"},
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert result.empty


def test_match_links_to_segments_skips_unparseable_link():
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-1", "geometry": "LINESTRING (0 0, 100 0)", "is_routable": True},
    ])
    links_df = pd.DataFrame([
        {"link_id": "link-bad", "link_points": "40.700,-74.000"},  # 점 하나뿐 -> 파싱 실패
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert result.empty
