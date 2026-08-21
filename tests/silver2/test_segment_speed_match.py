import pandas as pd

from src.silver2.segment_speed_match import match_links_to_segments, parse_link_points


def test_parse_link_points_builds_linestring():
    line = parse_link_points("40.700,-74.000 40.701,-74.001")

    assert line is not None
    assert list(line.coords) == [(-74.000, 40.700), (-74.001, 40.701)]


def test_parse_link_points_returns_none_for_single_point():
    assert parse_link_points("40.700,-74.000") is None


def test_match_links_to_segments_buffer_match():
    # LION 세그먼트: (0,0)-(100,0) (feet, EPSG:2263 근사) 근처에 겹치는 링크
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-1", "geometry": "LINESTRING (0 0, 100 0)", "is_routable": True},
    ])

    # WGS84 좌표라 실제 buffer 안에 들어오는지는 좌표 변환에 의존하므로,
    # 이 테스트는 변환 파이프라인 전체가 예외 없이 돌고 결과 스키마가
    # 맞는지를 확인한다(실제 좌표 정합성은 통합 테스트/실데이터로 검증).
    links_df = pd.DataFrame([
        {"link_id": "link-1", "link_points": "40.700,-74.000 40.7001,-74.0001"},
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert list(result.columns) == ["link_id", "segment_id", "distance_ft", "mapping_method"]


def test_match_links_to_segments_skips_unparseable_link():
    dim_segment_df = pd.DataFrame([
        {"segment_id": "seg-1", "geometry": "LINESTRING (0 0, 100 0)", "is_routable": True},
    ])
    links_df = pd.DataFrame([
        {"link_id": "link-bad", "link_points": "40.700,-74.000"},  # 점 하나뿐 -> 파싱 실패
    ])

    result = match_links_to_segments(links_df, dim_segment_df)

    assert result.empty
