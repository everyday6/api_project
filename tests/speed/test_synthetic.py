import random

import pandas as pd
import pytest

from src.speed import synthetic


def test_clean_posted_speed_parses_numeric_values():
    raw = pd.DataFrame([
        {"SegmentID": "1", "POSTED_SPEED": "25"},
        {"SegmentID": "2", "POSTED_SPEED": "30"},
    ])

    result = synthetic._clean_posted_speed(raw)

    assert result.loc["1"] == 25.0
    assert result.loc["2"] == 30.0


def test_clean_posted_speed_blank_values_become_nan():
    # LION의 POSTED_SPEED는 결측이 NaN이 아니라 공백 문자열("  ")로
    # 들어있다(routable 세그먼트의 18.8%가 이 상태) - pandas isna()로
    # 안 잡혀서 여기서 명시적으로 NaN 처리한다.
    raw = pd.DataFrame([{"SegmentID": "1", "POSTED_SPEED": "  "}])

    result = synthetic._clean_posted_speed(raw)

    assert pd.isna(result.loc["1"])


def test_segment_geometry_to_link_points_converts_to_lat_lon_string():
    # 실제 LION 세그먼트(0078126, EAST 168 STREET) geometry, EPSG:2263(feet).
    wkt = "MULTILINESTRING ((1010964.447 241812.261, 1011265.495 241554.947))"

    result = synthetic.segment_geometry_to_link_points(wkt)

    points = result.split()
    assert len(points) == 2
    lat, lon = (float(v) for v in points[0].split(","))
    # 뉴욕시 위경도 범위 안에 들어와야 한다(변환이 제대로 됐는지 sanity check).
    assert 40.4 < lat < 40.95
    assert -74.3 < lon < -73.6


def test_random_speed_stays_within_variation_range():
    rng = random.Random(42)
    base = 25.0

    for _ in range(200):
        speed = synthetic._random_speed(base, rng)
        assert base * synthetic._SPEED_VARIATION_MIN <= speed <= base * synthetic._SPEED_VARIATION_MAX


def test_random_speed_never_below_minimum():
    rng = random.Random(1)
    # base가 아주 작아도(0.5mph) 최소값 아래로는 안 내려가야 한다.
    for _ in range(200):
        speed = synthetic._random_speed(0.5, rng)
        assert speed >= synthetic._MIN_SPEED_MPH


def _dim_segment_row(**overrides):
    row = {
        "segment_id": "1001",
        "street_name": "TEST STREET",
        "borough_code": "1",
        "geometry": "MULTILINESTRING ((1010964.447 241812.261, 1011265.495 241554.947))",
        "length_ft": 396.0,
        "is_routable": True,
    }
    row.update(overrides)
    return row


def test_build_synthetic_rows_uses_posted_speed_as_base():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1001")])
    posted_speed = pd.Series({"1001": 30.0})
    rng = random.Random(0)

    result = synthetic.build_synthetic_rows(
        dim_segment, ["1001"], posted_speed, "2026-08-24T00:00:00.000", rng=rng
    )

    assert len(result) == 1
    row = result.iloc[0]
    speed = float(row["speed"])
    assert 30.0 * synthetic._SPEED_VARIATION_MIN <= speed <= 30.0 * synthetic._SPEED_VARIATION_MAX


def test_build_synthetic_rows_falls_back_to_default_when_posted_speed_missing():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1002")])
    posted_speed = pd.Series({"1002": float("nan")})
    rng = random.Random(0)

    result = synthetic.build_synthetic_rows(
        dim_segment, ["1002"], posted_speed, "2026-08-24T00:00:00.000", rng=rng
    )

    row = result.iloc[0]
    speed = float(row["speed"])
    lo = synthetic.DEFAULT_SPEED_MPH * synthetic._SPEED_VARIATION_MIN
    hi = synthetic.DEFAULT_SPEED_MPH * synthetic._SPEED_VARIATION_MAX
    assert lo <= speed <= hi


def test_build_synthetic_rows_matches_real_speed_api_schema():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1001")])
    posted_speed = pd.Series({"1001": 25.0})

    result = synthetic.build_synthetic_rows(
        dim_segment, ["1001"], posted_speed, "2026-08-24T00:00:00.000", rng=random.Random(0)
    )

    assert list(result.columns) == synthetic.SPEED_COLUMNS


def test_build_synthetic_rows_only_includes_requested_uncovered_segments():
    dim_segment = pd.DataFrame([
        _dim_segment_row(segment_id="1001"),
        _dim_segment_row(segment_id="1002"),
    ])
    posted_speed = pd.Series({"1001": 25.0, "1002": 25.0})

    result = synthetic.build_synthetic_rows(
        dim_segment, ["1001"], posted_speed, "2026-08-24T00:00:00.000", rng=random.Random(0)
    )

    assert list(result["link_id"]) == ["1001"]


def test_build_synthetic_rows_sets_link_id_and_status():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1001")])
    posted_speed = pd.Series({"1001": 25.0})

    result = synthetic.build_synthetic_rows(
        dim_segment, ["1001"], posted_speed, "2026-08-24T00:00:00.000", rng=random.Random(0)
    )

    row = result.iloc[0]
    assert row["link_id"] == "1001"
    assert row["status"] == "0"
    assert row["data_as_of"] == "2026-08-24T00:00:00.000"
