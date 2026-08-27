import random

import pandas as pd

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
    # 들어있다(세그먼트의 18.8%가 이 상태) - pandas isna()로
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
    }
    row.update(overrides)
    return row


# ---------------------------------------------------------------------------
# 참고표(reference table) - 무거운 geometry 변환/POSTED_SPEED 조회를 미리
# 한 번만 해서 저장해두는 부분. LION은 분기에 한 번만 바뀌는데 이 계산을
# collect_speed_data()가 30분마다 새로 하면 낭비라(gdb 로드만 9초+) 캐싱한다.
# ---------------------------------------------------------------------------

def test_build_reference_table_uses_posted_speed_as_base():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1001")])
    posted_speed = pd.Series({"1001": 30.0})

    result = synthetic.build_reference_table(dim_segment, posted_speed)

    assert len(result) == 1
    assert result.iloc[0]["base_speed"] == 30.0


def test_build_reference_table_falls_back_to_default_when_posted_speed_missing():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1002")])
    posted_speed = pd.Series({"1002": float("nan")})

    result = synthetic.build_reference_table(dim_segment, posted_speed)

    assert result.iloc[0]["base_speed"] == synthetic.DEFAULT_SPEED_MPH


def test_build_reference_table_precomputes_link_points():
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1001")])
    posted_speed = pd.Series({"1001": 25.0})

    result = synthetic.build_reference_table(dim_segment, posted_speed)

    assert result.iloc[0]["link_points"] == synthetic.segment_geometry_to_link_points(
        _dim_segment_row(segment_id="1001")["geometry"]
    )


def test_build_reference_table_covers_every_segment():
    dim_segment = pd.DataFrame([
        _dim_segment_row(segment_id="1001"),
        _dim_segment_row(segment_id="1002"),
    ])
    posted_speed = pd.Series({"1001": 25.0, "1002": 30.0})

    result = synthetic.build_reference_table(dim_segment, posted_speed)

    assert set(result["segment_id"]) == {"1001", "1002"}
    assert list(result.columns) == synthetic.REFERENCE_TABLE_COLUMNS


# ---------------------------------------------------------------------------
# load_or_build_reference_table - 캐시 파일 있으면 그대로 읽고(gdb 재로드
# 없음), 없으면 한 번 만들어서 저장.
# ---------------------------------------------------------------------------

def test_load_or_build_reference_table_reads_cache_when_present(tmp_path):
    reference_path = tmp_path / "reference.parquet"
    cached = pd.DataFrame(
        [{"segment_id": "1001", "link_points": "40.0,-73.0", "base_speed": 25.0,
          "street_name": "CACHED ST", "borough": "Manhattan", "length_ft": 100.0}],
        columns=synthetic.REFERENCE_TABLE_COLUMNS,
    )
    cached.to_parquet(reference_path, index=False)

    def _should_not_be_called():
        raise AssertionError("캐시가 있으면 다시 빌드하면 안 된다")

    result = synthetic.load_or_build_reference_table(
        reference_path, dim_segment_loader=_should_not_be_called, posted_speed_loader=_should_not_be_called,
    )

    assert result.iloc[0]["street_name"] == "CACHED ST"


def test_load_or_build_reference_table_builds_and_saves_when_missing(tmp_path):
    reference_path = tmp_path / "reference.parquet"
    dim_segment = pd.DataFrame([_dim_segment_row(segment_id="1001")])
    posted_speed = pd.Series({"1001": 25.0})

    result = synthetic.load_or_build_reference_table(
        reference_path, dim_segment_loader=lambda: dim_segment, posted_speed_loader=lambda: posted_speed,
    )

    assert len(result) == 1
    assert reference_path.exists()


# ---------------------------------------------------------------------------
# build_synthetic_rows - 이제 참고표를 입력으로 받아서 실제 speed API
# 스키마 row를 만든다(geometry 변환 등 무거운 계산은 이미 참고표에 끝나있음).
# ---------------------------------------------------------------------------

def _reference_row(**overrides):
    row = {
        "segment_id": "1001",
        "link_points": "40.8303538,-73.9034669",
        "base_speed": 25.0,
        "street_name": "TEST STREET",
        "borough": "Manhattan",
        "length_ft": 396.0,
    }
    row.update(overrides)
    return row


def test_build_synthetic_rows_matches_real_speed_api_schema():
    reference = pd.DataFrame([_reference_row()], columns=synthetic.REFERENCE_TABLE_COLUMNS)

    result = synthetic.build_synthetic_rows(reference, ["1001"], "2026-08-24T00:00:00.000", rng=random.Random(0))

    assert list(result.columns) == synthetic.SPEED_COLUMNS


def test_build_synthetic_rows_speed_varies_around_reference_base_speed():
    reference = pd.DataFrame([_reference_row(segment_id="1001", base_speed=30.0)], columns=synthetic.REFERENCE_TABLE_COLUMNS)
    rng = random.Random(0)

    result = synthetic.build_synthetic_rows(reference, ["1001"], "2026-08-24T00:00:00.000", rng=rng)

    speed = float(result.iloc[0]["speed"])
    assert 30.0 * synthetic._SPEED_VARIATION_MIN <= speed <= 30.0 * synthetic._SPEED_VARIATION_MAX


def test_build_synthetic_rows_only_includes_requested_uncovered_segments():
    reference = pd.DataFrame([
        _reference_row(segment_id="1001"),
        _reference_row(segment_id="1002"),
    ], columns=synthetic.REFERENCE_TABLE_COLUMNS)

    result = synthetic.build_synthetic_rows(reference, ["1001"], "2026-08-24T00:00:00.000", rng=random.Random(0))

    assert list(result["link_id"]) == ["1001"]


def test_build_synthetic_rows_sets_link_id_and_status():
    reference = pd.DataFrame([_reference_row(segment_id="1001")], columns=synthetic.REFERENCE_TABLE_COLUMNS)

    result = synthetic.build_synthetic_rows(reference, ["1001"], "2026-08-24T00:00:00.000", rng=random.Random(0))

    row = result.iloc[0]
    assert row["link_id"] == "1001"
    assert row["status"] == "0"
    assert row["data_as_of"] == "2026-08-24T00:00:00.000"


def test_build_synthetic_rows_reuses_precomputed_link_points_without_recomputing():
    # 참고표에 이미 저장된 link_points를 그대로 써야 한다 - geometry
    # 재계산(shapely/geopandas 호출)이 없어야 빠르다.
    reference = pd.DataFrame(
        [_reference_row(segment_id="1001", link_points="1.234567,7.654321")],
        columns=synthetic.REFERENCE_TABLE_COLUMNS,
    )

    result = synthetic.build_synthetic_rows(reference, ["1001"], "2026-08-24T00:00:00.000", rng=random.Random(0))

    assert result.iloc[0]["link_points"] == "1.234567,7.654321"
