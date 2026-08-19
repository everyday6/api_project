from pathlib import Path

import pandas as pd
import pytest
from shapely.geometry import LineString, Point, Polygon

from src.mapping.segment_spatial_weight import (
    _match_points_to_segment,
    _match_points_to_zone,
    _points_from_grid,
    ingest_hotspot_grid,
)


def test_ingest_hotspot_grid_copies_columns_and_adds_metadata(tmp_path):
    source_csv = tmp_path / "bq-results.csv"
    pd.DataFrame({
        "lat_bin": [40.75, 40.76],
        "lon_bin": [-73.98, -73.97],
        "dropoff_count": [100, 50],
    }).to_csv(source_csv, index=False)

    bronze_path = tmp_path / "bronze" / "dropoff_grid.parquet"
    out_path = ingest_hotspot_grid(source_csv_path=source_csv, bronze_path=bronze_path)

    assert out_path == str(bronze_path)
    df = pd.read_parquet(bronze_path)
    assert len(df) == 2
    assert list(df["lat_bin"]) == [40.75, 40.76]
    assert list(df["dropoff_count"]) == [100, 50]
    assert (df["_source"] == "bq_2016_dropoff_grid").all()
    assert df["_ingested_at"].notna().all()


def test_ingest_hotspot_grid_missing_column_raises(tmp_path):
    source_csv = tmp_path / "bq-results.csv"
    pd.DataFrame({"lat_bin": [40.75], "lon_bin": [-73.98]}).to_csv(source_csv, index=False)

    with pytest.raises(ValueError, match="필수 컬럼"):
        ingest_hotspot_grid(source_csv_path=source_csv, bronze_path=tmp_path / "out.parquet")


def test_points_from_grid_reprojects_to_lion_crs():
    bronze_df = pd.DataFrame({
        "lat_bin": [40.75],
        "lon_bin": [-73.98],
        "dropoff_count": [42],
    })

    result = _points_from_grid(bronze_df)

    assert len(result) == 1
    assert result.iloc[0]["dropoff_count"] == 42
    point = result.iloc[0]["geometry"]
    # pyproj Transformer.from_crs("EPSG:4326", "EPSG:2263", always_xy=True)로
    # (-73.98, 40.75)를 직접 변환해 확인한 실측값.
    assert point.x == pytest.approx(989791.457, abs=0.01)
    assert point.y == pytest.approx(212522.519, abs=0.01)


def test_match_points_to_zone_assigns_zone_id(monkeypatch):
    zones = pd.DataFrame({
        "LocationID": [1, 2],
        "borough": ["Manhattan", "Manhattan"],
        "geom": [
            Polygon([(0, 0), (10, 0), (10, 10), (0, 10)]),
            Polygon([(20, 0), (30, 0), (30, 10), (20, 10)]),
        ],
    })
    monkeypatch.setattr("src.mapping.segment_spatial_weight._load_zones", lambda path: zones)

    points = pd.DataFrame({
        "geometry": [Point(5, 5), Point(25, 5), Point(100, 100)],  # 마지막은 어느 zone에도 없음
        "dropoff_count": [10, 20, 30],
    })

    result = _match_points_to_zone(points, zone_shapefile_path=Path("unused"))

    assert len(result) == 2  # 매칭 안 된 포인트는 제외
    assert result.set_index("dropoff_count")["zone_id"].to_dict() == {10: 1, 20: 2}
    assert result["zone_id"].dtype == "int64"


def test_match_points_to_zone_logs_unmatched_count(monkeypatch, caplog):
    zones = pd.DataFrame({
        "LocationID": [1],
        "borough": ["Manhattan"],
        "geom": [Polygon([(0, 0), (10, 0), (10, 10), (0, 10)])],
    })
    monkeypatch.setattr("src.mapping.segment_spatial_weight._load_zones", lambda path: zones)

    points = pd.DataFrame({
        "geometry": [Point(5, 5), Point(100, 100)],
        "dropoff_count": [10, 20],
    })

    with caplog.at_level("WARNING"):
        result = _match_points_to_zone(points, zone_shapefile_path=Path("unused"))

    assert len(result) == 1
    assert any("1건" in rec.message for rec in caplog.records)


def test_match_points_to_segment_restricts_to_same_zone():
    # zone 1에 세그먼트 A(x=0)만 있고, zone 2에 세그먼트 C(x=5)가 있다.
    # point(x=4, zone=1)는 C(x=5, 거리=1)가 A(x=0, 거리=4)보다 더 가깝지만,
    # zone 경계를 넘어 매칭되면 zone별 spatial_weight 합이 깨지므로 zone 1의
    # 후보(A)만 고려해야 한다 — zone 1엔 세그먼트가 A 하나뿐이라 배분 비율은
    # 자동으로 100%가 된다.
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "C"],
        "zone_id": [1, 2],
    })
    dim_segment = pd.DataFrame({
        "segment_id": ["A", "C"],
        "geometry": [
            LineString([(0, 0), (0, 10)]).wkt,
            LineString([(5, 0), (5, 10)]).wkt,
        ],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(4, 5)],
        "dropoff_count": [77],
        "zone_id": [1],
    })

    result = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "A"
    assert result.iloc[0]["dropoff_count"] == pytest.approx(77.0)


def test_match_points_to_segment_skips_zone_with_no_segments():
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A"],
        "geometry": [LineString([(0, 0), (0, 10)]).wkt],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(1, 1)],
        "dropoff_count": [5],
        "zone_id": [99],  # map_zone_segment에 없는 zone
    })

    result = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)

    assert len(result) == 0


def test_match_points_to_segment_falls_back_to_nearest_when_buffer_empty():
    # 세그먼트 A(x=0)가 point(x=1000)에서 100ft(=buffer_ft) 훨씬 밖에 있다.
    # 반경 안에 후보가 없으므로 zone 내 최근접(유일한 세그먼트 A)로 fallback,
    # dropoff_count 전부가 A로 간다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A"], "zone_id": [1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A"],
        "geometry": [LineString([(0, 0), (0, 10)]).wkt],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(1000, 5)],
        "dropoff_count": [30],
        "zone_id": [1],
    })

    result = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment, buffer_ft=100.0)

    assert len(result) == 1
    assert result.iloc[0]["segment_id"] == "A"
    assert result.iloc[0]["dropoff_count"] == pytest.approx(30.0)


def test_match_points_to_segment_splits_by_inverse_distance_within_buffer():
    # zone 1에 세그먼트 A(x=0)와 B(x=20)가 있고, point(x=10, y=0)에서 둘 다
    # buffer_ft=100 이내다. A까지 거리=10, B까지 거리=10으로 같으므로
    # 1/(10+eps) 가중치가 같아 절반씩 나뉘어야 한다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A", "B"],
        "geometry": [
            LineString([(0, -10), (0, 10)]).wkt,
            LineString([(20, -10), (20, 10)]).wkt,
        ],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(10, 0)],
        "dropoff_count": [100],
        "zone_id": [1],
    })

    result = _match_points_to_segment(
        points_with_zone, map_zone_segment, dim_segment, buffer_ft=100.0, epsilon_ft=1.0,
    )

    by_segment = result.set_index("segment_id")["dropoff_count"]
    assert len(result) == 2
    assert by_segment["A"] == pytest.approx(50.0)
    assert by_segment["B"] == pytest.approx(50.0)
    assert by_segment.sum() == pytest.approx(100.0)  # 원래 dropoff_count 보존


def test_match_points_to_segment_closer_segment_gets_more_share():
    # A까지 거리=5, B까지 거리=45 -> A가 훨씬 더 많이 받아야 한다.
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    dim_segment = pd.DataFrame({
        "segment_id": ["A", "B"],
        "geometry": [
            LineString([(5, -10), (5, 10)]).wkt,
            LineString([(45, -10), (45, 10)]).wkt,
        ],
    })
    points_with_zone = pd.DataFrame({
        "geometry": [Point(0, 0)],
        "dropoff_count": [100],
        "zone_id": [1],
    })

    result = _match_points_to_segment(
        points_with_zone, map_zone_segment, dim_segment, buffer_ft=100.0, epsilon_ft=1.0,
    )

    by_segment = result.set_index("segment_id")["dropoff_count"]
    # A: 1/(5+1)=0.1667, B: 1/(45+1)=0.02174 -> A share = 0.1667/(0.1667+0.02174) ≈ 0.8846
    assert by_segment["A"] > by_segment["B"]
    assert by_segment["A"] == pytest.approx(100 * (1 / 6) / (1 / 6 + 1 / 46), rel=1e-6)
    assert by_segment.sum() == pytest.approx(100.0)
