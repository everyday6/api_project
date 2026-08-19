from pathlib import Path

import pandas as pd
import pytest
from shapely.geometry import Point, Polygon

from src.mapping.segment_spatial_weight import (
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
