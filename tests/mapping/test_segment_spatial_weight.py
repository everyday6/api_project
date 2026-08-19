from pathlib import Path

import pandas as pd
import pytest
from shapely.geometry import LineString, Point, Polygon

from src.mapping.segment_spatial_weight import (
    _aggregate_hotspot_counts,
    _compute_spatial_weight,
    _match_points_to_segment,
    _match_points_to_zone,
    _points_from_grid,
    build_map_segment_spatial_weight,
    ingest_hotspot_grid,
    validate_map_segment_spatial_weight,
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


def test_aggregate_hotspot_counts_fills_unmatched_segments_with_zero():
    map_zone_segment = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
    })
    matched_points = pd.DataFrame({
        "segment_id": ["A", "A", "C"],
        "dropoff_count": [10, 5, 3],
    })

    result = _aggregate_hotspot_counts(matched_points, map_zone_segment)

    counts = result.set_index("segment_id")["segment_hotspot_count"]
    assert counts["A"] == 15
    assert counts["B"] == 0  # 매칭된 grid point 없음
    assert counts["C"] == 3
    assert len(result) == 3


def test_aggregate_hotspot_counts_handles_no_matches_at_all():
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    matched_points = pd.DataFrame({"segment_id": pd.Series(dtype="object"), "dropoff_count": pd.Series(dtype="float64")})

    result = _aggregate_hotspot_counts(matched_points, map_zone_segment)

    assert len(result) == 2
    assert (result["segment_hotspot_count"] == 0).all()


def test_compute_spatial_weight_sums_to_one_per_zone():
    df = pd.DataFrame({
        "segment_id": ["A", "B", "C"],
        "zone_id": [1, 1, 2],
        "segment_hotspot_count": [90, 0, 5],
    })

    result = _compute_spatial_weight(df, alpha=1.0)

    zone1 = result[result["zone_id"] == 1].set_index("segment_id")["spatial_weight"]
    assert zone1["A"] == pytest.approx(91 / 92)
    assert zone1["B"] == pytest.approx(1 / 92)
    assert zone1.sum() == pytest.approx(1.0)

    zone2 = result[result["zone_id"] == 2]["spatial_weight"]
    assert zone2.iloc[0] == pytest.approx(1.0)  # zone에 세그먼트가 하나뿐이면 무조건 1


def test_compute_spatial_weight_never_fully_zero():
    df = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1], "segment_hotspot_count": [1000, 0]})

    result = _compute_spatial_weight(df, alpha=1.0)

    assert (result["spatial_weight"] > 0).all()


def test_compute_spatial_weight_alpha_zero_raises():
    # 실 데이터에는 zone 내 세그먼트 전부가 segment_hotspot_count=0인 zone이
    # 있다(예: zone 103, 세그먼트 6개 전부 0). alpha<=0이면 그 zone의
    # zone_totals가 0이 되어 spatial_weight가 0으로 나누기(NaN)가 되므로,
    # alpha는 반드시 0보다 커야 한다.
    df = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [103, 103], "segment_hotspot_count": [0, 0]})

    with pytest.raises(ValueError, match="alpha"):
        _compute_spatial_weight(df, alpha=0)


def test_aggregate_hotspot_counts_returns_float64_even_when_all_inputs_are_int():
    # merge 결과가 int64인 채로 남아 있어도(모든 세그먼트가 매칭돼 NaN이 전혀
    # 없는 경우) segment_hotspot_count는 항상 float64여야 한다 —
    # .loc[:, col] = ...astype("float64")가 기존 int64 블록 dtype을 그대로
    # 유지해버려 이 계약이 조용히 깨졌던 적이 있다(pandas 3.x).
    map_zone_segment = pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]})
    matched_points = pd.DataFrame({
        "segment_id": ["A", "B"],  # 둘 다 매칭됨 -> merge에 NaN이 전혀 없음
        "dropoff_count": [10, 5],  # 정수만
    })

    result = _aggregate_hotspot_counts(matched_points, map_zone_segment)

    assert result["segment_hotspot_count"].dtype == "float64"


def test_build_and_validate_map_segment_spatial_weight(tmp_path, monkeypatch):
    zones = pd.DataFrame({
        "LocationID": [1],
        "borough": ["Manhattan"],
        "geom": [Polygon([(980000, 200000), (1000000, 200000), (1000000, 220000), (980000, 220000)])],
    })
    monkeypatch.setattr("src.mapping.segment_spatial_weight._load_zones", lambda path: zones)

    bronze_path = tmp_path / "dropoff_grid.parquet"
    pd.DataFrame({
        "lat_bin": [40.75, 40.76],
        "lon_bin": [-73.98, -73.97],
        "dropoff_count": [100, 10],
    }).to_parquet(bronze_path, index=False)

    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]}).to_parquet(map_zone_segment_path, index=False)

    dim_segment_path = tmp_path / "dim_segment.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B"],
        "geometry": [
            LineString([(989780, 212510), (989800, 212530)]).wkt,  # (-73.98, 40.75) 근처 -> point1(100건)
            LineString([(992550, 216150), (992570, 216170)]).wkt,  # (-73.97, 40.76) 근처 -> point2(10건)
        ],
    }).to_parquet(dim_segment_path, index=False)

    out_path = build_map_segment_spatial_weight(
        bronze_path=bronze_path,
        map_zone_segment_path=map_zone_segment_path,
        dim_segment_path=dim_segment_path,
        zone_shapefile_path=Path("unused"),
        silver_root=tmp_path,
        alpha=1.0,
    )
    validated_path = validate_map_segment_spatial_weight(out_path, map_zone_segment_path=map_zone_segment_path)
    assert validated_path == out_path

    df = pd.read_parquet(out_path).set_index("segment_id")
    assert df.loc["A", "segment_hotspot_count"] == 100
    assert df.loc["B", "segment_hotspot_count"] == 10
    assert df.loc["A", "spatial_weight"] == pytest.approx(101 / 112)
    assert df.loc["B", "spatial_weight"] == pytest.approx(11 / 112)


def test_validate_map_segment_spatial_weight_rejects_zone_sum_not_one(tmp_path):
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]}).to_parquet(map_zone_segment_path, index=False)

    bad_path = tmp_path / "map_segment_spatial_weight.parquet"
    pd.DataFrame({
        "segment_id": ["A", "B"],
        "zone_id": [1, 1],
        "segment_hotspot_count": [10, 5],
        "spatial_weight": [0.5, 0.6],  # 합이 1이 아님(고장난 데이터를 흉내)
    }).to_parquet(bad_path, index=False)

    with pytest.raises(AssertionError, match="합이 1이 아님"):
        validate_map_segment_spatial_weight(str(bad_path), map_zone_segment_path=map_zone_segment_path)


def test_validate_map_segment_spatial_weight_rejects_missing_segment(tmp_path):
    map_zone_segment_path = tmp_path / "map_zone_segment.parquet"
    pd.DataFrame({"segment_id": ["A", "B"], "zone_id": [1, 1]}).to_parquet(map_zone_segment_path, index=False)

    bad_path = tmp_path / "map_segment_spatial_weight.parquet"
    pd.DataFrame({
        "segment_id": ["A"],  # B가 빠짐
        "zone_id": [1],
        "segment_hotspot_count": [10],
        "spatial_weight": [1.0],
    }).to_parquet(bad_path, index=False)

    with pytest.raises(AssertionError, match="일치하지 않음"):
        validate_map_segment_spatial_weight(str(bad_path), map_zone_segment_path=map_zone_segment_path)
