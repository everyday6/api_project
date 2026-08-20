"""
Silver 매핑: 2016년 하차 위경도 grid -> zone 내부 세그먼트별 공간 가중치(spatial_weight)

TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 이 모듈이 쓰는
2016년 grid(temp/bq-results.csv, BigQuery로 받아온 결과)는 위경도 기준으로
zone 내부 분포를 직접 볼 수 있는 마지막 스냅샷이다. 재수집할 근거 데이터가
없는 정적 값이라 DAG 연결이나 재실행 스케줄은 두지 않는다 — 스크립트로
직접 실행하는 한 번짜리 산출물이다.

`src/mapping/zone_segment.py`의 세그먼트-zone 1:1 매핑에 이어, 그 zone 내부에서
세그먼트별 상대 밀집도를 추가로 산정한다. 배경/설계 근거는
docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from shapely import wkt
from shapely.geometry import Point
from shapely.strtree import STRtree

from src.common.config import (
    BQ_HOTSPOT_CRS,
    BRONZE_DIR,
    HOTSPOT_INVERSE_DISTANCE_EPSILON_FT,
    HOTSPOT_SEGMENT_BUFFER_FT,
    LAPLACE_SMOOTHING_ALPHA,
    LION_CRS,
    PROJECT_ROOT,
    SILVER_DIR,
)
from src.common.logger import get_logger
from src.lion.gold2 import DIM_SEGMENT_PATH
from src.silver2.zone_segment import MAP_ZONE_SEGMENT_PATH, TAXI_ZONE_SHAPEFILE, _load_zones

logger = get_logger(__name__, log_to_file=True, log_file_stem="map_segment_spatial_weight")

# temp/bq-results.csv는 이 저장소(my-project-new) 바깥, 프로젝트 루트의
# 스크래치 위치에 있다. PROJECT_ROOT(my-project-new) 기준이 아니라 그
# 부모 디렉터리 기준이다.
HOTSPOT_CSV_SOURCE_PATH = PROJECT_ROOT.parent / "temp" / "bq-results.csv"
BRONZE_HOTSPOT_PATH = BRONZE_DIR / "tlc" / "hotspot_2016" / "dropoff_grid.parquet"
MAP_SEGMENT_SPATIAL_WEIGHT_PATH = SILVER_DIR / "map_segment_spatial_weight.parquet"


def ingest_hotspot_grid(
    source_csv_path: Path = HOTSPOT_CSV_SOURCE_PATH,
    bronze_path: Path = BRONZE_HOTSPOT_PATH,
) -> str:
    """2016년 하차 위경도 grid CSV(BigQuery 결과)를 변환 없이 Bronze parquet로 옮긴다.

    `src/taxi_zone/bronze.py`와 동일 관례로 메타데이터 컬럼만 붙인다. 재실행할
    근거 데이터가 없는 정적 스냅샷이라 한 번만 실행하면 된다.
    """
    df = pd.read_csv(source_csv_path)

    required = {"lat_bin", "lon_bin", "dropoff_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[map_segment_spatial_weight] 필수 컬럼 없음: {missing}")

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "bq_2016_dropoff_grid"

    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_path, index=False)

    logger.info(f"[map_segment_spatial_weight] hotspot grid {len(df)}행 저장 완료 -> {bronze_path}")
    return str(bronze_path)


def _points_from_grid(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Bronze grid(lat_bin, lon_bin, dropoff_count, EPSG:4326)를 EPSG:2263 Point로 변환한다."""
    transformer = Transformer.from_crs(BQ_HOTSPOT_CRS, LION_CRS, always_xy=True)
    x, y = transformer.transform(bronze_df["lon_bin"].to_numpy(), bronze_df["lat_bin"].to_numpy())

    result = bronze_df[["dropoff_count"]].copy()
    result["geometry"] = [Point(xi, yi) for xi, yi in zip(x, y)]
    return result


def _match_points_to_zone(
    points: pd.DataFrame,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
) -> pd.DataFrame:
    """grid point(EPSG:2263 Point)를 Taxi Zone 폴리곤에 point-in-polygon으로 매칭한다.

    `src/mapping/zone_segment.py`의 세그먼트-zone 매칭과 동일한 STRtree 패턴이다.
    매칭 안 되는 포인트는 제외하고 건수만 로그로 남긴다.
    """
    zones = _load_zones(zone_shapefile_path)
    tree = STRtree(zones["geom"].tolist())

    zone_ids: list[int | None] = []
    unmatched = 0
    multi_match = 0
    for point in points["geometry"]:
        idxs = tree.query(point, predicate="intersects")
        if len(idxs) == 0:
            unmatched += 1
            zone_ids.append(None)
            continue
        if len(idxs) > 1:
            multi_match += 1
        zone_ids.append(zones.iloc[idxs[0]]["LocationID"])

    result = points.copy()
    result["zone_id"] = zone_ids

    if multi_match:
        logger.warning(f"[map_segment_spatial_weight] grid point가 zone 경계에 걸쳐 2개 이상 매칭 {multi_match}건 (첫 번째로 결정)")
    if unmatched:
        logger.warning(f"[map_segment_spatial_weight] zone을 못 찾은 grid point {unmatched}건 (제외)")

    matched = result.dropna(subset=["zone_id"]).copy()
    matched = matched.astype({"zone_id": "int64"})
    return matched[["geometry", "dropoff_count", "zone_id"]]


def _match_points_to_segment(
    points_with_zone: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
    dim_segment: pd.DataFrame,
    buffer_ft: float = HOTSPOT_SEGMENT_BUFFER_FT,
    epsilon_ft: float = HOTSPOT_INVERSE_DISTANCE_EPSILON_FT,
) -> pd.DataFrame:
    """zone_id별로 그룹화해, 그 zone에 속한 세그먼트 중 point 반경 buffer_ft(feet)
    이내 전부에 거리 역가중(1/(distance+epsilon_ft))으로 dropoff_count를 나눠
    배분한다. 반경 안에 세그먼트가 하나도 없으면 zone 내 최근접 세그먼트 1개로
    fallback한다(그때는 dropoff_count 전부가 그 세그먼트로 간다) —
    `src/mapping/ticketmaster_lion.py`의 buffer+nearest-fallback 패턴과 동일하다.

    zone 경계를 넘는 매칭을 막아야 zone 내부 spatial_weight 합이 정확히 1이
    된다. 세그먼트 집계(같은 segment_id로 여러 point가 매칭되는 경우 합산)는
    이 함수의 책임이 아니라 다음 단계(_aggregate_hotspot_counts)에서 한다.
    """
    segments = dim_segment[["segment_id", "geometry"]].merge(
        map_zone_segment[["segment_id", "zone_id"]], on="segment_id", how="inner"
    )
    segments = segments.assign(geom=segments["geometry"].apply(wkt.loads))

    matched_rows = []
    skipped_zones = 0
    skipped_points = 0
    skipped_dropoff_count = 0.0
    for zone_id, zone_points in points_with_zone.groupby("zone_id"):
        zone_segments = segments[segments["zone_id"] == zone_id]
        if zone_segments.empty:
            skipped_zones += 1
            skipped_points += len(zone_points)
            skipped_dropoff_count += float(zone_points["dropoff_count"].sum())
            continue

        geoms = zone_segments["geom"].tolist()
        segment_ids = zone_segments["segment_id"].tolist()
        tree = STRtree(geoms)

        for point, dropoff_count in zip(zone_points["geometry"], zone_points["dropoff_count"]):
            idxs = tree.query(point.buffer(buffer_ft), predicate="intersects")

            if len(idxs) == 0:
                nearest_idx = tree.nearest(point)
                matched_rows.append({
                    "segment_id": segment_ids[nearest_idx],
                    "dropoff_count": float(dropoff_count),
                })
                continue

            distances = np.array([point.distance(geoms[i]) for i in idxs])
            inv_distance = 1.0 / (distances + epsilon_ft)
            shares = inv_distance / inv_distance.sum()

            for idx, share in zip(idxs, shares):
                matched_rows.append({
                    "segment_id": segment_ids[idx],
                    "dropoff_count": float(dropoff_count) * float(share),
                })

    if skipped_zones:
        logger.warning(
            f"[map_segment_spatial_weight] 세그먼트가 없는 zone {skipped_zones}개, "
            f"grid point {skipped_points}건, dropoff_count {skipped_dropoff_count:.1f} 제외"
        )

    if not matched_rows:
        return pd.DataFrame({"segment_id": pd.Series(dtype="object"), "dropoff_count": pd.Series(dtype="float64")})

    return pd.DataFrame(matched_rows)


def _aggregate_hotspot_counts(
    matched_points: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
) -> pd.DataFrame:
    """매칭된 grid point의 dropoff_count를 segment_id별로 합산한다.

    map_zone_segment 전체(그 zone에 속한 세그먼트 전부)에 left join해서, 매칭이
    0건인 세그먼트도 segment_hotspot_count=0으로 명시적으로 포함시킨다 — 이래야
    다음 단계(_compute_spatial_weight)의 zone 내부 정규화가 zone에 속한 세그먼트
    전부를 커버한다.
    """
    hotspot_counts = (
        matched_points.groupby("segment_id")["dropoff_count"].sum().rename("segment_hotspot_count")
    )

    result = map_zone_segment[["segment_id", "zone_id"]].merge(
        hotspot_counts, on="segment_id", how="left"
    )
    result["segment_hotspot_count"] = result["segment_hotspot_count"].fillna(0.0).astype("float64")
    return result


def _compute_spatial_weight(
    df: pd.DataFrame,
    alpha: float = LAPLACE_SMOOTHING_ALPHA,
) -> pd.DataFrame:
    """zone 내부에서 라플라스 스무딩 후 정규화한다 (zone별 spatial_weight 합 = 1).

    alpha는 정성적 초안이다(TODO, 팀 검토 필요) — 매칭 0건 세그먼트가 완전히
    0이 되지 않게 하는 최소한의 목적만 반영했다.
    """
    if alpha <= 0:
        raise ValueError(
            f"alpha는 0보다 커야 합니다: {alpha}. 실 데이터에는 zone 내 세그먼트 전부가 "
            "segment_hotspot_count=0인 zone이 있어(예: zone 103), alpha<=0이면 그 zone의 "
            "zone_totals(= Σ(segment_hotspot_count + alpha))가 0이 되어 spatial_weight가 "
            "0으로 나누기(NaN)가 됩니다."
        )
    result = df.copy()
    result["_smoothed"] = result["segment_hotspot_count"] + alpha
    zone_totals = result.groupby("zone_id")["_smoothed"].transform("sum")
    result["spatial_weight"] = result["_smoothed"] / zone_totals
    return result.drop(columns=["_smoothed"])


def build_map_segment_spatial_weight(
    bronze_path: Path = BRONZE_HOTSPOT_PATH,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
    silver_root: Path = SILVER_DIR,
    alpha: float = LAPLACE_SMOOTHING_ALPHA,
) -> str:
    """2016 hotspot grid Bronze + map_zone_segment + dim_segment로 map_segment_spatial_weight를 만든다."""
    bronze_df = pd.read_parquet(bronze_path, columns=["lat_bin", "lon_bin", "dropoff_count"])
    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id"])
    dim_segment = pd.read_parquet(dim_segment_path, columns=["segment_id", "geometry"])

    points = _points_from_grid(bronze_df)
    points_with_zone = _match_points_to_zone(points, zone_shapefile_path=zone_shapefile_path)
    matched_points = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)
    aggregated = _aggregate_hotspot_counts(matched_points, map_zone_segment)
    result = _compute_spatial_weight(aggregated, alpha=alpha)

    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "map_segment_spatial_weight.parquet"
    result.to_parquet(out_path, index=False)

    logger.info(f"[map_segment_spatial_weight] {len(result)}행 저장 -> {out_path}")
    return str(out_path)


def validate_map_segment_spatial_weight(
    path: str,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
) -> str:
    """map_segment_spatial_weight.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)
    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id"])

    assert df["segment_id"].is_unique, "segment_id 중복 발견"
    assert set(df["segment_id"]) == set(map_zone_segment["segment_id"]), (
        "map_zone_segment의 세그먼트와 정확히 일치하지 않음"
    )
    assert (df["segment_hotspot_count"] >= 0).all(), "segment_hotspot_count에 음수 있음"
    assert df["spatial_weight"].gt(0).all() and df["spatial_weight"].le(1).all(), (
        "spatial_weight가 (0, 1] 범위를 벗어남"
    )

    zone_sums = df.groupby("zone_id")["spatial_weight"].sum()
    assert np.allclose(zone_sums.to_numpy(), 1.0, atol=1e-9), "zone별 spatial_weight 합이 1이 아님"

    logger.info(f"[map_segment_spatial_weight] 검증 통과 ({len(df)}행, zone {df['zone_id'].nunique()}개)")
    return path


if __name__ == "__main__":
    bronze_out = ingest_hotspot_grid()
    silver_out = build_map_segment_spatial_weight(bronze_path=Path(bronze_out))
    validate_map_segment_spatial_weight(silver_out)
