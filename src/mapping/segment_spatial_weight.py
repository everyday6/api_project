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
    LAPLACE_SMOOTHING_ALPHA,
    LION_CRS,
    PROJECT_ROOT,
    SILVER_DIR,
)
from src.common.logger import get_logger
from src.lion.silver import DIM_SEGMENT_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH, TAXI_ZONE_SHAPEFILE, _load_zones

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
