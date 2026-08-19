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
