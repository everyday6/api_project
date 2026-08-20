"""
Silver1 ingestion: NYC TLC Taxi Zone

taxi_zone Bronze(lookup CSV + shapefile ZIP 그대로)를 검증하고, 검증을 통과한
원본을 Silver1 경로로 옮긴다. taxi_zone은 정적 참조 테이블이라 변환할 내용이
사실상 없어서(값 자체를 정제/가공하지 않음), Silver1의 역할은 "Bronze가
충분히 온전한지 확인"에 가깝다 — 다른 도메인이라면 Bronze의 존재 여부만
확인하고 필수컬럼/유니크/row-count 범위 같은 무거운 검증은 Silver1이 맡는
것과 같은 원칙이다.
"""

from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, SILVER1_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone")

BRONZE_ROOT = BRONZE_DIR / "taxi_zone"
SILVER1_ROOT = SILVER1_DIR / "taxi_zone"


def validate_taxi_zone_lookup(path: str) -> str:
    """taxi_zone_lookup.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(str(path))

    required_cols = {"LocationID", "Borough", "Zone", "service_zone"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"필수 컬럼 없음: {missing}")

    if df["LocationID"].isna().any():
        raise ValueError("LocationID NULL 발생")

    if not df["LocationID"].is_unique:
        raise ValueError("LocationID 중복 발생")

    # 실측 기준 TLC Taxi Zone은 265개 zone(103~105 등 결번 포함) — 여유를 두고 범위 확인.
    n = len(df)
    if not (250 <= n <= 280):
        raise ValueError(f"행 수가 예상 범위(250~280) 밖입니다: {n}")

    logger.info(f"[taxi_zone_lookup] 검증 통과: {n}행")
    return path


def validate_taxi_zone_shapefile(path: str) -> str:
    """taxi_zones shapefile이 실제로 열리고 zone 폴리곤 개수가 예상 범위인지 확인한다."""
    shapefile_path = Path(path) / "taxi_zones" / "taxi_zones.shp"
    if not shapefile_path.exists():
        raise FileNotFoundError(f"taxi_zones.shp가 없습니다: {shapefile_path}")

    result = subprocess.run(
        ["ogrinfo", "-so", str(shapefile_path), "taxi_zones"],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.error(f"[taxi_zone_shapefile] ogrinfo 실패: {result.stderr}")
        raise RuntimeError(f"shapefile을 열 수 없습니다: {result.stderr}")

    match = re.search(r"Feature Count:\s*(\d+)", result.stdout)
    feature_count = int(match.group(1)) if match else 0
    if not (250 <= feature_count <= 280):
        raise ValueError(f"zone 폴리곤 개수가 예상 범위(250~280) 밖입니다: {feature_count}")

    logger.info(f"[taxi_zone_shapefile] 검증 통과: {feature_count}개 zone")
    return path


def build(
    bronze_root: Path = BRONZE_ROOT,
    silver1_root: Path = SILVER1_ROOT,
) -> Path:
    """lookup/shapefile Bronze를 검증하고, 통과한 원본을 Silver1로 복사한다."""
    lookup_path = bronze_root / "lookup" / "taxi_zone_lookup.parquet"
    shapefile_dir = bronze_root / "shapefile"

    validate_taxi_zone_lookup(str(lookup_path))
    validate_taxi_zone_shapefile(str(shapefile_dir))

    silver1_root.mkdir(parents=True, exist_ok=True)

    shutil.copy(lookup_path, silver1_root / "taxi_zone_lookup.parquet")
    shutil.copytree(shapefile_dir, silver1_root / "shapefile", dirs_exist_ok=True)

    logger.info(f"[taxi_zone] Silver1 저장 완료 -> {silver1_root}")
    return silver1_root
