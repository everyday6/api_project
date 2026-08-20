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
import tempfile
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, SILVER1_DIR, TMP_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone")

BRONZE_ROOT = BRONZE_DIR / "taxi_zone"
SILVER1_ROOT = SILVER1_DIR / "taxi_zone"


def _stage_shapefile_locally(shapefile_path, work_dir: Path) -> Path:
    """ogrinfo가 읽도록 S3 Shapefile과 필수 sidecar 파일을 로컬에 받는다."""

    if isinstance(shapefile_path, Path):
        return shapefile_path

    local_dir = work_dir / shapefile_path.parent.name
    downloaded_dir = Path(shapefile_path.parent.download_to(local_dir))
    local_shapefile = downloaded_dir / shapefile_path.name

    required_files = [
        local_shapefile,
        local_shapefile.with_suffix(".dbf"),
        local_shapefile.with_suffix(".shx"),
    ]
    missing = [path.name for path in required_files if not path.exists()]
    if missing:
        raise FileNotFoundError(
            f"Taxi Zone Shapefile 로컬 다운로드 누락: {missing}"
        )

    return local_shapefile


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


def validate_taxi_zone_shapefile(path) -> str:
    """taxi_zones shapefile이 실제로 열리고 zone 폴리곤 개수가 예상 범위인지 확인한다."""
    shapefile_path = path / "taxi_zones" / "taxi_zones.shp"

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="taxi_zone_validate_", dir=TMP_DIR) as tmp:
        local_shapefile = _stage_shapefile_locally(shapefile_path, Path(tmp))
        result = subprocess.run(
            ["ogrinfo", "-so", str(local_shapefile), "taxi_zones"],
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
    validate_taxi_zone_shapefile(shapefile_dir)

    silver1_root.mkdir(parents=True, exist_ok=True)

    silver_lookup_path = silver1_root / "taxi_zone_lookup.parquet"
    silver_shapefile_dir = silver1_root / "shapefile"

    if isinstance(lookup_path, Path):
        shutil.copy(lookup_path, silver_lookup_path)
        shutil.copytree(shapefile_dir, silver_shapefile_dir, dirs_exist_ok=True)
    else:
        lookup_path.copy(silver_lookup_path)
        shapefile_dir.copytree(silver_shapefile_dir)

    silver_shapefile = silver_shapefile_dir / "taxi_zones" / "taxi_zones.shp"
    if not silver_lookup_path.exists() or not silver_shapefile.exists():
        raise RuntimeError(f"Taxi Zone Silver1 저장 검증 실패: {silver1_root}")

    logger.info(f"[taxi_zone] Silver1 저장 완료 -> {silver1_root}")
    return str(silver1_root)
