"""Taxi Zone Bronze를 검증해 S3 Silver1 참조 데이터로 승격한다."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, SILVER1_DIR, TMP_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone_silver1")

BRONZE_ROOT = BRONZE_DIR / "taxi_zone"
SILVER1_ROOT = SILVER1_DIR / "taxi_zone"


def _stage_shapefile_locally(shapefile_path, work_dir: Path) -> Path:
    """GDAL이 읽을 수 있도록 S3 Shapefile 묶음을 임시 로컬에 받는다."""

    if isinstance(shapefile_path, Path):
        return shapefile_path

    local_dir = work_dir / shapefile_path.parent.name
    downloaded_dir = Path(shapefile_path.parent.download_to(local_dir))
    local_shapefile = downloaded_dir / shapefile_path.name

    required = [
        local_shapefile,
        local_shapefile.with_suffix(".dbf"),
        local_shapefile.with_suffix(".shx"),
    ]
    missing = [path.name for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Taxi Zone Shapefile 로컬 다운로드 누락: {missing}")
    return local_shapefile


def validate_taxi_zone_lookup(path) -> str:
    """lookup의 필수 컬럼, LocationID 유일성, 행 수를 검증한다."""

    frame = pd.read_parquet(str(path))
    required = {"LocationID", "Borough", "Zone", "service_zone"}
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"필수 컬럼 없음: {missing}")
    if frame["LocationID"].isna().any():
        raise ValueError("LocationID NULL 발생")
    if not frame["LocationID"].is_unique:
        raise ValueError("LocationID 중복 발생")
    if not 250 <= len(frame) <= 280:
        raise ValueError(f"행 수가 예상 범위(250~280) 밖입니다: {len(frame)}")
    return str(path)


def validate_taxi_zone_shapefile(shapefile_dir) -> str:
    """Shapefile을 실제로 열어 feature 수를 검증한다."""

    shapefile_path = shapefile_dir / "taxi_zones" / "taxi_zones.shp"
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="taxi_zone_validate_", dir=TMP_DIR) as tmp:
        local_path = _stage_shapefile_locally(shapefile_path, Path(tmp))
        result = subprocess.run(
            ["ogrinfo", "-so", str(local_path), "taxi_zones"],
            capture_output=True,
            text=True,
        )

    if result.returncode != 0:
        raise RuntimeError(f"Taxi Zone Shapefile을 열 수 없습니다: {result.stderr}")
    match = re.search(r"Feature Count:\s*(\d+)", result.stdout)
    feature_count = int(match.group(1)) if match else 0
    if not 250 <= feature_count <= 280:
        raise ValueError(
            f"zone 폴리곤 수가 예상 범위(250~280) 밖입니다: {feature_count}"
        )
    return str(shapefile_dir)


def build(bronze_root=BRONZE_ROOT, silver1_root=SILVER1_ROOT) -> str:
    """검증을 통과한 lookup과 Shapefile 원본을 Silver1에 복사한다."""

    lookup_path = bronze_root / "lookup" / "taxi_zone_lookup.parquet"
    shapefile_dir = bronze_root / "shapefile"
    validate_taxi_zone_lookup(lookup_path)
    validate_taxi_zone_shapefile(shapefile_dir)

    silver1_root.mkdir(parents=True, exist_ok=True)
    silver_lookup = silver1_root / "taxi_zone_lookup.parquet"
    silver_shapes = silver1_root / "shapefile"

    if isinstance(lookup_path, Path):
        shutil.copy2(lookup_path, silver_lookup)
        shutil.copytree(shapefile_dir, silver_shapes, dirs_exist_ok=True)
    else:
        lookup_path.copy(silver_lookup)
        shapefile_dir.copytree(silver_shapes)

    expected_shape = silver_shapes / "taxi_zones" / "taxi_zones.shp"
    if not silver_lookup.exists() or not expected_shape.exists():
        raise RuntimeError(f"Taxi Zone Silver1 저장 검증 실패: {silver1_root}")

    logger.info("Taxi Zone Silver1 저장 완료: %s", silver1_root)
    return str(silver1_root)
