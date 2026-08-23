"""Taxi Zone Bronze(Shapefile)를 검증해 S3 Silver1 참조 데이터로 승격한다."""

from __future__ import annotations

import re
import shutil
import subprocess
import tempfile
from pathlib import Path

from airflow.exceptions import AirflowSkipException

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


def build(shapefile_result: dict, bronze_root=BRONZE_ROOT, silver1_root=SILVER1_ROOT) -> str:
    """검증을 통과한 Shapefile 원본을 Silver1에 복사한다.

    shapefile_result는 ingest_taxi_zone_shapefile의 반환값(XCom)이다.
    원본이 안 바뀌었으면(changed=False) Silver1을 다시 만들 필요도,
    Asset("taxi_zone_silver1_updated")를 emit할 필요도 없다 — 스킵해서
    zone_segment_pipeline이 매달 헛돌지 않게 한다.
    """

    if not shapefile_result.get("changed", True):
        raise AirflowSkipException(
            "Taxi Zone 원본이 안 바뀌어 Silver1 재생성을 건너뜁니다"
        )

    shapefile_dir = bronze_root / "shapefile"
    validate_taxi_zone_shapefile(shapefile_dir)

    silver1_root.mkdir(parents=True, exist_ok=True)
    silver_shapes = silver1_root / "shapefile"

    if isinstance(shapefile_dir, Path):
        shutil.copytree(shapefile_dir, silver_shapes, dirs_exist_ok=True)
    else:
        shapefile_dir.copytree(silver_shapes)

    expected_shape = silver_shapes / "taxi_zones" / "taxi_zones.shp"
    if not expected_shape.exists():
        raise RuntimeError(f"Taxi Zone Silver1 저장 검증 실패: {silver1_root}")

    logger.info("Taxi Zone Silver1 저장 완료: %s", silver1_root)
    return str(silver1_root)
