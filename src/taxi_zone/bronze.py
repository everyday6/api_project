"""NYC TLC Taxi Zone lookup과 Shapefile 원본을 S3 Bronze에 저장한다."""

from __future__ import annotations

import io
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.config import BRONZE_DIR, TMP_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone")

LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"
BRONZE_ROOT = BRONZE_DIR / "taxi_zone"


def _upload_tree(local_root: Path, destination) -> None:
    """로컬 디렉터리 트리를 로컬/S3 목적지에 복사한다."""

    if isinstance(destination, Path):
        shutil.copytree(local_root, destination, dirs_exist_ok=True)
        return

    destination.mkdir(parents=True, exist_ok=True)
    for local_path in local_root.rglob("*"):
        if local_path.is_file():
            relative_path = local_path.relative_to(local_root)
            (destination / relative_path.as_posix()).upload_from(local_path)


def ingest_taxi_zone_lookup(bronze_root=BRONZE_ROOT) -> str:
    """LocationID와 Borough/Zone 이름 원본을 Parquet으로 저장한다."""

    response = requests.get(LOOKUP_URL, timeout=30)
    response.raise_for_status()

    frame = pd.read_csv(io.BytesIO(response.content))
    frame["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    frame["_source"] = "nyc_tlc_taxi_zone_lookup"

    destination = bronze_root / "lookup" / "taxi_zone_lookup.parquet"
    destination.parent.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(str(destination), index=False)

    if not destination.exists():
        raise RuntimeError(f"Taxi Zone lookup 저장 검증 실패: {destination}")

    logger.info("Taxi Zone lookup %s행 저장 완료: %s", len(frame), destination)
    return str(destination)


def ingest_taxi_zone_shapefile(bronze_root=BRONZE_ROOT) -> str:
    """Taxi Zone Shapefile ZIP을 받아 원본 묶음 그대로 저장한다."""

    response = requests.get(SHAPEFILE_URL, timeout=30)
    response.raise_for_status()

    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="taxi_zone_bronze_", dir=TMP_DIR) as tmp:
        extract_dir = Path(tmp) / "extracted"
        extract_dir.mkdir()

        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            archive.extractall(extract_dir)

        local_shapefile = extract_dir / "taxi_zones" / "taxi_zones.shp"
        if not local_shapefile.exists():
            raise FileNotFoundError(
                f"Taxi Zone ZIP 안에 taxi_zones.shp가 없습니다: {local_shapefile}"
            )

        destination = bronze_root / "shapefile"
        _upload_tree(extract_dir, destination)

        remote_shapefile = destination / "taxi_zones" / "taxi_zones.shp"
        if not remote_shapefile.exists():
            raise RuntimeError(f"Taxi Zone Shapefile 저장 검증 실패: {remote_shapefile}")

        (destination / "_metadata.txt").write_text(
            f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
            "_source=nyc_tlc_taxi_zone_shapefile\n"
        )

    logger.info("Taxi Zone Shapefile 저장 완료: %s", destination)
    return str(destination)
