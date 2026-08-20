"""
Bronze ingestion: NYC TLC Taxi Zone

두 가지 소스를 받아온다:
1. Taxi Zone Lookup Table (CSV) — LocationID, Borough, Zone, service_zone
2. Taxi Zone Shapefile (ZIP) — 존별 경계 폴리곤 (지도 시각화, 공간 join용)

정적 참조 테이블이라 날짜 파라미터가 없고, 파티션도 나누지 않는다.
직접 실행(python taxi_zone.py)도 가능하고, Airflow PythonOperator가
ingest_taxi_zone_lookup / ingest_taxi_zone_shapefile 함수를 그대로 가져다 쓸 수도 있다.

필수컬럼/유니크/row-count 범위 같은 무거운 검증은 src/taxi_zone/silver1.py로
옮겼다 — Bronze는 파일이 실제로 받아졌는지(존재 여부)만 확인한다.
"""

from __future__ import annotations

import io
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone")

LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

BRONZE_ROOT = BRONZE_DIR / "taxi_zone"


def ingest_taxi_zone_lookup(bronze_root: Path = BRONZE_ROOT) -> Path:
    """LocationID <-> Borough/Zone 매핑 테이블을 받아서 Parquet로 저장한다."""

    logger.info(f"[taxi_zone_lookup] 다운로드 시작: {LOOKUP_URL}")

    try:
        resp = requests.get(LOOKUP_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        logger.exception(f"[taxi_zone_lookup] 다운로드 실패: {LOOKUP_URL}")
        raise

    df = pd.read_csv(io.BytesIO(resp.content))

    # 메타데이터 컬럼 추가 (원본 컬럼은 그대로 유지)
    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "nyc_tlc_taxi_zone_lookup"

    dest_dir = bronze_root / "lookup"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "taxi_zone_lookup.parquet"

    df.to_parquet(dest_path, index=False)
    logger.info(f"[taxi_zone_lookup] {len(df)}행 저장 완료 -> {dest_path}")
    return str(dest_path)


def ingest_taxi_zone_shapefile(bronze_root: Path = BRONZE_ROOT) -> Path:
    """존 경계 shapefile(zip)을 받아서 그대로 압축 해제해 저장한다."""

    logger.info(f"[taxi_zone_shapefile] 다운로드 시작: {SHAPEFILE_URL}")

    try:
        resp = requests.get(SHAPEFILE_URL, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        logger.exception(f"[taxi_zone_shapefile] 다운로드 실패: {SHAPEFILE_URL}")
        raise

    dest_dir = bronze_root / "shapefile"
    dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(dest_dir)

    # 메타데이터는 별도 마커 파일로 남김 (shapefile 자체는 원본 그대로 보존)
    marker_path = dest_dir / "_metadata.txt"
    marker_path.write_text(
        f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
        f"_source=nyc_tlc_taxi_zone_shapefile\n"
    )

    logger.info(f"[taxi_zone_shapefile] 압축 해제 완료 -> {dest_dir}")
    return str(dest_dir)


def validate_bronze_output(lookup_path: Path, shapefile_dir: Path) -> None:
    """Bronze가 실제로 받아졌는지(존재 여부)만 확인한다 — 내용 검증은 Silver1의 몫."""
    if not lookup_path.exists():
        raise FileNotFoundError(f"taxi_zone lookup bronze 파일이 없습니다: {lookup_path}")
    if not shapefile_dir.exists() or not any(shapefile_dir.iterdir()):
        raise FileNotFoundError(f"taxi_zone shapefile bronze가 없습니다: {shapefile_dir}")


if __name__ == "__main__":
    ingest_taxi_zone_lookup()
    ingest_taxi_zone_shapefile()
