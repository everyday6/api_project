"""
Bronze ingestion: NYC TLC Taxi Zone

두 가지 소스를 받아온다:
1. Taxi Zone Lookup Table (CSV) — LocationID, Borough, Zone, service_zone
2. Taxi Zone Shapefile (ZIP) — 존별 경계 폴리곤 (지도 시각화, 공간 join용)

정적 참조 테이블이라 날짜 파라미터가 없고, 파티션도 나누지 않는다.
직접 실행(python taxi_zone.py)도 가능하고, Airflow PythonOperator가
ingest_taxi_zone_lookup / ingest_taxi_zone_shapefile 함수를 그대로 가져다 쓸 수도 있다.
"""

from __future__ import annotations

import io
import re
import subprocess
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone")

LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SHAPEFILE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zones.zip"

from src.common.config import BRONZE_DIR
BRONZE_ROOT = BRONZE_DIR / "taxi_zone"

def ingest_taxi_zone_lookup(bronze_root: Path = BRONZE_ROOT) -> Path:
    """LocationID <-> Borough/Zone 매핑 테이블을 받아서 Parquet로 저장한다."""

    resp = requests.get(LOOKUP_URL, timeout=30)
    resp.raise_for_status()

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

    resp = requests.get(SHAPEFILE_URL, timeout=30)
    resp.raise_for_status()

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


def validate_taxi_zone_lookup(path: str) -> str:
    """taxi_zone_lookup.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)

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
        raise RuntimeError(f"shapefile을 열 수 없습니다: {result.stderr}")

    match = re.search(r"Feature Count:\s*(\d+)", result.stdout)
    feature_count = int(match.group(1)) if match else 0
    if not (250 <= feature_count <= 280):
        raise ValueError(f"zone 폴리곤 개수가 예상 범위(250~280) 밖입니다: {feature_count}")

    logger.info(f"[taxi_zone_shapefile] 검증 통과: {feature_count}개 zone")
    return path


def get_manhattan_zone_ids(lookup_path: Path = BRONZE_ROOT / "lookup" / "taxi_zone_lookup.parquet") -> list[int]:
    """
    Bronze에 저장된 lookup 테이블에서 Borough == 'Manhattan'인 LocationID만 뽑는다.

    지도에서 눈으로 세면 육지에서 떨어진 103(Governor's Island 등),
    104(Marble Hill), 105(Roosevelt Island) 같은 존을 놓치기 쉬운데,
    행정구역 분류는 Borough 컬럼 기준이 정확하다.
    """
    df = pd.read_parquet(lookup_path)
    manhattan_ids = sorted(df.loc[df["Borough"] == "Manhattan", "LocationID"].tolist())
    logger.info(f"[manhattan_zone_ids] 맨해튼 zone {len(manhattan_ids)}개 확인")
    return manhattan_ids


if __name__ == "__main__":
    ingest_taxi_zone_lookup()
    ingest_taxi_zone_shapefile()

    manhattan_ids = get_manhattan_zone_ids()
    logger.info(manhattan_ids)