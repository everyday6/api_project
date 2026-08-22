"""
Bronze — 통행료 요금표/시설목록/CBD Geofence 폴리곤

세 가지 다 자동 수집이 아니라 사람이 관리하는 참조 데이터다(공식 요금
API가 없다 — docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md
참고). 이 파일은 로컬 config/*.yaml 파일과 CBD 폴리곤을 그대로
Bronze(S3 또는 로컬)에 올리는 역할만 한다 — 변환/파싱 없음(Bronze 원칙).

CBD Geofence는 data.ny.gov(Socrata)의 "MTA Central Business District
Geofence: Beginning June 2024" 데이터셋(srxy-5nxn)에서 받는다. 비슷한
이름의 vaq5-qfkz 데이터셋은 geometry가 비어있는 잘못된 데이터셋이라
혼동하지 말 것(curl로 실제 응답 내용 확인해서 검증함).
"""

from __future__ import annotations

import shutil
from pathlib import Path

import requests

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="toll_bronze")

SOURCE = "toll"
BRONZE_ROOT = BRONZE_DIR / SOURCE

CBD_GEOFENCE_URL = "https://data.ny.gov/resource/srxy-5nxn.geojson"


def _copy_file_to_bronze(source_path: str, out_path) -> None:
    """로컬 목적지는 파일 복사, S3 목적지는 객체 업로드로 저장한다."""

    source = Path(source_path)
    if isinstance(out_path, Path):
        shutil.copyfile(source, out_path)
    else:
        out_path.upload_from(source)


def upload_rates(source_path: str = "config/toll_rates.yaml", bronze_root: Path = BRONZE_ROOT) -> Path:
    """toll_rates.yaml을 그대로 Bronze에 올린다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "toll_rates.yaml"
    _copy_file_to_bronze(source_path, out_path)

    logger.info(f"[toll_bronze] 요금표 업로드 완료 -> {out_path}")
    return out_path


def upload_facilities(source_path: str = "config/toll_facilities.yaml", bronze_root: Path = BRONZE_ROOT) -> Path:
    """toll_facilities.yaml을 그대로 Bronze에 올린다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "toll_facilities.yaml"
    _copy_file_to_bronze(source_path, out_path)

    logger.info(f"[toll_bronze] 시설목록 업로드 완료 -> {out_path}")
    return out_path


def upload_cbd_geofence(url: str = CBD_GEOFENCE_URL, bronze_root: Path = BRONZE_ROOT) -> Path:
    """MTA CBD Geofence GeoJSON을 받아서 그대로 Bronze에 저장한다."""

    bronze_root.mkdir(parents=True, exist_ok=True)
    out_path = bronze_root / "cbd_geofence.geojson"

    logger.info(f"[toll_bronze] CBD geofence 다운로드 시작: {url}")
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    out_path.write_bytes(resp.content)

    logger.info(f"[toll_bronze] CBD geofence 업로드 완료 -> {out_path}")
    return out_path


def main() -> None:
    upload_rates()
    upload_facilities()
    upload_cbd_geofence()


if __name__ == "__main__":
    main()
