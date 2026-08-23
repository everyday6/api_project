"""NYC TLC Taxi Zone Shapefile 원본을 S3 Bronze에 저장한다.

lookup CSV(taxi_zone_lookup.csv)는 예전엔 같이 받았지만, shapefile의 속성
테이블에 이미 LocationID/zone(이름)/borough가 다 들어있고 lookup에만 있는
service_zone 컬럼은 아무 데서도 안 쓰여서 제거했다 — 순수 중복 수집이었다.
"""

from __future__ import annotations

import io
import shutil
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.common.config import BRONZE_DIR, TMP_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="taxi_zone")

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


def _read_previous_etag(destination) -> str | None:
    """이전 Bronze 적재 때 남겨둔 ETag를 _metadata.txt에서 읽는다.

    파일이 없거나(첫 실행) _etag 줄이 없으면(과거 버전 메타데이터) None을
    반환한다 — 호출부는 None을 "무조건 새로 받아야 함"으로 취급한다.
    """

    metadata_path = destination / "_metadata.txt"
    if not metadata_path.exists():
        return None

    for line in metadata_path.read_text().splitlines():
        if line.startswith("_etag="):
            return line.split("=", 1)[1].strip()
    return None


def ingest_taxi_zone_shapefile(bronze_root=BRONZE_ROOT) -> dict:
    """Taxi Zone Shapefile ZIP을 받아 원본 묶음 그대로 저장한다.

    Wayback Machine 스냅샷으로 실측한 결과 원본이 바뀌는 주기는 1~2년에
    한 번 수준이다(2024-03~2024-10 사이엔 변경 없었고, 다음 변경은
    2026-02). 그렇다고 확인 자체를 안 할 순 없어서, 다운로드 전에 HEAD로
    ETag만 먼저 확인해 이전과 같으면 재다운로드를 건너뛴다. 반환값의
    "changed"는 build_taxi_zone_silver1이 실제로 바뀐 경우에만 Silver1
    재생성 + Asset emit을 하도록 판단하는 데 쓰인다(원본이 그대로인데도
    매번 downstream인 zone_segment_pipeline까지 깨우는 걸 막기 위함).
    """

    head_response = requests.head(SHAPEFILE_URL, timeout=30)
    head_response.raise_for_status()
    current_etag = head_response.headers.get("ETag", "").strip('"')

    destination = bronze_root / "shapefile"
    previous_etag = _read_previous_etag(destination)

    if current_etag and current_etag == previous_etag:
        logger.info(
            "Taxi Zone Shapefile 변경 없음(ETag %s 동일) — 다운로드 스킵: %s",
            current_etag,
            destination,
        )
        return {"path": str(destination), "changed": False}

    if previous_etag:
        logger.info(
            "Taxi Zone Shapefile 원본 변경 감지(ETag %s -> %s) — 재다운로드",
            previous_etag,
            current_etag,
        )

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

        _upload_tree(extract_dir, destination)

        remote_shapefile = destination / "taxi_zones" / "taxi_zones.shp"
        if not remote_shapefile.exists():
            raise RuntimeError(f"Taxi Zone Shapefile 저장 검증 실패: {remote_shapefile}")

        (destination / "_metadata.txt").write_text(
            f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
            "_source=nyc_tlc_taxi_zone_shapefile\n"
            f"_etag={current_etag}\n"
        )

    logger.info("Taxi Zone Shapefile 저장 완료: %s", destination)
    return {"path": str(destination), "changed": True}
