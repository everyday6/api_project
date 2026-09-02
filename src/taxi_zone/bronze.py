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


def _read_previous_etag(bronze_root: Path) -> str | None:
    """직전에 Silver1까지 성공적으로 반영된 ETag를 읽는다.

    _metadata.txt(Bronze 저장 직후 정보성으로 남기는 파일)가 아니라
    bronze_root 바로 아래의 별도 마커 파일(_latest_etag.txt)에서 읽는다 -
    이 마커는 mark_taxi_zone_etag()가 Silver1 build까지 성공한 뒤에만
    쓴다(src/lion/bronze.py와 동일한 패턴, 2026-08-26 수정). Bronze 저장
    직후에 바로 마커를 갱신하면, 그 뒤 Silver1이 실패해도 마커는 이미 새
    버전을 가리켜 다음 스케줄 실행이 "원본 그대로"로 보고 재시도를
    영원히 건너뛰는 사고가 난다. 마커가 없거나(첫 실행) _etag 줄이
    없으면 None을 반환하고, 호출부는 이를 "무조건 새로 받아야 함"으로
    취급한다."""

    marker_path = bronze_root / "_latest_etag.txt"
    if not marker_path.exists():
        return None

    for line in marker_path.read_text().splitlines():
        if line.startswith("_etag="):
            return line.split("=", 1)[1].strip()
    return None


def _write_latest_etag(bronze_root: Path, etag: str) -> None:
    (bronze_root / "_latest_etag.txt").write_text(
        f"_etag={etag}\n_updated_at={datetime.now(timezone.utc).isoformat()}\n"
    )


def ingest_taxi_zone_shapefile(version_date: str | None = None, bronze_root=BRONZE_ROOT) -> dict:
    """Taxi Zone Shapefile ZIP을 받아 원본 묶음 그대로 저장한다.

    Wayback Machine 스냅샷으로 실측한 결과 원본이 바뀌는 주기는 1~2년에
    한 번 수준이다(2024-03~2024-10 사이엔 변경 없었고, 다음 변경은
    2026-02). 그렇다고 확인 자체를 안 할 순 없어서, 다운로드 전에 HEAD로
    ETag만 먼저 확인해 이전과 같으면 재다운로드를 건너뛴다. 반환값의
    "changed"는 build_taxi_zone_silver1이 실제로 바뀐 경우에만 Silver1
    재생성 + Asset emit을 하도록 판단하는 데 쓰인다(원본이 그대로인데도
    매번 downstream인 zone_segment_pipeline까지 깨우는 걸 막기 위함).

    변경이 감지되면 `version_date=<YYYY-MM-DD>/shapefile/`이라는 새 파티션에
    저장한다 - 예전엔 `shapefile/` 고정 경로를 in-place로 덮어써서 이전
    경계 스냅샷이 사라졌다(src/lion/bronze.py와 같은 파티션 스킴으로 통일,
    RELIABILITY_PRINCIPLES.md Tier 2 #7 참고). ETag 가드가 진짜 변경일 때만
    새 파티션을 만드므로 스냅샷이 무한정 쌓이지 않는다.
    """

    if version_date is None:
        version_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    head_response = requests.head(SHAPEFILE_URL, timeout=30)
    head_response.raise_for_status()
    current_etag = head_response.headers.get("ETag", "").strip('"')

    destination = bronze_root / f"version_date={version_date}" / "shapefile"
    previous_etag = _read_previous_etag(bronze_root)

    if current_etag and current_etag == previous_etag:
        logger.info(
            "Taxi Zone Shapefile 변경 없음(ETag %s 동일) — 다운로드 스킵", current_etag
        )
        return {"path": None, "changed": False, "etag": current_etag}

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

        # _metadata.txt는 이번 Bronze 저장이 언제/무엇으로 이뤄졌는지
        # 남기는 정보성 기록일 뿐이다(파티션 루트에 둔다) - 재처리 여부를
        # 가르는 ETag 마커는 더 이상 이 파일이 아니라 mark_taxi_zone_etag()가
        # Silver1 build 성공 후에 별도로 쓴다(아래 함수 docstring 참고).
        (destination.parent / "_metadata.txt").write_text(
            f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
            "_source=nyc_tlc_taxi_zone_shapefile\n"
            f"_etag={current_etag}\n"
        )

    logger.info("Taxi Zone Shapefile 저장 완료: %s", destination)
    return {"path": str(destination), "changed": True, "etag": current_etag}


def mark_taxi_zone_etag(shapefile_result: dict, bronze_root=BRONZE_ROOT) -> None:
    """ETag 마커(_latest_etag.txt)를 이제야 갱신한다 - Airflow DAG에서
    Silver1 build_taxi_zone_silver1까지 성공했을 때만(그 태스크 뒤에 이어서)
    호출돼야 한다. ingest_taxi_zone_shapefile() 자체가 이 마커를 쓰지 않는
    이유는 그 함수 docstring 참고(src/lion/bronze.py의 mark_lion_etag와
    동일한 패턴).

    changed=False(원본 그대로라 이번 실행이 처음부터 스킵된 경우)면
    build_taxi_zone_silver1이 AirflowSkipException을 던져서 이 태스크까지
    기본 trigger_rule(all_success)로 자동 스킵된다 - 마커가 이미 맞는
    값을 가리키고 있으니 다시 쓸 필요가 없어 별문제 없다."""
    etag = shapefile_result.get("etag")
    if etag:
        _write_latest_etag(bronze_root, etag)
