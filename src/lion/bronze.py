"""
Bronze ingestion: NYC DCP LION (Single Line Street Base Map)

주의: LION은 Socrata에서 "non-tabular"(지도 전용) 자산으로 등록되어 있어서
$limit/$offset 같은 행 단위 API 조회가 불가능하다 (실제로 시도하면
"no row or column access to non-tabular tables" 에러가 남).

그래서 taxi_zone의 shapefile과 동일한 방식으로, NYC DCP가 제공하는
파일(zip) 원본을 통째로 받아서 그대로 압축 해제한다.
분기마다 새 버전이 나오는 전체 스냅샷 데이터라, 증분 개념 없이
매번 전체를 받고 version_date로만 구분한다.
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

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion")

# NYC Open Data 데이터셋 페이지(data.cityofnewyork.us/City-Government/LION/2v4z-66xt)에서
# 직접 확인한 실제 다운로드 링크.
LION_ZIP_URL = "https://data.cityofnewyork.us/download/2v4z-66xt/application%2Fzip"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

BRONZE_ROOT = BRONZE_DIR / "lion"


def _contains_gdb_file(root) -> bool:
    """root 아래에 실제 내용이 들어 있는 File Geodatabase가 있는지 확인한다."""

    return any(
        path.is_file() and ".gdb/" in str(path).replace("\\", "/").lower()
        for path in root.rglob("*")
    )


def _upload_tree(local_root: Path, destination) -> None:
    """로컬 디렉터리의 파일을 로컬/S3 목적지에 재귀적으로 복사한다."""

    if isinstance(destination, Path):
        shutil.copytree(local_root, destination, dirs_exist_ok=True)
        return

    destination.mkdir(parents=True, exist_ok=True)
    for local_path in local_root.rglob("*"):
        if not local_path.is_file():
            continue
        relative_path = local_path.relative_to(local_root)
        (destination / relative_path.as_posix()).upload_from(local_path)


def ingest_lion(version_date: str | None = None, bronze_root=BRONZE_ROOT) -> str:
    """
    version_date: 'YYYY-MM-DD' 형식. 안 주면 오늘 날짜로 자동 태깅.
    (Airflow에서는 '{{ ds }}'를 그대로 넘기면 됨)
    """

    if version_date is None:
        version_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(f"[lion] version_date={version_date} 다운로드 시작: {LION_ZIP_URL}")

    try:
        resp = requests.get(LION_ZIP_URL, headers=HEADERS, timeout=180)
        resp.raise_for_status()
    except requests.RequestException:
        logger.exception(f"[lion] version_date={version_date} 다운로드 실패: {LION_ZIP_URL}")
        raise

    # zipfile은 S3Path에 직접 압축을 풀 수 없다. 로컬 스크래치 공간에 먼저
    # 압축을 푼 다음 완성된 파일만 Bronze(S3)에 올린다.
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lion_bronze_", dir=TMP_DIR) as tmp:
        extract_dir = Path(tmp) / "extracted"
        extract_dir.mkdir()

        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            z.extractall(extract_dir)

        if not _contains_gdb_file(extract_dir):
            raise RuntimeError("LION ZIP 안에 유효한 .gdb 파일이 없습니다")

        dest_dir = bronze_root / f"version_date={version_date}"
        _upload_tree(extract_dir, dest_dir)

        # 업로드가 일부만 됐는데 성공 로그가 찍히는 false success를 막는다.
        if not _contains_gdb_file(dest_dir):
            raise RuntimeError(f"LION .gdb S3 업로드 검증 실패: {dest_dir}")

        marker_path = dest_dir / "_metadata.txt"
        marker_path.write_text(
            f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
            f"_source=nyc_dcp_lion\n"
            f"_source_url={LION_ZIP_URL}\n"
        )

        if not marker_path.exists():
            raise RuntimeError(f"LION 메타데이터 업로드 검증 실패: {marker_path}")

    logger.info(f"[lion] version_date={version_date} 압축 해제 완료 -> {dest_dir}")
    return str(dest_dir)


if __name__ == "__main__":
    ingest_lion()
