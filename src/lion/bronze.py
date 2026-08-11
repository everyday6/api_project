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
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion")

# NYC Open Data 데이터셋 페이지(data.cityofnewyork.us/City-Government/LION/2v4z-66xt)에서
# 직접 확인한 실제 다운로드 링크.
LION_ZIP_URL = "https://data.cityofnewyork.us/download/2v4z-66xt/application%2Fzip"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

from src.common.config import BRONZE_DIR
BRONZE_ROOT = BRONZE_DIR / "lion"

def ingest_lion(version_date: str | None = None, bronze_root: Path = BRONZE_ROOT) -> Path:
    """
    version_date: 'YYYY-MM-DD' 형식. 안 주면 오늘 날짜로 자동 태깅.
    (Airflow에서는 '{{ ds }}'를 그대로 넘기면 됨)
    """

    if version_date is None:
        version_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    resp = requests.get(LION_ZIP_URL, headers=HEADERS, timeout=180)
    resp.raise_for_status()

    dest_dir = bronze_root / f"version_date={version_date}"
    dest_dir.mkdir(parents=True, exist_ok=True)

    with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
        z.extractall(dest_dir)

    marker_path = dest_dir / "_metadata.txt"
    marker_path.write_text(
        f"_ingested_at={datetime.now(timezone.utc).isoformat()}\n"
        f"_source=nyc_dcp_lion\n"
        f"_source_url={LION_ZIP_URL}\n"
    )

    logger.info(f"[lion] version_date={version_date} 압축 해제 완료 -> {dest_dir}")
    return str(dest_dir)


if __name__ == "__main__":
    ingest_lion()