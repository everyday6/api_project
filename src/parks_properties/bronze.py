"""
Bronze ingestion: NYC Parks Properties

Socrata dataset ID: enfh-gkve
https://data.cityofnewyork.us/Recreation/Parks-Properties/enfh-gkve/about_data

NYC Parks가 관리하는 부지(공원) 경계 폴리곤 + 속성 정보. 전체 2,059행 규모라
페이지네이션은 사실상 1페이지로 끝나지만, road_closures.py와 동일하게
$limit/$offset 방식을 그대로 둔다 (행 수가 늘어나도 안전).

"땅이 새로 편입/편출될 때만 레코드가 추가/변경"되는 완만한 참조 데이터라,
road_closures처럼 날짜 구간으로 증분받지 않고 lion.py와 동일하게
매번 전체를 통째로 받아 version_date로만 구분한다.

multipolygon 필드는 원본이 중첩 dict(GeoJSON)라서 그대로 parquet에 넣으면
프로젝트의 다른 지오메트리 컬럼(the_geom, WKT)과 타입이 달라진다 — 문자열(JSON)로
직렬화해서 저장한다. 좌표계는 WGS84 위경도(다른 소스처럼 State Plane 변환 불필요).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="parks_properties")

DATASET_ID = "enfh-gkve"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"
PAGE_SIZE = 50_000

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

BRONZE_ROOT = BRONZE_DIR / "parks_properties"


def _fetch_all_pages(where_clause: str | None = None, select: str | None = None) -> list[dict]:
    """Socrata는 한 번에 최대 몇만 건만 주기 때문에 offset을 늘려가며 다 받는다."""
    records: list[dict] = []
    offset = 0

    while True:
        params = {"$limit": PAGE_SIZE, "$offset": offset}
        if where_clause:
            params["$where"] = where_clause
        if select:
            params["$select"] = select

        resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=60)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break

        records.extend(page)
        offset += PAGE_SIZE

        if len(page) < PAGE_SIZE:
            break

    return records


def ingest_parks_properties(version_date: str | None = None, bronze_root: Path = BRONZE_ROOT) -> Path:
    """
    version_date: 'YYYY-MM-DD' 형식. 안 주면 오늘 날짜로 자동 태깅.
    (Airflow에서는 '{{ ds }}'를 그대로 넘기면 됨)

    증분 없이 매번 전체를 받는다 — 편입/편출이 드물고 총량도 2천여 건뿐이라
    매번 전체를 다시 받는 비용이 날짜 구간 필터링의 복잡도보다 훨씬 싸다.
    """

    if version_date is None:
        version_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    records = _fetch_all_pages()
    df = pd.DataFrame.from_records(records)

    # GeoJSON(dict)을 그대로 parquet에 넣으면 다른 소스의 문자열 지오메트리(the_geom, WKT)와
    # 타입이 달라지므로, 동일하게 문자열로 직렬화해서 저장한다.
    df["multipolygon"] = df["multipolygon"].apply(lambda g: json.dumps(g) if g is not None else None)

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "nyc_parks_properties"

    dest_dir = bronze_root / f"version_date={version_date}"
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / "parks_properties.parquet"

    df.to_parquet(dest_path, index=False)
    logger.info(f"[parks_properties] version_date={version_date} {len(df)}행 저장 -> {dest_path}")
    return str(dest_path)


if __name__ == "__main__":
    ingest_parks_properties()
