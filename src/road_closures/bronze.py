"""
Bronze ingestion: NYC DOT Street Closures due to Construction Activities
(by Block AND Intersection 통합 버전)

Socrata dataset ID: ezy6-djsf

실제 확인된 필드 9개(Socrata 응답은 소문자로 내려오지만 SoQL $where는
대소문자 구분 안 함 — 실제로 둘 다 테스트해서 확인함):
  OnStreetName, FromStreetName, ToStreetName, BoroughName,
  WorkStartDate, WorkEndDate, Purpose, OFTCode, WKT

예전에는 주 단위 증분(week_start= 파티션)으로 받았는데, 두 가지 이유로
"매번 BACKFILL_START~end_date 전체를 통째로 다시 받아서 파일 하나로 저장"
방식으로 바꿨다:
1. 이 데이터셋은 전체를 받아도 1.5년치 기준 수만 행 수준이라 굳이 증분/파티션이
   필요 없다 (LION처럼 매번 전체 스냅샷 방식이 더 단순하고 견고함).
2. 증분 방식은 Airflow DAG를 수동 트리거하면 data_interval_start/end가 둘 다
   "트리거 시각"으로 찌그러져서 [오늘, 오늘) 같은 빈 구간이 되는 버그가 있었다
   (실제로 84개 주간 파티션이 이 문제로 전부 0행이 됨). 고정된 BACKFILL_START ~
   end_date 방식은 end_date가 뭐가 되든(수동/스케줄 무관) 항상 유효한 구간이라
   이 문제 자체가 발생하지 않는다.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd
import requests

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger
from src.common.socrata import fetch_all

logger = get_logger(__name__, log_to_file=True, log_file_stem="road_closures")

DATASET_ID = "ezy6-djsf"
BASE_URL = f"https://data.cityofnewyork.us/resource/{DATASET_ID}.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"
}

BRONZE_ROOT = BRONZE_DIR / "road_closures"

# 예전 backfill_road_closures.py가 쓰던 시작일과 동일 — 이 데이터셋 실제
# 최초 기록일보다 한참 뒤지만(check_earliest_work_start_date로 확인 가능),
# 프로젝트에서 필요한 범위가 2025년 이후라 이 값을 그대로 유지한다.
BACKFILL_START = date(2025, 1, 1)


def check_earliest_work_start_date() -> str | None:
    """
    진단용 함수. 이 데이터셋에 실제로 WorkStartDate가 언제부터 있는지 확인한다.
    """
    params = {
        "$select": "WorkStartDate",
        "$order": "WorkStartDate ASC",
        "$limit": 1,
    }
    try:
        resp = requests.get(BASE_URL, params=params, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException:
        logger.exception("[check_earliest] 조회 실패")
        raise

    rows = resp.json()

    if not rows:
        logger.warning("[check_earliest] 데이터가 아예 없음")
        return None

    earliest = rows[0].get("WorkStartDate") or rows[0].get("workstartdate")
    logger.info(f"[check_earliest] 가장 오래된 WorkStartDate: {earliest}")
    return earliest


def latest_bronze_file(bronze_root: Path = BRONZE_ROOT) -> Path | None:
    """가장 최근에 받은 road_closures_<date>.parquet 스냅샷을 찾는다. 없으면 None."""
    files = sorted(bronze_root.glob("road_closures_*.parquet"))
    return files[-1] if files else None


def ingest_road_closures(end_date: str | None = None, bronze_root: Path = BRONZE_ROOT) -> str:
    """
    BACKFILL_START(2025-01-01) ~ end_date(기본값: 오늘) 전체를 매번 통째로 받아
    road_closures_<end_date>.parquet 하나로 저장한다. (더 이상 주 단위로 안 나눔)
    """
    if end_date is None:
        end_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    where_clause = (
        f"WorkStartDate >= '{BACKFILL_START}T00:00:00' "
        f"AND WorkStartDate < '{end_date}T00:00:00'"
    )

    # 정렬 기준 컬럼이 마땅치 않아(고유 permitnumber 같은 게 없음) Socrata
    # 내부 행 식별자인 :id 하나만으로 keyset 페이지네이션한다 — 유일성만
    # 보장되면 되고 순서 자체엔 의미가 없어서 이걸로 충분하다.
    records = fetch_all(BASE_URL, where=where_clause, order=":id")
    df = pd.DataFrame.from_records(records)

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "nyc_dot_street_closures_by_block_and_intersection"

    bronze_root.mkdir(parents=True, exist_ok=True)
    dest_path = bronze_root / f"road_closures_{end_date}.parquet"

    df.to_parquet(str(dest_path), index=False)
    logger.info(f"[road_closures] {BACKFILL_START}~{end_date} 구간 {len(df)}행 저장 -> {dest_path}")
    return str(dest_path)


# 새 스냅샷 행 수가 직전 스냅샷 대비 이 비율 밑으로 떨어지면 validate에서 실패시킨다.
# 오늘 실제로 겪은 사고(84개 주간 파티션이 Socrata 일시 오류로 전부 0행이 됐는데
# 아무 검증 없이 그대로 저장됨)를 막기 위한 안전장치.
MIN_RETENTION_RATIO = 0.5


def validate_road_closures(path: str, bronze_root: Path = BRONZE_ROOT) -> str:
    """
    road_closures_<date>.parquet의 최소 불변식을 확인한다.
    - 필수 컬럼이 있고 대부분 비어있지 않은지
    - boroughname이 NYC 5개 자치구 안에 있는지
    - 직전 스냅샷 대비 행 수가 급감하지 않았는지 (fetch 실패를 조용히 덮어쓰는 것 방지)
    """
    df = pd.read_parquet(str(path))

    required_cols = {"onstreetname", "workstartdate", "workenddate", "boroughname"}
    missing = required_cols - set(df.columns)
    assert not missing, f"필수 컬럼 없음: {missing} (컬럼명 대소문자 등 API 응답 형식이 바뀌었을 수 있음)"

    for col in required_cols:
        null_ratio = df[col].isna().mean()
        assert null_ratio < 0.05, f"{col} 결측 비율이 비정상적으로 높음: {null_ratio:.1%}"

    valid_boroughs = {"MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"}
    bad_boroughs = set(df["boroughname"].str.upper().unique()) - valid_boroughs
    assert not bad_boroughs, f"알 수 없는 boroughname 값: {bad_boroughs}"

    # 직전 스냅샷과 비교 — 이번 파일 자신은 이미 디스크에 있으므로 그 이전 것과 비교한다.
    previous_snapshots = sorted(bronze_root.glob("road_closures_*.parquet"))
    previous_snapshots = [p for p in previous_snapshots if str(p) != str(Path(path))]
    if previous_snapshots:
        prev_path = previous_snapshots[-1]
        prev_n = len(pd.read_parquet(str(prev_path), columns=["onstreetname"]))
        n = len(df)
        if prev_n > 0:
            ratio = n / prev_n
            assert ratio >= MIN_RETENTION_RATIO, (
                f"행 수가 직전 스냅샷({prev_path.name}, {prev_n}행) 대비 {ratio:.1%}로 급감했습니다 "
                f"(이번: {n}행) — fetch가 일부만 되고 조용히 저장됐을 가능성이 큽니다."
            )

    logger.info(f"[road_closures] 검증 통과 ({len(df)}행) -> {path}")
    return path


if __name__ == "__main__":
    check_earliest_work_start_date()
    out = ingest_road_closures()
    validate_road_closures(out)
