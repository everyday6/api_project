"""
Silver — 공사 허가 시간대 제약 (construction_work_hours)

construction Gold(manhattan_construction_events, Traffic Score에 필요한 permit만
남은 상태) + construction_stipulations Bronze(조건/유의사항 텍스트)에서 "WORK 9AM
- 4PM, MONDAY TO FRIDAY" 류 작업 시간대 제약만 뽑아서 조인한 결과. 차선 유지/폭
관련 stipulation은 파싱하지 않는다(요청 범위 밖 — 너무 복잡함).

전체 stipulation 텍스트 1,157만 건 중 고유 문구는 12,627개뿐이고, 시간대 제약
패턴에 해당하는 건 27개 고유 문구 · 52만여 행(4.5%)이다. 나머지 90%+는 소음
저감 인증, 자전거 랙 손상 금지 같은 법적/행정 보일러플레이트라 traffic score
관점에서 신호가 아니라 노이즈로 보고 버린다.

주의: 허가 하나가 요일별로 다른 시간대 제약을 여러 개 가질 수 있다(예: 평일은
"10AM-4PM", 토요일은 "8AM-4PM"을 별도 stipulation으로 둘 다 가짐 — 실제로 전체
매칭 허가의 약 28%가 이런 경우). 그래서 이 결과는 "허가 하나당 한 행"이 아니라
"허가 x 시간대 규칙 하나당 한 행"이다 — 시간대 제약이 없는 허가는 정확히 한 행
(전부 null), 여러 개 있는 허가는 그만큼 여러 행으로 나온다.

Bronze 파티션(수백 개 날짜별 파일)을 한꺼번에 pandas로 합쳐 읽으면 컨테이너
메모리 한도에서 OOM이 나서(정확한 원인 미확인 — permitnumber 컬럼을 함께 읽을 때만
발생), 파티션 파일을 하나씩 순회하며 매칭되는 행만 누적하는 방식으로 우회한다.
전체 589개 파티션 기준 4초 안쪽으로 끝나 성능 문제는 없다.
"""

from __future__ import annotations

import glob
import os
import re
from datetime import date
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, GOLD_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.construction_stipulations.bronze import SOURCE as STIPULATIONS_SOURCE

logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_stipulations_silver")

OUT_SOURCE = "construction_work_hours"

# construction Silver가 아니라 Gold(manhattan_construction_events)를 읽는다 —
# Manhattan/상태/시리즈로 이미 걸러진, Traffic Score에 실제로 필요한 permit만
# 대상으로 작업시간 stipulation을 매칭하기 위함 (src/construction/gold.py 참고).
CONSTRUCTION_GOLD_DIR = GOLD_DIR / "construction"

# "WORK 9AM - 4PM, MONDAY TO FRIDAY" / "WORK 10PM - 6AM NIGHTLY. SECTION 24-224..." 류를
# 매칭해서 시작/종료 시각 + 요일 구절(raw)을 뽑는다. 요일 구절 뒤에 붙는
# "SECTION 24-224 ADMINISTRATIVE CODE..." 보일러플레이트는 버린다.
WORK_HOUR_RE = re.compile(
    r"^WORK\s+(\d{1,2})\s*(AM|PM)\s*-\s*(\d{1,2})\s*(AM|PM)\s*,?\s*"
    r"([^.]*?)\s*\.?\s*(?:SECTION\s+24-224.*)?$",
    re.IGNORECASE,
)

# 요일 구절(raw, 대문자 정규화 후) -> 요일 코드. 매칭 안 되는 구절(차선 관련 문구가
# 섞여 있거나 복잡한 복합 요일 표현)은 OTHER로 두고 raw 텍스트를 그대로 보존한다.
DAY_MAP = {
    "": "DAILY",  # 요일 명시 없음 = 매일
    "NIGHTLY": "DAILY",
    "MONDAY TO FRIDAY": "WEEKDAY",
    "MONDAY THROUGH FRIDAY": "WEEKDAY",
    "WEEKNIGHTS": "WEEKDAY",
    "WEEKNIGHTS, NO WEEKENDS": "WEEKDAY",
    "SATURDAY AND SUNDAY": "WEEKEND",
    "SATURDAY": "SATURDAY",
    "SUNDAY": "SUNDAY",
    "EXCEPT SUNDAY": "EXCEPT_SUNDAY",
}


def _to_hour24(hour: str, ampm: str) -> int:
    h = int(hour)
    if ampm.upper() == "AM":
        return 0 if h == 12 else h
    return 12 if h == 12 else h + 12


def _parse_work_hours(text: str) -> tuple[int, int, str, str] | None:
    m = WORK_HOUR_RE.match(text)
    if not m:
        return None

    start_h, start_ap, end_h, end_ap, days_raw = m.groups()
    days_raw = days_raw.strip().upper()
    day_code = DAY_MAP.get(days_raw, "OTHER")

    return _to_hour24(start_h, start_ap), _to_hour24(end_h, end_ap), day_code, days_raw


def extract_work_hours(bronze_root: Path = BRONZE_DIR / STIPULATIONS_SOURCE) -> pd.DataFrame:
    """
    stipulations Bronze 전체 파티션에서 작업 시간대 제약만 뽑는다.

    파티션(날짜)별로 하나씩 읽어서 매칭된 행만 누적한다 — 전체를 한 번에
    합쳐 읽으면 OOM이 나기 때문(모듈 docstring 참고).
    """
    files = sorted(glob.glob(str(bronze_root / "dt=*" / "data.parquet")))

    matched = []
    for f in files:
        day_df = pd.read_parquet(f, columns=["permitnumber", "stipulationfulltext"])
        mask = day_df["stipulationfulltext"].str.match(
            r"^WORK\s+\d{1,2}\s*[AP]M\s*-\s*\d{1,2}\s*[AP]M", case=False, na=False
        )
        if mask.any():
            matched.append(day_df[mask])

    if not matched:
        return pd.DataFrame(
            columns=["permitnumber", "work_start_hour", "work_end_hour", "work_days_code", "work_days_raw"]
        )

    raw = pd.concat(matched, ignore_index=True).drop_duplicates()

    parsed = raw["stipulationfulltext"].map(_parse_work_hours)
    raw = raw[parsed.notna()].copy()
    parsed = parsed[parsed.notna()]

    raw[["work_start_hour", "work_end_hour", "work_days_code", "work_days_raw"]] = pd.DataFrame(
        parsed.tolist(), index=raw.index
    )

    return (
        raw[["permitnumber", "work_start_hour", "work_end_hour", "work_days_code", "work_days_raw"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )


def load_construction_gold(run_date: str) -> pd.DataFrame:
    path = CONSTRUCTION_GOLD_DIR / f"dt={run_date}" / "data.parquet"
    return pd.read_parquet(path)


def _merge_work_hours(construction: pd.DataFrame) -> pd.DataFrame:
    work_hours = extract_work_hours()

    return construction.merge(
        work_hours,
        left_on="permit_id",
        right_on="permitnumber",
        how="left",
    ).drop(columns=["permitnumber"])


def validate(df: pd.DataFrame, construction_rows: int) -> None:
    if df.empty:
        raise ValueError("construction_work_hours Silver 결과가 비었습니다.")

    if df["permit_id"].isna().any():
        raise ValueError("permit_id NULL 발생")

    # LEFT JOIN이라 원본(construction) 행수보다 적을 수 없다 (여러 시간대 규칙이면 더 늘어남).
    if len(df) < construction_rows:
        raise ValueError(
            f"조인 후 행수({len(df)})가 원본 construction 행수({construction_rows})보다 적음 — LEFT JOIN 오류 가능성"
        )

    has_rule = df["work_start_hour"].notna().sum()
    logger.info(
        "construction_work_hours Silver 검증 완료: rows=%d (원본 construction=%d), 시간대 제약 있는 행=%d (%.1f%%)",
        len(df), construction_rows, has_rule, has_rule / len(df) * 100,
    )
    logger.info("work_days_code 분포:\n%s", df["work_days_code"].value_counts(dropna=False).to_string())


def build(run_date: str | None = None) -> str:
    """load -> merge -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("construction_work_hours Silver 변환 시작: run_date=%s", run_date)

    construction = load_construction_gold(run_date)
    df = _merge_work_hours(construction)

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "construction_work_hours Silver 빌드 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


def validate_output(path: str, run_date: str) -> str:
    """build()가 저장한 결과를 다시 읽어, 그 run_date의 construction Gold
    행수와 비교하며 validate()를 돌린다."""
    df = pd.read_parquet(path)
    construction_rows = len(load_construction_gold(run_date))
    validate(df, construction_rows=construction_rows)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())
    path = build(run_date)
    validate_output(path, run_date)
    return path


if __name__ == "__main__":
    main()
