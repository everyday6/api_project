"""
Silver1 — TicketMaster

- id 중복 제거
- venue JSON 파싱 → 장소명, 좌표
- venue 이름 표준화
- 행사 날짜 보존
- 시간이 있을 때만 start_ts 생성
- 종료 시각은 원본에 있을 때만 사용
- Traffic Score에 필요한 최소 컬럼만 저장

맨해튼 범위 필터, run_date 기준 지난 행사 제외는 src/ticketmaster/gold1.py에서
한다. LION 거리 계산·좌표계 변환은 공용 Silver2에서 처리한다.
"""

import sys
import os
import json
import re
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BRONZE_DIR, SILVER1_DIR
from common.utils import save_parquet
from common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="ticketmaster_silver")

SOURCE = "ticketmaster"

# TicketMaster가 같은 장소명 뒤에 붙이는 지역 접미어
SUFFIX_PATTERNS = [
    r"\s*-\s*NY$",
    r"\s*-\s*NYC$",
    r"\s*-\s*NEW YORK$",
    r"\s+NEW YORK$",
    r"\s+NYC$",
]

# 접미어 제거만으로 합쳐지지 않는 실제 중복 표기
MANUAL_VENUE_ALIASES = {
    "JACOBS THEATRE": "BERNARD B JACOBS THEATRE",
    "STERN AUDITORIUM / PERELMAN STAGE AT CARNEGIE HALL": "CARNEGIE HALL",
    "JACOB JAVITS CENTER": "JAVITS CONVENTION CENTER",
    "IRVING PLAZA POWERED BY VERIZON 5G": "IRVING PLAZA",
    "NIGHTCLUB 101": "NIGHT CLUB 101",
    "DR2": "DR2 THEATRE",
    "RACKET": "RACKET NYC",
    "HILL COUNTRY LIVE": "HILL COUNTRY",
    "HILL COUNTRY NYC": "HILL COUNTRY",
    "HARD ROCK CAFE": "HARD ROCK CAFE NYC",
    "LINCOLN CENTER - VIVIAN BEAUMONT": "LINCOLN CENTER - VIVIAN BEAUMONT THEATRE",
    "LINCOLN CENTER - MITZI E NEWHOUSE": "LINCOLN CENTER - MITZI E NEWHOUSE THEATRE",
    "CIRCLE LINE CRUISES, PIER 83": "CIRCLE LINE CRUISES",
}


def normalize_venue(value) -> str | None:
    """venue 이름의 의미는 유지하면서 비교 가능한 표기로 정규화한다."""
    if not isinstance(value, str):
        return None

    name = re.sub(r"\s+", " ", value).strip().upper()
    if not name:
        return None

    name = name.replace(".", "").replace("'", "").replace("\u2019", "")
    name = re.sub(r"\s*-\s*", " - ", name)
    name = re.sub(r"\bTHEATER\b", "THEATRE", name)

    for pattern in SUFFIX_PATTERNS:
        name = re.sub(pattern, "", name)

    name = re.sub(r"\s+", " ", name).strip()
    return MANUAL_VENUE_ALIASES.get(name, name)


def load_bronze(run_date):

    path = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
        / "data.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(
            f"{SOURCE}: Bronze 파일 없음 - {path}"
        )

    return pd.read_parquet(str(path))


def parse_venue(raw):
    """venue JSON에서 장소명과 좌표 추출."""

    if not isinstance(raw, str):
        return {}

    try:
        venues = json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}

    if not venues:
        return {}

    venue = venues[0]
    location = venue.get("location", {}) or {}

    return {
        "venue_name": venue.get("name"),
        "lat": pd.to_numeric(
            location.get("latitude"),
            errors="coerce",
        ),
        "lon": pd.to_numeric(
            location.get("longitude"),
            errors="coerce",
        ),
    }


def transform(df):

    # 1. 중복 컬럼 제거
    df = df.loc[
        :,
        ~df.columns.duplicated()
    ].copy()

    # 2. 이벤트 ID 중복 제거
    df = df.drop_duplicates(
        subset="id",
        keep="last",
    ).copy()

    # 3. venue 파싱
    venue = (
        df["_embedded_venues"]
        .apply(parse_venue)
        .apply(pd.Series)
    )

    df = pd.concat(
        [
            df.reset_index(drop=True),
            venue.reset_index(drop=True),
        ],
        axis=1,
    )

    # 좌표 없는 이벤트 제외
    df = df[
        df["lat"].notna()
        & df["lon"].notna()
    ].copy()

    # 4. 행사 날짜
    df["event_date"] = pd.to_datetime(
        df["dates_start_localDate"],
        errors="coerce",
    ).dt.date

    # 날짜가 없는 이벤트는 사용 불가
    df = df[
        df["event_date"].notna()
    ].copy()

    # 5. venue 이름 표준화 — 원본 값을 새로 판단하지 않고 표현만 통일한다.
    df["venue_name_norm"] = df["venue_name"].map(normalize_venue)

    # 6. 시작 시각
    # 시간이 있는 경우에만 timestamp 생성
    df["start_ts"] = pd.NaT

    has_start_time = (
        df["dates_start_localTime"].notna()
    )

    df.loc[
        has_start_time,
        "start_ts",
    ] = pd.to_datetime(
        df.loc[
            has_start_time,
            "dates_start_localDate",
        ].astype(str)
        + " "
        + df.loc[
            has_start_time,
            "dates_start_localTime",
        ].astype(str),
        errors="coerce",
    )

    # 7. 종료 시각
    df["end_ts"] = pd.NaT

    has_end = (
        df.get(
            "dates_end_localDate",
            pd.Series(index=df.index, dtype=object),
        ).notna()
        & df.get(
            "dates_end_localTime",
            pd.Series(index=df.index, dtype=object),
        ).notna()
    )

    df.loc[
        has_end,
        "end_ts",
    ] = pd.to_datetime(
        df.loc[
            has_end,
            "dates_end_localDate",
        ].astype(str)
        + " "
        + df.loc[
            has_end,
            "dates_end_localTime",
        ].astype(str),
        errors="coerce",
    )

    # 8. 필요한 컬럼만 — lat/lon/event_date는 gold1의 지역·활성기간
    # 필터가 써야 하므로 남겨둔다.
    return df[[
        "id",
        "event_date",
        "start_ts",
        "end_ts",
        "venue_name",
        "venue_name_norm",
        "lat",
        "lon",
    ]].rename(
        columns={
            "id": "event_id",
        }
    ).reset_index(drop=True)


def validate(df):

    if df.empty:
        raise ValueError(
            "Ticketmaster Silver1 결과가 비었습니다."
        )

    if not df["event_id"].is_unique:
        raise ValueError(
            "Ticketmaster event_id 중복 발생"
        )

    if df["event_date"].isna().any():
        raise ValueError(
            "Ticketmaster event_date 결측 발생"
        )

    logger.info(
        "Ticketmaster Silver1 검증 완료: rows=%d",
        len(df),
    )


def build(run_date: str | None = None) -> str:
    """load -> transform -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv(
            "RUN_DATE",
            date.today().isoformat(),
        )

    logger.info(
        "Ticketmaster Silver1 변환 시작: run_date=%s",
        run_date,
    )

    df = load_bronze(run_date)
    df = transform(df)

    path = save_parquet(
        df,
        SILVER1_DIR
        / SOURCE
        / f"dt={run_date}",
    )

    logger.info(
        "Ticketmaster Silver1 빌드 완료: "
        "rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )
    return str(path)


def validate_output(path: str) -> str:
    """build()가 저장한 결과를 다시 읽어 validate()를 돌린다."""
    df = pd.read_parquet(str(path))
    validate(df)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()
