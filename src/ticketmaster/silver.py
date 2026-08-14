"""
Silver — TicketMaster

- id 중복 제거
- venue JSON 파싱 → 장소명, 좌표
- 맨해튼 범위 필터
- 행사 날짜 보존
- 시간이 있을 때만 start_ts 생성
- 종료 시각은 원본에 있을 때만 사용
- Traffic Score에 필요한 최소 컬럼만 저장

거리 계산 및 좌표계 변환은 Gold에서 처리한다.
"""

import sys
import os
import json
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BRONZE_DIR, SILVER_DIR
from common.utils import save_parquet
from common.logger import get_logger

logger = get_logger(__name__)

SOURCE = "ticketmaster"

# 맨해튼 대략 범위
MH_LAT = (40.68, 40.88)
MH_LON = (-74.03, -73.90)


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

    return pd.read_parquet(path)


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


def transform(df, run_date):

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

    # 4. 맨해튼 대략 범위 필터
    df = df[
        df["lat"].between(*MH_LAT)
        & df["lon"].between(*MH_LON)
    ].copy()


    # 5. 행사 날짜
    df["event_date"] = pd.to_datetime(
        df["dates_start_localDate"],
        errors="coerce",
    ).dt.date

    # 날짜가 없는 이벤트는 사용 불가
    df = df[
        df["event_date"].notna()
    ].copy()

    # 5-1. run_date 기준으로 이미 지난 행사 제외
    cutoff = date.fromisoformat(run_date)
    before = len(df)
    df = df[df["event_date"] >= cutoff].copy()
    logger.info(
        "지난 행사 제외: %d → %d (기준 %s)",
        before, len(df), run_date,
    )


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

    # 8. 필요한 컬럼만
    return df[[
        "id",
        "event_date",
        "start_ts",
        "end_ts",
        "venue_name",
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
            "Ticketmaster Silver 결과가 비었습니다."
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
        "Ticketmaster Silver 검증 완료: rows=%d",
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
        "Ticketmaster Silver 변환 시작: run_date=%s",
        run_date,
    )

    df = load_bronze(run_date)
    df = transform(df, run_date)

    path = save_parquet(
        df,
        SILVER_DIR
        / SOURCE
        / f"dt={run_date}",
    )

    logger.info(
        "Ticketmaster Silver 빌드 완료: "
        "rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )
    return str(path)


def validate_output(path: str) -> str:
    """build()가 저장한 결과를 다시 읽어 validate()를 돌린다."""
    df = pd.read_parquet(path)
    validate(df)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()