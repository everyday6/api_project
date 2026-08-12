"""
Silver — NYC 행사 (Permitted Event Information)

Traffic Score에 사용할 "차량 통행에 영향을 주는 도로 통제 행사"만 남긴다.

필터 순서
1. 맨해튼
2. 시작/종료 시각 유효성
3. run_date 기준 종료된 행사 제외
4. 도로 통제 없는 장소 행사(place_event) 제외
5. 보도만 막는 유형 제외 (차량 무관)

다일 행사는 같은 event_id가 날짜별로 여러 건 들어온다.
따라서 키는 (event_id, start_ts)이다.

closure_type별 가중치는 Gold에서 계산한다.
"""

import sys
import os
import re
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BRONZE_DIR, SILVER_DIR
from common.utils import save_parquet
from common.logger import get_logger

logger = get_logger(__name__)

SOURCE = "event"
BOROUGH = "Manhattan"

# 보도만 막아 차량 통행에는 영향이 없는 유형
SIDEWALK_ONLY = [
    "Partial Sidewalk Closure",
    "Full Sidewalk Closure",
]

RE_BETWEEN = re.compile(
    r"^(.*?)\s+between\s+(.*?)\s+and\s+(.*)$",
    re.IGNORECASE,
)

READ_COLS = [
    "event_id",
    "event_borough",
    "start_date_time",
    "end_date_time",
    "event_location",
    "street_closure_type",
]


def load_bronze(run_date):
    """run_date에 해당하는 Bronze 스냅샷을 읽는다."""

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

    logger.info(
        "행사 Bronze 로드: path=%s",
        path,
    )

    return pd.read_parquet(
        path,
        columns=READ_COLS,
    )


def clean_street(value):
    """도로명 공백/대소문자 정리."""

    if not isinstance(value, str):
        return None

    return re.sub(
        r"\s+",
        " ",
        value,
    ).strip().upper()


def parse_location(raw):
    """행사 위치에서 도로 구간 정보를 분리."""

    if not isinstance(raw, str) or not raw.strip():
        return {
            "on_street": None,
            "from_street": None,
            "to_street": None,
        }

    first = raw.split(",")[0].strip()

    # 예: WEST 23 STREET between 8 AVENUE and 9 AVENUE
    match = RE_BETWEEN.match(first)

    if match:
        return {
            "on_street": clean_street(match.group(1)),
            "from_street": clean_street(match.group(2)),
            "to_street": clean_street(match.group(3)),
        }

    return {
        "on_street": clean_street(first),
        "from_street": None,
        "to_street": None,
    }


def transform(df, run_date):

    # 1. 맨해튼만
    df = df[
        df["event_borough"] == BOROUGH
    ].copy()

    # 2. 날짜/시간 변환
    df["start_ts"] = pd.to_datetime(
        df["start_date_time"],
        errors="coerce",
    )

    df["end_ts"] = pd.to_datetime(
        df["end_date_time"],
        errors="coerce",
    )

    # 시작/종료가 없거나 기간이 이상한 데이터 제외
    df = df[
        df["start_ts"].notna()
        & df["end_ts"].notna()
        & (df["end_ts"] > df["start_ts"])
    ].copy()

    # 3. run_date 기준으로 이미 끝난 행사 제외
    cutoff = pd.Timestamp(run_date)
    before = len(df)
    df = df[df["end_ts"] >= cutoff].copy()
    logger.info(
        "종료 행사 제외: %d → %d (기준 %s)",
        before,
        len(df),
        run_date,
    )

    # 4. 차량 통행에 영향 있는 도로 통제만 유지
    df["closure_type"] = (
        df["street_closure_type"]
        .fillna("N/A")
    )

    before = len(df)
    df = df[
        df["closure_type"].ne("N/A")
        & ~df["closure_type"].isin(SIDEWALK_ONLY)
    ].copy()
    logger.info(
        "차량 무관 행사 제외: %d → %d",
        before,
        len(df),
    )

    # 5. 도로 구간 파싱
    location = (
        df["event_location"]
        .apply(parse_location)
        .apply(pd.Series)
    )

    df = pd.concat(
        [
            df.reset_index(drop=True),
            location.reset_index(drop=True),
        ],
        axis=1,
    )

    # 6. Silver 컬럼
    return df[[
        "event_id",
        "start_ts",
        "end_ts",
        "closure_type",
        "on_street",
        "from_street",
        "to_street",
    ]].reset_index(drop=True)


def validate(df):

    if df.empty:
        raise ValueError(
            "event Silver 결과가 비었습니다."
        )

    if df["event_id"].isna().any():
        raise ValueError(
            "event_id NULL 발생"
        )

    # 다일 행사는 event_id가 반복되므로
    # 발생 단위 키로 중복을 확인한다.
    if df.duplicated(
        subset=["event_id", "start_ts"]
    ).any():
        raise ValueError(
            "(event_id, start_ts) 중복 발생"
        )

    # 도로명을 못 얻은 행은 블록 조인이 불가능하다.
    no_street = int(df["on_street"].isna().sum())

    if no_street:
        logger.warning(
            "도로명 미확인: rows=%d",
            no_street,
        )

    logger.info(
        "행사 Silver 검증 완료: rows=%d events=%d",
        len(df),
        df["event_id"].nunique(),
    )

    logger.info(
        "closure_type 분포:\n%s",
        df["closure_type"].value_counts().to_string(),
    )


def main(run_date: str | None = None):

    if run_date is None:
        run_date = os.getenv(
            "RUN_DATE",
            date.today().isoformat(),
        )

    logger.info(
        "행사 Silver 변환 시작: run_date=%s",
        run_date,
    )

    df = load_bronze(run_date)
    df = transform(df, run_date)
    validate(df)

    path = save_parquet(
        df,
        SILVER_DIR
        / SOURCE
        / f"dt={run_date}",
    )

    logger.info(
        "행사 Silver 저장 완료: "
        "rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )


if __name__ == "__main__":
    main()