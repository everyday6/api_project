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

from common.config import BRONZE_DIR, SILVER_DIR, BOROUGH_EVENT
from common.utils import save_parquet, clean_street
from common.logger import get_logger

logger = get_logger(__name__)

SOURCE = "event"

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


def normalize_event_street(value):
    """
    Event 도로명을 LION street_name 형식에 최대한 맞춘다.
    """
    value = clean_street(value)

    if not value:
        return None

    # 방향 약어
    value = re.sub(r"^W\.?\s+", "WEST ", value)
    value = re.sub(r"^E\.?\s+", "EAST ", value)

    # 서수 제거: 34TH -> 34, 32ND -> 32
    value = re.sub(
        r"\b(\d+)(ST|ND|RD|TH)\b",
        r"\1",
        value,
    )

    # 도로명 표기 통일
    replacements = {
        "FIRST AVENUE": "1 AVENUE",
        "SECOND AVENUE": "2 AVENUE",
        "THIRD AVENUE": "3 AVENUE",
        "FT WASHINGTON AVENUE": "FORT WASHINGTON AVENUE",
        "GANSEVOORT ST": "GANSEVOORT STREET",
        "FREDRICK DOUGLAS BOULEVARD":
            "FREDERICK DOUGLASS BOULEVARD",
        "ADAM CLAYTON POWELL BOULEVARD":
            "ADAM CLAYTON POWELL JR BOULEVARD",
        "MACDOUGAL STREET":
            "MAC DOUGAL STREET",
        "6 AVENUE": "AVENUE OF THE AMERICAS",
    }

    value = replacements.get(
        value,
        value,
    )

    # Plaza 이름에서 실제 도로명 추출
    if ":" in value:

        if value.endswith(" BROADWAY"):
            return "BROADWAY"

        if value.endswith(" 9 AVENUE"):
            return "9 AVENUE"

        # Pershing Square의 실제 on street
        if value.startswith(
            "PERSHING SQUARE PLAZA:"
        ):
            return "PARK AVENUE"

    return value



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
            "on_street": normalize_event_street(match.group(1)),
            "from_street": normalize_event_street(match.group(2)),
            "to_street": normalize_event_street(match.group(3)),
        }

    return {
        "on_street": normalize_event_street(first),
        "from_street": None,
        "to_street": None,
    }


def transform(df, run_date):

    # 1. 맨해튼만
    df = df[
        df["event_borough"] == BOROUGH_EVENT
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


def build(run_date: str | None = None) -> str:
    """load -> transform -> save만 한다(validate 없음)."""
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

    path = save_parquet(
        df,
        SILVER_DIR
        / SOURCE
        / f"dt={run_date}",
    )

    logger.info(
        "행사 Silver 빌드 완료: "
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