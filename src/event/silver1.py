"""
Silver1 — NYC 행사 (Permitted Event Information) 정제

날짜 파싱, 도로 구간 파싱(도로 통제 매핑용) 등 event Bronze 자체의 정제만
한다. Manhattan 한정, "차량 통행에 영향을 주는 도로 통제만" 필터, run_date
기준 종료 행사 제외는 src/event/gold1.py에서 한다.

다일 행사는 같은 event_id가 날짜별로 여러 건 들어온다.
따라서 키는 (event_id, start_ts)이다.

closure_type별 가중치는 Gold2(교차도메인, src/gold2/event_boost.py)에서 계산한다.
"""

import sys
import os
import re
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BRONZE_DIR, SILVER1_DIR
from common.utils import save_parquet, clean_street
from common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="event_silver")

SOURCE = "event"

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
    """행사 위치에서 도로 구간 정보를 분리.

    행진/퍼레이드류 행사는 콤마로 구간을 여러 개 나열한다(예: "113 BAXTER
    STREET, BAXTER STREET between HESTER STREET and CANAL STREET, ...").
    첫 구간이 "between X and Y" 형태가 아닌 경우가 많다 — 시작 지점 주소나
    반복된 장소명이 먼저 나오고 실제 파싱 가능한 구간은 그 뒤에 있는 경우가
    실측으로 확인됨. 그래서 첫 구간만 보지 않고, 콤마로 나눈 구간을 순서대로
    보다가 "between" 패턴에 맞는 첫 구간을 쓴다. 여러 구간 전체를 다 담진
    않는다(Silver 스키마가 행사당 도로 구간 하나라 다른 구간들은 여전히
    버려짐 — 이건 범위 밖이고, 최소한 대표 구간 하나는 진짜 도로 구간으로
    잡히게 하는 게 목적).
    """

    if not isinstance(raw, str) or not raw.strip():
        return {
            "on_street": None,
            "from_street": None,
            "to_street": None,
        }

    segments = [s.strip() for s in raw.split(",") if s.strip()]

    for segment in segments:
        # 예: WEST 23 STREET between 8 AVENUE and 9 AVENUE
        match = RE_BETWEEN.match(segment)
        if match:
            return {
                "on_street": normalize_event_street(match.group(1)),
                "from_street": normalize_event_street(match.group(2)),
                "to_street": normalize_event_street(match.group(3)),
            }

    # 어느 구간도 "between" 패턴이 아니면(전 구간 통제, 단일 주소 등) 첫
    # 구간을 on_street로만 쓴다 — 기존 동작 유지.
    first = segments[0] if segments else raw.strip()
    return {
        "on_street": normalize_event_street(first),
        "from_street": None,
        "to_street": None,
    }


def transform(df):

    # 1. 날짜/시간 변환
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

    # 2. closure_type 정규화(필터는 gold1에서 — SIDEWALK_ONLY/N_A 제외)
    df["closure_type"] = (
        df["street_closure_type"]
        .fillna("N/A")
    )

    # 3. 도로 구간 파싱
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

    # 4. Silver1 컬럼 — event_borough/end_ts는 gold1의 지역·활성기간 필터가
    # 써야 하므로 남겨둔다.
    return df[[
        "event_id",
        "event_borough",
        "start_ts",
        "end_ts",
        "closure_type",
        "on_street",
        "from_street",
        "to_street",
    ]].reset_index(drop=True)


UNMATCHED_LOCATION_PATH = SILVER1_DIR / "event_location_parse_unmatched" / "data.parquet"
UNMATCHED_LOCATION_COLUMNS = ["on_street", "first_seen_date", "resolved"]


def _load_unmatched_locations() -> pd.DataFrame:
    if not UNMATCHED_LOCATION_PATH.exists():
        return pd.DataFrame(columns=UNMATCHED_LOCATION_COLUMNS)
    return pd.read_parquet(UNMATCHED_LOCATION_PATH)


def _save_unmatched_locations(df: pd.DataFrame) -> None:
    save_parquet(df, UNMATCHED_LOCATION_PATH.parent, filename=UNMATCHED_LOCATION_PATH.name)


def validate(df):

    if df.empty:
        raise ValueError(
            "event Silver1 결과가 비었습니다."
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
        "행사 Silver1 검증 완료: rows=%d events=%d",
        len(df),
        df["event_id"].nunique(),
    )

    logger.info(
        "closure_type 분포:\n%s",
        df["closure_type"].value_counts().to_string(),
    )

    # on_street는 있는데 from_street/to_street 중 하나라도 없으면 event_lion
    # 매핑 단계에서 block을 못 찾는다("missing_from_to"). LLM은 안 거치고
    # (자연어 해석이 아니라 콤마로 나열된 구간 중 쓸 만한 게 하나도 없다는
    # 뜻이라 LLM을 붙여도 얻을 게 적음 — parse_location() docstring 참고)
    # 대신, 이미 본 적 있는 on_street 패턴은 다시 신규로 안 세고, 처음 보는
    # 패턴이 하나라도 있으면 그때만 알린다. 실측 발생량이 극히 적어서(전체
    # 이력 기준 1,978행 중 5행, 그마저 이번에 2/3 고쳐서 더 줄었음) 건별이
    # 아니라 배치당 1번 체크해도 임계값을 0으로 잡아 노이즈가 안 된다 —
    # construction_stipulations의 동일한 판단 참고.
    unmatched_mask = df["on_street"].notna() & (df["from_street"].isna() | df["to_street"].isna())
    unmatched_count = int(unmatched_mask.sum())
    if unmatched_count:
        logger.warning("도로 구간(from/to) 미확인: rows=%d", unmatched_count)

        unmatched_streets = set(df.loc[unmatched_mask, "on_street"].unique())
        existing = _load_unmatched_locations()
        known_streets = set(existing["on_street"]) if not existing.empty else set()
        new_streets = unmatched_streets - known_streets

        if new_streets:
            new_rows = pd.DataFrame([
                {"on_street": s, "first_seen_date": date.today().isoformat(), "resolved": False}
                for s in new_streets
            ])
            combined = pd.concat([existing, new_rows], ignore_index=True) if not existing.empty else new_rows
            _save_unmatched_locations(combined)
            raise ValueError(
                f"행사 도로 구간(from/to) 파싱에 실패한 신규 on_street 패턴이 {len(new_streets)}개 발생 — "
                f"샘플: {list(new_streets)[:3]}"
            )


def build(run_date: str | None = None) -> str:
    """load -> transform -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv(
            "RUN_DATE",
            date.today().isoformat(),
        )

    logger.info(
        "행사 Silver1 변환 시작: run_date=%s",
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
        "행사 Silver1 빌드 완료: "
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
