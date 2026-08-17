"""
Silver — 공사 허가

Bronze 일별 전체 스냅샷을 정제한다. 여기서는 "이 행이 어떤 분석을 하더라도
유효한 데이터인가"만 본다 — Manhattan 한정, 상태/시리즈 기준 제외처럼
"Traffic Score에 필요한가"를 따지는 판단은 src/construction/gold.py로
옮겼다 (구분 기준: 데이터 자체가 잘못됐으면 Silver, 데이터는 멀쩡한데
우리 분석엔 안 맞으면 Gold).

- 컬럼명/타입 통일
- 날짜/시간 파싱, 시작/종료 없거나 기간이 잘못된 행 제거
- 갱신 허가의 작업 시작/종료 시각 복구
- 도로명 정규화 (다른 소스와의 JOIN 키)
- Gold가 필터링에 쓸 수 있게 status/borough는 남겨서 저장

permit_id 중복은 여기서 조용히 지우지 않고 validate()에서 에러로 막는다 —
Bronze의 keyset 페이지네이션(order=permitnumber, :id)이 tie-breaker를
갖췄으니 정상적으로는 안 생겨야 하고, 생긴다면 그 자체가 원인을 찾아야 할
신호다.
"""

import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BRONZE_DIR, SILVER_DIR
from common.logger import get_logger
from common.utils import clean_street, save_parquet

logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_silver")

SOURCE = "construction"

# Bronze에서 읽을 컬럼
READ_COLS = [
    "permitnumber",
    "previouspermitnumber",
    "permitseriesshortdesc",
    "permittypedesc",
    "permitstatusshortdesc",
    "permitlinearfeet",
    "onstreetname",
    "fromstreetname",
    "tostreetname",
    "wkt",
    "issuedworkstartdate",
    "issuedworkenddate",
    "boroughname",
    "permitissuedate",
]

STREET_COLS = [
    "on_street",
    "from_street",
    "to_street",
]

MAX_CHAIN_HOPS = 10


def load_bronze(run_date):
    """run_date에 해당하는 Bronze 스냅샷을 읽는다."""

    path = BRONZE_DIR / SOURCE / f"dt={run_date}" / "data.parquet"

    if not path.exists():
        raise FileNotFoundError(f"{SOURCE}: Bronze 파일 없음: {path}")

    logger.info("공사 Bronze 로드: path=%s", path)

    return pq.ParquetFile(path).read(columns=READ_COLS).to_pandas()


def resolve_time_chain(df):
    """
    종일(00시~23시)로 기록된 갱신 허가는
    previouspermitnumber를 따라가 이전 허가의
    실제 작업 시작/종료 시각을 복구한다.

    Gold에서 상태/시리즈로 걸러내기 전, Bronze에 가까운 원본 상태에서
    수행한다 — 이전 permit이 취소/제출 등 상태라도 체인은 그대로 따라갈
    수 있어야 하기 때문이다 (Silver에서는 상태로 행을 지우지 않으므로
    자연히 보장된다).

    복구 실패 여부는 내부 처리에만 사용하고
    최종 Silver에는 저장하지 않는다.
    """

    # 시작/종료 시간이 부정확하다고 판단되는 허가
    allday = (
        (df["work_start_ts"].dt.hour == 0)
        & (df["work_end_ts"].dt.hour == 23)
    )

    work = df.set_index("permit_id")
    work["_needs_recovery"] = allday.values

    for _ in range(MAX_CHAIN_HOPS):

        unresolved = (
            work["_needs_recovery"]
            & work["prev_permit_id"].notna()
        )

        if not unresolved.any():
            break

        previous = work.loc[unresolved, "prev_permit_id"]

        previous_start = previous.map(work["work_start_ts"])
        previous_end = previous.map(work["work_end_ts"])

        # 이전 허가의 시간이 존재하고 종일 형태가 아닌 경우만 사용
        valid_previous = (
            previous_start.notna()
            & previous_end.notna()
            & ~(
                (previous_start.dt.hour == 0)
                & (previous_end.dt.hour == 23)
            )
        )

        if valid_previous.any():

            idx = previous.index[valid_previous]

            old_start = work.loc[idx, "work_start_ts"]
            old_end = work.loc[idx, "work_end_ts"]

            source_start = previous_start[valid_previous]
            source_end = previous_end[valid_previous]

            # 현재 허가의 날짜는 유지하고 시간만 이전 허가에서 복구
            work.loc[idx, "work_start_ts"] = (
                old_start.dt.normalize()
                + pd.to_timedelta(source_start.dt.hour.values, unit="h")
                + pd.to_timedelta(source_start.dt.minute.values, unit="m")
            )

            work.loc[idx, "work_end_ts"] = (
                old_end.dt.normalize()
                + pd.to_timedelta(source_end.dt.hour.values, unit="h")
                + pd.to_timedelta(source_end.dt.minute.values, unit="m")
            )

            work.loc[idx, "_needs_recovery"] = False

        # 직전 허가에서도 복구에 실패하면 그 이전 permit을 따라간다.
        next_prev = previous.map(work["prev_permit_id"])
        work.loc[previous.index, "prev_permit_id"] = next_prev.values

    remaining = int(work["_needs_recovery"].sum())
    target = int(allday.sum())
    recovered = target - remaining

    logger.info(
        "작업 시각 복구: 대상=%d 성공=%d 실패=%d 복구율=%.1f%%",
        target,
        recovered,
        remaining,
        recovered / target * 100 if target else 0.0,
    )

    return work.drop(columns=["_needs_recovery"]).reset_index()


def transform(df):
    """Bronze 공사 데이터를 최소 정제한다 — 데이터 자체의 유효성만 본다."""

    # 1. 컬럼명 통일 (status/borough는 Gold의 필터 기준이라 남겨둔다)
    df = df.rename(columns={
        "permitnumber": "permit_id",
        "previouspermitnumber": "prev_permit_id",
        "permitseriesshortdesc": "permit_series",
        "permittypedesc": "permit_type",
        "permitstatusshortdesc": "status",
        "permitlinearfeet": "linear_feet",
        "onstreetname": "on_street",
        "fromstreetname": "from_street",
        "tostreetname": "to_street",
        "wkt": "geom_wkt",
        "boroughname": "borough",
    })

    # 2. 날짜/시간 변환
    df["work_start_ts"] = pd.to_datetime(
        df["issuedworkstartdate"], errors="coerce"
    )
    df["work_end_ts"] = pd.to_datetime(
        df["issuedworkenddate"], errors="coerce"
    )
    # permit_issue_ts: 실제 공사 기간(work_start_ts~end_ts)과 별개로 "이 허가
    # 자체가 언제 발급됐는지" — "해당 날짜에 새로 올라온 공사" 목록(대시보드)이
    # 이 값 기준으로 필터링한다.
    df["permit_issue_ts"] = pd.to_datetime(
        df["permitissuedate"], errors="coerce"
    )

    # 시작/종료가 없거나 기간이 잘못된 데이터 제외 — 어떤 분석을 하든 못 쓰는
    # 행이라 Silver에서 제거한다.
    df = df[
        df["work_start_ts"].notna()
        & df["work_end_ts"].notna()
        & (df["work_end_ts"] > df["work_start_ts"])
    ].copy()

    # 3. 갱신 허가의 작업 시각 복구
    df = resolve_time_chain(df)

    # 4. 도로명 정규화
    #
    # wkt가 비어 있는 경우가 많아 도로명이 사실상 유일한 JOIN 키다.
    # 원본은 "WEST   19 STREET"처럼 공백이 불규칙하므로
    # 다른 소스와 동일한 규칙으로 정리한다.
    for col in STREET_COLS:
        df[col] = df[col].map(clean_street)

    # 5. 수치 컬럼 변환
    df["linear_feet"] = pd.to_numeric(
        df["linear_feet"], errors="coerce"
    )

    # 6. Gold가 필터링에 쓸 status/borough까지 포함해서 저장
    return df[[
        "permit_id",
        "permit_series",
        "permit_type",
        "status",
        "borough",
        "linear_feet",
        "on_street",
        "from_street",
        "to_street",
        "geom_wkt",
        "work_start_ts",
        "work_end_ts",
        "permit_issue_ts",
    ]].reset_index(drop=True)


def validate(df):
    """Silver 결과 기본 품질 검증 — 데이터 자체의 유효성만 확인한다."""

    if df.empty:
        raise ValueError("construction Silver 결과가 비었습니다.")

    if df["permit_id"].isna().any():
        raise ValueError("permit_id NULL 발생")

    # permit 하나당 한 행이어야 함. Bronze의 keyset 페이지네이션에 :id
    # tie-breaker를 걸어뒀으니 정상적으로는 안 생겨야 하고, 생기면 조용히
    # 지우지 않고 여기서 바로 알린다.
    if not df["permit_id"].is_unique:
        n_dup = int(df["permit_id"].duplicated().sum())
        raise ValueError(f"permit_id 중복 발생: {n_dup}건")

    # 공간 JOIN도 도로명 JOIN도 불가능한 행 감시
    no_location = int(
        (df["geom_wkt"].isna() & df["on_street"].isna()).sum()
    )

    if no_location:
        logger.warning("위치 정보 없음: rows=%d", no_location)

    no_wkt = int(df["geom_wkt"].isna().sum())

    logger.info(
        "공사 Silver 검증 완료: rows=%d wkt없음=%d 위치없음=%d",
        len(df), no_wkt, no_location,
    )


def build(run_date: str | None = None) -> str:
    """load -> transform -> save만 한다(validate 없음) — build/validate를 별도
    Airflow 태스크로 나눠서, validate 실패로 재시도할 때 이 무거운 변환을
    다시 안 해도 되게 하기 위함."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("공사 Silver 변환 시작: run_date=%s", run_date)

    df = load_bronze(run_date)
    df = transform(df)

    path = save_parquet(df, SILVER_DIR / SOURCE / f"dt={run_date}")

    logger.info(
        "공사 Silver 빌드 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
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
