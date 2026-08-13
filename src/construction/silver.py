"""
Silver — 공사 허가

Bronze 일별 전체 스냅샷을 정제한다.

- 맨해튼만 필터
- 취소/행정 허가 제외
- 차량 통행과 무관한 허가 시리즈 제외
- 날짜/시간 표준화
- 갱신 허가의 작업 시작/종료 시각 복구
- run_date 기준 진행 중인 공사만 유지
- 도로명 정규화 (다른 소스와의 JOIN 키)
- Gold의 Traffic Score 분석에 활용할 컬럼만 저장

Traffic Score 가중치/영향도는 Gold에서 처리한다.
"""

import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd
import pyarrow.parquet as pq

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import BOROUGH, BRONZE_DIR, SILVER_DIR
from common.logger import get_logger
from common.utils import clean_street, save_parquet

logger = get_logger(__name__)

SOURCE = "construction"

# 행정성 permit type
ADMIN_TYPES = [
    "EMBARGO",
]

# 분석 대상에서 제외할 상태
DROP_STATUS = [
    "VOIDED AFTER ISSUE",
    "SUBMITTED",
    "SIM SUBMITTED",
    "PERMIT HELD FOR EXTERNAL REVIEW",
    "FEE REVIEW",
]

# 차량 통행 영향 분석과 직접 관련이 적은 허가 시리즈
# COMMERICAL은 원본 데이터의 오타 표기
DROP_SERIES = [
    "MISCELLANEOUS CASH RECEIPTS",
    "COMMERCIAL REFUSE CONTAINER PERMIT",
    "COMMERICAL REFUSE CONTAINER PERMIT",
    "SIDEWALK CONSTRUCTION PERMIT",
    "CANOPY PERMIT",
    "VAULT LICENSE",
]

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


def transform(df, run_date):
    """Bronze 공사 데이터를 Silver 형태로 정제한다."""

    # 1. 맨해튼만 유지
    df = df[df["boroughname"] == BOROUGH].copy()

    # 2. 컬럼명 통일
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
    })

    # 3. 날짜/시간 변환
    df["work_start_ts"] = pd.to_datetime(
        df["issuedworkstartdate"], errors="coerce"
    )
    df["work_end_ts"] = pd.to_datetime(
        df["issuedworkenddate"], errors="coerce"
    )

    # 시작/종료가 없거나 기간이 잘못된 데이터 제외
    df = df[
        df["work_start_ts"].notna()
        & df["work_end_ts"].notna()
        & (df["work_end_ts"] > df["work_start_ts"])
    ].copy()

    # 4. 취소 / 검토 중 상태 제외
    df = df[~df["status"].isin(DROP_STATUS)].copy()

    # 5. 행정성 허가 제외
    admin_pattern = "|".join(ADMIN_TYPES)

    df = df[
        ~df["permit_type"]
        .astype(str)
        .str.upper()
        .str.contains(admin_pattern, na=False)
    ].copy()

    # 6. 차량 통행과 직접 관련이 적은 허가 제외
    before = len(df)
    df = df[~df["permit_series"].isin(DROP_SERIES)].copy()
    logger.info("차량 무관 시리즈 제외: %d → %d", before, len(df))

    # 7. 갱신 허가의 작업 시각 복구
    #
    # 활성 공사 필터보다 먼저 수행한다.
    # 이전 permit은 이미 만료된 경우가 많기 때문에
    # 먼저 제거하면 이전 permit을 따라갈 수 없다.
    df = resolve_time_chain(df)

    # 8. run_date 기준 진행 중인 공사만 유지
    cutoff = pd.Timestamp(run_date)

    before = len(df)
    df = df[
        (df["work_start_ts"] <= cutoff)
        & (df["work_end_ts"] >= cutoff)
    ].copy()
    logger.info(
        "진행 중 공사만 유지: %d → %d (기준 %s)",
        before, len(df), run_date,
    )

    # 9. 도로명 정규화
    #
    # wkt가 비어 있는 경우가 많아 도로명이 사실상 유일한 JOIN 키다.
    # 원본은 "WEST   19 STREET"처럼 공백이 불규칙하므로
    # 다른 소스와 동일한 규칙으로 정리한다.
    for col in STREET_COLS:
        df[col] = df[col].map(clean_street)

    # 10. 수치 컬럼 변환
    df["linear_feet"] = pd.to_numeric(
        df["linear_feet"], errors="coerce"
    )

    # 11. Gold 분석에 활용할 컬럼만 저장
    return df[[
        "permit_id",
        "permit_series",
        "permit_type",
        "linear_feet",
        "on_street",
        "from_street",
        "to_street",
        "geom_wkt",
        "work_start_ts",
        "work_end_ts",
    ]].reset_index(drop=True)


def validate(df):
    """Silver 결과 기본 품질 검증."""

    if df.empty:
        raise ValueError("construction Silver 결과가 비었습니다.")

    if df["permit_id"].isna().any():
        raise ValueError("permit_id NULL 발생")

    # permit 하나당 한 행이어야 함
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

    logger.info(
        "permit_series 분포:\n%s",
        df["permit_series"].value_counts(dropna=False).to_string(),
    )


def main(run_date: str | None = None):

    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("공사 Silver 변환 시작: run_date=%s", run_date)

    df = load_bronze(run_date)
    df = transform(df, run_date)
    validate(df)

    path = save_parquet(df, SILVER_DIR / SOURCE / f"dt={run_date}")

    logger.info(
        "공사 Silver 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )


if __name__ == "__main__":
    main()