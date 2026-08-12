"""
Silver — 공사 허가

Bronze 일별 전체 스냅샷을 정제한다.

- 맨해튼만 필터
- 취소/행정 허가 제외
- 날짜/시간 표준화
- 갱신 허가의 작업 시작 시각 복구
- Traffic Score에 필요한 최소 컬럼만 저장
- RUN_DATE 기준으로 읽고 저장하여 재실행 시 동일 결과 유지

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
from common.utils import save_parquet


logger = get_logger(__name__)

SOURCE = "construction"

ADMIN_TYPES = ["EMBARGO"]
DROP_STATUS = [
    "VOIDED AFTER ISSUE",
    "SUBMITTED",
    "SIM SUBMITTED",
    "PERMIT HELD FOR EXTERNAL REVIEW",
    "FEE REVIEW",
]

READ_COLS = [
    "permitnumber",
    "previouspermitnumber",
    "permittypedesc",
    "permitstatusshortdesc",
    "onstreetname",
    "fromstreetname",
    "tostreetname",
    "wkt",
    "issuedworkstartdate",
    "issuedworkenddate",
    "boroughname",
]

MAX_CHAIN_HOPS = 10


def load_bronze(run_date):
    """
    RUN_DATE에 해당하는 Bronze 스냅샷을 읽는다.
    """

    path = (
        BRONZE_DIR
        / SOURCE
        / f"dt={run_date}"
        / "data.parquet"
    )

    if not path.exists():
        raise FileNotFoundError(
            f"{SOURCE}: Bronze 파일 없음: {path}"
        )

    logger.info(
        "공사 Bronze 로드: path=%s",
        path,
    )

    return (
        pq.ParquetFile(path)
        .read(columns=READ_COLS)
        .to_pandas()
    )


def resolve_start_ts_chain(df):
    """
    종일(00시~23시)로 기록된 갱신 허가는
    previouspermitnumber를 따라가 이전 허가의
    실제 시작 시각을 복구한다.
    """

    # 시작 시간이 부정확하다고 판단되는 허가
    allday = (
        (df["work_start_ts"].dt.hour == 0)
        & (df["work_end_ts"].dt.hour == 23)
    )

    work = df.set_index("permit_id")

    # 복구 대상 표시
    work["_needs_start_recovery"] = allday.values

    for _ in range(MAX_CHAIN_HOPS):

        unresolved = (
            work["_needs_start_recovery"]
            & work["prev_permit_id"].notna()
        )

        if not unresolved.any():
            break

        previous = work.loc[
            unresolved,
            "prev_permit_id",
        ]

        previous_start = previous.map(
            work["work_start_ts"]
        )

        # 이전 허가 역시 종일 시간이면 아직 사용할 수 없음
        previous_end = previous.map(
            work["work_end_ts"]
        )

        valid_previous = (
            previous_start.notna()
            & previous_end.notna()
            & ~(
                (previous_start.dt.hour == 0)
                & (previous_end.dt.hour == 23)
            )
        )

        if valid_previous.any():

            idx = previous.index[
                valid_previous
            ]

            # 날짜는 현재 허가 날짜 유지
            # 시간만 이전 허가에서 가져온다.
            old_ts = work.loc[
                idx,
                "work_start_ts",
            ]

            source_ts = previous_start[
                valid_previous
            ]

            work.loc[
                idx,
                "work_start_ts",
            ] = (
                old_ts.dt.normalize()
                + pd.to_timedelta(
                    source_ts.dt.hour.values,
                    unit="h",
                )
                + pd.to_timedelta(
                    source_ts.dt.minute.values,
                    unit="m",
                )
            )

            work.loc[
                idx,
                "_needs_start_recovery",
            ] = False

        # 바로 이전 허가에서도 시간을 못 찾으면
        # 그 이전 허가를 계속 따라간다.
        next_prev = previous.map(
            work["prev_permit_id"]
        )

        work.loc[
            previous.index,
            "prev_permit_id",
        ] = next_prev.values

    return (
        work
        .drop(columns=["_needs_start_recovery"])
        .reset_index()
    )


def transform(df):

    # 1. 맨해튼
    df = df[
        df["boroughname"] == BOROUGH
    ].copy()

    # 2. 컬럼명 통일
    df = df.rename(columns={
        "permitnumber": "permit_id",
        "previouspermitnumber": "prev_permit_id",
        "permittypedesc": "permit_type",
        "permitstatusshortdesc": "status",
        "onstreetname": "on_street",
        "fromstreetname": "from_street",
        "tostreetname": "to_street",
        "wkt": "geom_wkt",
    })

    # 3. 날짜 변환
    df["work_start_ts"] = pd.to_datetime(
        df["issuedworkstartdate"],
        errors="coerce",
    )

    df["work_end_ts"] = pd.to_datetime(
        df["issuedworkenddate"],
        errors="coerce",
    )

    # 시작/종료가 없거나 기간이 이상한 데이터 제외
    df = df[
        df["work_start_ts"].notna()
        & df["work_end_ts"].notna()
        & (df["work_end_ts"] > df["work_start_ts"])
    ].copy()

    # 4. 취소 건 제외
    df = df[
        ~df["status"].isin(DROP_STATUS)
    ].copy()

    # 5. 행정성 허가 제외
    admin_pattern = "|".join(ADMIN_TYPES)

    df = df[
        ~df["permit_type"]
        .astype(str)
        .str.upper()
        .str.contains(admin_pattern, na=False)
    ].copy()

    # 6. 갱신 허가의 시작 시각 복구
    df = resolve_start_ts_chain(df)

    # 7. Traffic Score에 필요한 컬럼만 저장
    return df[[
        "permit_id",
        "permit_type",
        "on_street",
        "from_street",
        "to_street",
        "geom_wkt",
        "work_start_ts",
        "work_end_ts",
    ]].reset_index(drop=True)


def validate(df):

    if df.empty:
        raise ValueError(
            "construction Silver 결과가 비었습니다."
        )

    if df["permit_id"].isna().any():
        raise ValueError(
            "permit_id NULL 발생"
        )

    if not df["permit_id"].is_unique:
        raise ValueError(
            "permit_id 중복 발생"
        )

    logger.info(
        "공사 Silver 검증 완료: rows=%d",
        len(df),
    )


def main():

    run_date = os.getenv(
        "RUN_DATE",
        date.today().isoformat(),
    )

    logger.info(
        "공사 Silver 변환 시작: run_date=%s",
        run_date,
    )

    df = load_bronze(run_date)

    df = transform(df)

    validate(df)

    path = save_parquet(
        df,
        SILVER_DIR
        / SOURCE
        / f"dt={run_date}",
    )

    logger.info(
        "공사 Silver 완료: "
        "rows=%d columns=%d path=%s",
        len(df),
        len(df.columns),
        path,
    )


if __name__ == "__main__":
    main()