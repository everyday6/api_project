"""
Silver — road_closures + construction_work_hours 통합 (road_control_events)

road_closures Bronze의 각 폐쇄 레코드를 construction_work_hours(허가 + 작업
시간대 제약, src/construction_stipulations/silver.py)와 "도로명 일치 + 기간
겹침"으로 대조한다.

- 겹치면: 이미 construction_work_hours에 있는 같은 공사로 보고 road_closures
  쪽 행은 버린다. construction_work_hours가 permit_id/작업시간대 등 정보가
  더 풍부해서 굳이 중복으로 안 남긴다.
- 안 겹치면: construction 데이터에 없는 별개의 도로 통제(응급 보수, DOT 자체
  작업 등 permit을 안 거치는 활동)로 보고 결과에 남긴다.

construction_work_hours 전체 + road_closures 중 매칭 안 된 것만 합쳐서 하나의
통합 테이블(road_control_events)을 만든다. control_type 컬럼으로 둘을 구분한다.

매칭 기준: on_street + from_street + to_street(구간) 일치 + 기간 겹침.

처음엔 on_street(도로명)만 보고 기간 겹침만 확인했는데, 실측해보니 매칭된 쌍의
94.7%가 같은 도로의 서로 다른 구간이었다(예: BROADWAY 위쪽 공사와 아래쪽 공사가
그냥 같은 도로라는 이유로 매칭됨) — 이러면 "같은 공사"가 아니라 그냥 같은
도로에서 벌어진 무관한 공사를 잘못 합치는 꼴이라 구간까지 정확히 맞추도록
강화했다. 대신 road_closures가 to_street를 비워두는 경우가 꽤 있어서, 그런
행은 애초에 구간이 안 맞아 전부 "별개 통제"로 분류된다 — 매칭 재현율은
낮아지지만(놓치는 게 늘어남), 잘못된 매칭(같은 공사로 오판)은 크게 줄어든다.
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import clean_street, save_parquet
from src.road_closures.bronze import latest_bronze_file

logger = get_logger(__name__, log_to_file=True, log_file_stem="road_control_events")

OUT_SOURCE = "road_control_events"
CONSTRUCTION_WORK_HOURS_DIR = SILVER_DIR / "construction_work_hours"

RC_READ_COLS = [
    "onstreetname", "fromstreetname", "tostreetname",
    "workstartdate", "workenddate", "purpose", "boroughname", "wkt",
]


def load_construction_work_hours(run_date: str) -> pd.DataFrame:
    path = CONSTRUCTION_WORK_HOURS_DIR / f"dt={run_date}" / "data.parquet"
    return pd.read_parquet(path)


def load_road_closures() -> pd.DataFrame:
    """
    road_closures는 ingest_weekly에서 주 단위로 갱신되는 별도 DAG라, 여기서는
    그 시점 기준 가장 최근에 받아둔 스냅샷을 그냥 읽는다(cross-DAG 의존 없이).
    """
    path = latest_bronze_file()
    if path is None:
        raise FileNotFoundError("road_closures bronze 파일이 없습니다 — ingest_weekly가 아직 안 돈 것 같습니다.")

    df = pd.read_parquet(path, columns=RC_READ_COLS)
    df = df.rename(columns={
        "onstreetname": "on_street",
        "fromstreetname": "from_street",
        "tostreetname": "to_street",
        "workstartdate": "work_start_ts",
        "workenddate": "work_end_ts",
        "wkt": "geom_wkt",
    })

    # construction Silver와 동일한 규칙으로 도로명 정규화 — 이게 유일한 JOIN 키다.
    for col in ["on_street", "from_street", "to_street"]:
        df[col] = df[col].map(clean_street)

    df["work_start_ts"] = pd.to_datetime(df["work_start_ts"], errors="coerce")
    df["work_end_ts"] = pd.to_datetime(df["work_end_ts"], errors="coerce")

    return df


def build(construction_work_hours: pd.DataFrame, road_closures: pd.DataFrame) -> pd.DataFrame:
    road_closures = road_closures.reset_index(drop=True).reset_index(names="_rc_id")

    # on_street + from_street + to_street(구간)까지 같아야 후보로 모으고,
    # 그중 기간이 겹치는 것만 "같은 공사"로 확정한다.
    # 날짜가 NaT인 행은 겹침 비교가 항상 False라 매칭 안 됨(누락으로 처리, 오탐 방지).
    candidates = road_closures.merge(
        construction_work_hours[["permit_id", "on_street", "from_street", "to_street", "work_start_ts", "work_end_ts"]],
        on=["on_street", "from_street", "to_street"], how="inner", suffixes=("_rc", "_c"),
    )
    overlaps = (
        (candidates["work_start_ts_rc"] <= candidates["work_end_ts_c"])
        & (candidates["work_end_ts_rc"] >= candidates["work_start_ts_c"])
    )
    matched_rc_ids = set(candidates.loc[overlaps, "_rc_id"])

    unmatched_rc = road_closures[~road_closures["_rc_id"].isin(matched_rc_ids)].drop(columns=["_rc_id"])

    construction_out = construction_work_hours.copy()
    construction_out["control_type"] = "construction"

    other_out = unmatched_rc.copy()
    other_out["control_type"] = "other_road_control"

    combined = pd.concat([construction_out, other_out], ignore_index=True, sort=False)

    logger.info(
        "road_closures %d건 중 construction과 겹쳐서 제외된 건수: %d, 별개로 남은 건수: %d",
        len(road_closures), len(matched_rc_ids), len(unmatched_rc),
    )

    return combined


def validate(df: pd.DataFrame, construction_rows: int, road_closures_rows: int) -> None:
    if df.empty:
        raise ValueError("road_control_events Silver 결과가 비었습니다.")

    # construction 쪽은 전부 살아남아야 하고(제외 로직이 없음), road_closures는
    # 매칭된 만큼만 줄어들 수 있다 — 그래서 최소 construction_rows개는 항상 있어야 한다.
    if len(df) < construction_rows:
        raise ValueError(
            f"결과 행수({len(df)})가 construction_work_hours 행수({construction_rows})보다 적음 — 병합 오류 가능성"
        )

    if len(df) > construction_rows + road_closures_rows:
        raise ValueError("결과 행수가 두 원본의 합보다 많음 — 병합 오류 가능성")

    logger.info(
        "road_control_events Silver 검증 완료: rows=%d (construction=%d, road_closures=%d)",
        len(df), construction_rows, road_closures_rows,
    )
    logger.info("control_type 분포:\n%s", df["control_type"].value_counts(dropna=False).to_string())


def main(run_date: str | None = None) -> str:
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("road_control_events Silver 변환 시작: run_date=%s", run_date)

    construction_work_hours = load_construction_work_hours(run_date)
    road_closures = load_road_closures()

    df = build(construction_work_hours, road_closures)
    validate(df, construction_rows=len(construction_work_hours), road_closures_rows=len(road_closures))

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "road_control_events Silver 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


if __name__ == "__main__":
    main()
