"""
Silver1 — road_closures Bronze 스냅샷을 정제한다(컬럼명 변경, 도로명 정규화,
날짜 캐스팅). construction과의 겹침 판단(conflation)은
src/silver2/road_closure_construction_conflation.py에서 한다.
"""

from __future__ import annotations

import pandas as pd

from src.common.logger import get_logger
from src.common.utils import clean_street
from src.road_closures.bronze import latest_bronze_file

logger = get_logger(__name__, log_to_file=True, log_file_stem="road_control_events")

RC_READ_COLS = [
    "onstreetname", "fromstreetname", "tostreetname",
    "workstartdate", "workenddate", "purpose", "boroughname", "wkt",
]


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

    # construction Silver1과 동일한 규칙으로 도로명 정규화 — 이게 유일한 JOIN 키다.
    for col in ["on_street", "from_street", "to_street"]:
        df[col] = df[col].map(clean_street)

    df["work_start_ts"] = pd.to_datetime(df["work_start_ts"], errors="coerce")
    df["work_end_ts"] = pd.to_datetime(df["work_end_ts"], errors="coerce")

    return df
