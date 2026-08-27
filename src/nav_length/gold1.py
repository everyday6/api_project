"""
Gold1 — LION dim_segment 중 실제 서빙 가능한 세그먼트만 남긴다.

type2(길이) 값은 길이가 0인 세그먼트에는 의미가 없으므로 걸러낸다.
Airflow 태스크(dags/segment_length_pipeline.py)가 이 함수를 직접 호출한다 -
10~30만 행 수준이라 EMR Serverless 없이 pandas로 충분하다(2026-08-25
Spark/EMR -> pandas 전환).
"""

from __future__ import annotations

import pandas as pd


def filter_valid_length_segments(df: pd.DataFrame) -> pd.DataFrame:
    """길이가 0보다 큰 세그먼트만 (segment_id, length_ft)로 남긴다."""

    return df.loc[df["length_ft"] > 0, ["segment_id", "length_ft"]]
