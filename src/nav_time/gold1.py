"""
Gold1 — 최근 N일 윈도우 + 유효 속도만 남긴다.

type1(시간) 버킷 평균 계산의 입력을 좁힌다: 너무 오래된 판독값과
0 이하(또는 비정상적으로 낮은) 속도 판독값은 제외한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

from src.common.config import MIN_VALID_SPEED_MPH, ROLLING_WINDOW_DAYS


def filter_recent_valid_speed(
    df: DataFrame,
    as_of: datetime,
    window_days: int = ROLLING_WINDOW_DAYS,
) -> DataFrame:
    cutoff = as_of - timedelta(days=window_days)

    return df.filter(
        (col("observed_at") >= lit(cutoff)) & (col("speed") >= MIN_VALID_SPEED_MPH)
    )
