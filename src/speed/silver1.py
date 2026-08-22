"""
Silver1 변환: 속도 Bronze -> 정제된 판독값

결측치 제거(speed/link_points 없는 행), 필요 컬럼 프루닝, 컬럼명/타입
통일만 한다. LION 세그먼트 매핑(Silver2)이나 시간대 집계(Gold1/Gold2)는
여기서 하지 않는다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_timestamp


def clean_speed_silver1(df: DataFrame) -> DataFrame:
    """speed/link_points 결측 행을 제거하고 컬럼명·타입을 통일한다."""

    cleaned = (
        df.filter(col("speed").isNotNull() & col("link_points").isNotNull())
        .withColumn("speed", col("speed").cast("double"))
        .withColumn("observed_at", to_timestamp(col("data_as_of")))
        .select("link_id", "link_points", "speed", "observed_at")
    )

    return cleaned
