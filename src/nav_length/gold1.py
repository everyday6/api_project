"""
Gold1 — LION dim_segment 중 실제 서빙 가능한 세그먼트만 남긴다.

type2(길이) 값은 routable하지 않은(차량 통행 불가) 세그먼트나 길이가 0인
세그먼트에는 의미가 없으므로 걸러낸다. EMR Serverless Spark job
(spark_jobs/nav_length_job.py)이 이 함수를 호출한다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col


def filter_routable_segments(df: DataFrame) -> DataFrame:
    """routable하고 길이가 0보다 큰 세그먼트만 (segment_id, length_ft)로 남긴다."""

    return (
        df.filter(col("is_routable") & (col("length_ft") > 0))
        .select("segment_id", "length_ft")
    )
