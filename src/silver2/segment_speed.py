"""
Silver2 — 속도 링크 판독값을 LION segment로 펼친다.

distinct link(수천 개 규모)만 pandas/geopandas(segment_speed_match)로
매칭하고, 그 결과(작은 매핑 테이블)를 Spark 조인으로 훨씬 큰 시계열
속도 판독값에 펼친다. 한 링크가 여러 segment에 매핑되면 그 판독값도
그만큼 여러 행으로 복제된다(각 segment가 그 시각 그 속도를 관측했다고
취급).
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame

from src.common.logger import get_logger
from src.silver2.segment_speed_match import match_links_to_segments

logger = get_logger(__name__, log_to_file=True, log_file_stem="segment_speed_silver2")


def build_segment_speed_silver2(speed_silver1_df: DataFrame, dim_segment_df: pd.DataFrame) -> DataFrame:
    """속도 Silver1(link 단위)을 segment 단위로 펼친 Silver2를 만든다."""

    spark = speed_silver1_df.sparkSession

    distinct_links = (
        speed_silver1_df.select("link_id", "link_points").distinct().toPandas()
    )

    mapping_pdf = match_links_to_segments(distinct_links, dim_segment_df)

    logger.info(
        f"[segment_speed_silver2] distinct_links={len(distinct_links)} mapped_rows={len(mapping_pdf)}"
    )

    if mapping_pdf.empty:
        return spark.createDataFrame([], schema="segment_id string, speed double, observed_at timestamp")

    mapping_df = spark.createDataFrame(mapping_pdf[["link_id", "segment_id"]])

    return (
        speed_silver1_df.join(mapping_df, on="link_id", how="inner")
        .select("segment_id", "speed", "observed_at")
    )
