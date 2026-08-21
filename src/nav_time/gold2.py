"""
Gold2 — type1(시간) 최종 산출물 계산 + DynamoDB 포맷/upsert

30분 버킷별 평균 속도를 계산하고, LION 길이(length_ft)로 나눠 세그먼트별
통행시간(초)을 구한다. 세그먼트 전체 평균(AVG, fallback 2단계)도 같이
계산한다. DynamoDB에는 버킷 값과 AVG를 모두 upsert한다(설계 문서 7절).

단위: SPEED는 mph, length_ft는 feet. 시간(초) = (길이_ft / 5280) / 속도_mph * 3600.
"""

from __future__ import annotations

import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, concat, floor, hour, lpad, minute

from src.common.config import AVG_SORT_KEY, BUCKET_MINUTES
from src.common.dynamodb import batch_write_items
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_time_gold2")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0


def _bucket_column():
    bucket_minute = floor(minute("observed_at") / BUCKET_MINUTES) * BUCKET_MINUTES
    return concat(
        lpad(hour("observed_at").cast("string"), 2, "0"),
        lpad(bucket_minute.cast("int").cast("string"), 2, "0"),
    )


def compute_time_seconds(silver2_df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame:
    """(segment_id, speed, observed_at)를 30분 버킷별 평균 통행시간(초)으로 집계한다."""

    spark = silver2_df.sparkSession
    length_df = spark.createDataFrame(dim_segment_length_df[["segment_id", "length_ft"]])

    bucketed = silver2_df.withColumn("bucket", _bucket_column())

    bucket_avg_speed = (
        bucketed.groupBy("segment_id", "bucket")
        .agg(avg("speed").alias("avg_speed"))
    )

    joined = bucket_avg_speed.join(length_df, on="segment_id", how="inner")

    return joined.select(
        "segment_id",
        "bucket",
        (
            (col("length_ft") / _FEET_PER_MILE) / col("avg_speed") * _SECONDS_PER_HOUR
        ).alias("time_seconds"),
    )


def to_dynamodb_items(bucket_df: DataFrame) -> list[dict]:
    """버킷별 값 + 세그먼트별 평균(AVG)을 DynamoDB 항목 리스트로 변환한다."""

    rows = bucket_df.collect()

    items = [
        {"segment_id": row["segment_id"], "sk": row["bucket"], "value": round(row["time_seconds"])}
        for row in rows
    ]

    avg_df = bucket_df.groupBy("segment_id").agg(avg("time_seconds").alias("avg_time_seconds"))
    for row in avg_df.collect():
        items.append(
            {"segment_id": row["segment_id"], "sk": AVG_SORT_KEY, "value": round(row["avg_time_seconds"])}
        )

    return items


def write_to_dynamodb(items: list[dict], table_name: str) -> int:
    batch_write_items(table_name, items)
    logger.info(f"[nav_time_gold2] DynamoDB upsert 완료: table={table_name} count={len(items)}")
    return len(items)
