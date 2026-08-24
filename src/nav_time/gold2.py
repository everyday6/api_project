"""
Gold2 — type1(시간) 최종 산출물 계산 + RDS 포맷/upsert

30분 버킷 하나엔 그 30분 동안 들어온 5분 단위 판독값이 최대 6개 있다.
시간순으로 1,2,...,n번째 판독값에 1:2:...:n 비율로 증가하는 가중치(최근
값이 가장 큰 비중)를 준 가중평균 속도를 구하고, LION 길이(length_ft)로
나눠 세그먼트별 통행시간(초)을 구한다.

과거 평균(avg)은 세그먼트 전체가 아니라 "이 (segment_id, time) 슬롯"
단위다 - 한 행 안에 오늘 실측값(value)과 그 슬롯의 과거 평균(avg)이 같이
있어서, 서빙 쪽(src/serving/nav_lookup.py)이 "오늘 값이 있으면 그걸,
없으면 평균을" 판단을 조회 한 번으로 끝낼 수 있다. 이번 실행에서 바뀐
슬롯 하나만큼만 증분 갱신한다(48개 슬롯을 매번 다 다시 읽지 않음).

RDS에는 슬롯 값과 그 슬롯의 avg를 함께 upsert하고, 성공하면 S3 Gold
스냅샷도 갱신한다(src/common/gold_snapshot.py) - DynamoDB에서 RDS로
옮기며 잃은 멀티 AZ 자동 failover를 보완하기 위해, RDS 자체가 응답
불가능할 때 서빙 쪽이 이 스냅샷으로 대체한다(src/serving/nav_lookup.py 참고).

단위: SPEED는 mph, length_ft는 feet. 시간(초) = (길이_ft / 5280) / 속도_mph * 3600.
"""

from __future__ import annotations

from datetime import date

import pandas as pd
from psycopg2 import sql
from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import (
    col,
    concat,
    count as spark_count,
    floor,
    hour,
    lpad,
    max as spark_max,
    minute,
    row_number,
    sum as spark_sum,
    to_date,
)

from src.common import gold_snapshot
from src.common.config import BUCKET_MINUTES, SERVING_TABLE_TYPE1_KEY_COLUMNS
from src.common.db import batch_get_items, batch_write_items, get_shared_connection
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_time_gold2")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0

# 슬롯별 증분 평균 갱신 시 count의 상한. 하루 슬롯 수(48개)를 그대로 쓴다 -
# 그 이상 쌓아봐야 과거 데이터의 반영 비중이 무의미하게 작아지기만 한다.
BUCKETS_PER_DAY = 24 * 60 // BUCKET_MINUTES


def _bucket_column():
    bucket_minute = floor(minute("observed_at") / BUCKET_MINUTES) * BUCKET_MINUTES
    return concat(
        lpad(hour("observed_at").cast("string"), 2, "0"),
        lpad(bucket_minute.cast("int").cast("string"), 2, "0"),
    )


def compute_time_seconds(silver2_df: DataFrame, dim_segment_length_df: pd.DataFrame) -> DataFrame:
    """(segment_id, speed, observed_at)를 30분 버킷별 가중평균 통행시간(초)으로 집계한다.

    한 버킷 안에서 시간순으로 매긴 순위(rank)를 가중치로 쓴다 — n개 판독값이면
    1:2:...:n 비율(최근 값일수록 크게), 삼각수 n*(n+1)/2로 정규화한다.

    collected_date는 그 버킷을 구성한 판독값들의 observed_at 중 가장 최근 값의
    날짜다 — RDS에 저장된 버킷 값이 며칠자 원본 데이터로 계산됐는지 표시하고,
    서빙 쪽이 "오늘 값인지"(freshness) 판단하는 데도 그대로 쓰인다.
    """

    spark = silver2_df.sparkSession
    length_df = spark.createDataFrame(dim_segment_length_df[["segment_id", "length_ft"]])

    bucketed = silver2_df.withColumn("bucket", _bucket_column())

    window_spec = Window.partitionBy("segment_id", "bucket").orderBy("observed_at")
    ranked = bucketed.withColumn("rank", row_number().over(window_spec))

    counts = ranked.groupBy("segment_id", "bucket").agg(spark_count("*").alias("n"))
    ranked = ranked.join(counts, on=["segment_id", "bucket"])

    weighted = ranked.withColumn(
        "weighted_speed",
        col("speed") * col("rank") / (col("n") * (col("n") + 1) / 2),
    )

    bucket_avg_speed = (
        weighted.groupBy("segment_id", "bucket")
        .agg(
            spark_sum("weighted_speed").alias("avg_speed"),
            to_date(spark_max("observed_at")).alias("collected_date"),
        )
        .filter(col("avg_speed") > 0)
    )

    joined = bucket_avg_speed.join(length_df, on="segment_id", how="inner")

    return joined.select(
        "segment_id",
        "bucket",
        "collected_date",
        (
            (col("length_ft") / _FEET_PER_MILE) / col("avg_speed") * _SECONDS_PER_HOUR
        ).alias("time_seconds"),
    )


def to_serving_items(bucket_df: DataFrame, table_name: str, *, today: date | None = None) -> list[dict]:
    """버킷별 값을 RDS 항목 리스트로 변환한다 - 슬롯별 과거 평균(avg)도
    증분 갱신해서 같은 행에 같이 싣는다.

    bucket_df는 compute_time_seconds의 반환값이어야 한다 - segment_id/bucket/
    time_seconds뿐 아니라 collected_date(DateType) 컬럼도 필수다.

    avg는 이 (segment_id, bucket) 슬롯 자체의 과거 평균이다(세그먼트 전체
    평균이 아니다) - 증분 갱신 공식:
      - 이 슬롯에 기존 값이 없으면: new_avg = old_avg + (new_value - old_avg) / new_count
        (new_count = min(old_count + 1, BUCKETS_PER_DAY))
      - 이미 있던 슬롯 값을 교체하는 거면(count를 아는 경우):
        new_avg = old_avg + (new_value - old_value) / count (count는 그대로)
      - count를 모르는 레거시 행(예전 스키마가 남긴 값)이면 몇 개로 만들어진
        평균인지 알 수 없어 리셋한다(new_avg = new_value, new_count = 1).

    today는 collected_date/updated_date에 쓸 오늘 날짜다 - 테스트에서 고정된
    날짜로 검증하기 위해 인자로 받고, 안 넘기면 실제 오늘을 쓴다.
    """
    today = (today or date.today()).isoformat()

    rows = bucket_df.collect()

    new_items = [
        {
            "segment_id": row["segment_id"],
            "time": row["bucket"],
            "value": round(row["time_seconds"]),
            "collected_date": row["collected_date"].isoformat(),
        }
        for row in rows
    ]

    if not new_items:
        return []

    lookup_keys = [
        {"segment_id": item["segment_id"], "time": item["time"]} for item in new_items
    ]
    existing = batch_get_items(table_name, lookup_keys)

    items = []
    for item in new_items:
        key = (item["segment_id"], item["time"])
        old_row = existing.get(key)
        old_avg = float(old_row.get("avg", 0)) if old_row else 0.0
        old_count = int(old_row["count"]) if old_row and old_row.get("count") is not None else 0
        new_value = item["value"]

        if old_row is None:
            new_count = min(old_count + 1, BUCKETS_PER_DAY)
            new_avg = old_avg + (new_value - old_avg) / new_count
        elif old_count == 0:
            # count 없는 레거시 행(값은 있는데 몇 개로 만들어진 평균인지
            # 모름) - 델타를 섞으면 평균이 발산하므로 리셋한다.
            new_count = 1
            new_avg = new_value
        else:
            old_value = float(old_row.get("value", 0))
            new_count = old_count
            new_avg = old_avg + (new_value - old_value) / new_count

        items.append({
            **item,
            "avg": round(new_avg),
            "count": new_count,
            "updated_date": today,
        })

    return items


def _export_snapshot(table_name: str) -> dict[str, dict[str, dict]]:
    """S3 Gold 스냅샷(gold_snapshot.py)에 실어보낼 원본을 RDS 전체에서 뽑는다.

    부분 병합이 아니라 매번 테이블 전체를 다시 내보낸다 - 이 파이프라인
    하나만 이 스냅샷 파일을 쓰므로 경합 문제는 없지만, "RDS 현재 상태를
    그대로 다시 내보내기"가 어차피 제일 단순하고 안전하다. 반환값은
    segment_id -> {time: {"value","avg","collected_date"}}."""
    conn = get_shared_connection()
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT segment_id, time, value, avg, collected_date FROM {table}").format(
                table=sql.Identifier(table_name)
            )
        )
        rows = cur.fetchall()

    snapshot: dict[str, dict[str, dict]] = {}
    for segment_id, time_slot, value, avg, collected_date in rows:
        entry = {}
        if value is not None and collected_date is not None:
            entry["value"] = float(value)
            entry["collected_date"] = collected_date.isoformat()
        if avg is not None:
            entry["avg"] = float(avg)
        if entry:
            snapshot.setdefault(segment_id, {})[time_slot] = entry
    return snapshot


def write_to_rds(items: list[dict], table_name: str) -> int:
    """RDS(PostgreSQL)에 upsert하고, 성공하면 S3 Gold 스냅샷도 최신
    상태로 다시 내보낸다(src/serving/nav_lookup.py의 RDS 장애 폴백이
    읽는 것). 스냅샷 갱신 자체가 실패해도 RDS 쓰기는 이미 끝난 뒤라
    파이프라인을 실패시키지 않는다 - 다음 정상 실행 때 다시 시도되면
    충분하다."""
    batch_write_items(table_name, items, key_columns=SERVING_TABLE_TYPE1_KEY_COLUMNS)
    logger.info(f"[nav_time_gold2] RDS upsert 완료: table={table_name} count={len(items)}")

    try:
        snapshot = _export_snapshot(table_name)
        gold_snapshot.write_snapshot("type1", snapshot)
    except Exception:
        logger.exception("[nav_time_gold2] S3 Gold 스냅샷 갱신 실패(RDS 쓰기 자체는 성공)")

    return len(items)
