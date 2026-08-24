"""
Gold2 — type1(시간) 최종 산출물 계산 + RDS 포맷/upsert

30분 버킷 하나엔 그 30분 동안 들어온 5분 단위 판독값이 최대 6개 있다.
시간순으로 1,2,...,n번째 판독값에 1:2:...:n 비율로 증가하는 가중치(최근
값이 가장 큰 비중)를 준 가중평균 속도를 구하고, LION 길이(length_ft)로
나눠 세그먼트별 통행시간(초)을 구한다. 세그먼트 전체 평균(AVG)은 세그먼트당
버킷을 한 번에 하나씩만 계산하는 구조에 맞춰, 이번 실행에서 바뀐 버킷 하나
만큼만 증분 갱신한다(설계 문서 7절). RDS(src/common/rds.py)에는 버킷 값과
AVG를 모두 upsert한다 — 버킷 값에는 원본 데이터 날짜(collected_date)와
신선도 판단용 타임스탬프(observed_at)도 함께 저장한다.

compute_spec_travel_seconds/spec_estimate_items는 별개 흐름이다 - 실시간
속도 판독값이 아니라 LION 도로 스펙(길이/제한속도)만으로 통과시간을
추정해서, type1 fallback 체인의 3단계(SPEC Estimate, src/serving/nav_lookup.py
참고)로 쓴다. segment_length_pipeline이 LION 갱신 때(quarterly)마다
다시 계산해서 쓴다 - 실시간 버킷 계산(30분 주기)과는 트리거 자체가 다르다.

단위: SPEED는 mph, length_ft는 feet. 시간(초) = (길이_ft / 5280) / 속도_mph * 3600.
"""

from __future__ import annotations

import pandas as pd
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

from src.common import gold_snapshot, rds
from src.common.config import AVG_SORT_KEY, BUCKET_MINUTES, SPEC_SORT_KEY
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_time_gold2")

_FEET_PER_MILE = 5280.0
_SECONDS_PER_HOUR = 3600.0

# 하루를 30분 단위로 나눈 버킷 수(00:00~23:30 -> 48개). AVG 증분 갱신 시
# count의 상한으로 쓴다.
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
    날짜다 — DynamoDB에 저장된 버킷 값이 며칠자 원본 데이터로 계산됐는지 표시하기
    위함(docs/superpowers/specs/2026-08-24-type1-collected-date-design.md).

    observed_at(타임스탬프 그대로, 날짜로 안 자름)은 별개 목적이다 — 서빙
    쪽(nav_lookup._is_fresh)이 "이 버킷 값이 지금으로부터 몇 초 전 데이터인지"를
    자동으로 판단하는 데 쓴다. collected_date(날짜, 사람이 보는 디버깅용)와
    같은 원본 컬럼에서 나오지만 용도가 달라 둘 다 남긴다.
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
            spark_max("observed_at").alias("observed_at"),
            to_date(spark_max("observed_at")).alias("collected_date"),
        )
        .filter(col("avg_speed") > 0)
    )

    joined = bucket_avg_speed.join(length_df, on="segment_id", how="inner")

    return joined.select(
        "segment_id",
        "bucket",
        "collected_date",
        "observed_at",
        (
            (col("length_ft") / _FEET_PER_MILE) / col("avg_speed") * _SECONDS_PER_HOUR
        ).alias("time_seconds"),
    )


def to_type1_items(bucket_df: DataFrame, table_name: str) -> list[dict]:
    """버킷별 값 + 세그먼트별 평균(AVG, 증분 갱신)을 RDS 행 리스트로 변환한다.

    bucket_df는 compute_time_seconds의 반환값이어야 한다 — segment_id/bucket/
    time_seconds뿐 아니라 collected_date(DateType), observed_at(TimestampType)
    컬럼도 필수다. 버킷 항목에는 collected_date를 ISO 날짜 문자열로,
    observed_at을 epoch seconds로 같이 실어 보내고, AVG 항목에는 둘 다
    안 붙인다(AVG는 여러 버킷의 누적값이라 특정 시각으로 대표할 수 없음).

    AVG는 세그먼트의 (최대 BUCKETS_PER_DAY개) 버킷 전체 평균이어야 하는데,
    이번 실행은 세그먼트당 버킷을 하나만 계산한다. 48개를 매번 다 읽는 대신,
    바뀐 버킷 하나만큼만 평균에 반영하는 증분 갱신 공식을 쓴다:
      - 그 버킷이 처음 생기는 거면(기존 값 없음):
          new_avg = old_avg + (new_value - old_avg) / new_count   (new_count = old_count + 1)
      - 이미 있던 버킷 값을 교체하는 거면(count를 아는 경우):
          new_avg = old_avg + (new_value - old_bucket_value) / count   (count는 그대로)
      - 이미 있던 버킷 값을 교체하는데 count를 모르는 경우(레거시 AVG, count 필드가
        없던 옛 버전이 저장한 레코드): 몇 개로 만들어진 평균인지 알 수 없어 기존
        값을 델타에 섞을 수 없으므로, 그 값을 버리고 new_value로 리셋한다.
    """

    rows = bucket_df.collect()

    bucket_items = [
        {
            "segment_id": row["segment_id"],
            "sk": row["bucket"],
            "value": round(row["time_seconds"]),
            "collected_date": row["collected_date"].isoformat(),
            "observed_at": row["observed_at"].timestamp(),
        }
        for row in rows
    ]

    if not bucket_items:
        return []

    lookup_keys = list({
        (item["segment_id"], item["sk"])
        for item in bucket_items
    } | {
        (item["segment_id"], AVG_SORT_KEY)
        for item in bucket_items
    })
    existing = rds.batch_get_rows(table_name, lookup_keys)

    # 세그먼트별 현재 (avg, count) 상태 — 한 배치 안에 같은 세그먼트의 버킷이
    # 여러 개 섞여 있어도(예: 수집 구간 경계 겹침) 순차적으로 접어(fold) 반영한다.
    running_state: dict[str, tuple[float, int]] = {}

    def _current_avg_count(sid: str) -> tuple[float, int]:
        if sid in running_state:
            return running_state[sid]
        old_avg_item = existing.get((sid, AVG_SORT_KEY))
        old_avg = float(old_avg_item["value"]) if old_avg_item else 0.0
        # RDS(Postgres)에서 count는 NULL 가능 컬럼이라, 레거시 AVG 행은 키가
        # 아예 없는 게 아니라 값이 None으로 들어온다(dict.get(key, default)는
        # 키가 있고 값이 None이면 default를 안 돌려주므로 명시적으로 확인해야
        # int(None)에서 안 터진다).
        old_count = (
            int(old_avg_item["count"])
            if old_avg_item and old_avg_item.get("count") is not None
            else 0
        )
        return old_avg, old_count

    for item in bucket_items:
        sid, sk, new_value = item["segment_id"], item["sk"], item["value"]

        old_avg, old_count = _current_avg_count(sid)
        old_bucket_item = existing.get((sid, sk))

        if old_bucket_item is None:
            new_count = min(old_count + 1, BUCKETS_PER_DAY)
            new_avg = old_avg + (new_value - old_avg) / new_count
        elif old_count == 0:
            # 레거시 AVG(count 없음)인데 버킷 값은 이미 있는 경우. old_count를 1로
            # 우겨서 델타를 그대로 반영하면 old_avg를 "1개짜리 평균"으로 오인해
            # 매번 큰 델타가 그대로 반영되고, 평균이 무한정 발산한다(음수까지 감).
            # 몇 개로 만들어진 평균인지 모르므로 리셋한다.
            new_count = 1
            new_avg = new_value
        else:
            old_bucket_value = float(old_bucket_item.get("value", 0))
            new_count = old_count
            new_avg = old_avg + (new_value - old_bucket_value) / new_count

        running_state[sid] = (new_avg, new_count)

    avg_items = [
        {
            "segment_id": sid,
            "sk": AVG_SORT_KEY,
            "value": round(final_avg),
            "count": final_count,
        }
        for sid, (final_avg, final_count) in running_state.items()
    ]

    return bucket_items + avg_items


def write_to_rds(items: list[dict], table_name: str) -> int:
    """RDS에 upsert하고, 성공하면 S3 Gold 스냅샷도 최신 상태로 다시
    내보낸다(src/serving/nav_lookup.py의 RDS 장애 폴백이 읽는 것).
    스냅샷 갱신 자체가 실패해도 RDS 쓰기는 이미 끝난 뒤라 파이프라인을
    실패시키지 않는다 - 다음 정상 실행 때 다시 시도되면 충분하다."""
    rds.ensure_table(table_name)
    count = rds.upsert_items(items, table_name)
    logger.info(f"[nav_time_gold2] RDS upsert 완료: table={table_name} count={count}")

    try:
        snapshot = rds.export_snapshot_source(table_name)
        gold_snapshot.write_snapshot("type1", snapshot)
    except Exception:
        logger.exception("[nav_time_gold2] S3 Gold 스냅샷 갱신 실패(RDS 쓰기 자체는 성공)")

    return count


def compute_spec_travel_seconds(dim_segment_df: pd.DataFrame) -> pd.DataFrame:
    """type1 3단계 폴백(SPEC Estimate) 소스. 실시간 속도 판독값 없이 도로
    스펙(길이/제한속도)만으로 통과시간을 추정한다 - Exact/Historical AVG가
    둘 다 없을 때(신규 segment, 장기 장애 등) 쓰는 값이라 세그먼트당 정적으로
    하나만 있으면 된다.

    length_ft나 speed_limit_mph가 없거나(제한속도 미표기 segment가 실측
    기준 약 32%) 0 이하면 추정 자체가 불가능하므로 그 segment는 결과에서
    빠진다 - 값을 지어내지 않고, nav_lookup이 다음 폴백 단계(코드 상수)로
    넘어가게 한다."""

    valid = dim_segment_df[
        dim_segment_df["length_ft"].notna()
        & (dim_segment_df["length_ft"] > 0)
        & dim_segment_df["speed_limit_mph"].notna()
        & (dim_segment_df["speed_limit_mph"] > 0)
    ]

    spec_travel_time_sec = (
        (valid["length_ft"] / _FEET_PER_MILE) / valid["speed_limit_mph"] * _SECONDS_PER_HOUR
    )

    return pd.DataFrame({
        "segment_id": valid["segment_id"],
        "spec_travel_time_sec": spec_travel_time_sec,
    })


def spec_estimate_items(spec_df: pd.DataFrame) -> list[dict]:
    """SPEC Estimate를 RDS 행으로 변환한다. type2(길이)처럼 시간과
    무관한 정적 값이라 세그먼트당 항목 1개, sort key는 고정 SPEC_SORT_KEY다."""

    return [
        {"segment_id": row["segment_id"], "sk": SPEC_SORT_KEY, "value": round(row["spec_travel_time_sec"])}
        for row in spec_df.to_dict("records")
    ]
