"""
TLC Gold — 세그먼트x평일시간대 통행량

TLC 하차(dropoff) 데이터를 "택시 수요"가 아니라 "일반적인 도로 교통량 프록시"로
간주하고, 세그먼트별로 평일 0~23시 각 시간대에 상대적으로 얼마나 붐비는지를
나타내는 Gold 테이블(dim_segment_tlc_volume)을 만든다.

자세한 배경은 docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md
참고.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, dayofweek, hour as hour_of_day

from src.common.config import BOROUGH_EVENT, SILVER_DIR, TAXI_TYPES
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_gold")

DIM_SEGMENT_TLC_VOLUME_PATH = SILVER_DIR / "dim_segment_tlc_volume.parquet"

HOURS = list(range(24))
DEFAULT_HOPS = 3


def _read_zone_hour_counts(
    spark: SparkSession,
    silver_dir: Path = SILVER_DIR,
    taxi_types: list[str] = TAXI_TYPES,
) -> pd.DataFrame:
    """TLC silver 파일 전부를 읽어 평일(월~금) 기준 zone x hour 하차수를 센다.

    매번 그 시점에 존재하는 파일 전부를 다시 읽어 처음부터 계산한다(전체
    재계산, 증분 아님). group by count는 파티션별 부분 집계 후 작은 결과만
    합치는 구조라 원본 규모(3년치, 약 140개 파일)와 무관하게 메모리 사용량이
    작다.
    """

    paths = [
        str(path)
        for taxi_type in taxi_types
        for path in sorted(silver_dir.glob(f"{taxi_type}_tripdata_*"))
    ]
    if not paths:
        raise FileNotFoundError(f"TLC silver 파일을 찾을 수 없습니다: {silver_dir}")

    logger.info(f"[tlc_gold] TLC silver 파일 {len(paths)}개 읽기 시작")

    df = spark.read.parquet(*paths).select("dropoff_datetime", "dropoff_location_id")

    # Spark의 dayofweek: 일요일=1 ~ 토요일=7. 평일(월~금) = 2~6.
    weekday = df.filter(dayofweek(col("dropoff_datetime")).between(2, 6))

    counted = (
        weekday
        .withColumn("hour", hour_of_day(col("dropoff_datetime")))
        .groupBy(col("dropoff_location_id").alias("zone_id"), "hour")
        .count()
        .withColumnRenamed("count", "dropoff_count")
    )

    result = counted.toPandas()
    result["zone_id"] = result["zone_id"].astype("int64")
    result["hour"] = result["hour"].astype("int64")
    result["dropoff_count"] = result["dropoff_count"].astype("int64")

    logger.info(f"[tlc_gold] zone x hour 집계 완료: {len(result)}행")
    return result


def _expand_zone_to_segment_hour(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
) -> pd.DataFrame:
    """zone x hour 하차수를 segment x hour로 펼친다.

    같은 zone에 속한 세그먼트는 zone 총합을 그대로 나눠 갖지 않고 동일하게
    받는다(세그먼트 수로 나누지 않음). 매치 안 된 시간대는 0으로 채워서
    세그먼트마다 정확히 24행을 보장한다.
    """

    segment_zone = map_zone_segment[["segment_id", "zone_id"]].copy()
    segment_zone["zone_id"] = segment_zone["zone_id"].astype("int64")

    hours = pd.DataFrame({"hour": HOURS})

    grid = segment_zone.merge(hours, how="cross")

    counts = zone_hour_counts.copy()
    counts["zone_id"] = counts["zone_id"].astype("int64")
    counts["hour"] = counts["hour"].astype("int64")

    merged = grid.merge(counts, on=["zone_id", "hour"], how="left")
    merged["dropoff_count_raw"] = merged["dropoff_count"].fillna(0).astype("int64")

    return merged[["segment_id", "hour", "dropoff_count_raw"]]


def _normalize_tlc_volume(df: pd.DataFrame) -> pd.DataFrame:
    """dropoff_count_raw를 전체 (segment_id, hour) 조합 기준 global percentile
    rank(0~1)로 정규화한다. dim_segment_traffic_score_v0의 demand_raw(중심성)를
    만들 때 쓴 방식과 동일하다 — 세그먼트/시간대별로 따로 rank하지 않고 전부
    하나로 묶어서 비교한다.
    """

    result = df.copy()
    result["tlc_volume"] = result["dropoff_count_raw"].rank(pct=True, method="average")
    return result


def build_dim_segment_tlc_volume(
    spark: SparkSession,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    silver_dir: Path = SILVER_DIR,
    taxi_types: list[str] = TAXI_TYPES,
    borough: str = BOROUGH_EVENT,
) -> str:
    """dim_segment_tlc_volume.parquet을 처음부터 다시 계산해서 저장한다.

    공사 허가 신청이 맨해튼 한정이라, map_zone_segment의 borough 컬럼으로
    맨해튼 세그먼트만 걸러서 쓴다. TLC silver 자체(팀 공용 코드)는 도시 전체를
    유지하고, 이 Gold 단계에서만 필터링한다.
    """

    zone_hour_counts = _read_zone_hour_counts(spark, silver_dir=silver_dir, taxi_types=taxi_types)

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id", "borough"])
    map_zone_segment = map_zone_segment.loc[map_zone_segment["borough"] == borough, ["segment_id", "zone_id"]]

    expanded = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment)
    result = _normalize_tlc_volume(expanded)

    out_path = silver_dir / "dim_segment_tlc_volume.parquet"
    silver_dir.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_path, index=False)

    logger.info(f"[tlc_gold] dim_segment_tlc_volume 저장 완료: {len(result)}행 -> {out_path}")
    return str(out_path)


def validate_dim_segment_tlc_volume(
    path: str,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    borough: str = BOROUGH_EVENT,
) -> str:
    """dim_segment_tlc_volume.parquet의 최소 불변식을 확인한다."""

    df = pd.read_parquet(path)

    assert not df.duplicated(subset=["segment_id", "hour"]).any(), "(segment_id, hour) 중복 발견"
    assert df["hour"].between(0, 23).all(), "hour가 0~23 범위를 벗어남"
    assert df["tlc_volume"].between(0, 1).all(), "tlc_volume이 0~1 범위를 벗어남"
    assert (df["dropoff_count_raw"] >= 0).all(), "dropoff_count_raw에 음수 있음"

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "borough"])
    segment_count = map_zone_segment.loc[map_zone_segment["borough"] == borough, "segment_id"].nunique()
    expected_rows = segment_count * len(HOURS)
    assert len(df) == expected_rows, f"행 수가 예상과 다릅니다: {len(df)} != {expected_rows}"

    hours_per_segment = df.groupby("segment_id")["hour"].nunique()
    assert (hours_per_segment == len(HOURS)).all(), "일부 세그먼트에 24개 시간대가 다 없음"

    logger.info(f"[tlc_gold] 검증 통과 ({len(df)}행)")
    return path


def _neighbor_hop_distances(
    segment_id: str,
    adjacency: pd.DataFrame,
    hops: int = DEFAULT_HOPS,
) -> dict[str, int]:
    """segment_id로부터 hops단계 이내(자기 자신 포함)의 세그먼트별 최단 hop 수를 구한다."""

    neighbor_map: dict[str, set[str]] = {}
    for seg, nbr in zip(adjacency["segment_id"], adjacency["neighbor_segment_id"]):
        neighbor_map.setdefault(seg, set()).add(nbr)

    distances: dict[str, int] = {segment_id: 0}
    frontier = {segment_id}

    for depth in range(1, hops + 1):
        next_frontier: set[str] = set()
        for seg in frontier:
            for nbr in neighbor_map.get(seg, set()):
                if nbr not in distances:
                    distances[nbr] = depth
                    next_frontier.add(nbr)
        if not next_frontier:
            break
        frontier = next_frontier

    return distances


def get_tlc_traffic_score_for_construction(
    segment_id: str,
    hour: int,
    hops: int = DEFAULT_HOPS,
    gold_path: Path = DIM_SEGMENT_TLC_VOLUME_PATH,
    adjacency_path: Path = GRAPH_SEGMENT_ADJACENCY_PATH,
) -> list[dict]:
    """공사 위치 segment_id + 인접 hops단계 이내 세그먼트들의 TLC 기반 점수.

    지금은 tlc_volume 하나만 반영한 임시 점수다. 나중에 팀 공용
    scoring/traffic_score.py가 다른 요인(중심성, capacity, event, closure)과
    합칠 때 이 값을 가져다 쓸 수 있다.
    """

    if not 0 <= hour <= 23:
        raise ValueError(f"hour는 0~23 범위여야 합니다: {hour}")

    gold = pd.read_parquet(gold_path)
    if segment_id not in gold["segment_id"].values:
        raise KeyError(f"segment_id를 찾을 수 없습니다: {segment_id}")

    adjacency = pd.read_parquet(adjacency_path, columns=["segment_id", "neighbor_segment_id"])
    hop_distances = _neighbor_hop_distances(segment_id, adjacency, hops=hops)

    hour_scores = gold[gold["hour"] == hour].set_index("segment_id")["tlc_volume"]

    results = [
        {
            "segment_id": seg,
            "hop_distance": dist,
            "hour": hour,
            "traffic_score": float(hour_scores.loc[seg]),
        }
        for seg, dist in hop_distances.items()
        if seg in hour_scores.index
    ]

    return sorted(results, key=lambda r: r["hop_distance"])


if __name__ == "__main__":
    from src.common.spark import get_spark

    spark_session = get_spark()
    try:
        out = build_dim_segment_tlc_volume(spark_session)
        validate_dim_segment_tlc_volume(out)
    finally:
        spark_session.stop()
