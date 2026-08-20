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

from src.common.config import BOROUGH_EVENT, GOLD_DIR, SILVER_DIR, TAXI_TYPES
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.mapping.segment_spatial_weight import MAP_SEGMENT_SPATIAL_WEIGHT_PATH
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_gold")

DIM_SEGMENT_TLC_VOLUME_PATH = GOLD_DIR / "dim_segment_tlc_volume.parquet"

HOURS = list(range(24))
DEFAULT_HOPS = 3

# map_zone_segment.parquet은 LION 분기별 갱신(dags/lion_pipeline.py)마다 자동으로
# 다시 만들어지지만, map_segment_spatial_weight.parquet은 정적 스냅샷이라(2016년
# 한 해 데이터 기반, DAG 없음) 함께 갱신되지 않는다. 그래서 LION이 새 세그먼트를
# 추가하면 spatial_weight 테이블에는 아직 없는 상태가 생길 수 있다. 소수는
# _expand_zone_to_segment_hour의 1.0 폴백으로 안전하게 넘어가지만, spatial_weight
# 테이블이 여러 분기 방치돼 결측 비율이 커지면 그 폴백이 zone 총합을 크게 부풀린다
# — 이를 하드 실패로 막는 기준선. 정성적 초안이다(TODO, 팀 검토 필요).
MAX_MISSING_SPATIAL_WEIGHT_FRACTION = 0.05


def collect_zone_hour_counts(
    spark: SparkSession,
    silver_dir: Path = SILVER_DIR,
    taxi_types: list[str] = TAXI_TYPES,
) -> pd.DataFrame:
    """TLC silver 파일 전부를 읽어 평일(월~금) 기준 zone x hour 하차수를 센다.

    매번 그 시점에 존재하는 파일 전부를 다시 읽어 처음부터 계산한다(전체
    재계산, 증분 아님). group by count는 파티션별 부분 집계 후 작은 결과만
    합치는 구조라 원본 규모(3년치, 약 140개 파일)와 무관하게 메모리 사용량이
    작다.

    무거운(3년치 전체 스캔) 부분이라 build_dim_segment_tlc_volume과 별도
    태스크로 분리돼 있다. 반환값은 zone x hour 조합(최대 수천 행) 정도로
    작아서 XCom으로 다음 태스크에 그대로 넘길 수 있다.
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

    # 실 데이터의 dropoff_location_id는 nullable(src/tlc/transform.py의
    # SILVER_SCHEMA)이고 결측치를 삭제하지 않는다. Spark groupBy는 NULL도
    # 자기 그룹으로 유지하므로, 여기서 걸러내지 않으면 zone_id 컬럼에 NaN이
    # 남아 바로 아래 int64 캐스팅이 깨진다.
    null_zone = result["zone_id"].isna()
    if null_zone.any():
        dropped = int(result.loc[null_zone, "dropoff_count"].sum())
        logger.warning(f"[tlc_gold] dropoff_location_id 결측으로 제외: {dropped}건")
        result = result.loc[~null_zone].copy()

    result["zone_id"] = result["zone_id"].astype("int64")
    result["hour"] = result["hour"].astype("int64")
    result["dropoff_count"] = result["dropoff_count"].astype("int64")

    logger.info(f"[tlc_gold] zone x hour 집계 완료: {len(result)}행")
    return result


def _expand_zone_to_segment_hour(
    zone_hour_counts: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
    map_segment_spatial_weight: pd.DataFrame,
) -> pd.DataFrame:
    """zone x hour 하차수를 segment x hour로 펼친다.

    같은 zone에 속한 세그먼트라도 동일하게 나눠 갖지 않고,
    map_segment_spatial_weight의 spatial_weight(zone 내부 상대 밀집도, zone별
    합=1, docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md
    참고)만큼 비례해서 나눠 갖는다. spatial_weight가 없는 세그먼트는 1.0으로
    폴백한다 — 조용히 0이 되어 사라지는 것보다 예전 균등분배와 같은 결과를
    내는 쪽이 안전하다. 매치 안 된 시간대는 0으로 채워서 세그먼트마다 정확히
    24행을 보장한다.
    """

    segment_zone = map_zone_segment[["segment_id", "zone_id"]].copy()
    segment_zone = segment_zone.assign(zone_id=segment_zone["zone_id"].astype("int64"))

    weights = map_segment_spatial_weight[["segment_id", "spatial_weight"]]
    segment_zone = segment_zone.merge(weights, on="segment_id", how="left")

    missing_weight = segment_zone["spatial_weight"].isna()
    if missing_weight.any():
        logger.warning(
            f"[tlc_gold] map_segment_spatial_weight에 없는 세그먼트 {int(missing_weight.sum())}개, "
            "spatial_weight=1.0으로 폴백"
        )
        segment_zone = segment_zone.assign(spatial_weight=segment_zone["spatial_weight"].fillna(1.0))

    hours = pd.DataFrame({"hour": HOURS})

    grid = segment_zone.merge(hours, how="cross")

    counts = zone_hour_counts.copy()
    counts = counts.assign(
        zone_id=counts["zone_id"].astype("int64"),
        hour=counts["hour"].astype("int64"),
    )

    merged = grid.merge(counts, on=["zone_id", "hour"], how="left")
    merged = merged.assign(dropoff_count=merged["dropoff_count"].fillna(0))
    merged = merged.assign(dropoff_count_raw=merged["dropoff_count"] * merged["spatial_weight"])

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
    zone_hour_counts: pd.DataFrame,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    map_segment_spatial_weight_path: Path = MAP_SEGMENT_SPATIAL_WEIGHT_PATH,
    gold_dir: Path = GOLD_DIR,
    borough: str = BOROUGH_EVENT,
) -> str:
    """zone x hour 집계 결과를 받아 dim_segment_tlc_volume.parquet을 만든다.

    무거운 Silver 전체 스캔(collect_zone_hour_counts)은 별도 태스크에서
    이미 끝내고 그 결과를 받는다 — 여기서 실패해도(예: 저장 경로 문제) 그
    스캔을 다시 하지 않아도 된다.

    공사 허가 신청이 맨해튼 한정이라, map_zone_segment의 borough 컬럼으로
    맨해튼 세그먼트만 걸러서 쓴다. TLC silver 자체(팀 공용 코드)는 도시 전체를
    유지하고, 이 Gold 단계에서만 필터링한다.

    zone -> segment 분배는 균등 복사가 아니라 map_segment_spatial_weight의
    spatial_weight 비례 분배다 (2026-08-19 개정,
    docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md).

    map_zone_segment는 LION 분기 갱신마다 자동으로 다시 만들어지지만
    map_segment_spatial_weight는 정적 스냅샷이라 그렇지 않다 — 그래서 여기서
    두 테이블 사이의 결측 세그먼트 비율을 확인해, 소수(폴백으로 안전하게
    처리 가능)를 넘어 spatial_weight 테이블이 방치돼 낡아진 상황을 하드 실패로
    잡아낸다(MAX_MISSING_SPATIAL_WEIGHT_FRACTION). _expand_zone_to_segment_hour
    자체의 세그먼트별 1.0 폴백은 그대로 유지된다 — 이 체크는 그 폴백이 감당할
    수 없는 규모로 커지는 것만 막는다.
    """

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id", "borough"])
    map_zone_segment = map_zone_segment.loc[map_zone_segment["borough"] == borough, ["segment_id", "zone_id"]]

    map_segment_spatial_weight = pd.read_parquet(
        map_segment_spatial_weight_path, columns=["segment_id", "spatial_weight"]
    )

    zone_segment_ids = set(map_zone_segment["segment_id"])
    if zone_segment_ids:
        missing_segment_ids = zone_segment_ids - set(map_segment_spatial_weight["segment_id"])
        missing_fraction = len(missing_segment_ids) / len(zone_segment_ids)
        if missing_fraction > MAX_MISSING_SPATIAL_WEIGHT_FRACTION:
            raise RuntimeError(
                f"[tlc_gold] map_segment_spatial_weight에 없는 map_zone_segment 세그먼트가 "
                f"{missing_fraction:.1%}({len(missing_segment_ids)}/{len(zone_segment_ids)}개)로 "
                f"허용 기준({MAX_MISSING_SPATIAL_WEIGHT_FRACTION:.0%})을 초과합니다. "
                "map_zone_segment는 LION 분기 갱신마다 자동으로 갱신되지만 "
                "map_segment_spatial_weight는 정적 테이블이라 그렇지 않아 낡았을 수 있습니다 — "
                "src/mapping/segment_spatial_weight.py의 빌드 파이프라인(ingest_hotspot_grid -> "
                "build_map_segment_spatial_weight -> validate_map_segment_spatial_weight)을 다시 "
                "실행해 map_segment_spatial_weight.parquet을 갱신하세요."
            )

    # zone_id가 '{borough}' 세그먼트 중 어디에도 안 붙는 트립: TLC 특수 zone
    # 코드(264/265 등 zone_id 1~263 밖)나 다른 자치구 zone이 여기 해당한다.
    # 결과에서는 자연히 빠지지만(join 대상이 아니므로) 몇 건이 빠졌는지는
    # 로그로 남긴다 (spec의 "제외 대상" 항목).
    matched_zone_ids = set(map_zone_segment["zone_id"])
    unmatched = ~zone_hour_counts["zone_id"].isin(matched_zone_ids)
    if unmatched.any():
        unmatched_trips = int(zone_hour_counts.loc[unmatched, "dropoff_count"].sum())
        logger.warning(
            f"[tlc_gold] zone이 '{borough}' 세그먼트에 매칭되지 않아 제외된 하차 {unmatched_trips}건 "
            "(TLC 특수 zone 코드 또는 다른 자치구 zone)"
        )

    expanded = _expand_zone_to_segment_hour(zone_hour_counts, map_zone_segment, map_segment_spatial_weight)
    result = _normalize_tlc_volume(expanded)

    out_path = gold_dir / "dim_segment_tlc_volume.parquet"
    gold_dir.mkdir(parents=True, exist_ok=True)
    result.to_parquet(out_path, index=False)

    logger.info(f"[tlc_gold] dim_segment_tlc_volume 저장 완료: {len(result)}행 -> {out_path}")
    return str(out_path)


def validate_dim_segment_tlc_volume(
    path: str,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    borough: str = BOROUGH_EVENT,
    min_segments: int = 15_000,
    max_segments: int = 25_000,
) -> str:
    """dim_segment_tlc_volume.parquet의 최소 불변식을 확인한다.

    min_segments/max_segments는 실 운영(맨해튼, 약 19,574개 세그먼트)을
    기준으로 한 기본값이다. 테스트에서 작은 픽스처를 쓸 때는 이 범위를
    맞게 좁혀서 넘기면 된다 — borough 파라미터와 같은 이유(테스트 가능성)로
    인자화했다.
    """

    df = pd.read_parquet(path)

    assert not df.duplicated(subset=["segment_id", "hour"]).any(), "(segment_id, hour) 중복 발견"
    assert df["hour"].between(0, 23).all(), "hour가 0~23 범위를 벗어남"
    assert df["tlc_volume"].between(0, 1).all(), "tlc_volume이 0~1 범위를 벗어남"
    assert (df["dropoff_count_raw"] >= 0).all(), "dropoff_count_raw에 음수 있음"

    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "borough"])
    segment_count = map_zone_segment.loc[map_zone_segment["borough"] == borough, "segment_id"].nunique()
    assert segment_count > 0, (
        f"borough='{borough}'에 해당하는 세그먼트가 없습니다 (map_zone_segment의 borough 표기를 확인하세요)"
    )
    # 실측 기준 맨해튼 세그먼트는 약 19,574개(design.md 참고). 기본 범위를
    # 벗어나면 borough 필터가 잘못됐거나(오타 등) map_zone_segment 자체가
    # 깨진 것으로 본다.
    assert min_segments <= segment_count <= max_segments, (
        f"borough='{borough}' 세그먼트 수가 예상 범위({min_segments:,}~{max_segments:,}, "
        f"실측 기준 약 19,574개) 밖입니다: {segment_count}"
    )

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
        counts = collect_zone_hour_counts(spark_session)
    finally:
        spark_session.stop()

    out = build_dim_segment_tlc_volume(counts)
    validate_dim_segment_tlc_volume(out)
