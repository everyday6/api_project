"""
TLC Gold2 — 세그먼트x평일시간대 통행량 + zone 내부 공간 가중치

TLC 하차(dropoff) 데이터를 "택시 수요"가 아니라 "일반적인 도로 교통량 프록시"로
간주하고, 세그먼트별로 평일 0~23시 각 시간대에 상대적으로 얼마나 붐비는지를
나타내는 Gold2 테이블(dim_segment_tlc_volume)을 만든다. 평일/zone_id 결측
필터는 src/tlc/gold1.py가 맡고, 여기서는 집계(zone x hour 카운트)와 zone
내부 세그먼트로의 비례 분배, percentile 정규화 등 "새 지표를 만드는" 연산만
한다.

zone -> segment 분배에 쓰는 spatial_weight(zone 내부 상대 밀집도)도 이
파일에서 계산한다(구 src/mapping/segment_spatial_weight.py) — 2016년 하차
위경도 grid를 세그먼트에 거리역가중으로 매칭하는 것 역시 "새 파생 수치를
만드는" 연산이라 Silver2(구조적 조인)가 아니라 tlc 전용 Gold2로 재분류했다.
이 hotspot grid 자체가 tlc 소유 데이터이고 lion은 참조용으로만 쓰이므로
공용 폴더가 아니라 이 도메인 폴더에 둔다.

자세한 배경은 docs/superpowers/specs/2026-08-13-tlc-segment-hour-volume-design.md,
docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyproj import Transformer
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour as hour_of_day
from shapely import wkt
from shapely.geometry import Point
from shapely.strtree import STRtree

from src.common.config import (
    BOROUGH_EVENT,
    BQ_HOTSPOT_CRS,
    BRONZE_DIR,
    GOLD2_DIR,
    HOTSPOT_INVERSE_DISTANCE_EPSILON_FT,
    HOTSPOT_SEGMENT_BUFFER_FT,
    LAPLACE_SMOOTHING_ALPHA,
    LION_CRS,
    PROJECT_ROOT,
    SILVER1_DIR,
    TAXI_TYPES,
)
from src.common.logger import get_logger
from src.lion.gold2 import DIM_SEGMENT_PATH
from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH
from src.silver2.zone_segment import MAP_ZONE_SEGMENT_PATH, TAXI_ZONE_SHAPEFILE, _load_zones
from src.tlc.gold1 import drop_null_zone, filter_weekday

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_gold")

DIM_SEGMENT_TLC_VOLUME_PATH = GOLD2_DIR / "dim_segment_tlc_volume.parquet"

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

# temp/bq-results.csv는 이 저장소(my-project-new) 바깥, 프로젝트 루트의
# 스크래치 위치에 있다. PROJECT_ROOT(my-project-new) 기준이 아니라 그
# 부모 디렉터리 기준이다.
HOTSPOT_CSV_SOURCE_PATH = PROJECT_ROOT.parent / "temp" / "bq-results.csv"
BRONZE_HOTSPOT_PATH = BRONZE_DIR / "tlc" / "hotspot_2016" / "dropoff_grid.parquet"
MAP_SEGMENT_SPATIAL_WEIGHT_PATH = GOLD2_DIR / "map_segment_spatial_weight.parquet"


def collect_zone_hour_counts(
    spark: SparkSession,
    silver_dir: Path = SILVER1_DIR,
    taxi_types: list[str] = TAXI_TYPES,
) -> pd.DataFrame:
    """TLC silver1 파일 전부를 읽어 평일(월~금) 기준 zone x hour 하차수를 센다.

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
        raise FileNotFoundError(f"TLC silver1 파일을 찾을 수 없습니다: {silver_dir}")

    logger.info(f"[tlc_gold] TLC silver1 파일 {len(paths)}개 읽기 시작")

    df = spark.read.parquet(*paths).select("dropoff_datetime", "dropoff_location_id")

    weekday = filter_weekday(df)

    counted = (
        weekday
        .withColumn("hour", hour_of_day(col("dropoff_datetime")))
        .groupBy(col("dropoff_location_id").alias("zone_id"), "hour")
        .count()
        .withColumnRenamed("count", "dropoff_count")
    )

    result = counted.toPandas()

    result = drop_null_zone(result)

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
    gold_dir: Path = GOLD2_DIR,
    borough: str = BOROUGH_EVENT,
) -> str:
    """zone x hour 집계 결과를 받아 dim_segment_tlc_volume.parquet을 만든다.

    무거운 Silver1 전체 스캔(collect_zone_hour_counts)은 별도 태스크에서
    이미 끝내고 그 결과를 받는다 — 여기서 실패해도(예: 저장 경로 문제) 그
    스캔을 다시 하지 않아도 된다.

    공사 허가 신청이 맨해튼 한정이라, map_zone_segment의 borough 컬럼으로
    맨해튼 세그먼트만 걸러서 쓴다. TLC silver1 자체(팀 공용 코드)는 도시 전체를
    유지하고, 이 Gold2 단계에서만 필터링한다.

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
                "src/tlc/gold2.py의 빌드 파이프라인(ingest_hotspot_grid -> "
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
    gold2/traffic_score.py가 다른 요인(중심성, capacity, event, closure)과
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


# =========================================================
# zone 내부 세그먼트별 공간 가중치(spatial_weight)
# =========================================================
#
# 2016년 하차 위경도 grid(temp/bq-results.csv, BigQuery로 받아온 결과)를 쓴다
# — TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 위경도 기준으로
# zone 내부 분포를 직접 볼 수 있는 마지막 스냅샷이다. 재수집할 근거 데이터가
# 없는 정적 값이라 DAG 연결이나 재실행 스케줄은 두지 않는다 — 스크립트로
# 직접 실행하는 한 번짜리 산출물이다.
#
# src/silver2/zone_segment.py의 세그먼트-zone 1:1 매핑에 이어, 그 zone 내부에서
# 세그먼트별 상대 밀집도를 추가로 산정한다. 배경/설계 근거는
# docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
#
# (HOTSPOT_CSV_SOURCE_PATH/BRONZE_HOTSPOT_PATH/MAP_SEGMENT_SPATIAL_WEIGHT_PATH는
# build_dim_segment_tlc_volume의 기본 인자로 먼저 쓰여야 해서 파일 상단에 정의돼 있다.)


def ingest_hotspot_grid(
    source_csv_path: Path = HOTSPOT_CSV_SOURCE_PATH,
    bronze_path: Path = BRONZE_HOTSPOT_PATH,
) -> str:
    """2016년 하차 위경도 grid CSV(BigQuery 결과)를 변환 없이 Bronze parquet로 옮긴다.

    `src/taxi_zone/bronze.py`와 동일 관례로 메타데이터 컬럼만 붙인다. 재실행할
    근거 데이터가 없는 정적 스냅샷이라 한 번만 실행하면 된다.
    """
    df = pd.read_csv(source_csv_path)

    required = {"lat_bin", "lon_bin", "dropoff_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"[tlc_gold] 필수 컬럼 없음: {missing}")

    df["_ingested_at"] = datetime.now(timezone.utc).isoformat()
    df["_source"] = "bq_2016_dropoff_grid"

    bronze_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(bronze_path, index=False)

    logger.info(f"[tlc_gold] hotspot grid {len(df)}행 저장 완료 -> {bronze_path}")
    return str(bronze_path)


def _points_from_grid(bronze_df: pd.DataFrame) -> pd.DataFrame:
    """Bronze grid(lat_bin, lon_bin, dropoff_count, EPSG:4326)를 EPSG:2263 Point로 변환한다."""
    transformer = Transformer.from_crs(BQ_HOTSPOT_CRS, LION_CRS, always_xy=True)
    x, y = transformer.transform(bronze_df["lon_bin"].to_numpy(), bronze_df["lat_bin"].to_numpy())

    result = bronze_df[["dropoff_count"]].copy()
    result["geometry"] = [Point(xi, yi) for xi, yi in zip(x, y)]
    return result


def _match_points_to_zone(
    points: pd.DataFrame,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
) -> pd.DataFrame:
    """grid point(EPSG:2263 Point)를 Taxi Zone 폴리곤에 point-in-polygon으로 매칭한다.

    `src/silver2/zone_segment.py`의 세그먼트-zone 매칭과 동일한 STRtree 패턴이다.
    매칭 안 되는 포인트는 제외하고 건수만 로그로 남긴다.
    """
    zones = _load_zones(zone_shapefile_path)
    tree = STRtree(zones["geom"].tolist())

    zone_ids: list[int | None] = []
    unmatched = 0
    multi_match = 0
    for point in points["geometry"]:
        idxs = tree.query(point, predicate="intersects")
        if len(idxs) == 0:
            unmatched += 1
            zone_ids.append(None)
            continue
        if len(idxs) > 1:
            multi_match += 1
        zone_ids.append(zones.iloc[idxs[0]]["LocationID"])

    result = points.copy()
    result["zone_id"] = zone_ids

    if multi_match:
        logger.warning(f"[tlc_gold] grid point가 zone 경계에 걸쳐 2개 이상 매칭 {multi_match}건 (첫 번째로 결정)")
    if unmatched:
        logger.warning(f"[tlc_gold] zone을 못 찾은 grid point {unmatched}건 (제외)")

    matched = result.dropna(subset=["zone_id"]).copy()
    matched = matched.astype({"zone_id": "int64"})
    return matched[["geometry", "dropoff_count", "zone_id"]]


def _match_points_to_segment(
    points_with_zone: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
    dim_segment: pd.DataFrame,
    buffer_ft: float = HOTSPOT_SEGMENT_BUFFER_FT,
    epsilon_ft: float = HOTSPOT_INVERSE_DISTANCE_EPSILON_FT,
) -> pd.DataFrame:
    """zone_id별로 그룹화해, 그 zone에 속한 세그먼트 중 point 반경 buffer_ft(feet)
    이내 전부에 거리 역가중(1/(distance+epsilon_ft))으로 dropoff_count를 나눠
    배분한다. 반경 안에 세그먼트가 하나도 없으면 zone 내 최근접 세그먼트 1개로
    fallback한다(그때는 dropoff_count 전부가 그 세그먼트로 간다) —
    `src/silver2/ticketmaster_lion.py`의 buffer+nearest-fallback 패턴과 동일하다.

    zone 경계를 넘는 매칭을 막아야 zone 내부 spatial_weight 합이 정확히 1이
    된다. 세그먼트 집계(같은 segment_id로 여러 point가 매칭되는 경우 합산)는
    이 함수의 책임이 아니라 다음 단계(_aggregate_hotspot_counts)에서 한다.
    """
    segments = dim_segment[["segment_id", "geometry"]].merge(
        map_zone_segment[["segment_id", "zone_id"]], on="segment_id", how="inner"
    )
    segments = segments.assign(geom=segments["geometry"].apply(wkt.loads))

    matched_rows = []
    skipped_zones = 0
    skipped_points = 0
    skipped_dropoff_count = 0.0
    for zone_id, zone_points in points_with_zone.groupby("zone_id"):
        zone_segments = segments[segments["zone_id"] == zone_id]
        if zone_segments.empty:
            skipped_zones += 1
            skipped_points += len(zone_points)
            skipped_dropoff_count += float(zone_points["dropoff_count"].sum())
            continue

        geoms = zone_segments["geom"].tolist()
        segment_ids = zone_segments["segment_id"].tolist()
        tree = STRtree(geoms)

        for point, dropoff_count in zip(zone_points["geometry"], zone_points["dropoff_count"]):
            idxs = tree.query(point.buffer(buffer_ft), predicate="intersects")

            if len(idxs) == 0:
                nearest_idx = tree.nearest(point)
                matched_rows.append({
                    "segment_id": segment_ids[nearest_idx],
                    "dropoff_count": float(dropoff_count),
                })
                continue

            distances = np.array([point.distance(geoms[i]) for i in idxs])
            inv_distance = 1.0 / (distances + epsilon_ft)
            shares = inv_distance / inv_distance.sum()

            for idx, share in zip(idxs, shares):
                matched_rows.append({
                    "segment_id": segment_ids[idx],
                    "dropoff_count": float(dropoff_count) * float(share),
                })

    if skipped_zones:
        logger.warning(
            f"[tlc_gold] 세그먼트가 없는 zone {skipped_zones}개, "
            f"grid point {skipped_points}건, dropoff_count {skipped_dropoff_count:.1f} 제외"
        )

    if not matched_rows:
        return pd.DataFrame({"segment_id": pd.Series(dtype="object"), "dropoff_count": pd.Series(dtype="float64")})

    return pd.DataFrame(matched_rows)


def _aggregate_hotspot_counts(
    matched_points: pd.DataFrame,
    map_zone_segment: pd.DataFrame,
) -> pd.DataFrame:
    """매칭된 grid point의 dropoff_count를 segment_id별로 합산한다.

    map_zone_segment 전체(그 zone에 속한 세그먼트 전부)에 left join해서, 매칭이
    0건인 세그먼트도 segment_hotspot_count=0으로 명시적으로 포함시킨다 — 이래야
    다음 단계(_compute_spatial_weight)의 zone 내부 정규화가 zone에 속한 세그먼트
    전부를 커버한다.
    """
    hotspot_counts = (
        matched_points.groupby("segment_id")["dropoff_count"].sum().rename("segment_hotspot_count")
    )

    result = map_zone_segment[["segment_id", "zone_id"]].merge(
        hotspot_counts, on="segment_id", how="left"
    )
    result["segment_hotspot_count"] = result["segment_hotspot_count"].fillna(0.0).astype("float64")
    return result


def _compute_spatial_weight(
    df: pd.DataFrame,
    alpha: float = LAPLACE_SMOOTHING_ALPHA,
) -> pd.DataFrame:
    """zone 내부에서 라플라스 스무딩 후 정규화한다 (zone별 spatial_weight 합 = 1).

    alpha는 정성적 초안이다(TODO, 팀 검토 필요) — 매칭 0건 세그먼트가 완전히
    0이 되지 않게 하는 최소한의 목적만 반영했다.
    """
    if alpha <= 0:
        raise ValueError(
            f"alpha는 0보다 커야 합니다: {alpha}. 실 데이터에는 zone 내 세그먼트 전부가 "
            "segment_hotspot_count=0인 zone이 있어(예: zone 103), alpha<=0이면 그 zone의 "
            "zone_totals(= Σ(segment_hotspot_count + alpha))가 0이 되어 spatial_weight가 "
            "0으로 나누기(NaN)가 됩니다."
        )
    result = df.copy()
    result["_smoothed"] = result["segment_hotspot_count"] + alpha
    zone_totals = result.groupby("zone_id")["_smoothed"].transform("sum")
    result["spatial_weight"] = result["_smoothed"] / zone_totals
    return result.drop(columns=["_smoothed"])


def build_map_segment_spatial_weight(
    bronze_path: Path = BRONZE_HOTSPOT_PATH,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    zone_shapefile_path: Path = TAXI_ZONE_SHAPEFILE,
    gold_root: Path = GOLD2_DIR,
    alpha: float = LAPLACE_SMOOTHING_ALPHA,
) -> str:
    """2016 hotspot grid Bronze + map_zone_segment + dim_segment로 map_segment_spatial_weight를 만든다."""
    bronze_df = pd.read_parquet(bronze_path, columns=["lat_bin", "lon_bin", "dropoff_count"])
    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id", "zone_id"])
    dim_segment = pd.read_parquet(dim_segment_path, columns=["segment_id", "geometry"])

    points = _points_from_grid(bronze_df)
    points_with_zone = _match_points_to_zone(points, zone_shapefile_path=zone_shapefile_path)
    matched_points = _match_points_to_segment(points_with_zone, map_zone_segment, dim_segment)
    aggregated = _aggregate_hotspot_counts(matched_points, map_zone_segment)
    result = _compute_spatial_weight(aggregated, alpha=alpha)

    gold_root.mkdir(parents=True, exist_ok=True)
    out_path = gold_root / "map_segment_spatial_weight.parquet"
    result.to_parquet(out_path, index=False)

    logger.info(f"[tlc_gold] map_segment_spatial_weight {len(result)}행 저장 -> {out_path}")
    return str(out_path)


def validate_map_segment_spatial_weight(
    path: str,
    map_zone_segment_path: Path = MAP_ZONE_SEGMENT_PATH,
) -> str:
    """map_segment_spatial_weight.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)
    map_zone_segment = pd.read_parquet(map_zone_segment_path, columns=["segment_id"])

    assert df["segment_id"].is_unique, "segment_id 중복 발견"
    assert set(df["segment_id"]) == set(map_zone_segment["segment_id"]), (
        "map_zone_segment의 세그먼트와 정확히 일치하지 않음"
    )
    assert (df["segment_hotspot_count"] >= 0).all(), "segment_hotspot_count에 음수 있음"
    assert df["spatial_weight"].gt(0).all() and df["spatial_weight"].le(1).all(), (
        "spatial_weight가 (0, 1] 범위를 벗어남"
    )

    zone_sums = df.groupby("zone_id")["spatial_weight"].sum()
    assert np.allclose(zone_sums.to_numpy(), 1.0, atol=1e-9), "zone별 spatial_weight 합이 1이 아님"

    logger.info(f"[tlc_gold] map_segment_spatial_weight 검증 통과 ({len(df)}행, zone {df['zone_id'].nunique()}개)")
    return path


if __name__ == "__main__":
    from src.common.spark import get_spark

    spark_session = get_spark()
    try:
        counts = collect_zone_hour_counts(spark_session)
    finally:
        spark_session.stop()

    out = build_dim_segment_tlc_volume(counts)
