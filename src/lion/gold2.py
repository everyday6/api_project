"""
Gold2 — LION dim_segment 파생 지표(road_class/capacity/is_routable) +
매개중심성 기반 traffic_score_v0

src/lion/silver1.py가 만든 dim_segment(기본 컬럼 + 원본 코드 컬럼)를 읽어서
road_class/is_routable/is_two_way/capacity_per_hour/lane_miles를 계산해
붙이고, 같은 파일 이름(dim_segment.parquet)으로 "완성본"을 저장한다 — 8곳의
기존 소비처(mapping/scoring 모듈)가 컬럼 위치와 무관하게 하나의 완성된
dim_segment만 알면 되도록 하기 위함이다.

road_class / capacity 관련 숫자는 팀에서 확정한 기준이 없어 HCM(Highway Capacity
Manual) 개념을 참고한 초안이다 — 반드시 검토/조정이 필요하다 (BASE_CAPACITY_PER_LANE 참고).

이어서 graph_segment_adjacency(Silver2) + 이 dim_segment를 가지고 매개중심성
(betweenness centrality) 기반 traffic_score_v0도 이 파일에서 계산한다(구
src/lion/traffic_score.py) — 두 계산 모두 "LION 데이터만으로 새 지표를
만든다"는 같은 성격(Gold2)이라 하나의 파일로 합쳤다.

1. graph_segment_adjacency(엣지 리스트)를 networkx 무방향 그래프로 로드한다.
   인접 그래프에 이웃이 하나도 없어 엣지 리스트 자체에 안 나오는 세그먼트도
   dim_segment 기준으로 노드에 명시적으로 추가한다 (실제로 157,153건 중 50건이
   완전 고립 상태임을 확인함).

2. 매개중심성(betweenness centrality)을 근사 계산한다. 세그먼트 15.7만 개
   규모에서 정확한 betweenness는 O(V·E)라 현실적으로 못 돌린다. k개 노드를
   무작위로 샘플링하는 근사법을 쓴다(nx.betweenness_centrality(G, k=..., seed=...)).
   실제로 k=50/200으로 시간을 재보니 노드당 약 0.48초로 거의 선형이라
   k=1000이면 약 8~9분 — 분기 1회 배치 작업이라 이 정도는 문제없다.
   seed를 고정해서 실행할 때마다 같은 노드를 샘플링하게 함(재현성 확보).

3. 그래프는 연결요소 143개로 나뉘어 있다 — 거대 요소 하나(156,600개, 99.65%)와
   나머지 작은 조각들(완전 고립 50개 포함). 작은 조각에 속한 세그먼트는
   다른 어디와도 "사이"에 있을 수 없어 중심성이 낮게/0으로 나오는 게 맞다.
   섬처럼 고립된 실제 도로 구조(막다른 길 클러스터 등)로 보이며 별도 보정은
   하지 않는다.

4. raw betweenness는 분포가 심하게 치우쳐 있어(대부분 0에 가깝고 극소수만 큼)
   percentile rank로 0~1 범위로 펴서 demand_raw로 쓴다. (log1p도 검토했으나
   0값이 워낙 많아 log1p로는 안 펴짐 — 실제 분포 확인 후 percentile rank로 결정)

5. traffic_score_v0 = demand_raw / capacity_per_hour. capacity_per_hour가
   결측인 세그먼트(차로수 결측 — dim_segment 단계에서 이미 알려진 데이터 공백,
   약 17,278건)는 나눗셈이 안 되어 traffic_score_v0도 결측으로 남긴다.
   임의로 차로수를 추정해서 메우지 않는다.
"""

from __future__ import annotations

from pathlib import Path

import networkx as nx
import numpy as np
import pandas as pd

from src.common import db
from src.common.config import GOLD2_DIR, SILVER1_DIR
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="lion_gold2")

DIM_SEGMENT_BASE_PATH = SILVER1_DIR / "dim_segment.parquet"
DIM_SEGMENT_PATH = GOLD2_DIR / "dim_segment.parquet"

# RW_TYPE(도로유형 코드, 공식 정의) -> road_class 1차 분류
HIGHWAY_RW_TYPES = ["2", "9"]                              # Highway, Ramp
NON_ROUTABLE_RW_TYPES = ["5", "6", "7", "8", "10", "11", "12", "13", "14"]
# Boardwalk, Path/Trail, Step Street, Driveway, Alley, Unknown, Non-Physical Segment, U-Turn, Ferry Route
# RW_TYPE 1(Street)/3(Bridge)/4(Tunnel)은 등급 구분이 없어서 TRUCK_ROUTE_TYPE(1=Limited
# Local, 2=Local, 3=Through)과 차로수로 arterial/local을 보조 판정한다.
ARTERIAL_TRUCK_ROUTE_TYPES = ["2", "3"]
ARTERIAL_MIN_LANES = 3

# TODO(팀 검토 필요): 확정된 사내 기준이 없어 HCM(도로용량편람) 개념 기반 초안.
# 단위: 차로당 시간당 승용차환산대수(pcphpl)
BASE_CAPACITY_PER_LANE = {
    "highway": 1900,   # HCM 자유류(freeway) 이상적 포화교통류율 근사치
    "arterial": 900,   # HCM 신호교차로 도시간선도로 차로당 용량 근사치
    "local": 600,       # 저속/주차회전 마찰이 있는 국지도로 근사치
    "non_routable": 0,
}

# TODO(팀 검토 필요): 방향계수 확정 기준 없어 1.0(보정 없음)으로 둠.
DIRECTION_FACTOR = {
    "one_way": 1.0,
    "two_way": 1.0,
}

VALID_ROAD_CLASSES = ["highway", "arterial", "local", "non_routable"]
VALID_BOROUGH_CODES = ["1", "2", "3", "4", "5"]
MIN_EXPECTED_ROWS = 100_000
MAX_EXPECTED_ROWS = 300_000

# TODO(팀 검토 필요): k가 클수록 정확하지만 느려짐. 실측(k=50→24.9초, k=200→95.7초,
# 노드당 약 0.48초) 기준 k=1000이면 약 8~9분. 분기 1회 배치라 이 값으로 시작.
BETWEENNESS_K = 1000
BETWEENNESS_SEED = 42  # 재현성 확보 — 실행마다 같은 노드를 샘플링하게 고정


def _classify_road_class(df: pd.DataFrame) -> pd.Series:
    """RW_TYPE(+TRUCK_ROUTE_TYPE, 차로수 보조)로 road_class를 매긴다."""
    is_highway = df["RW_TYPE"].isin(HIGHWAY_RW_TYPES)
    is_non_routable = df["RW_TYPE"].isin(NON_ROUTABLE_RW_TYPES) | df["RW_TYPE"].isna() | (df["RW_TYPE"] == "")
    is_arterial = (
        df["TRUCK_ROUTE_TYPE"].isin(ARTERIAL_TRUCK_ROUTE_TYPES)
        | (df["lanes_total"] >= ARTERIAL_MIN_LANES)
    )

    return pd.Series(
        np.select(
            [is_highway, is_non_routable, is_arterial],
            ["highway", "non_routable", "arterial"],
            default="local",
        ),
        index=df.index,
    )


def build_dim_segment(dim_segment_base_path: Path = DIM_SEGMENT_BASE_PATH) -> str:
    """dim_segment(Silver1)를 읽어 road_class/is_routable/is_two_way/capacity_per_hour/
    lane_miles를 계산해 붙인 완성본을 저장한다."""

    df = pd.read_parquet(str(dim_segment_base_path))

    df["road_class"] = _classify_road_class(df)
    df["is_routable"] = (df["road_class"] != "non_routable") & (df["FeatureTyp"] == "0")
    df["is_two_way"] = df["TrafDir"] == "T"

    df["base_capacity_per_lane"] = df["road_class"].map(BASE_CAPACITY_PER_LANE)
    direction_factor = np.where(df["is_two_way"], DIRECTION_FACTOR["two_way"], DIRECTION_FACTOR["one_way"])
    df["capacity_per_hour"] = df["lanes_total"] * df["base_capacity_per_lane"] * direction_factor
    df["lane_miles"] = (df["length_ft"] * df["lanes_total"]) / 5280.0

    dim_segment = df[[
        "segment_id", "street_name", "borough_code", "geometry", "length_ft", "road_class",
        "is_two_way", "lanes_total", "lane_miles", "base_capacity_per_lane",
        "capacity_per_hour", "is_routable", "node_from", "node_to",
    ]]

    GOLD2_DIR.mkdir(parents=True, exist_ok=True)
    dim_segment.to_parquet(str(DIM_SEGMENT_PATH), index=False)

    # 서빙 API(gold2/traffic_score.py의 _load_base_data)가 RDS에서 읽으므로
    # 서빙 테이블도 같이 갱신한다.
    db.write_table(dim_segment, "dim_segment")

    logger.info(f"[lion_gold2] dim_segment(Gold2) {len(dim_segment)}행 저장 -> {DIM_SEGMENT_PATH} (+ RDS)")
    return str(DIM_SEGMENT_PATH)


def validate_dim_segment(path: str) -> str:
    """
    dim_segment.parquet(Gold2 완성본)이 지켜야 할 최소한의 불변식을 확인한다.
    하나라도 깨지면 AssertionError를 던져서 태스크를 실패시킨다 — 조용히 잘못된
    데이터가 다음 단계로 넘어가는 걸 막는 게 목적이다.
    """
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견 (dedupe 로직 확인 필요)"

    routable_missing_geom = df.loc[df["is_routable"], "geometry"].isna()
    assert not routable_missing_geom.any(), (
        f"is_routable=True인데 geometry가 없는 행 {routable_missing_geom.sum()}개 발견"
    )

    assert df["road_class"].isin(VALID_ROAD_CLASSES).all(), (
        f"알 수 없는 road_class 값: {sorted(set(df['road_class']) - set(VALID_ROAD_CLASSES))}"
    )

    assert df["borough_code"].isin(VALID_BOROUGH_CODES + [""]).all(), (
        f"알 수 없는 borough_code 값: {sorted(set(df['borough_code']) - set(VALID_BOROUGH_CODES) - {''})}"
    )

    n = len(df)
    assert MIN_EXPECTED_ROWS <= n <= MAX_EXPECTED_ROWS, (
        f"행 수가 예상 범위({MIN_EXPECTED_ROWS}~{MAX_EXPECTED_ROWS}) 밖입니다: {n}"
    )

    logger.info(f"[lion_gold2] dim_segment(Gold2) 검증 통과 ({n}행) -> {path}")
    return path


DIM_SEGMENT_TRAFFIC_SCORE_PATH = GOLD2_DIR / "dim_segment_traffic_score_v0.parquet"


def build_dim_segment_traffic_score(
    graph_path: Path,
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    silver_root: Path = GOLD2_DIR,
    k: int = BETWEENNESS_K,
) -> str:
    """graph_path는 호출자가 명시적으로 넘긴다(src.lion.silver2.
    GRAPH_SEGMENT_ADJACENCY_PATH) — gold2가 silver2를 모듈 최상단에서 import하면
    silver2가 gold2의 DIM_SEGMENT_PATH를 import하는 것과 순환참조가 되므로
    피한다."""
    dim = pd.read_parquet(str(dim_segment_path), columns=["segment_id", "is_routable", "capacity_per_hour"])
    routable = dim.loc[dim["is_routable"], ["segment_id", "capacity_per_hour"]].copy()

    adj = pd.read_parquet(str(graph_path))
    G = nx.from_pandas_edgelist(adj, source="segment_id", target="neighbor_segment_id")
    G.add_nodes_from(routable["segment_id"])  # 인접 없는 고립 세그먼트도 노드로 포함

    logger.info(
        f"[lion_gold2] 그래프: 노드 {G.number_of_nodes()}개, 엣지 {G.number_of_edges()}개, "
        f"k={k}, seed={BETWEENNESS_SEED}"
    )
    centrality = nx.betweenness_centrality(G, k=k, normalized=True, seed=BETWEENNESS_SEED)

    routable["centrality_raw"] = routable["segment_id"].map(centrality)

    # 심하게 치우친(0에 몰린) 분포라 percentile rank로 0~1 범위로 편다.
    # 동률(0값 다수)은 method="average"로 처리 — 전부 같은 중간 순위를 받는다.
    routable["demand_raw"] = routable["centrality_raw"].rank(pct=True, method="average")

    routable["traffic_score_v0"] = routable["demand_raw"] / routable["capacity_per_hour"]

    out = routable[["segment_id", "centrality_raw", "demand_raw", "capacity_per_hour", "traffic_score_v0"]]

    silver_root.mkdir(parents=True, exist_ok=True)
    out_path = silver_root / "dim_segment_traffic_score_v0.parquet"
    out.to_parquet(str(out_path), index=False)

    db.write_table(out, "dim_segment_traffic_score_v0")

    logger.info(f"[lion_gold2] traffic_score {len(out)}행 저장 -> {out_path} (+ RDS)")
    return str(out_path)


def validate_dim_segment_traffic_score(path: str) -> str:
    """dim_segment_traffic_score_v0.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견"
    assert df["demand_raw"].between(0, 1).all(), "demand_raw가 0~1 범위를 벗어남 (percentile rank 깨짐)"
    assert (df["traffic_score_v0"].dropna() >= 0).all(), "traffic_score_v0에 음수 있음"

    missing_ratio = df["capacity_per_hour"].isna().mean()
    logger.info(f"[lion_gold2] capacity_per_hour 결측 비율: {missing_ratio:.1%} (dim_segment 단계의 알려진 공백)")
    assert missing_ratio < 0.15, f"capacity_per_hour 결측 비율이 예상(약 11%)보다 훨씬 높음: {missing_ratio:.1%}"

    n = len(df)
    assert 100_000 <= n <= 200_000, f"행 수가 예상 범위 밖입니다: {n}"

    logger.info(f"[lion_gold2] 검증 통과 ({n}행)")
    return path


if __name__ == "__main__":
    from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH

    out = build_dim_segment()
    validate_dim_segment(out)
    graph_out = build_dim_segment_traffic_score(GRAPH_SEGMENT_ADJACENCY_PATH)
    validate_dim_segment_traffic_score(graph_out)
