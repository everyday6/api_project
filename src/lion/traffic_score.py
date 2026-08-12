"""
Silver 변환: graph_segment_adjacency + dim_segment -> dim_segment_traffic_score_v0

이번 사이클(LION만으로 파이프라인을 끝까지 관통)의 마지막 계산 단계.

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
   나머지 작은 조각들(완전 고립 50개 포함). 작은 조각에 속한 세그�먼트는
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
import pandas as pd

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.silver import DIM_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="dim_segment_traffic_score")

DIM_SEGMENT_TRAFFIC_SCORE_PATH = SILVER_DIR / "dim_segment_traffic_score_v0.parquet"

# TODO(팀 검토 필요): k가 클수록 정확하지만 느려짐. 실측(k=50→24.9초, k=200→95.7초,
# 노드당 약 0.48초) 기준 k=1000이면 약 8~9분. 분기 1회 배치라 이 값으로 시작.
BETWEENNESS_K = 1000
BETWEENNESS_SEED = 42  # 재현성 확보 — 실행마다 같은 노드를 샘플링하게 고정


def build_dim_segment_traffic_score(
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    graph_path: Path = GRAPH_SEGMENT_ADJACENCY_PATH,
    silver_root: Path = SILVER_DIR,
    k: int = BETWEENNESS_K,
) -> str:
    dim = pd.read_parquet(dim_segment_path, columns=["segment_id", "is_routable", "capacity_per_hour"])
    routable = dim.loc[dim["is_routable"], ["segment_id", "capacity_per_hour"]].copy()

    adj = pd.read_parquet(graph_path)
    G = nx.from_pandas_edgelist(adj, source="segment_id", target="neighbor_segment_id")
    G.add_nodes_from(routable["segment_id"])  # 인접 없는 고립 세그먼트도 노드로 포함

    logger.info(
        f"[traffic_score] 그래프: 노드 {G.number_of_nodes()}개, 엣지 {G.number_of_edges()}개, "
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
    out.to_parquet(out_path, index=False)

    logger.info(f"[traffic_score] {len(out)}행 저장 -> {out_path}")
    return str(out_path)


def validate_dim_segment_traffic_score(path: str) -> str:
    """dim_segment_traffic_score_v0.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(path)

    assert df["segment_id"].is_unique, "segment_id 중복 발견"
    assert df["demand_raw"].between(0, 1).all(), "demand_raw가 0~1 범위를 벗어남 (percentile rank 깨짐)"
    assert (df["traffic_score_v0"].dropna() >= 0).all(), "traffic_score_v0에 음수 있음"

    missing_ratio = df["capacity_per_hour"].isna().mean()
    logger.info(f"[traffic_score] capacity_per_hour 결측 비율: {missing_ratio:.1%} (dim_segment 단계의 알려진 공백)")
    assert missing_ratio < 0.15, f"capacity_per_hour 결측 비율이 예상(약 11%)보다 훨씬 높음: {missing_ratio:.1%}"

    n = len(df)
    assert 100_000 <= n <= 200_000, f"행 수가 예상 범위 밖입니다: {n}"

    logger.info(f"[traffic_score] 검증 통과 ({n}행)")
    return path


if __name__ == "__main__":
    out = build_dim_segment_traffic_score()
    validate_dim_segment_traffic_score(out)
