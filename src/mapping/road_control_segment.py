"""
Silver 매핑: road_control_events(construction) x dim_segment(LION) -> map_road_control_segment

construction 허가(road_control_events의 control_type="construction")는
geometry가 없어서 on_street/from_street/to_street 같은 도로명 텍스트로만
segment_id를 찾아야 한다. 문제는 dim_segment가 도로명(street_name)은 있지만
교차로(from/to street) 정보가 없다는 것 — on_street 하나만으로 매칭하면 그
도로의 모든 블록(세그먼트)이 다 후보로 걸린다(예: BROADWAY 하나만 세그먼트
1,126개, 직접 확인함).

그래서 graph_segment_adjacency(세그먼트가 같은 교차로 노드를 공유하면 인접으로
보는 그래프, src/lion/segment_adjacency.py)를 이용한다: on_street이 같은 후보
세그먼트에서 출발해서, on_street을 따라 최대 MAX_HOPS(3)까지 이웃을 넓혀가며
from_street/to_street 이름의 세그먼트가 나오는지 확인한다.

1홉(직접 이웃)만 보면 놓치는 경우가 실제로 있었다 — 예: ARDEN STREET은
BROADWAY 쪽 끝과 DONGAN PLACE 쪽 끝이 서로 다른 두 세그먼트로 갈라져 있어서,
어느 세그먼트도 "양 끝에 BROADWAY와 DONGAN PLACE"를 동시에 이웃으로 갖지
못했다. LION이 이름 있는 교차로보다 더 잘게(중간의 이름 없는 노드에서도)
세그먼트를 쪼개는 반면, 허가 시스템은 "가장 가까운 이름 있는 교차로 2개"로만
구간을 설명하기 때문 — 두 시스템이 "구간"을 나누는 단위 자체가 다르다.

처음엔 "시작 세그먼트에서 사방으로 N홉 안에 from/to_street가 둘 다 있으면
매칭"으로 짰는데, 실측해보니 홉을 늘릴수록 오탐이 폭증했다(2홉만 해도 다중
매칭 91.6%, 3홉은 98.1% — 같은 방향으로 계속 가도 매칭돼버리는 게 문제였음).
그래서 세그먼트의 두 끝(shared_node_id)을 구분해서, **한쪽 끝에서
from_street, 반대쪽 끝에서 to_street**를 찾도록(양쪽에서 둘 다 찾는 게 아니라)
바꿨다 — "이 블록의 시작과 끝에 각각 다른 교차로가 있다"는 실제 구조를 그대로
반영한 것. 이렇게 하니 다중 매칭이 2홉 28.5%, 3홉 37.6%로 크게 줄었다.

- 홉을 넓힐수록 재현율은 올라가지만 정밀도는 떨어진다 — 다중 매칭(permit
  하나가 segment 여러 개) 비율로 감시한다. MAX_HOPS=3이 실측 기준 균형점.
- to_street가 없는 행(construction의 약 8.8%)은 두 조건을 다 만족시킬 수
  없어서 애초에 매칭 대상에서 빠진다.
- other_road_control(road_closures 출신)은 geometry가 99.99% 있어서 이 방식이
  아니라 공간 조인이 맞는 트랙이다 — 여기서 다루지 않는다.

도로명 자체가 dim_segment 사전에 없는 경우(미매칭의 약 26%)도 있다 — 실제로
하나씩 대조해보니 두 종류였다: (1) FT WASHINGTON AVENUE(LION은 FORT WASHINGTON
AVENUE), FDR DRIVE(LION은 FRANKLIN D ROOSEVELT DRIVE), WEST 110 STREET(LION은
공식 병기명인 CATHEDRAL PARKWAY)처럼 표기만 다른 경우 — STREET_ALIASES/
STREET_PREFIX_ALIASES로 정규화해서 회수, (2) METRO-NORTH RR/AMTRAK RR(철도),
BEND/DEAD END(교차로 아님), 터널·다리 진출입로(LION에 대응 세그먼트가 아예
없음)처럼 원천적으로 매칭 대상이 아닌 경우 — 이건 그대로 미매칭으로 둔다.
"""

from __future__ import annotations

import os
import re
from datetime import date

import pandas as pd

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.silver import DIM_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="map_road_control_segment")

OUT_SOURCE = "map_road_control_segment"
ROAD_CONTROL_EVENTS_DIR = SILVER_DIR / "road_control_events"

MAX_HOPS = 3

# construction 쪽 도로명이 LION(dim_segment)과 다르게 축약/표기된 경우의 별칭.
# 실제로 미매칭 상위 값을 하나씩 dim_segment 사전과 대조해서 확인한 것만 넣었다
# (추측으로 채우지 않음 — 터널/다리/램프/철도 계열은 LION에 대응하는 이름이
# 아예 없어서 별칭으로 못 살리므로 여기 넣지 않았다. BEND/DEAD END도 실제
# 교차로가 아니라 별칭 대상이 아니다).
STREET_ALIASES = {
    "FDR DRIVE": "FRANKLIN D ROOSEVELT DRIVE",
    "F D R DRIVE": "FRANKLIN D ROOSEVELT DRIVE",
    "N D PERLMAN PLACE": "NATHAN D PERLMAN PLACE",
    "WEST 110 STREET": "CATHEDRAL PARKWAY",  # 서110가의 뉴욕시 공식 병기명
}

# 접두사 축약은 패턴으로 일반화 — FT WASHINGTON AVENUE, FT GEORGE HILL,
# MT MORRIS PARK WEST 등 여러 값에서 반복 확인된 규칙.
STREET_PREFIX_ALIASES = {
    "FT ": "FORT ",
    "MT ": "MOUNT ",
}


def normalize_street_alias(value: str | None) -> str | None:
    """construction 쪽 도로명 표기를 LION(dim_segment) 표기에 맞춰 정규화한다."""
    if value is None:
        return None

    if value in STREET_ALIASES:
        return STREET_ALIASES[value]

    for prefix, replacement in STREET_PREFIX_ALIASES.items():
        if value.startswith(prefix):
            return replacement + value[len(prefix):]

    return value


def load_dim_segment() -> pd.DataFrame:
    return pd.read_parquet(DIM_SEGMENT_PATH, columns=["segment_id", "street_name"])


def build_adjacency_index(dim_segment: pd.DataFrame) -> tuple[dict, dict]:
    """
    (segment_id -> [neighbor_segment_id, ...], segment_id -> street_name) +
    (segment_id -> {shared_node_id: [neighbor_segment_id, ...]}) 딕셔너리.
    후자는 세그먼트의 "양 끝"을 구분해서 각 끝에서 따로 탐색하기 위함 —
    한쪽 끝에서 from_street, 반대쪽 끝에서 to_street를 찾아야 정확한 블록으로
    좁혀진다(모듈 docstring 참고).
    """
    graph = pd.read_parquet(
        GRAPH_SEGMENT_ADJACENCY_PATH, columns=["segment_id", "neighbor_segment_id", "shared_node_id"]
    )
    adjacency = graph.groupby("segment_id")["neighbor_segment_id"].apply(list).to_dict()
    street_by_segment = dim_segment.set_index("segment_id")["street_name"].to_dict()

    endpoint_index: dict[str, dict[str, list[str]]] = {}
    for seg_id, node_id, neighbor_id in zip(
        graph["segment_id"], graph["shared_node_id"], graph["neighbor_segment_id"]
    ):
        endpoint_index.setdefault(seg_id, {}).setdefault(node_id, []).append(neighbor_id)

    return adjacency, street_by_segment, endpoint_index


def reachable_streets_by_end(
    start_segment_id: str,
    on_street: str,
    adjacency: dict,
    street_by_segment: dict,
    endpoint_index: dict,
    max_hops: int = MAX_HOPS,
) -> list[set[str]]:
    """
    start_segment_id의 각 끝(node)에서 따로 출발해, on_street 이름의 세그먼트를
    통해서만 최대 max_hops까지 확장하며 마주치는 도로명을 모은다. 끝(node)별로
    별도 집합을 리턴한다 — from_street/to_street를 "같은 집합 안"이 아니라
    "서로 다른 집합"에서 찾아야 정확한 매칭이 된다.
    """
    ends = endpoint_index.get(start_segment_id, {})
    results = []

    for _, neighbor_ids in ends.items():
        visited = {start_segment_id}
        frontier = set(neighbor_ids)
        found: set[str] = set()

        for _ in range(max_hops):
            next_frontier = set()

            for seg_id in frontier:
                if seg_id in visited:
                    continue
                visited.add(seg_id)

                street = street_by_segment.get(seg_id)
                if street is None:
                    continue

                found.add(street)

                if street == on_street:
                    for nb in adjacency.get(seg_id, []):
                        if nb not in visited:
                            next_frontier.add(nb)

            frontier = next_frontier
            if not frontier:
                break

        results.append(found)

    return results


def load_construction_events(run_date: str) -> pd.DataFrame:
    path = ROAD_CONTROL_EVENTS_DIR / f"dt={run_date}" / "data.parquet"
    df = pd.read_parquet(
        path,
        columns=[
            "permit_id", "on_street", "from_street", "to_street", "control_type",
            "work_start_ts", "work_end_ts", "work_start_hour", "work_end_hour", "work_days_code",
        ],
    )
    df = df[df["control_type"] == "construction"].drop(columns=["control_type"])

    for col in ["on_street", "from_street", "to_street"]:
        df[col] = df[col].map(normalize_street_alias)

    return df


def match(
    construction_events: pd.DataFrame,
    dim_segment: pd.DataFrame,
    adjacency: dict,
    street_by_segment: dict,
    endpoint_index: dict,
) -> pd.DataFrame:
    events = construction_events.reset_index(drop=True).reset_index(names="ce_id")

    # on_street 기준 후보 (도로 단위로만 좁혀진 상태 — 이 시점엔 한 허가당 세그먼트 여러 개)
    candidates = events.merge(
        dim_segment, left_on="on_street", right_on="street_name", how="inner",
    )[["ce_id", "segment_id", "on_street", "from_street", "to_street"]]

    # 세그먼트별 "끝별 도달가능 도로명"은 (segment_id, on_street) 조합당 한 번만
    # 계산해서 캐싱한다 — 같은 도로에 permit이 여러 개면 후보 세그먼트가 반복되기 때문.
    cache: dict[tuple[str, str], list[set[str]]] = {}

    def _reachable_by_end(segment_id: str, on_street: str) -> list[set[str]]:
        key = (segment_id, on_street)
        if key not in cache:
            cache[key] = reachable_streets_by_end(
                segment_id, on_street, adjacency, street_by_segment, endpoint_index
            )
        return cache[key]

    matched_pairs = []
    for row in candidates.itertuples(index=False):
        ends = _reachable_by_end(row.segment_id, row.on_street)

        # from_street와 to_street가 "같은 끝"이 아니라 "서로 다른 끝"에서 각각
        # 발견돼야 매칭으로 본다 — 그래야 실제 블록(구간)에 해당한다.
        ok = any(
            row.from_street in ends[i] and row.to_street in ends[j]
            for i in range(len(ends))
            for j in range(len(ends))
            if i != j
        )
        if ok:
            matched_pairs.append((row.ce_id, row.segment_id))

    matched = pd.DataFrame(matched_pairs, columns=["ce_id", "segment_id"])

    result = events.merge(matched, on="ce_id", how="left").drop(columns=["ce_id"])
    return result


def validate(df: pd.DataFrame, construction_rows: int) -> None:
    if df.empty:
        raise ValueError("map_road_control_segment 결과가 비었습니다.")

    matched = df["segment_id"].notna().sum()
    matched_permits = df.loc[df["segment_id"].notna(), "permit_id"].nunique()

    logger.info(
        "map_road_control_segment 검증 완료: 원본 construction 행수=%d, 매칭된 행=%d(%.1f%%), 매칭된 permit=%d",
        construction_rows, matched, matched / construction_rows * 100, matched_permits,
    )

    multi = df.dropna(subset=["segment_id"]).groupby("permit_id")["segment_id"].nunique()
    multi_over_one = (multi > 1).sum()
    if multi_over_one:
        logger.warning(
            "허가 하나가 segment 2개 이상에 매칭된 경우: %d건 (from/to_street 조합이 애매한 교차로일 수 있음)",
            multi_over_one,
        )


def main(run_date: str | None = None) -> str:
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("map_road_control_segment 변환 시작: run_date=%s", run_date)

    dim_segment = load_dim_segment()
    adjacency, street_by_segment, endpoint_index = build_adjacency_index(dim_segment)
    construction_events = load_construction_events(run_date)

    df = match(construction_events, dim_segment, adjacency, street_by_segment, endpoint_index)
    validate(df, construction_rows=len(construction_events))

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "map_road_control_segment 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


if __name__ == "__main__":
    main()
