"""
Gold: construction + road_closures 진앙 segment을 합쳐서 segment별 closure_penalty
(용량 감소량)를 계산한다 -> dim_segment_closure_penalty.parquet

설계:
1. "진앙(ground zero)" segment 집합 = construction(map_road_control_segment,
   도로명 기반)과 road_closures(map_road_closure_segment, 공간 조인 기반) 둘 다
   합쳐서 만든다 — 사용자가 "겹치는 것도 있어서 하나로 묶고 싶다"고 한 부분.
   두 소스 다 segment_id가 있는 행만 쓴다(매칭 안 된 행은 애초에 반영 불가).
   - construction은 permit_id 단위로 세는 게 맞다(같은 permit이 요일별로 여러
     행을 가질 수 있어서 — src/construction_stipulations/silver.py 참고).
   - road_closures는 permit_id 같은 고유 키가 없어서, (on_street, from_street,
     to_street, work_start_ts, work_end_ts, segment_id) 조합으로 중복 제거한다
     — 같은 폐쇄 건이 purpose만 다르게 여러 행으로 쪼개져 들어오는 경우가
     실제로 있었기 때문(같은 위치·기간에 "PLACE EQUIPMENT..."와 "OCCUPANCY OF
     ROADWAY..."가 별도 행으로 존재) — 이걸 안 걸러내면 같은 폐쇄 건을
     여러 건으로 중복 계산하게 된다.
   - 이렇게 만든 segment당 "closure_count"(그 세그먼트에 직접 걸린 활성
     공사/통제 개수)가 진앙의 기본 강도(intensity)가 된다.

2. graph_segment_adjacency(도로명 매칭 때 쓴 것과 같은 그래프, 이번엔 공간
   확산 용도로 재사용)로 각 진앙에서 최대 MAX_HOPS까지 퍼뜨린다. 홉이 멀수록
   영향이 줄어들도록 HOP_DECAY로 가중치를 곱한다 — 0홉(진앙 자신) 100%,
   1홉 75%, 2홉 50%, 3홉 25%. 이 숫자 자체는 근거 있는 값이 아니라 "홉당
   영향도가 떨어진다"는 정성적 요구만 반영한 초안이다 — TODO(팀 검토 필요).

3. 한 segment가 여러 진앙의 영향권에 겹치면(공사/통제가 여러 개 근처에 있으면)
   각 진앙의 (intensity x hop_decay) 기여도를 전부 합산(누적)한다 — 사용자가
   요청한 "여러 진앙 기여도 합산해서 누적 효과 반영" 부분.

4. 누적된 강도를 실제 용량 감소량(capacity_per_hour와 같은 단위, 음수)으로
   변환한다: closure_capacity_reduction = -min(capacity_per_hour,
   PENALTY_RATIO * capacity_per_hour * intensity). 고정값이 아니라
   capacity_per_hour에 비례한 비율로 깎는다 — 고정값(예: "-300")이면 local
   도로(600)는 반토막 나는데 highway(1900)는 거의 안 줄어드는 문제가 있어서.
   PENALTY_RATIO=0.3도 근거 있는 값이 아닌 초안이다 — TODO(팀 검토 필요).
   min()으로 capacity_per_hour를 넘어서 마이너스 용량이 되는 것은 막는다.

closure_penalty 값은 반드시 <= 0이어야 한다 — get_traffic_score()의
capacity_value = sum(weight * value) 식이 전부 덧셈이라, base_capacity(양수)와
같은 방향으로 더해지려면 closure_penalty 자체가 음수(용량 감소)여야
"용량이 줄어드는" 효과가 난다(양수로 넣으면 반대로 용량이 늘어나버림).
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.silver import DIM_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="closure_penalty")

OUT_SOURCE = "dim_segment_closure_penalty"
MAP_ROAD_CONTROL_SEGMENT_DIR = SILVER_DIR / "map_road_control_segment"
MAP_ROAD_CLOSURE_SEGMENT_DIR = SILVER_DIR / "map_road_closure_segment"

MAX_HOPS = 3
# TODO(팀 검토 필요): 근거 없는 초안 — "홉이 멀수록 영향이 줄어든다"는 정성적
# 요구만 반영한 선형 감쇠.
HOP_DECAY = {0: 1.0, 1: 0.75, 2: 0.5, 3: 0.25}

# 실측해보니(2026-08-13 기준) 진앙 강도가 겹쳐서 누적되는 세그먼트가 많아
# intensity 중앙값이 이미 3.5, 평균 10.4, 최대 664까지 나왔다. 처음엔
# reduction_ratio = min(1, PENALTY_RATIO * intensity) 식의 선형 캡을 썼는데,
# intensity가 1/PENALTY_RATIO(=3.33)만 넘어도 바로 100% 깎여버려서 영향받는
# 세그먼트의 48.8%가 용량 0(=traffic_score 정의 불가, None)이 되는 문제가
# 있었다 — 혼잡도가 클수록 점수가 아주 높아야지 "값 없음"이 되면 안 된다.
#
# 그래서 점근선(포화) 형태로 바꿨다: reduction_ratio = intensity / (intensity + K)
# intensity가 아무리 커져도 100%에 절대 안 닿고(점근선), 강도가 클수록 계속
# 조금씩 더 깎이기만 한다. K(반포화 강도)는 "예전 PENALTY_RATIO=0.3이 의도했던
# intensity=1일 때 30% 감소"와 같은 지점을 지나도록 역산한 값이다:
# intensity/(intensity+K) = 0.3 at intensity=1 => K = (1-0.3)/0.3.
# 둘 다 TODO(팀 검토 필요) — 근거 있는 값이 아니라 "1건 있으면 30% 정도,
# 많아질수록 완만하게 더 깎이되 0은 안 됨"이라는 정성적 의도만 반영한 초안이다.
PENALTY_RATIO = 0.3
HALF_SATURATION_INTENSITY = (1 - PENALTY_RATIO) / PENALTY_RATIO  # ≈ 2.33


def load_ground_zero_intensity(run_date: str) -> pd.Series:
    """segment_id -> closure_count(직접 걸린 활성 공사/통제 개수)."""
    construction_path = MAP_ROAD_CONTROL_SEGMENT_DIR / f"dt={run_date}" / "data.parquet"
    construction = pd.read_parquet(construction_path, columns=["permit_id", "segment_id"])
    construction = construction[construction["segment_id"].notna()]
    construction_counts = (
        construction.drop_duplicates(subset=["permit_id", "segment_id"])
        .groupby("segment_id")
        .size()
    )

    closure_path = MAP_ROAD_CLOSURE_SEGMENT_DIR / f"dt={run_date}" / "data.parquet"
    closures = pd.read_parquet(
        closure_path,
        columns=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"],
    )
    closures = closures[closures["segment_id"].notna()]
    closure_counts = (
        closures.drop_duplicates(subset=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"])
        .groupby("segment_id")
        .size()
    )

    combined = construction_counts.add(closure_counts, fill_value=0)
    logger.info(
        "진앙 segment: construction=%d개, road_closures=%d개, 합쳐서=%d개",
        len(construction_counts), len(closure_counts), len(combined),
    )
    return combined


def load_adjacency() -> dict:
    graph = pd.read_parquet(GRAPH_SEGMENT_ADJACENCY_PATH, columns=["segment_id", "neighbor_segment_id"])
    return graph.groupby("segment_id")["neighbor_segment_id"].apply(list).to_dict()


def spread_with_decay(
    ground_zero: pd.Series,
    adjacency: dict,
    max_hops: int = MAX_HOPS,
    decay: dict[int, float] = HOP_DECAY,
) -> dict[str, float]:
    """
    각 진앙 segment에서 BFS로 max_hops까지 퍼뜨리며 decay[hop] * intensity를
    누적한다. 여러 진앙의 영향권이 겹치는 segment는 기여도가 합산된다.
    """
    accum: dict[str, float] = {}

    for seg_id, intensity in ground_zero.items():
        visited = {seg_id}
        frontier = {seg_id}
        accum[seg_id] = accum.get(seg_id, 0.0) + decay[0] * intensity

        for hop in range(1, max_hops + 1):
            next_frontier = set()
            for s in frontier:
                for nb in adjacency.get(s, []):
                    if nb in visited:
                        continue
                    visited.add(nb)
                    next_frontier.add(nb)
                    accum[nb] = accum.get(nb, 0.0) + decay[hop] * intensity
            frontier = next_frontier
            if not frontier:
                break

    return accum


def to_capacity_reduction(accum: dict[str, float], half_saturation: float = HALF_SATURATION_INTENSITY) -> pd.DataFrame:
    dim = pd.read_parquet(DIM_SEGMENT_PATH, columns=["segment_id", "capacity_per_hour"])
    capacity_by_segment = dim.set_index("segment_id")["capacity_per_hour"].to_dict()

    rows = []
    for seg_id, intensity in accum.items():
        cap = capacity_by_segment.get(seg_id)
        if cap is None or cap <= 0:
            continue
        reduction_ratio = intensity / (intensity + half_saturation)
        reduction = -(cap * reduction_ratio)
        rows.append({"segment_id": seg_id, "closure_intensity": intensity, "closure_capacity_reduction": reduction})

    return pd.DataFrame(rows)


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("dim_segment_closure_penalty 결과가 비었습니다.")

    if (df["closure_capacity_reduction"] > 0).any():
        raise ValueError("closure_capacity_reduction에 양수가 있습니다 — 용량 감소량은 반드시 0 이하여야 합니다.")

    logger.info(
        "dim_segment_closure_penalty 검증 완료: 영향받는 segment=%d개, "
        "평균 감소량=%.1f, 최대 감소량=%.1f",
        len(df), df["closure_capacity_reduction"].mean(), df["closure_capacity_reduction"].min(),
    )


def main(run_date: str | None = None) -> str:
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("closure_penalty 계산 시작: run_date=%s", run_date)

    ground_zero = load_ground_zero_intensity(run_date)
    adjacency = load_adjacency()
    accum = spread_with_decay(ground_zero, adjacency)
    df = to_capacity_reduction(accum)

    validate(df)

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "closure_penalty 완료: rows=%d path=%s",
        len(df), path,
    )
    return str(path)


if __name__ == "__main__":
    main()
