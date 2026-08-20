"""
Gold: construction + road_closures 진앙 segment을 합쳐서 segment x hour별
closure_penalty(용량 감소량)를 계산한다 -> dim_segment_closure_penalty.parquet

설계:
1. "진앙(ground zero)" 레코드 = construction(map_road_control_segment, 도로명
   기반)과 road_closures(map_road_closure_segment, 공간 조인 기반) 둘 다 합쳐서
   만든다 — segment_id가 매칭된 행만 쓴다(매칭 안 된 행은 애초에 반영 불가).
   - construction은 permit_id 단위로 중복 제거한다(같은 permit이 요일별로
     여러 행을 가질 수 있어서 — src/construction_stipulations/silver.py 참고).
     work_start_hour/work_end_hour/work_days_code(스티퓰레이션에서 뽑은 작업
     시간대 제약)를 그대로 들고 온다 — 매칭 안 된 permit은 이 값들이 비어있고,
     이 경우 "언제 활성인지 모름 = 항상 활성"으로 보수적으로 취급한다(3번 참고).
   - road_closures는 permit_id 같은 고유 키가 없어서, (on_street, from_street,
     to_street, work_start_ts, work_end_ts, segment_id) 조합으로 중복 제거한다.
     시간대 제약 자체가 원본에 없어서 work_start_hour 등은 전부 비어있다(=항상
     활성으로 취급).

2. query_date(그 날짜 자체)와 요일, 0~23시 각 시간에 대해, "활성"인 레코드만
   골라서 segment별 개수(intensity)를 센다. 활성 여부 판단:
   - 먼저 query_date가 [work_start_ts, work_end_ts] 날짜 범위 안에 있는지
     확인한다(_date_mask) — 허가 자체의 공사 기간을 벗어난 날짜는 애초에
     대상이 아니다. 이 permit 전체 날짜 범위는 항상 존재하는 값이라(bronze
     원본 permit의 핵심 필드) 결측 처리가 필요 없다.
   - work_start_hour/work_end_hour가 있으면 [start, end) 구간 안에 있는지
     확인(자정을 넘기는 야간 구간, 예: 22시~6시도 처리).
   - work_days_code(WEEKDAY/WEEKEND/SATURDAY/SUNDAY/DAILY/EXCEPT_SUNDAY)가
     있으면 query_date의 요일과 맞는지 확인. OTHER(파싱 실패, 차선 문구가 섞여
     복잡한 경우)나 None(애초에 시간대 문구가 없거나 매칭 안 됨)은 "항상
     활성"으로 간주한다 — 모르면 안전하게(더 넓게) 잡는 쪽.

   mapping_dt(어느 dt= 파티션을 읽을지 — map_road_control_segment/
   map_road_closure_segment 매핑 스냅샷)와 query_date(그 permit들 중 어느 게
   "이 날짜"에 활성인지 판단하는 기준 날짜)는 서로 다른 개념이다. main()처럼
   "오늘 갱신된 데이터로 오늘 상태를 본다"는 두 값이 같지만, 대시보드에서
   과거/미래 날짜를 넘겨보는 경우는 mapping_dt(최신 매핑 스냅샷 고정)는 그대로
   두고 query_date만 바뀐다.

3. graph_segment_adjacency로 각 시간대의 진앙에서 최대 MAX_HOPS까지 퍼뜨린다.
   홉이 멀수록 영향이 줄어들도록 HOP_DECAY로 가중치를 곱한다 — 0홉(진앙 자신)
   100%, 1홉 75%, 2홉 50%, 3홉 25%. 이 숫자 자체는 근거 있는 값이 아니라
   "홉당 영향도가 떨어진다"는 정성적 요구만 반영한 초안이다 — TODO(팀 검토 필요).

4. 한 segment가 여러 진앙의 영향권에 겹치면 각 진앙의 (intensity x hop_decay)
   기여도를 전부 합산(누적)한다.

5. 누적된 강도를 실제 용량 감소량(capacity_per_hour와 같은 단위, 음수)으로
   변환한다: reduction_ratio = intensity / (intensity + K) (점근선/포화 곡선 —
   intensity가 아무리 커도 100%에 도달하지 않는다. 처음엔 선형 캡을 썼다가
   영향받는 세그먼트의 48.8%가 용량 0이 되는 문제를 발견해서 이 방식으로
   교체했다). K는 이제 고정 상수가 아니라 NCHRP Report 03-107(HCM 작업구간
   용량 방법론) 실측 범위를 세그먼트의 실제 차로 수(lanes_total)에 맞게
   보정한 값이다 — "차로 1개만 남았을 때 용량이 68%(감소 32%)로 떨어진다"는
   실측 앵커를 지나가도록 세그먼트별로 K를 역산한다(_lane_aware_half_saturation
   참고). lanes_total을 모르는 세그먼트만 예전 고정값(HALF_SATURATION_INTENSITY,
   PENALTY_RATIO=0.3 기준)으로 폴백한다.

결과 스키마: segment_id, hour(0~23), closure_intensity, closure_capacity_reduction.
같은 segment도 시간대별로 다른 행을 가질 수 있다(예: 야간 공사면 낮 시간대엔
이 segment의 행 자체가 없고 → 조회 시 0으로 처리됨).

closure_capacity_reduction 값은 반드시 <= 0이어야 한다 — get_traffic_score()의
capacity_value = sum(weight * value) 식이 전부 덧셈이라, base_capacity(양수)와
같은 방향으로 더해지려면 closure_penalty 자체가 음수(용량 감소)여야
"용량이 줄어드는" 효과가 난다(양수로 넣으면 반대로 용량이 늘어나버림).
"""

from __future__ import annotations

import os
from datetime import date

import pandas as pd

from src.common.config import GOLD1_DIR, GOLD2_DIR, SILVER2_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.construction_stipulations.silver1 import load_built_embargoes
from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.gold2 import DIM_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="closure_penalty")

OUT_SOURCE = "dim_segment_closure_penalty"
MAP_ROAD_CONTROL_SEGMENT_DIR = SILVER2_DIR / "map_road_control_segment"
MAP_ROAD_CLOSURE_SEGMENT_DIR = SILVER2_DIR / "map_road_closure_segment"
# construction Gold1(Manhattan/상태/시리즈로 걸러진 permit)을 읽는다.
CONSTRUCTION_GOLD_DIR = GOLD1_DIR / "construction"

MAX_HOPS = 3
# TODO(팀 검토 필요): 근거 없는 초안 — "홉이 멀수록 영향이 줄어든다"는 정성적
# 요구만 반영한 감쇠. 진앙 segment 자체의 최대 감소율을 100%가 아니라
# URBAN_WORK_ZONE_MAX_REDUCTION(58%)로 캡을 씌운 대신(아래 참고), 그 도로가
# 막혀서 못 가는 차량이 실제로는 주변 도로로 우회하는 효과를 반영하기 위해
# 홉 감쇠값을 기존(0.75/0.5/0.25)보다 올렸다 — 실측 검증된 값은 아니고
# "진앙 하나에 캡을 씌운 만큼 주변으로 더 퍼지게 한다"는 정성적 보정이다.
HOP_DECAY = {0: 1.0, 1: 0.85, 2: 0.65, 3: 0.4}

# PENALTY_RATIO=0.3(고정값) 대신 NCHRP Report 03-107(HCM 작업구간 용량
# 방법론) 실측 범위로 half-saturation을 세그먼트의 실제 차로 수(lanes_total)
# 기반으로 계산한다 — _lane_aware_half_saturation() 참고. lanes_total을 모르는
# 세그먼트(LION 원본에 없는 경우, 맨해튼 기준 약 15%)를 위한 폴백값으로만
# 이 고정 상수를 남겨둔다.
PENALTY_RATIO = 0.3
HALF_SATURATION_INTENSITY = (1 - PENALTY_RATIO) / PENALTY_RATIO  # ≈ 2.33

# NCHRP Report 03-107(FREEVAL-WZ/HCM 7판이 쓰는 작업구간 용량 조정계수,
# CAF) 실측 범위: 다차선 도로에서 "차로 1개만 남았을 때" 용량이 원래의
# 약 68%(감소 32%)로 떨어진다고 보고한다. 이 프로젝트는 정확히 몇 차로가
# 막혔는지는 모르지만(허가 데이터에 없음) LION에 세그먼트별 전체 차로 수
# (lanes_total)는 있어서, "intensity가 늘어나 차로 1개만 남은 것과 같아지는
# 지점"에서 이 실측 감소율이 나오도록 half-saturation을 세그먼트별로 역산한다.
NCHRP_ONE_LANE_OPEN_CAF = 0.68

# reduction_ratio = intensity/(intensity+K)는 K가 아무리 작아도(예: 1차로
# 도로) intensity가 커지면 결국 100%(완전폐쇄)에 수렴한다 — 근데 100% 완전
# 폐쇄는 어떤 레퍼런스로도 검증 안 된 극단값이다(1차로 도로 자체를 측정한
# 연구가 아니라 "다차선 도로가 1차로까지 좁아진" 경우만 측정됨). 대신 HCM
# 도심 도로(urban street) 실측 연구 중 "미드블록 작업구간 존재 시 관측된
# 심각한 사례"(58% 감소, 1,040 vphpl 감소)를 절대 상한으로 쓴다 — 아무리
# intensity가 커져도(=lanes_total 기준 완전폐쇄 시점을 넘어서도) 이 상한
# 이상은 안 깎는다. 대신 그만큼 못 지나가는 차량이 주변 도로로 우회하는
# 효과를 HOP_DECAY를 올려서 반영한다(위 참고).
URBAN_WORK_ZONE_MAX_REDUCTION = 0.58


def _lane_aware_half_saturation(lanes_total: float | None) -> float:
    """reduction_ratio = URBAN_WORK_ZONE_MAX_REDUCTION * intensity/(intensity+K)
    (to_capacity_reduction 참고)에 쓰이는 K를, "차로 (lanes_total-1)개가
    막혀서 1개만 남으면 NCHRP 실측대로 32% 감소" 지점을 지나가도록
    lanes_total 기준으로 계산한다 — K가 작을수록(=차로가 적을수록) 같은
    intensity에도 URBAN_WORK_ZONE_MAX_REDUCTION 상한에 더 빨리 도달한다.

    lanes_total이 1이면(편도 1차로) 그 유일한 차로가 곧 "남은 차로 0개"
    상태와 같아서, 활성 공사가 하나라도 있으면 즉시(또는 거의 즉시) 상한에
    도달한다 — 실제로 1차선 도로는 공사 하나만 걸려도 체감상 거의 막힌 것과
    같다는 상식과 맞지만, 상한 자체가 100%가 아니라 58%로 캡이 걸려 있어
    "완전폐쇄"까지 주장하진 않는다.

    lanes_total을 모르면(결측) 기존 고정값(HALF_SATURATION_INTENSITY,
    PENALTY_RATIO=0.3 기준)으로 폴백한다 — 데이터가 없을 때의 안전한 기본값.
    """
    if lanes_total is None or pd.isna(lanes_total) or lanes_total < 1:
        return HALF_SATURATION_INTENSITY
    if lanes_total <= 1:
        return 1e-6  # 사실상 즉시 포화 — 위 docstring 참고
    lanes_closed_for_one_open = lanes_total - 1
    # reduction_ratio(intensity=lanes_closed_for_one_open) = 1 - CAF가 되도록 역산:
    # (L-1)/((L-1)+K) = 1-CAF  =>  K = (L-1) * CAF / (1-CAF)
    return lanes_closed_for_one_open * NCHRP_ONE_LANE_OPEN_CAF / (1 - NCHRP_ONE_LANE_OPEN_CAF)

# work_days_code -> 그 요일 코드가 활성인 요일(weekday(), 0=월~6=일) 조건.
# 여기 없는 코드(None, "OTHER" 등)는 활성 여부를 모른다는 뜻이라 "항상 활성"으로
# 취급한다(_day_mask 참고).
DAY_CODE_ACTIVE = {
    "WEEKDAY": lambda wd: wd < 5,
    "WEEKEND": lambda wd: wd >= 5,
    "SATURDAY": lambda wd: wd == 5,
    "SUNDAY": lambda wd: wd == 6,
    "DAILY": lambda wd: True,
    "EXCEPT_SUNDAY": lambda wd: wd != 6,
}


def load_ground_zero_records(mapping_dt: str) -> pd.DataFrame:
    """
    mapping_dt= 파티션의 "진앙 후보" 레코드 전체 — segment_id x work_start_ts x
    work_end_ts x work_start_hour x work_end_hour x work_days_code. 특정
    query_date에 실제로 활성인지는 여기서 거르지 않는다(compute_hourly_penalty
    에서 처리) — 후보 목록 자체는 날짜와 무관하게 한 번만 로드해서 여러
    query_date에 재사용할 수 있게 하기 위함.

    on_street/from_street/to_street도 (집계 자체엔 안 쓰이지만) 남겨둔다 —
    permit_id는 물리적 현장 중복 제거 과정에서 이미 버려져서, "이 현장 하나만
    빼고 계산" 같은 요청은 permit_id로는 할 수가 없다. 대신 이 세 필드 +
    work_start_ts/work_end_ts/segment_id 조합이 get_newly_issued_closures()가
    쓰는 것과 동일한 "물리적 현장" 식별 기준이라, compute_site_exclusion_delta()가
    이 조합으로 매칭한다.
    """
    construction_path = MAP_ROAD_CONTROL_SEGMENT_DIR / f"dt={mapping_dt}" / "data.parquet"
    construction = pd.read_parquet(
        construction_path,
        columns=[
            "permit_id", "segment_id", "on_street", "from_street", "to_street",
            "work_start_ts", "work_end_ts", "work_start_hour", "work_end_hour", "work_days_code",
        ],
    )
    construction = construction[construction["segment_id"].notna()]
    # permit_id만으로 중복 제거하면 안 된다 — NYC DOT는 같은 물리적 공사 현장도
    # 규제 항목(장비 배치/자재 적치/도로·보도 점용 등)마다 permit_id를 따로
    # 발급해서, 한 현장이 permit_id 5~30개로 쪼개진 경우가 흔하다(get_newly_issued_
    # closures()에서 먼저 발견한 문제와 동일 원인). 이걸 그대로 두면 "물리적으로
    # 하나인 공사"가 intensity 계산에서 여러 건으로 잡혀 closure_penalty가
    # 실제보다 부풀려진다(실측: 세그먼트 하나에서 permit_id 기준 intensity가
    # 현장 기준의 2배 이상, 극단적으로는 658 vs 178까지 벌어짐 — 그 결과
    # 이미 대부분 세그먼트가 포화 곡선 상단에 몰려 있어서 신규 permit 하나를
    # 추가해도 marginal 영향이 거의 안 보였음). road_closures 쪽과 동일하게
    # (주소+기간+segment) 기준으로 묶어서 "물리적으로 같은 현장"을 하나로 취급한다.
    construction = construction.drop_duplicates(
        subset=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"]
    )
    construction = construction[
        ["segment_id", "on_street", "from_street", "to_street",
         "work_start_ts", "work_end_ts", "work_start_hour", "work_end_hour", "work_days_code"]
    ]

    closure_path = MAP_ROAD_CLOSURE_SEGMENT_DIR / f"dt={mapping_dt}" / "data.parquet"
    closures = pd.read_parquet(
        closure_path,
        columns=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"],
    )
    closures = closures[closures["segment_id"].notna()]
    closures = closures.drop_duplicates(
        subset=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"]
    )
    closures = closures[["segment_id", "on_street", "from_street", "to_street", "work_start_ts", "work_end_ts"]].copy()
    # road_closures 원본엔 시간대 제약 자체가 없다 — NaN으로 두면 아래
    # _hour_mask/_day_mask에서 "항상 활성"으로 처리된다. construction 쪽과
    # 동일한 dtype으로 명시해서 concat 시 전부-NA 컬럼 dtype 추론 경고를 피한다.
    closures["work_start_hour"] = pd.Series(float("nan"), index=closures.index, dtype="float64")
    closures["work_end_hour"] = pd.Series(float("nan"), index=closures.index, dtype="float64")
    closures["work_days_code"] = pd.Series(None, index=closures.index, dtype="object")

    combined = pd.concat([construction, closures], ignore_index=True)
    logger.info(
        "진앙 레코드: construction=%d건, road_closures=%d건, 합쳐서=%d건 (고유 segment=%d개)",
        len(construction), len(closures), len(combined), combined["segment_id"].nunique(),
    )
    return combined


def _date_mask(query_date: str, start: pd.Series, end: pd.Series) -> pd.Series:
    """query_date가 [work_start_ts, work_end_ts] 날짜 범위(시각은 무시하고 날짜만) 안에
    있는지 확인 — 이게 없으면 이미 끝난 과거 공사나 아직 시작 안 한 미래 공사까지
    전부 그 permit이 매핑에 존재한다는 이유만으로 "활성"으로 잘못 집계된다."""
    ts = pd.Timestamp(query_date)
    return (start.dt.normalize() <= ts) & (ts <= end.dt.normalize())


def _hour_mask(hour: int, start: pd.Series, end: pd.Series) -> pd.Series:
    """work_start_hour/end가 없으면(모름) 항상 활성. 있으면 [start, end) 안에
    있는지 확인 — 자정을 넘기는 야간 구간(예: 22시~6시)도 처리한다."""
    known = (start.notna() & end.notna()).to_numpy()
    wraps = known & (start > end).to_numpy(na_value=False)
    normal = known & ~wraps

    mask = pd.Series(True, index=start.index, dtype=bool)
    mask.loc[normal] = (start[normal] <= hour) & (hour < end[normal])
    mask.loc[wraps] = (hour >= start[wraps]) | (hour < end[wraps])
    return mask


def _day_mask(weekday: int, codes: pd.Series) -> pd.Series:
    """work_days_code가 없거나(모름) 매핑에 없는 코드(OTHER 등)면 항상 활성."""
    mask = pd.Series(True, index=codes.index, dtype=bool)
    for code, check in DAY_CODE_ACTIVE.items():
        mask.loc[codes == code] = bool(check(weekday))
    return mask


# segment 기준 홉 거리를 "홉" 대신 사용자에게 보여줄 쉬운 표현으로 바꾼 것 —
# 대시보드 "현재 영향받는 공사" 목록용. 정확히 "블록"과 일치하진 않지만(도로
# 그래프상 인접 segment 기준이라) 사용자에게 익숙한 감각적 표현으로 근사했다.
HOP_LABELS = {
    0: "바로 이 구간",
    1: "한 블록 거리",
    2: "두 블록 거리",
    3: "세 블록 거리",
}


def load_ground_zero_details(mapping_dt: str) -> pd.DataFrame:
    """
    load_ground_zero_records()는 집계(intensity 계산)에 필요한 컬럼만 남기지만,
    여기서는 대시보드 "현재 영향받는 공사" 상세 목록에 보여줄 on_street/
    from_street/to_street/work_start_ts/work_end_ts/purpose까지 전부 보존한다.
    """
    construction_path = MAP_ROAD_CONTROL_SEGMENT_DIR / f"dt={mapping_dt}" / "data.parquet"
    construction = pd.read_parquet(
        construction_path,
        columns=[
            "permit_id", "segment_id", "on_street", "from_street", "to_street",
            "work_start_ts", "work_end_ts", "work_start_hour", "work_end_hour", "work_days_code",
            "permit_issue_ts",
        ],
    )
    construction = construction[construction["segment_id"].notna()]
    construction = construction.drop_duplicates(subset=["permit_id", "segment_id"])
    construction = construction.copy()
    construction["source"] = "construction"
    construction["purpose"] = pd.Series(None, index=construction.index, dtype="object")

    closure_path = MAP_ROAD_CLOSURE_SEGMENT_DIR / f"dt={mapping_dt}" / "data.parquet"
    closures = pd.read_parquet(
        closure_path,
        columns=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id", "purpose"],
    )
    closures = closures[closures["segment_id"].notna()]
    closures = closures.drop_duplicates(
        subset=["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"]
    )
    closures = closures.copy()
    closures["permit_id"] = pd.Series(None, index=closures.index, dtype="object")
    closures["work_start_hour"] = pd.Series(float("nan"), index=closures.index, dtype="float64")
    closures["work_end_hour"] = pd.Series(float("nan"), index=closures.index, dtype="float64")
    closures["work_days_code"] = pd.Series(None, index=closures.index, dtype="object")
    # road_closures 원본엔 "언제 허가가 올라왔는지" 자체가 없다(work 기간만
    # 있음) — "새로 올라온 공사" 목록은 이 필드가 있는 construction만 대상으로
    # 하므로, 결측으로 두면 자연히 그 목록에서 제외된다.
    closures["permit_issue_ts"] = pd.Series(pd.NaT, index=closures.index, dtype="datetime64[ns]")
    closures["source"] = "road_closure"

    cols = [
        "segment_id", "source", "permit_id", "on_street", "from_street", "to_street",
        "work_start_ts", "work_end_ts", "work_start_hour", "work_end_hour", "work_days_code", "purpose",
        "permit_issue_ts",
    ]
    return pd.concat([construction[cols], closures[cols]], ignore_index=True)


def _segments_within_hops(segment_id: str, adjacency: dict, max_hops: int = MAX_HOPS) -> dict[str, int]:
    """segment_id 자신(0홉)부터 max_hops까지 BFS로 도달 가능한 segment별 최소 홉 수."""
    hops = {segment_id: 0}
    frontier = {segment_id}
    for hop in range(1, max_hops + 1):
        next_frontier = set()
        for s in frontier:
            for nb in adjacency.get(s, []):
                if nb in hops:
                    continue
                hops[nb] = hop
                next_frontier.add(nb)
        frontier = next_frontier
        if not frontier:
            break
    return hops


def get_nearby_closures(
    segment_id: str,
    mapping_dt: str,
    query_date: str,
    hour: int,
    adjacency: dict,
    max_hops: int = MAX_HOPS,
) -> list[dict]:
    """
    segment_id 기준 max_hops 이내에서, (query_date, hour) 시점에 실제로 활성인
    공사/통제를 홉 거리와 함께 반환한다 — 대시보드 "현재 영향받는 공사" 목록용.
    가까운 순(홉 오름차순, 그다음 시작일순)으로 정렬해서 반환한다.
    """
    weekday = date.fromisoformat(query_date).weekday()
    hops_by_segment = _segments_within_hops(segment_id, adjacency, max_hops)

    details = load_ground_zero_details(mapping_dt)
    nearby = details[details["segment_id"].isin(hops_by_segment.keys())]
    if nearby.empty:
        return []

    active = nearby[
        _date_mask(query_date, nearby["work_start_ts"], nearby["work_end_ts"])
        & _hour_mask(hour, nearby["work_start_hour"], nearby["work_end_hour"])
        & _day_mask(weekday, nearby["work_days_code"])
    ]
    if active.empty:
        return []

    # 같은 구간의 같은 도로/기간을 permit_id만 다르게(하나의 공사가 여러 개
    # 허가 번호로 쪼개진 경우) 여러 번 들고 있는 경우가 있다 — UI엔 permit_id를
    # 안 보여주므로 이대로 두면 사용자에게 완전히 동일해 보이는 항목이 여러 번
    # 뜬다. 표시 기준(구간/도로/기간)으로 중복 제거해서 하나만 보여준다.
    active = active.drop_duplicates(
        subset=["segment_id", "on_street", "from_street", "to_street", "work_start_ts", "work_end_ts"]
    )

    rows = []
    for row in active.itertuples(index=False):
        hop = hops_by_segment[row.segment_id]
        rows.append({
            "segment_id": row.segment_id,
            "hop": hop,
            "hop_label": HOP_LABELS.get(hop, f"{hop}블록 거리"),
            "source": row.source,
            "on_street": row.on_street,
            "from_street": row.from_street,
            "to_street": row.to_street,
            "work_start_ts": None if pd.isna(row.work_start_ts) else str(row.work_start_ts),
            "work_end_ts": None if pd.isna(row.work_end_ts) else str(row.work_end_ts),
            "purpose": None if row.purpose is None or pd.isna(row.purpose) else row.purpose,
        })

    rows.sort(key=lambda r: (r["hop"], r["work_start_ts"] or ""))
    return rows


def get_active_closures(mapping_dt: str, query_date: str, hour: int) -> list[dict]:
    """
    (query_date, hour) 시점에 실제로 활성인 공사/통제 전체 목록 — get_nearby_closures()
    와 달리 특정 segment 인접 여부와 무관하게 맨해튼 전체를 대상으로 한다.
    대시보드 "이 날짜에 활성인 공사" 목록(검색/클릭해서 지도 이동)용이라, 하나의
    permit이 여러 segment_id에 매핑된 경우 대표 segment_id 하나만 남긴다
    (지도 이동/선택 앵커로 아무 세그먼트나 하나면 충분하므로).
    시작일 오름차순으로 정렬해서 반환한다.
    """
    weekday = date.fromisoformat(query_date).weekday()
    details = load_ground_zero_details(mapping_dt)

    active = details[
        _date_mask(query_date, details["work_start_ts"], details["work_end_ts"])
        & _hour_mask(hour, details["work_start_hour"], details["work_end_hour"])
        & _day_mask(weekday, details["work_days_code"])
    ]
    if active.empty:
        return []

    # 같은 공사가 여러 segment_id에 매핑된 경우 대표 하나만 남긴다(정렬 후
    # groupby().first() — segment_id 오름차순 중 가장 앞선 것을 대표로 고정).
    active = active.sort_values("segment_id")
    representative = active.groupby(
        ["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "source"],
        as_index=False,
    ).first()

    rows = []
    for row in representative.itertuples(index=False):
        rows.append({
            "segment_id": row.segment_id,
            "source": row.source,
            "on_street": row.on_street,
            "from_street": row.from_street,
            "to_street": row.to_street,
            "work_start_ts": None if pd.isna(row.work_start_ts) else str(row.work_start_ts),
            "work_end_ts": None if pd.isna(row.work_end_ts) else str(row.work_end_ts),
            "purpose": None if row.purpose is None or pd.isna(row.purpose) else row.purpose,
        })

    rows.sort(key=lambda r: r["work_start_ts"] or "")
    return rows


def load_permit_types() -> pd.DataFrame:
    """공사 permit_id -> permit_type(공사 종류, 예: "PLACE EQUIPMENT OTHER THAN
    CRANE OR SHOV") 매핑 — construction Gold 최신 파티션에서 가져온다.
    "이 날짜에 새로 올라온 공사" 목록에서 어떤 공사인지 보여주는 용도.
    permit_series(대분류, 4종류뿐)보다 permit_type(154종류)이 더 구체적이라
    이쪽을 쓴다."""
    partitions = sorted(CONSTRUCTION_GOLD_DIR.glob("dt=*/data.parquet"))
    if not partitions:
        return pd.DataFrame(columns=["permit_id", "permit_type"])
    return pd.read_parquet(partitions[-1], columns=["permit_id", "permit_type"]).drop_duplicates()


def load_embargoes_by_permit() -> dict[str, list[dict]]:
    """permitnumber -> 그 permit의 embargo(행사 때문에 작업 일시 중단) 기간
    목록. "이 날짜에 새로 올라온 공사" 상세 표시(참고 정보)용이다 — embargo는
    연중 특정 날짜에만 있는 예외적인 사건이라 closure_penalty/traffic_score
    계산에는 반영하지 않는다(compute_hourly_penalty() docstring 참고). 하나의
    permit이 여러 embargo 기간을 가질 수 있어 permit 기준으로 그룹핑한다.

    load_built_embargoes()를 쓴다 — construction_pipeline.py의
    extract_embargoes 태스크(build_embargoes())가 정규식+LLM 폴백까지 미리
    다 처리해서 저장해 둔 결과라, 이 온디맨드 API 경로에서는 동기 LLM 호출이
    전혀 일어나지 않는다."""
    embargoes = load_built_embargoes()
    if embargoes.empty:
        return {}

    result: dict[str, list[dict]] = {}
    for row in embargoes.itertuples(index=False):
        result.setdefault(row.permitnumber, []).append({
            "start_date": row.embargo_start_date.isoformat(),
            "end_date": row.embargo_end_date.isoformat(),
            "start_hour": int(row.embargo_start_hour),
            "end_hour": int(row.embargo_end_hour),
            "reason": row.embargo_reason,
        })
    return result


def get_newly_issued_closures(
    mapping_dt: str,
    query_date: str,
    permit_types: pd.DataFrame | None = None,
    embargoes_by_permit: dict[str, list[dict]] | None = None,
) -> list[dict]:
    """
    query_date에 새로 발급된(permit_issue_ts) 공사 permit 목록 —
    get_active_closures()가 "그 날짜에 공사가 진행 중인지"를 보는 것과 달리
    "그 날짜에 허가 자체가 올라왔는지"가 기준이다. road_closures는 발급일
    개념이 원본 데이터에 없어 permit_issue_ts가 항상 결측이므로 자연히
    이 목록에서 빠진다(construction만 대상).

    하나의 permit이 여러 segment_id에 매핑된 경우 대표 segment_id 하나만
    남긴다.

    NYC DOT는 같은 공사 현장이라도 규제 항목(장비 배치/자재 적치/도로 점용/
    보도 점용/폐기물 컨테이너 등)마다 permit_id를 따로 발급한다 — 그래서
    (on_street, from_street, to_street, work_start_ts, work_end_ts,
    segment_id)가 완전히 같은 permit이 5~6건씩 나오는 경우가 흔하다(전체
    데이터 기준 4만7천여 그룹, 심하면 한 그룹에 30건 이상). 이 조합이 같은
    permit들은 가장 먼저 발급된 1건만 대표로 남긴다 — 대신 permit_type을
    같이 내려줘서 "왜 여러 건처럼 보였는지"(규제 항목이 다름)를 사용자가
    확인할 수 있게 한다.

    permit_types/embargoes_by_permit을 넘기면 각각 permit_type(공사 종류),
    embargoes(행사로 인한 임시 중단 기간 목록)를 결과에 붙인다 — 둘 다
    load에 비용이 있어(embargoes는 특히 Bronze 전체 스캔) 호출부가 캐싱해서
    넘기는 걸 전제로 한다(traffic_score.py 참고). 안 넘기면(None) 각각
    permit_type=None, embargoes=[]로 채운다.

    발급 시각 오름차순으로 정렬해서 반환한다.
    """
    details = load_ground_zero_details(mapping_dt)
    target = pd.Timestamp(query_date)

    issued_today = details[
        details["permit_issue_ts"].notna()
        & (details["permit_issue_ts"].dt.normalize() == target)
    ]
    if issued_today.empty:
        return []

    issued_today = issued_today.sort_values("segment_id")
    representative = issued_today.groupby("permit_id", as_index=False).first()

    representative = representative.sort_values("permit_issue_ts")
    representative = representative.groupby(
        ["on_street", "from_street", "to_street", "work_start_ts", "work_end_ts", "segment_id"],
        as_index=False,
    ).first()

    if permit_types is not None and not permit_types.empty:
        representative = representative.merge(permit_types, on="permit_id", how="left")
    else:
        representative["permit_type"] = None

    embargoes_by_permit = embargoes_by_permit or {}

    rows = []
    for row in representative.itertuples(index=False):
        rows.append({
            "segment_id": row.segment_id,
            "permit_id": row.permit_id,
            "permit_type": None if pd.isna(row.permit_type) else row.permit_type,
            "on_street": row.on_street,
            "from_street": row.from_street,
            "to_street": row.to_street,
            "work_start_ts": None if pd.isna(row.work_start_ts) else str(row.work_start_ts),
            "work_end_ts": None if pd.isna(row.work_end_ts) else str(row.work_end_ts),
            "permit_issue_ts": None if pd.isna(row.permit_issue_ts) else str(row.permit_issue_ts),
            "work_start_hour": None if pd.isna(row.work_start_hour) else int(row.work_start_hour),
            "work_end_hour": None if pd.isna(row.work_end_hour) else int(row.work_end_hour),
            "work_days_code": row.work_days_code,
            "embargoes": embargoes_by_permit.get(row.permit_id, []),
        })

    rows.sort(key=lambda r: r["permit_issue_ts"] or "")
    return rows


def get_data_date_range(mapping_dt: str) -> tuple[str, str]:
    """mapping_dt= 파티션에 있는 permit들의 work_start_ts~work_end_ts 전체 범위
    (날짜 문자열) — 대시보드 날짜 선택기에서 "이 범위를 벗어나면 봐도 영향 없는
    날짜다"를 안내하는 min/max 힌트용."""
    records = load_ground_zero_records(mapping_dt)
    return records["work_start_ts"].min().date().isoformat(), records["work_end_ts"].max().date().isoformat()


def load_adjacency() -> dict:
    graph = pd.read_parquet(GRAPH_SEGMENT_ADJACENCY_PATH, columns=["segment_id", "neighbor_segment_id"])
    return graph.groupby("segment_id")["neighbor_segment_id"].apply(list).to_dict()


def load_capacity_by_segment() -> dict:
    dim = pd.read_parquet(DIM_SEGMENT_PATH, columns=["segment_id", "capacity_per_hour"])
    return dim.set_index("segment_id")["capacity_per_hour"].to_dict()


def load_lanes_by_segment() -> dict:
    """_lane_aware_half_saturation()에 넘길 세그먼트별 전체 차로 수."""
    dim = pd.read_parquet(DIM_SEGMENT_PATH, columns=["segment_id", "lanes_total"])
    return dim.set_index("segment_id")["lanes_total"].to_dict()


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


def to_capacity_reduction(
    accum: dict[str, float],
    capacity_by_segment: dict,
    lanes_by_segment: dict | None = None,
) -> pd.DataFrame:
    """lanes_by_segment가 있으면 세그먼트별 차로 수 기준 NCHRP 보정 half
    saturation을 쓰고(_lane_aware_half_saturation), 없으면(예: 기존 호출부
    호환) 고정값(HALF_SATURATION_INTENSITY)을 그대로 쓴다."""
    rows = []
    for seg_id, intensity in accum.items():
        cap = capacity_by_segment.get(seg_id)
        if cap is None or cap <= 0:
            continue
        if lanes_by_segment is not None:
            half_saturation = _lane_aware_half_saturation(lanes_by_segment.get(seg_id))
        else:
            half_saturation = HALF_SATURATION_INTENSITY
        reduction_ratio = URBAN_WORK_ZONE_MAX_REDUCTION * intensity / (intensity + half_saturation)
        reduction = -(cap * reduction_ratio)
        rows.append({"segment_id": seg_id, "closure_intensity": intensity, "closure_capacity_reduction": reduction})

    return pd.DataFrame(rows)


def compute_hourly_penalty(
    records: pd.DataFrame,
    query_date: str,
    adjacency: dict,
    capacity_by_segment: dict,
    lanes_by_segment: dict | None = None,
) -> pd.DataFrame:
    """query_date 기준 날짜 범위로 먼저 걸러낸 뒤, 0~23시 각각에 대해 그 시각
    기준 활성인 레코드만으로 감쇠/합산/용량감소를 계산한다.

    embargo(행사 때문에 작업이 일시 중단되는 기간)는 여기서 반영하지 않는다 —
    한 번 시도했다가 되돌렸다. embargo는 연중 특정 날짜에만 있는 예외적인
    사건인데, 이걸 traffic_score(평소 언제 공사 영향이 있는지 보여주는 지표)에
    바로 반영하면 "그날 마침 대형 행사가 있었는지"에 따라 평소 패턴이 왜곡돼
    보인다(실측: Summer Streets embargo가 걸린 날 하루 전체 공사 영향이
    0으로 보였음). embargo 정보는 대신 get_newly_issued_closures()에서
    "이 permit에 이런 embargo 기간이 있다"는 참고 정보로만 보여준다.

    "이 현장 하나만 빼고 계산" 같은 요청은 이 함수를 다시 통째로 돌리지 않고
    compute_site_exclusion_delta()로 처리한다 — records가 17만 건이 넘어
    도시 전체 hop 전파 자체가 무겁다(실측 ~5초). 대시보드에서 카드를 클릭할
    때마다 이 함수를 다시 부르면 그때마다 5초씩 걸린다 — 대신 이미 계산된
    전체 결과에서 그 사이트 하나의 기여분만 빼는 쪽이 훨씬 싸다(아래 함수
    참고).
    """
    weekday = date.fromisoformat(query_date).weekday()
    in_range = records[_date_mask(query_date, records["work_start_ts"], records["work_end_ts"])]

    frames = []

    for hour in range(24):
        active = in_range[
            _hour_mask(hour, in_range["work_start_hour"], in_range["work_end_hour"])
            & _day_mask(weekday, in_range["work_days_code"])
        ]
        if active.empty:
            continue

        intensity = active.groupby("segment_id").size()
        accum = spread_with_decay(intensity, adjacency)
        hour_df = to_capacity_reduction(accum, capacity_by_segment, lanes_by_segment)
        if hour_df.empty:
            continue
        hour_df["hour"] = hour
        frames.append(hour_df)

    if not frames:
        return pd.DataFrame(columns=["segment_id", "closure_intensity", "closure_capacity_reduction", "hour"])

    return pd.concat(frames, ignore_index=True)


def compute_site_exclusion_delta(
    records: pd.DataFrame,
    exclude_site: dict,
    query_date: str,
    hour: int,
    adjacency: dict,
    target_segment_id: str,
    max_hops: int = MAX_HOPS,
    decay: dict[int, float] = HOP_DECAY,
) -> float:
    """exclude_site 진앙 레코드 하나가 (query_date, hour)에 활성이면서
    target_segment_id에 hop 감쇠로 기여하는 intensity량 — "도시 전체 결과에서
    이 사이트 하나만 뺀 값"을 얻기 위한 저비용 헬퍼다.

    compute_hourly_penalty()에 exclude_site를 넘겨서 records에서 그 한 행만
    뺀 뒤 처음부터 다시 계산해도 결과는 동일하지만, records가 17만 건이 넘어
    도시 전체 hop 전파 자체가 무겁다(실측 ~5초). intensity 누적
    (spread_with_decay)이 진앙별로 독립적으로 더해지는 선형 합이라는 점을
    이용하면, "전체 결과 - 이 사이트 하나의 기여분"이 "이 사이트를 뺀 전체
    재계산"과 수학적으로 완전히 동일하다. 그래서 이 함수는 그 사이트 하나가
    실제로 활성인지만 확인하고(boolean mask, 벡터 연산이라 빠름) hop 거리를
    구해서(BFS가 max_hops로 깊이 제한돼 있어 그래프 크기와 무관하게 빠름)
    감쇠값 하나만 돌려준다 — 호출부(get_traffic_score)가 캐싱된 전체
    intensity에서 이 값을 빼기만 하면 된다.

    exclude_site와 매칭되는 레코드가 없거나(이미 지워졌거나 오타 등), 그
    시각에 비활성이거나, target_segment_id가 hop 범위 밖이면 0.0을 반환한다
    (= 뺄 게 없다 = 전체 결과 그대로).
    """
    match = (
        (records["on_street"] == exclude_site["on_street"])
        & (records["from_street"] == exclude_site["from_street"])
        & (records["to_street"] == exclude_site["to_street"])
        & (records["work_start_ts"] == pd.Timestamp(exclude_site["work_start_ts"]))
        & (records["work_end_ts"] == pd.Timestamp(exclude_site["work_end_ts"]))
        & (records["segment_id"] == exclude_site["segment_id"])
    )
    site_row = records[match].head(1)
    if site_row.empty:
        return 0.0

    weekday = date.fromisoformat(query_date).weekday()
    in_range = site_row[_date_mask(query_date, site_row["work_start_ts"], site_row["work_end_ts"])]
    if in_range.empty:
        return 0.0
    active = in_range[
        _hour_mask(hour, in_range["work_start_hour"], in_range["work_end_hour"])
        & _day_mask(weekday, in_range["work_days_code"])
    ]
    if active.empty:
        return 0.0

    hops = _segments_within_hops(exclude_site["segment_id"], adjacency, max_hops)
    hop = hops.get(target_segment_id)
    if hop is None:
        return 0.0
    return decay[hop]


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("dim_segment_closure_penalty 결과가 비었습니다.")

    if (df["closure_capacity_reduction"] > 0).any():
        raise ValueError("closure_capacity_reduction에 양수가 있습니다 — 용량 감소량은 반드시 0 이하여야 합니다.")

    if not df["hour"].between(0, 23).all():
        raise ValueError("hour 값이 0~23 범위를 벗어난 행이 있습니다.")

    logger.info(
        "dim_segment_closure_penalty 검증 완료: 행=%d(segment x hour), 영향받는 고유 segment=%d개, "
        "평균 감소량=%.1f, 최대 감소량=%.1f",
        len(df), df["segment_id"].nunique(),
        df["closure_capacity_reduction"].mean(), df["closure_capacity_reduction"].min(),
    )
    logger.info("시간대별 영향받는 segment 수:\n%s", df.groupby("hour")["segment_id"].nunique().to_string())


def build(run_date: str | None = None) -> str:
    """load -> compute -> save만 한다(validate 없음). run_date 하루치를
    mapping_dt(어느 매핑 스냅샷을 읽을지)이자 query_date(그 permit들 중 어느
    게 이 날짜에 활성인지)로 동일하게 쓴다("오늘 갱신된 데이터로 오늘 상태를
    본다"는 배치 시나리오라 둘이 항상 같음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("closure_penalty 계산 시작: run_date=%s", run_date)

    records = load_ground_zero_records(run_date)
    adjacency = load_adjacency()
    capacity_by_segment = load_capacity_by_segment()
    lanes_by_segment = load_lanes_by_segment()

    df = compute_hourly_penalty(records, run_date, adjacency, capacity_by_segment, lanes_by_segment)

    path = save_parquet(df, GOLD2_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "closure_penalty 빌드 완료: rows=%d path=%s",
        len(df), path,
    )
    return str(path)


def validate_output(path: str) -> str:
    """build()가 저장한 결과를 다시 읽어 validate()를 돌린다."""
    df = pd.read_parquet(path)
    validate(df)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    path = build(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()
