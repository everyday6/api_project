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
   교체했다). K(HALF_SATURATION_INTENSITY)도 근거 있는 값이 아닌 초안이다 —
   TODO(팀 검토 필요).

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

# TODO(팀 검토 필요): PENALTY_RATIO=0.3은 "활성 공사/통제 1건(intensity=1)당
# capacity_per_hour의 30%를 깎는다"는 의도를 반영한 값이고, 이걸 점근선 곡선
# intensity/(intensity+K)가 intensity=1에서 지나가도록 역산해서 K를 구했다.
# 자세한 경위(선형 캡의 문제)는 모듈 docstring 5번 참고.
PENALTY_RATIO = 0.3
HALF_SATURATION_INTENSITY = (1 - PENALTY_RATIO) / PENALTY_RATIO  # ≈ 2.33

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
    """
    construction_path = MAP_ROAD_CONTROL_SEGMENT_DIR / f"dt={mapping_dt}" / "data.parquet"
    construction = pd.read_parquet(
        construction_path,
        columns=[
            "permit_id", "segment_id", "work_start_ts", "work_end_ts",
            "work_start_hour", "work_end_hour", "work_days_code",
        ],
    )
    construction = construction[construction["segment_id"].notna()]
    construction = construction.drop_duplicates(subset=["permit_id", "segment_id"])
    construction = construction[
        ["segment_id", "work_start_ts", "work_end_ts", "work_start_hour", "work_end_hour", "work_days_code"]
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
    closures = closures[["segment_id", "work_start_ts", "work_end_ts"]].copy()
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


def get_newly_issued_closures(mapping_dt: str, query_date: str) -> list[dict]:
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
    데이터 기준 4만7천여 그룹, 심하면 한 그룹에 30건 이상). permit_type을
    보여주지 않는 이 목록에서는 사용자에게 "완전 중복"으로 보이므로, 이
    조합이 같은 permit들은 가장 먼저 발급된 1건만 대표로 남긴다.

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

    rows = []
    for row in representative.itertuples(index=False):
        rows.append({
            "segment_id": row.segment_id,
            "permit_id": row.permit_id,
            "on_street": row.on_street,
            "from_street": row.from_street,
            "to_street": row.to_street,
            "work_start_ts": None if pd.isna(row.work_start_ts) else str(row.work_start_ts),
            "work_end_ts": None if pd.isna(row.work_end_ts) else str(row.work_end_ts),
            "permit_issue_ts": None if pd.isna(row.permit_issue_ts) else str(row.permit_issue_ts),
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
    half_saturation: float = HALF_SATURATION_INTENSITY,
) -> pd.DataFrame:
    rows = []
    for seg_id, intensity in accum.items():
        cap = capacity_by_segment.get(seg_id)
        if cap is None or cap <= 0:
            continue
        reduction_ratio = intensity / (intensity + half_saturation)
        reduction = -(cap * reduction_ratio)
        rows.append({"segment_id": seg_id, "closure_intensity": intensity, "closure_capacity_reduction": reduction})

    return pd.DataFrame(rows)


def compute_hourly_penalty(
    records: pd.DataFrame,
    query_date: str,
    adjacency: dict,
    capacity_by_segment: dict,
) -> pd.DataFrame:
    """query_date 기준 날짜 범위로 먼저 걸러낸 뒤, 0~23시 각각에 대해 그 시각
    기준 활성인 레코드만으로 감쇠/합산/용량감소를 계산한다."""
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
        hour_df = to_capacity_reduction(accum, capacity_by_segment)
        if hour_df.empty:
            continue
        hour_df["hour"] = hour
        frames.append(hour_df)

    if not frames:
        return pd.DataFrame(columns=["segment_id", "closure_intensity", "closure_capacity_reduction", "hour"])

    return pd.concat(frames, ignore_index=True)


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


def main(run_date: str | None = None) -> str:
    """일 배치용 진입점 — run_date 하루치를 mapping_dt(어느 매핑 스냅샷을 읽을지)
    이자 query_date(그 permit들 중 어느 게 이 날짜에 활성인지)로 동일하게 쓴다
    ("오늘 갱신된 데이터로 오늘 상태를 본다"는 배치 시나리오라 둘이 항상 같음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("closure_penalty 계산 시작: run_date=%s", run_date)

    records = load_ground_zero_records(run_date)
    adjacency = load_adjacency()
    capacity_by_segment = load_capacity_by_segment()

    df = compute_hourly_penalty(records, run_date, adjacency, capacity_by_segment)
    validate(df)

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "closure_penalty 완료: rows=%d path=%s",
        len(df), path,
    )
    return str(path)


if __name__ == "__main__":
    main()
