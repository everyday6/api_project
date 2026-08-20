"""
Traffic Score 조회 계층 — segment_id x ts_hour 기준 단일 조회 인터페이스.

설계 원칙 (전부 확장성 때문에 이렇게 짬):
1. 조회 로직은 이 파일의 get_traffic_score() 하나로 모은다. 프론트/API는 이
   함수(또는 이 함수를 감싼 API 엔드포인트)만 호출하고 parquet을 직접 안 읽는다.
2. demand/capacity를 구성하는 컴포넌트(중심성, TLC 수요, 행사, 공사 등)는
   config/traffic_score_weights.yaml에서 가중치·on/off를 관리한다. 코드에
   가중치를 하드코딩하지 않는다 — 새 컴포넌트가 생기면 yaml에 줄만 추가하고
   COMPONENT_SOURCES에 데이터 매핑만 붙이면 된다.
3. ts_hour/ts_date: closure_penalty는 segment_id x hour(0~23) 단위로 계산되고,
   그 활성 여부 자체가 permit의 실제 날짜 범위(work_start_ts~work_end_ts)와
   요일에 달려 있어서(src/scoring/closure_penalty.py) 조회 시각뿐 아니라
   "어느 날짜를 볼 것인지"도 필요하다. 둘 다 None이면 현재 시각/날짜
   (America/New_York 기준)로 자동 대체한다. centrality/base_capacity는 여전히
   분기 1회 갱신되는 정적값이라 시간/날짜에 따라 안 바뀐다 — 지금 실제로
   변하는 건 closure_penalty뿐이다(TLC 수요가 나중에 붙으면 그것도 시간대/
   날짜별이 될 예정).
4. closure_penalty는 "최신 매핑 스냅샷(mapping_dt) 하나"에서 나온 permit
   후보들을, 조회하려는 ts_date 기준으로 활성 여부만 다시 판정하는 식이라
   ts_date를 바꿀 때마다 온디맨드로 재계산한다(디스크에 날짜별로 미리 계산해
   두지 않음 — 과거/미래 날짜를 자유롭게 넘겨볼 수 있어야 해서 모든 날짜를
   미리 구워둘 수 없다). 계산 자체는 몇 초 걸리므로 (component, ts_date)별로
   캐싱해서 같은 날짜 재조회는 즉시 응답한다.
"""

from __future__ import annotations

import math
from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from src.common.config import CONFIG_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.gold2 import closure_penalty, event_boost
from src.lion.gold2 import DIM_SEGMENT_PATH, DIM_SEGMENT_TRAFFIC_SCORE_PATH
from src.tlc.gold import DIM_SEGMENT_TLC_VOLUME_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="scoring_traffic_score")

LOCAL_TZ = ZoneInfo("America/New_York")  # ingest_daily DAG와 동일 기준 시간대

WEIGHTS_CONFIG_PATH = CONFIG_DIR / "traffic_score_weights.yaml"

# 컴포넌트 이름 -> 실제 데이터가 있는 컬럼명 매핑.
# 아직 구현 안 된 컴포넌트(tlc_volume, event_boost)는 값이 None이다 — 누군가
# yaml에서 enabled: true로 켰는데 여기 매핑이 없으면 load_weights()가 바로
# 에러를 낸다(조용히 무시하고 넘어가지 않음). 다른 조원이 새 컴포넌트를
# 구현하면 여기에 한 줄만 추가하면 된다.
COMPONENT_SOURCES: dict[str, str | None] = {
    "centrality": "demand_raw",
    "tlc_volume": "tlc_volume",
    "event_boost": "event_boost",
    "base_capacity": "capacity_per_hour",
    "closure_penalty": "closure_capacity_reduction",
}

# 시간대(hour) x 날짜(ts_date)별로 값이 달라지는 컴포넌트 목록 — value는
# "이 컴포넌트의 (segment_id, hour, value_col) 테이블을 ts_date 하나 기준으로
# 만들어주는 함수"다. 여기 없는 컴포넌트는 _load_base_data()의 정적 테이블
# (분기 1회 갱신)에서 값을 가져온다고 간주한다.
#
# 새 시간대별 컴포넌트(예: TLC 통행량)를 추가할 때 할 일은 딱 두 가지뿐이다 —
# ① COMPONENT_SOURCES에 컬럼명 추가, ② 여기에 "그 컴포넌트 이름: ts_date를
# 받아 segment_id x hour x <값 컬럼> DataFrame을 반환하는 함수" 한 줄 추가.
# get_traffic_score()/get_map_data() 코드는 안 건드려도 된다.
#
# "이 현장 하나만 빼고 계산"(exclude_site)은 여기 없다 — closure_penalty가
# 도시 전체 hop 전파라 무거워서(실측 ~5초), ts_date별로 한 번만 계산해서
# 캐싱해 두고 exclude_site는 _closure_penalty_value()에서 그 캐싱된 결과 위에
# 훨씬 싼 delta 계산으로 따로 처리한다(closure_penalty.compute_site_exclusion_delta
# 참고) — 이 딕셔너리에 넣으면 exclude_site별로 캐시 키가 갈라져서 카드를
# 클릭할 때마다 도시 전체를 다시 계산하게 된다.
HOURLY_COMPONENT_LOADERS: dict[str, "Callable[[str], pd.DataFrame]"] = {
    "closure_penalty": lambda ts_date: closure_penalty.compute_hourly_penalty(
        closure_penalty.load_ground_zero_records(_latest_mapping_dt()),
        ts_date,
        _load_adjacency(),
        _load_capacity_by_segment(),
        _load_lanes_by_segment(),
    ),
    "tlc_volume": lambda ts_date: _load_tlc_volume_table(),
    "event_boost": lambda ts_date: _event_boost_table_for_date(ts_date),
}

_cache: dict[str, Any] = {}


def _latest_partition_path(source_dir_name: str) -> Path | None:
    """<source_dir_name>의 dt= 파티션 중 가장 최근 것을 찾는다.

    ingest_daily가 매일 새로 만드는 테이블들은 dim_segment_traffic_score_v0
    (분기 1회)처럼 고정 경로가 아니라 그날그날 파티션을 스스로 찾아야 한다.
    """
    partitions = sorted((SILVER_DIR / source_dir_name).glob("dt=*/data.parquet"))
    return partitions[-1] if partitions else None


def _latest_run_date(source_dir_name: str) -> str | None:
    """<source_dir_name>의 dt= 파티션 중 가장 최근 날짜 문자열(예: "2026-08-13")."""
    path = _latest_partition_path(source_dir_name)
    if path is None:
        return None
    return path.parent.name.split("=", 1)[1]


def _latest_mapping_dt() -> str:
    """map_road_control_segment(map_road_closure_segment도 같은 날 함께 갱신됨)의
    최신 dt= 파티션 날짜 — closure_penalty 계산의 "진앙 후보" 데이터 스냅샷
    기준이다. 대시보드에서 사용자가 고르는 조회 날짜(ts_date)와는 다른
    개념이다: 이건 "우리가 아는 공사/통제 허가 목록을 언제 기준으로
    수집했는지"이고, ts_date는 "그 허가들 중 어느 게 그 날짜에 실제로
    활성인지" 판단 기준이다.

    _closure_penalty_value()가 exclude_site 경로에서 시간(hour)마다 이 함수를
    부르므로(하루 전체 조회 시 최대 24번) glob 스캔을 매번 반복하지 않도록
    캐싱한다 — 이 스냅샷은 프로세스 실행 중 바뀌지 않는다(매핑 파이프라인이
    새 파티션을 만들어도 서버를 재시작해야 반영되는 다른 캐시들과 동일).
    """
    if "latest_mapping_dt" in _cache:
        return _cache["latest_mapping_dt"]

    mapping_dt = _latest_run_date(closure_penalty.MAP_ROAD_CONTROL_SEGMENT_DIR.name)
    if mapping_dt is None:
        raise RuntimeError("map_road_control_segment 데이터가 없습니다 — 매핑 파이프라인을 먼저 실행하세요.")
    _cache["latest_mapping_dt"] = mapping_dt
    return mapping_dt


def _load_capacity_by_segment() -> dict:
    """segment_id -> capacity_per_hour. 요청마다 다시 읽지 않도록 캐싱한다."""
    if "capacity_by_segment" not in _cache:
        _cache["capacity_by_segment"] = closure_penalty.load_capacity_by_segment()
    return _cache["capacity_by_segment"]


def _load_lanes_by_segment() -> dict:
    """segment_id -> lanes_total. closure_penalty의 NCHRP 차로 기반 보정에
    쓰인다(_lane_aware_half_saturation) — 요청마다 다시 읽지 않도록 캐싱한다."""
    if "lanes_by_segment" not in _cache:
        _cache["lanes_by_segment"] = closure_penalty.load_lanes_by_segment()
    return _cache["lanes_by_segment"]


def _event_boost_table_for_date(ts_date: str) -> pd.DataFrame:
    """event_lion/ticketmaster_lion 매핑이 아직 한 번도 안 돌았으면(둘 다
    수동 트리거 DAG라 그럴 수 있음) 빈 테이블 — 이 경우 event_boost는 0(영향
    없음)으로 처리된다."""
    event_dt = _latest_run_date(str(event_boost.MAP_EVENT_LION_DIR.relative_to(SILVER_DIR)))
    ticketmaster_dt = _latest_run_date(str(event_boost.MAP_TICKETMASTER_LION_DIR.relative_to(SILVER_DIR)))
    if event_dt is None or ticketmaster_dt is None:
        logger.warning(
            "[scoring] event_lion/ticketmaster_lion 매핑이 없습니다 — event_boost를 0으로 처리합니다. "
            "dags/join_lion.py를 실행하면 반영됩니다."
        )
        return pd.DataFrame(columns=["segment_id", "hour", "event_boost"])

    records = event_boost.load_ground_zero_records(event_dt, ticketmaster_dt)
    return event_boost.compute_hourly_boost(records, ts_date, _load_adjacency())


def _load_tlc_volume_table() -> pd.DataFrame:
    """dim_segment_tlc_volume(segment_id, hour, tlc_volume) — 평일 하차량 기준
    percentile(0~1)이라 요일/날짜 구분이 없는 정적 테이블이다(ts_date와 무관 —
    HOURLY_COMPONENT_LOADERS의 다른 컴포넌트와 시그니처만 맞춘다).

    dags/tlc_gold_volume.py를 아직 실행하지 않아 파일이 없으면(수동 트리거 DAG라
    당장은 없을 수 있음) 빈 테이블을 반환한다 — 이 경우 tlc_volume은 모든 조회에서
    0(영향 없음)으로 처리되고, 나중에 파이프라인이 돌면 재시작 없이는 아니지만
    코드 변경 없이 자동으로 반영된다.
    """
    if "tlc_volume_table" in _cache:
        return _cache["tlc_volume_table"]

    if DIM_SEGMENT_TLC_VOLUME_PATH.exists():
        df = pd.read_parquet(DIM_SEGMENT_TLC_VOLUME_PATH, columns=["segment_id", "hour", "tlc_volume"])
    else:
        logger.warning(
            "[scoring] dim_segment_tlc_volume이 없습니다(%s) — tlc_volume을 0으로 처리합니다. "
            "dags/tlc_gold_volume.py를 실행하면 반영됩니다.",
            DIM_SEGMENT_TLC_VOLUME_PATH,
        )
        df = pd.DataFrame(columns=["segment_id", "hour", "tlc_volume"])

    _cache["tlc_volume_table"] = df
    return df


def load_weights(path: Path = WEIGHTS_CONFIG_PATH) -> dict:
    """traffic_score_weights.yaml을 읽고, enabled인데 데이터 소스가 없는 컴포넌트가 있으면 에러."""
    with open(path, encoding="utf-8") as f:
        weights = yaml.safe_load(f)

    for category, components in weights["components"].items():
        for name, cfg in components.items():
            if cfg.get("enabled") and COMPONENT_SOURCES.get(name) is None:
                raise RuntimeError(
                    f"'{category}.{name}'가 enabled=true인데 COMPONENT_SOURCES에 데이터 매핑이 없습니다. "
                    f"src/scoring/traffic_score.py의 COMPONENT_SOURCES에 먼저 연결하세요."
                )

    return weights


def _current_hour() -> int:
    """ts_hour=None일 때 쓸 기본값 — America/New_York 기준 현재 시각(0~23)."""
    return datetime.now(LOCAL_TZ).hour


def _current_date() -> str:
    """ts_date=None일 때 쓸 기본값 — America/New_York 기준 오늘 날짜."""
    return datetime.now(LOCAL_TZ).date().isoformat()


def _load_base_data() -> pd.DataFrame:
    """dim_segment(geometry 등) + dim_segment_traffic_score_v0(컴포넌트 값)를 합친 조회용 테이블.

    시간에 안 따라 바뀌는 정적 컴포넌트만 — closure_penalty(시간대/날짜별)는
    여기 안 넣고 HOURLY_COMPONENT_LOADERS/_load_hourly_table()에서 별도로 관리한다.

    API 요청마다 parquet을 다시 읽지 않도록 프로세스 안에서 한 번만 로드해 캐싱한다.
    """
    if "base_df" in _cache:
        return _cache["base_df"]

    dim = pd.read_parquet(
        DIM_SEGMENT_PATH,
        columns=["segment_id", "geometry", "road_class", "borough_code", "is_routable", "lanes_total"],
    )
    score = pd.read_parquet(DIM_SEGMENT_TRAFFIC_SCORE_PATH)

    df = dim.merge(score, on="segment_id", how="inner").set_index("segment_id", drop=False)
    _cache["base_df"] = df
    logger.info(f"[scoring] 조회용 데이터 로드 완료: {len(df)}행")
    return df


def _load_hourly_table(component_name: str, ts_date: str) -> pd.DataFrame:
    """HOURLY_COMPONENT_LOADERS에 등록된 컴포넌트의, ts_date 기준
    (segment_id, hour, value) 테이블. (component, ts_date) 조합별로 캐싱한다 —
    같은 날짜를 다시 조회할 땐 재계산하지 않는다.
    """
    cache_key = f"hourly_df::{component_name}::{ts_date}"
    if cache_key in _cache:
        return _cache[cache_key]

    df = HOURLY_COMPONENT_LOADERS[component_name](ts_date)
    _cache[cache_key] = df
    return df


def _hourly_lookup(component_name: str, ts_date: str) -> dict[tuple[str, int], float]:
    """(segment_id, hour) -> 값 딕셔너리. 단건 조회(get_traffic_score)용."""
    cache_key = f"hourly_lookup::{component_name}::{ts_date}"
    if cache_key in _cache:
        return _cache[cache_key]

    value_col = COMPONENT_SOURCES[component_name]
    df = _load_hourly_table(component_name, ts_date)
    lookup = {
        (seg, int(hour)): value
        for seg, hour, value in zip(df["segment_id"], df["hour"], df[value_col])
    }
    _cache[cache_key] = lookup
    return lookup


def _load_ground_zero_records_cached(mapping_dt: str) -> pd.DataFrame:
    """closure_penalty.load_ground_zero_records()는 parquet 두 개를 읽는
    비용이 있어서(실측 ~0.4초) 요청마다 다시 읽지 않도록 캐싱한다."""
    cache_key = f"ground_zero_records::{mapping_dt}"
    if cache_key in _cache:
        return _cache[cache_key]
    records = closure_penalty.load_ground_zero_records(mapping_dt)
    _cache[cache_key] = records
    return records


def _closure_intensity_lookup(ts_date: str) -> dict[tuple[str, int], float]:
    """(segment_id, hour) -> 도시 전체 기준 closure_intensity(가공 전 원값).
    _hourly_lookup("closure_penalty", ts_date)이 이미 계산해서 캐싱해 둔
    데이터프레임에서 컬럼만 다르게 뽑는 거라 추가 계산이 없다 —
    _closure_penalty_value()가 exclude_site를 뺄 때 이 원값이 필요하다
    (가공된 closure_capacity_reduction에서는 역산이 안 됨)."""
    cache_key = f"closure_intensity_lookup::{ts_date}"
    if cache_key in _cache:
        return _cache[cache_key]
    df = _load_hourly_table("closure_penalty", ts_date)
    lookup = {
        (seg, int(hour)): value
        for seg, hour, value in zip(df["segment_id"], df["hour"], df["closure_intensity"])
    }
    _cache[cache_key] = lookup
    return lookup


def _closure_penalty_value(segment_id: str, hour: int, ts_date: str, exclude_site: dict | None) -> float:
    """closure_penalty(closure_capacity_reduction) 값을 segment_id x hour
    기준 하나만 뽑는다.

    exclude_site가 없으면 도시 전체 계산 결과(ts_date별로 캐싱됨)에서 그냥
    조회한다 — 기존과 동일.

    exclude_site가 있으면("이 현장 하나만 없었다면") 도시 전체를 그 사이트
    하나 뺀 채로 다시 계산하지 않는다 — compute_hourly_penalty()가 records
    17만 건 이상을 홉 전파하는 무거운 연산이라(실측 ~5초), 카드를 클릭할
    때마다 그러면 대시보드가 그때마다 몇 초씩 멈춘다. 대신:
      1. 도시 전체 결과(이미 캐싱됨)에서 이 (segment_id, hour)의 raw
         closure_intensity를 가져오고,
      2. exclude_site 하나가 그 시각에 기여하는 양만 훨씬 싸게 계산해서
         (closure_penalty.compute_site_exclusion_delta) 빼고,
      3. 남은 intensity로 capacity_reduction만 다시 계산한다(segment 하나짜리
         계산이라 사실상 공짜) — intensity 누적이 진앙별 선형 합이라 이 결과는
         "그 사이트를 빼고 처음부터 다시 계산"한 것과 수학적으로 동일하다.
    """
    if not exclude_site:
        return _hourly_lookup("closure_penalty", ts_date).get((segment_id, hour), 0.0)

    full_intensity = _closure_intensity_lookup(ts_date).get((segment_id, hour), 0.0)
    records = _load_ground_zero_records_cached(_latest_mapping_dt())
    contribution = closure_penalty.compute_site_exclusion_delta(
        records, exclude_site, ts_date, hour, _load_adjacency(), segment_id,
    )
    remaining_intensity = max(0.0, full_intensity - contribution)
    if remaining_intensity <= 0:
        return 0.0

    capacity = _load_capacity_by_segment().get(segment_id)
    if not capacity:
        return 0.0
    half_saturation = closure_penalty._lane_aware_half_saturation(_load_lanes_by_segment().get(segment_id))
    reduction_ratio = (
        closure_penalty.URBAN_WORK_ZONE_MAX_REDUCTION * remaining_intensity / (remaining_intensity + half_saturation)
    )
    return -(capacity * reduction_ratio)


def _sanitize_nan(obj):
    """JSON(RFC 8259)은 NaN을 허용하지 않는데, Starlette의 JSONResponse가
    NaN을 만나면 그대로 ValueError로 죽어서 500이 난다(실측: 맨해튼
    세그먼트의 약 15%가 LION 원본에 lanes_total이 없어서 capacity_per_hour가
    NaN이고, 그 세그먼트를 조회하면 traffic_score 계산 전체가 NaN으로
    번져서 API가 죽었음). 응답에 들어갈 NaN float은 전부 None으로 바꿔서
    "값이 없다"는 의미는 그대로 유지하면서 직렬화 가능하게 만든다."""
    if isinstance(obj, float) and math.isnan(obj):
        return None
    if isinstance(obj, dict):
        return {k: _sanitize_nan(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_nan(v) for v in obj]
    return obj


def _enabled_items(components: dict, row: pd.Series) -> list[dict]:
    """설정에서 enabled인 항목만, {name, value, weight, contribution} 형태로 뽑는다."""
    items = []
    for name, cfg in components.items():
        if not cfg.get("enabled"):
            continue
        column = COMPONENT_SOURCES[name]  # load_weights()에서 이미 존재를 보장함
        value = float(row[column])
        weight = float(cfg["weight"])
        items.append({"name": name, "value": value, "weight": weight, "contribution": value * weight})
    return items


def get_traffic_score(
    segment_id: str,
    ts_hour: int | None = None,
    ts_date: str | None = None,
    include_closure_penalty: bool = True,
    exclude_site: dict | None = None,
) -> dict:
    """
    segment_id x ts_hour x ts_date 하나의 traffic_score와 구성 요소별 세부값을
    돌려주는 단일 조회 인터페이스.

    ts_hour/ts_date: None이면 현재 시각/날짜(America/New_York)로 자동 대체한다.
    closure_penalty가 segment_id x hour이고 그 활성 여부가 실제 permit 날짜
    범위/요일에 달려 있어서 둘 다 결과에 영향을 준다(centrality/base_capacity는
    여전히 분기 1회 정적값).

    include_closure_penalty=False: "공사/통제 영향이 아예 하나도 없었다면"이라는
    가상의 기준선(공사 전) 점수를 계산한다 — 대시보드에서 실제 값(공사 후)과
    나란히 비교해서 "이 시간대는 공사 때문에 점수가 얼마나 깎였는지" 보여주기
    위함이다. yaml 설정 자체를 바꾸는 게 아니라 이 호출 한 번에만 capacity
    쪽 closure_penalty를 로컬로 꺼서 계산하므로, load_weights()가 매 호출
    새로 파싱해 돌려주는 dict를 그대로 건드려도 다른 호출에 영향이 없다.

    exclude_site: include_closure_penalty=True인 상태에서, "이 현장 하나만
    없었다면"을 보고 싶을 때 쓴다 — 그 세그먼트에 다른 공사/통제가 같이
    겹쳐 있어도 그건 그대로 반영하고, 지정한 현장 하나의 기여분만 뺀다.
    closure_penalty.compute_site_exclusion_delta()/_closure_penalty_value() 참고. 카드
    클릭으로 특정 공사를 보고 있을 때(대시보드 "이 현장 없을 때")와,
    include_closure_penalty=False(지도/검색으로 세그먼트 전체를 보고 있을
    때 "모든 공사 없을 때")는 서로 다른 계산이라 라벨을 구분해야 한다.

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    if ts_hour is None:
        ts_hour = _current_hour()
    if ts_date is None:
        ts_date = _current_date()

    weights = load_weights()
    df = _load_base_data()

    if segment_id not in df.index:
        raise KeyError(f"segment_id를 찾을 수 없습니다: {segment_id}")
    row = df.loc[segment_id].to_dict()

    # 시간대별 컴포넌트는 전부 여기서 일괄 채운다 — HOURLY_COMPONENT_LOADERS에
    # 등록된 것만큼 자동으로 반영되고, 값이 없으면(그 시간엔 영향 없음) 0.0.
    # closure_penalty만 예외 — exclude_site가 있으면 도시 전체를 다시 계산하는
    # 대신 _closure_penalty_value()의 저비용 delta 경로를 쓴다(위 함수 참고).
    for name in HOURLY_COMPONENT_LOADERS:
        column = COMPONENT_SOURCES[name]
        if name == "closure_penalty":
            row[column] = _closure_penalty_value(segment_id, ts_hour, ts_date, exclude_site)
        else:
            row[column] = _hourly_lookup(name, ts_date).get((segment_id, ts_hour), 0.0)

    demand_items = _enabled_items(weights["components"]["demand"], row)

    capacity_components = weights["components"]["capacity"]
    if not include_closure_penalty and "closure_penalty" in capacity_components:
        capacity_components = {
            name: (cfg if name != "closure_penalty" else {**cfg, "enabled": False})
            for name, cfg in capacity_components.items()
        }
    capacity_items = _enabled_items(capacity_components, row)

    demand_value = sum(i["contribution"] for i in demand_items)
    capacity_value = sum(i["contribution"] for i in capacity_items)
    traffic_score = (demand_value / capacity_value) if capacity_value else None

    lanes_total = row.get("lanes_total")
    lanes_total = None if lanes_total is None or pd.isna(lanes_total) else int(lanes_total)

    # capacity_per_hour(따라서 base_capacity)가 결측인 세그먼트가 있어서
    # (LION 원본에 lanes_total이 없는 경우, 맨해튼 기준 약 15%) demand_value/
    # capacity_value/traffic_score, components 안의 개별 값까지 NaN으로
    # 번질 수 있다 — capacity_value가 0이 아니라 NaN이면 위 `if capacity_value`
    # 체크를 통과해버려서(NaN은 falsy가 아님) None이 아니라 NaN이 된다.
    # _sanitize_nan()이 이 전체 응답에서 NaN을 전부 None으로 바꿔서 API가
    # 죽지 않게 한다(_sanitize_nan 참고).
    return _sanitize_nan({
        "segment_id": segment_id,
        "ts_hour": ts_hour,
        "ts_date": ts_date,
        "include_closure_penalty": include_closure_penalty,
        "exclude_site": exclude_site,
        "traffic_score": traffic_score,
        "lanes_total": lanes_total,
        "components": {
            "demand": {"value": demand_value, "items": demand_items},
            "capacity": {"value": capacity_value, "items": capacity_items},
        },
    })


def get_traffic_score_hourly(
    segment_id: str,
    ts_date: str | None = None,
    include_closure_penalty: bool = True,
    exclude_site: dict | None = None,
) -> list[dict]:
    """segment_id 하나의 (ts_date 기준) 0~23시 전체 프로파일 — get_traffic_score()를
    24번 재사용한다.

    대시보드의 "하루 전체 막대 그래프"용. _load_base_data()/_hourly_lookup()이
    이미 캐싱돼 있어서 24번 호출해도 실제로는 가벼운 딕셔너리 조회 24번일 뿐이다
    (매번 다시 계산하지 않음).

    include_closure_penalty/exclude_site: get_traffic_score() 참고 — "공사
    전/후 비교" 토글용으로 그대로 전달만 한다.

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    return [
        get_traffic_score(
            segment_id,
            ts_hour=h,
            ts_date=ts_date,
            include_closure_penalty=include_closure_penalty,
            exclude_site=exclude_site,
        )
        for h in range(24)
    ]


def _load_adjacency() -> dict:
    """segment_id -> 인접 segment_id 목록. 요청마다 다시 읽지 않도록 캐싱한다."""
    if "adjacency" not in _cache:
        _cache["adjacency"] = closure_penalty.load_adjacency()
    return _cache["adjacency"]


def get_nearby_segment_scores(
    segment_id: str,
    ts_hour: int | None = None,
    ts_date: str | None = None,
    max_hops: int = 3,
    max_branches: int = 2,
) -> dict:
    """
    segment_id를 중심으로 인접 도로를 최대 max_hops홉까지 BFS로 묶은 트리 —
    대시보드에서 세그먼트를 선택했을 때 "주변 도로만 지도에서 강조"하는
    기능(어떤 segment_id들이 근처인지) + 그 근처 도로들의 호버 툴팁(공사
    전/후 비교)용 데이터를 겸한다.

    각 노드는 traffic_score(공사 후, 실제 반영값)와 traffic_score_before
    (공사/통제 영향이 전혀 없었다면의 가상값, include_closure_penalty=False)
    를 같이 담는다 — "이 근처 도로가 공사 때문에 얼마나 나빠졌는지" 호버로
    바로 비교할 수 있게 하기 위함이다.

    max_branches: 실제 교차로는 막다른 골목이 아닌 이상 인접 도로가 3개
    이상인 경우가 대부분이라, 다 담으면 근처 집합이 감당 안 될 만큼
    커진다(실측: 3홉이면 수십 개까지도 감). 노드당 자식 수를 이 값으로
    제한한다(우선순위 없이 adjacency 그래프에 담긴 순서대로 앞에서부터
    max_branches개).

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    adjacency = _load_adjacency()

    # 먼저 루트 조회로 KeyError를 앞에서 터뜨린다 — 존재하지 않는 segment_id면
    # 트리를 만들 필요도 없이 바로 404로 응답해야 한다.
    get_traffic_score(segment_id, ts_hour=ts_hour, ts_date=ts_date)

    def scores_of(seg_id: str) -> tuple[float | None, float | None]:
        try:
            after = get_traffic_score(seg_id, ts_hour=ts_hour, ts_date=ts_date)["traffic_score"]
        except KeyError:
            # 인접 그래프엔 있는데 dim_segment(+score)엔 없는 경우(비정상
            # 데이터) — 트리에서 빼지 않고 점수만 결측으로 표시한다.
            return None, None
        before = get_traffic_score(
            seg_id, ts_hour=ts_hour, ts_date=ts_date, include_closure_penalty=False
        )["traffic_score"]
        return after, before

    def build(seg_id: str, hop: int, visited: set) -> dict:
        after, before = scores_of(seg_id)
        node = {
            "segment_id": seg_id,
            "hop": hop,
            "traffic_score": after,
            "traffic_score_before": before,
            "is_construction": hop == 0,
            "children": [],
        }
        if hop >= max_hops:
            return node

        neighbors = [n for n in adjacency.get(seg_id, []) if n not in visited][:max_branches]
        visited.update(neighbors)
        node["children"] = [build(n, hop + 1, visited) for n in neighbors]
        return node

    return build(segment_id, 0, {segment_id})


def get_nearby_closures(segment_id: str, ts_hour: int | None = None, ts_date: str | None = None) -> list[dict]:
    """
    segment_id 기준 MAX_HOPS 이내에서 (ts_date, ts_hour) 시점에 활성인 공사/통제
    목록 — 대시보드 "현재 영향받는 공사" 상세 패널용. mapping_dt(최신 매핑
    스냅샷)는 고정하고 ts_date만 바꿔가며 조회할 수 있다.

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    if segment_id not in _load_base_data().index:
        raise KeyError(f"segment_id를 찾을 수 없습니다: {segment_id}")

    if ts_hour is None:
        ts_hour = _current_hour()
    if ts_date is None:
        ts_date = _current_date()

    return closure_penalty.get_nearby_closures(
        segment_id,
        mapping_dt=_latest_mapping_dt(),
        query_date=ts_date,
        hour=ts_hour,
        adjacency=_load_adjacency(),
    )


def get_closure_data_date_range() -> tuple[str, str]:
    """현재 매핑 스냅샷에 있는 공사/통제 permit들의 전체 날짜 범위 — 대시보드
    날짜 선택기 min/max 힌트용."""
    return closure_penalty.get_data_date_range(_latest_mapping_dt())


def get_active_closures(ts_hour: int | None = None, ts_date: str | None = None) -> list[dict]:
    """(ts_date, ts_hour) 시점에 맨해튼 전체에서 활성인 공사/통제 목록 —
    get_nearby_closures()와 달리 특정 segment에 anchor되지 않는다. 대시보드
    "이 날짜에 활성인 공사" 목록(클릭하면 그 segment로 지도 이동+점수 조회)용."""
    if ts_hour is None:
        ts_hour = _current_hour()
    if ts_date is None:
        ts_date = _current_date()

    return closure_penalty.get_active_closures(
        mapping_dt=_latest_mapping_dt(),
        query_date=ts_date,
        hour=ts_hour,
    )


def _load_permit_types() -> pd.DataFrame:
    """공사 permit_id -> permit_type(공사 종류) 매핑. 요청마다 다시 안 읽도록 캐싱."""
    if "permit_types" not in _cache:
        _cache["permit_types"] = closure_penalty.load_permit_types()
    return _cache["permit_types"]


def _load_embargoes_by_permit() -> dict[str, list[dict]]:
    """permit_id -> embargo 기간 목록. extract_work_embargoes()가 Bronze
    전체를 스캔해 비용이 있어 요청마다 다시 계산하지 않도록 캐싱한다."""
    if "embargoes_by_permit" not in _cache:
        _cache["embargoes_by_permit"] = closure_penalty.load_embargoes_by_permit()
    return _cache["embargoes_by_permit"]


def get_newly_issued_closures(ts_date: str | None = None) -> list[dict]:
    """ts_date에 새로 발급된 공사 permit 목록 — get_active_closures()가 "그
    날짜에 진행 중인지"를 보는 것과 달리 "그 날짜에 허가가 올라왔는지"가
    기준이다. road_closures는 발급일 개념이 없어 대상에서 자연히 빠진다
    (construction만)."""
    if ts_date is None:
        ts_date = _current_date()

    return closure_penalty.get_newly_issued_closures(
        mapping_dt=_latest_mapping_dt(),
        query_date=ts_date,
        permit_types=_load_permit_types(),
        embargoes_by_permit=_load_embargoes_by_permit(),
    )


# LION borough_code(문자열) — 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens,
# 5=Staten Island (NYC 공식 자치구 코드, WEST 81 STREET/ARDEN STREET 등 알려진
# 맨해튼 도로로 실측 대조해서 확인함). 지금 프로젝트 범위가 맨해튼뿐이라
# 대시보드 지도에만 적용 — get_traffic_score()는 segment_id 단건 조회라 다른
# 자치구 segment_id가 들어와도 그대로 조회는 되게 두고, "지도에 뭘 그릴지"만
# 여기서 좁힌다. Bronze/Silver 자체는 필터링하지 않는다(요청에 따름).
DASHBOARD_BOROUGH_CODE = "1"


def get_segment_geometries() -> pd.DataFrame:
    """지도 렌더링용 좌표 전용 테이블 — segment_id, geometry (Manhattan 한정).

    geometry는 시간/날짜와 무관한 정적 데이터라, closure_penalty 온디맨드
    계산까지 딸려오는 get_map_data() 대신 이걸 쓰면 가볍게 가져올 수 있다.
    """
    df = _load_base_data()
    df = df[df["borough_code"] == DASHBOARD_BOROUGH_CODE]
    return df[["segment_id", "geometry"]].reset_index(drop=True)


def get_map_data(ts_hour: int | None = None, ts_date: str | None = None) -> pd.DataFrame:
    """지도 렌더링용 벌크 데이터 — segment_id, geometry, road_class, traffic_score.

    맨해튼(DASHBOARD_BOROUGH_CODE)만 반환한다 — 프로젝트 범위 자체가 맨해튼이라.
    ts_hour/ts_date: None이면 현재 시각/날짜(America/New_York)로 자동 대체 —
    get_traffic_score()와 동일.
    """
    if ts_hour is None:
        ts_hour = _current_hour()
    if ts_date is None:
        ts_date = _current_date()

    weights = load_weights()
    df = _load_base_data()
    df = df[df["borough_code"] == DASHBOARD_BOROUGH_CODE].reset_index(drop=True)

    # 시간대별 컴포넌트는 전부 여기서 일괄 병합한다 — HOURLY_COMPONENT_LOADERS에
    # 등록된 것만큼 자동으로 반영된다. 이 ts_date x ts_hour 한 시간대분만 골라서
    # 병합하고, 매칭 안 되는(그 시간엔 영향 없는) segment는 0으로 채운다.
    for name in HOURLY_COMPONENT_LOADERS:
        column = COMPONENT_SOURCES[name]
        hourly = _load_hourly_table(name, ts_date)
        this_hour = hourly.loc[hourly["hour"] == ts_hour, ["segment_id", column]]
        df = df.merge(this_hour, on="segment_id", how="left")
        df[column] = df[column].fillna(0.0)

    demand_cols = [
        COMPONENT_SOURCES[name]
        for name, cfg in weights["components"]["demand"].items()
        if cfg.get("enabled")
    ]
    demand_weights = [
        cfg["weight"]
        for name, cfg in weights["components"]["demand"].items()
        if cfg.get("enabled")
    ]
    capacity_cols = [
        COMPONENT_SOURCES[name]
        for name, cfg in weights["components"]["capacity"].items()
        if cfg.get("enabled")
    ]
    capacity_weights = [
        cfg["weight"]
        for name, cfg in weights["components"]["capacity"].items()
        if cfg.get("enabled")
    ]

    demand_value = sum(df[c] * w for c, w in zip(demand_cols, demand_weights))
    capacity_value = sum(df[c] * w for c, w in zip(capacity_cols, capacity_weights))

    out = df[["segment_id", "geometry", "road_class"]].copy()
    out["traffic_score"] = demand_value / capacity_value.replace(0, pd.NA)
    return out
