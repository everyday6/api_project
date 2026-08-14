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

from datetime import datetime
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from src.common.config import CONFIG_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.lion.silver import DIM_SEGMENT_PATH
from src.lion.traffic_score import DIM_SEGMENT_TRAFFIC_SCORE_PATH
from src.scoring import closure_penalty, event_boost
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
HOURLY_COMPONENT_LOADERS: dict[str, "Callable[[str], pd.DataFrame]"] = {
    "closure_penalty": lambda ts_date: closure_penalty.compute_hourly_penalty(
        closure_penalty.load_ground_zero_records(_latest_mapping_dt()),
        ts_date,
        _load_adjacency(),
        _load_capacity_by_segment(),
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
    """
    mapping_dt = _latest_run_date(closure_penalty.MAP_ROAD_CONTROL_SEGMENT_DIR.name)
    if mapping_dt is None:
        raise RuntimeError("map_road_control_segment 데이터가 없습니다 — 매핑 파이프라인을 먼저 실행하세요.")
    return mapping_dt


def _load_capacity_by_segment() -> dict:
    """segment_id -> capacity_per_hour. 요청마다 다시 읽지 않도록 캐싱한다."""
    if "capacity_by_segment" not in _cache:
        _cache["capacity_by_segment"] = closure_penalty.load_capacity_by_segment()
    return _cache["capacity_by_segment"]


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


def get_traffic_score(segment_id: str, ts_hour: int | None = None, ts_date: str | None = None) -> dict:
    """
    segment_id x ts_hour x ts_date 하나의 traffic_score와 구성 요소별 세부값을
    돌려주는 단일 조회 인터페이스.

    ts_hour/ts_date: None이면 현재 시각/날짜(America/New_York)로 자동 대체한다.
    closure_penalty가 segment_id x hour이고 그 활성 여부가 실제 permit 날짜
    범위/요일에 달려 있어서 둘 다 결과에 영향을 준다(centrality/base_capacity는
    여전히 분기 1회 정적값).

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
    for name in HOURLY_COMPONENT_LOADERS:
        column = COMPONENT_SOURCES[name]
        row[column] = _hourly_lookup(name, ts_date).get((segment_id, ts_hour), 0.0)

    demand_items = _enabled_items(weights["components"]["demand"], row)
    capacity_items = _enabled_items(weights["components"]["capacity"], row)

    demand_value = sum(i["contribution"] for i in demand_items)
    capacity_value = sum(i["contribution"] for i in capacity_items)
    traffic_score = (demand_value / capacity_value) if capacity_value else None

    lanes_total = row.get("lanes_total")
    lanes_total = None if lanes_total is None or pd.isna(lanes_total) else int(lanes_total)

    return {
        "segment_id": segment_id,
        "ts_hour": ts_hour,
        "ts_date": ts_date,
        "traffic_score": traffic_score,
        "lanes_total": lanes_total,
        "components": {
            "demand": {"value": demand_value, "items": demand_items},
            "capacity": {"value": capacity_value, "items": capacity_items},
        },
    }


def get_traffic_score_hourly(segment_id: str, ts_date: str | None = None) -> list[dict]:
    """segment_id 하나의 (ts_date 기준) 0~23시 전체 프로파일 — get_traffic_score()를
    24번 재사용한다.

    대시보드의 "하루 전체 막대 그래프"용. _load_base_data()/_hourly_lookup()이
    이미 캐싱돼 있어서 24번 호출해도 실제로는 가벼운 딕셔너리 조회 24번일 뿐이다
    (매번 다시 계산하지 않음).

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    return [get_traffic_score(segment_id, ts_hour=h, ts_date=ts_date) for h in range(24)]


def _load_adjacency() -> dict:
    """segment_id -> 인접 segment_id 목록. 요청마다 다시 읽지 않도록 캐싱한다."""
    if "adjacency" not in _cache:
        _cache["adjacency"] = closure_penalty.load_adjacency()
    return _cache["adjacency"]


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
