"""
Traffic Score 조회 계층 — segment_id x ts_hour 기준 단일 조회 인터페이스.

설계 원칙 (전부 확장성 때문에 이렇게 짬):
1. 조회 로직은 이 파일의 get_traffic_score() 하나로 모은다. 프론트/API는 이
   함수(또는 이 함수를 감싼 API 엔드포인트)만 호출하고 parquet을 직접 안 읽는다.
2. demand/capacity를 구성하는 컴포넌트(중심성, TLC 수요, 행사, 공사 등)는
   config/traffic_score_weights.yaml에서 가중치·on/off를 관리한다. 코드에
   가중치를 하드코딩하지 않는다 — 새 컴포넌트가 생기면 yaml에 줄만 추가하고
   COMPONENT_SOURCES에 데이터 매핑만 붙이면 된다.
3. ts_hour: closure_penalty가 이제 segment_id x hour(0~23) 단위로 계산되므로
   (src/scoring/closure_penalty.py) 실제로 시간대별 조회가 된다. ts_hour=None이면
   "지금 몇 시인지"(America/New_York 기준)로 자동 대체한다 — 대시보드가 아직
   시간 선택 UI 없이 늘 ts_hour=None으로만 호출해도 "현재 시각 기준" 점수가
   바로 나오게 하기 위함. 나중에 프론트에 시간 선택이 생기면 그때는 명시적으로
   ts_hour를 넘기면 된다. centrality/base_capacity는 여전히 분기 1회 갱신되는
   정적값이라 시간에 따라 안 바뀐다 — 지금 시간대별로 실제 변하는 건
   closure_penalty뿐이다(TLC 수요가 나중에 붙으면 그것도 시간대별이 될 예정).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import yaml

from src.common.config import CONFIG_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.lion.silver import DIM_SEGMENT_PATH
from src.lion.traffic_score import DIM_SEGMENT_TRAFFIC_SCORE_PATH
from src.scoring.closure_penalty import OUT_SOURCE as CLOSURE_PENALTY_SOURCE

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
    "tlc_volume": None,
    "event_boost": None,
    "base_capacity": "capacity_per_hour",
    "closure_penalty": "closure_capacity_reduction",
}

# 시간대(hour)별로 값이 달라지는 컴포넌트 목록 — value는 그 컴포넌트의 Silver/Gold
# 산출물이 저장된 SILVER_DIR 밑 디렉터리 이름이다(dt= 파티션을 스스로 찾아서
# 읽는다, closure_penalty.py의 OUT_SOURCE와 동일 패턴). 여기 없는 컴포넌트는
# _load_base_data()의 정적 테이블(분기 1회 갱신)에서 값을 가져온다고 간주한다.
#
# 새 시간대별 컴포넌트(예: TLC 통행량)를 추가할 때 할 일은 딱 두 가지뿐이다 —
# ① COMPONENT_SOURCES에 컬럼명 추가, ② 여기에 "그 컴포넌트 이름: Silver 출력
# 디렉터리 이름" 한 줄 추가. get_traffic_score()/get_map_data() 코드는 안 건드려도
# 된다 — segment_id x hour 스키마(컬럼: segment_id, hour, <값 컬럼>)로 저장하는
# 것만 맞추면 자동으로 시간대별 조회에 포함된다.
HOURLY_COMPONENT_SOURCES: dict[str, str] = {
    "closure_penalty": CLOSURE_PENALTY_SOURCE,
}

_cache: dict[str, Any] = {}


def _latest_partition_path(source_dir_name: str) -> Path | None:
    """<source_dir_name>의 dt= 파티션 중 가장 최근 것을 찾는다.

    ingest_daily가 매일 새로 만드는 시간대별 테이블들은 dim_segment_traffic_score_v0
    (분기 1회)처럼 고정 경로가 아니라 그날그날 파티션을 스스로 찾아야 한다.
    """
    partitions = sorted((SILVER_DIR / source_dir_name).glob("dt=*/data.parquet"))
    return partitions[-1] if partitions else None


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


def _load_base_data() -> pd.DataFrame:
    """dim_segment(geometry 등) + dim_segment_traffic_score_v0(컴포넌트 값)를 합친 조회용 테이블.

    시간에 안 따라 바뀌는 정적 컴포넌트만 — closure_penalty(시간대별)는 여기
    안 넣고 _load_closure_penalty_hourly()에서 별도로 관리한다.

    API 요청마다 parquet을 다시 읽지 않도록 프로세스 안에서 한 번만 로드해 캐싱한다.
    """
    if "base_df" in _cache:
        return _cache["base_df"]

    dim = pd.read_parquet(
        DIM_SEGMENT_PATH,
        columns=["segment_id", "geometry", "road_class", "borough_code", "is_routable"],
    )
    score = pd.read_parquet(DIM_SEGMENT_TRAFFIC_SCORE_PATH)

    df = dim.merge(score, on="segment_id", how="inner").set_index("segment_id", drop=False)
    _cache["base_df"] = df
    logger.info(f"[scoring] 조회용 데이터 로드 완료: {len(df)}행")
    return df


def _load_hourly_table(component_name: str) -> pd.DataFrame:
    """HOURLY_COMPONENT_SOURCES에 등록된 컴포넌트의 (segment_id, hour, value) 테이블.

    아직 한 번도 안 만들어졌으면(파티션 없음) 빈 테이블 — 이 경우 모든 조회가
    이 컴포넌트=0(영향 없음)으로 처리된다.
    """
    cache_key = f"hourly_df::{component_name}"
    if cache_key in _cache:
        return _cache[cache_key]

    value_col = COMPONENT_SOURCES[component_name]
    source_dir = HOURLY_COMPONENT_SOURCES[component_name]
    path = _latest_partition_path(source_dir)
    if path is not None:
        df = pd.read_parquet(path, columns=["segment_id", "hour", value_col])
    else:
        df = pd.DataFrame(columns=["segment_id", "hour", value_col])

    _cache[cache_key] = df
    return df


def _hourly_lookup(component_name: str) -> dict[tuple[str, int], float]:
    """(segment_id, hour) -> 값 딕셔너리. 단건 조회(get_traffic_score)용."""
    cache_key = f"hourly_lookup::{component_name}"
    if cache_key in _cache:
        return _cache[cache_key]

    value_col = COMPONENT_SOURCES[component_name]
    df = _load_hourly_table(component_name)
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


def get_traffic_score(segment_id: str, ts_hour: int | None = None) -> dict:
    """
    segment_id x ts_hour 하나의 traffic_score와 구성 요소별 세부값을 돌려주는
    단일 조회 인터페이스.

    ts_hour: None이면 현재 시각(America/New_York, 0~23)으로 자동 대체한다.
    closure_penalty가 segment_id x hour 단위라 시간대별로 실제 값이 달라진다
    (centrality/base_capacity는 여전히 분기 1회 정적값).

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    if ts_hour is None:
        ts_hour = _current_hour()

    weights = load_weights()
    df = _load_base_data()

    if segment_id not in df.index:
        raise KeyError(f"segment_id를 찾을 수 없습니다: {segment_id}")
    row = df.loc[segment_id].to_dict()

    # 시간대별 컴포넌트는 전부 여기서 일괄 채운다 — HOURLY_COMPONENT_SOURCES에
    # 등록된 것만큼 자동으로 반영되고, 값이 없으면(그 시간엔 영향 없음) 0.0.
    for name, source_dir in HOURLY_COMPONENT_SOURCES.items():
        column = COMPONENT_SOURCES[name]
        row[column] = _hourly_lookup(name).get((segment_id, ts_hour), 0.0)

    demand_items = _enabled_items(weights["components"]["demand"], row)
    capacity_items = _enabled_items(weights["components"]["capacity"], row)

    demand_value = sum(i["contribution"] for i in demand_items)
    capacity_value = sum(i["contribution"] for i in capacity_items)
    traffic_score = (demand_value / capacity_value) if capacity_value else None

    return {
        "segment_id": segment_id,
        "ts_hour": ts_hour,
        "traffic_score": traffic_score,
        "components": {
            "demand": {"value": demand_value, "items": demand_items},
            "capacity": {"value": capacity_value, "items": capacity_items},
        },
    }


def get_traffic_score_hourly(segment_id: str) -> list[dict]:
    """segment_id 하나의 0~23시 전체 프로파일 — get_traffic_score()를 24번 재사용한다.

    대시보드의 "하루 전체 막대 그래프"용. _load_base_data()/_hourly_lookup()이
    이미 캐싱돼 있어서 24번 호출해도 실제로는 가벼운 딕셔너리 조회 24번일 뿐이다
    (매번 parquet을 다시 읽지 않음).

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    return [get_traffic_score(segment_id, ts_hour=h) for h in range(24)]


# LION borough_code(문자열) — 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens,
# 5=Staten Island (NYC 공식 자치구 코드, WEST 81 STREET/ARDEN STREET 등 알려진
# 맨해튼 도로로 실측 대조해서 확인함). 지금 프로젝트 범위가 맨해튼뿐이라
# 대시보드 지도에만 적용 — get_traffic_score()는 segment_id 단건 조회라 다른
# 자치구 segment_id가 들어와도 그대로 조회는 되게 두고, "지도에 뭘 그릴지"만
# 여기서 좁힌다. Bronze/Silver 자체는 필터링하지 않는다(요청에 따름).
DASHBOARD_BOROUGH_CODE = "1"


def get_map_data(ts_hour: int | None = None) -> pd.DataFrame:
    """지도 렌더링용 벌크 데이터 — segment_id, geometry, road_class, traffic_score.

    맨해튼(DASHBOARD_BOROUGH_CODE)만 반환한다 — 프로젝트 범위 자체가 맨해튼이라.
    ts_hour: None이면 현재 시각(America/New_York)으로 자동 대체 — get_traffic_score()와 동일.
    """
    if ts_hour is None:
        ts_hour = _current_hour()

    weights = load_weights()
    df = _load_base_data()
    df = df[df["borough_code"] == DASHBOARD_BOROUGH_CODE].reset_index(drop=True)

    # 시간대별 컴포넌트는 전부 여기서 일괄 병합한다 — HOURLY_COMPONENT_SOURCES에
    # 등록된 것만큼 자동으로 반영된다. 이 ts_hour 한 시간대분만 골라서 병합하고,
    # 매칭 안 되는(그 시간엔 영향 없는) segment는 0으로 채운다.
    for name, source_dir in HOURLY_COMPONENT_SOURCES.items():
        column = COMPONENT_SOURCES[name]
        hourly = _load_hourly_table(name)
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
