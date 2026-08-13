"""
Traffic Score 조회 계층 — segment_id(+ 나중엔 ts_hour) 기준 단일 조회 인터페이스.

설계 원칙 (전부 확장성 때문에 이렇게 짬):
1. 조회 로직은 이 파일의 get_traffic_score() 하나로 모은다. 프론트/API는 이
   함수(또는 이 함수를 감싼 API 엔드포인트)만 호출하고 parquet을 직접 안 읽는다.
   나중에 segment_id x hour 시계열 테이블이 생기면 이 함수 내부 구현만 바꾸면
   되고, 함수 시그니처/리턴 스키마는 그대로 유지한다.
2. demand/capacity를 구성하는 컴포넌트(중심성, TLC 수요, 행사, 공사 등)는
   config/traffic_score_weights.yaml에서 가중치·on/off를 관리한다. 코드에
   가중치를 하드코딩하지 않는다 — 새 컴포넌트가 생기면 yaml에 줄만 추가하고
   COMPONENT_SOURCES에 데이터 매핑만 붙이면 된다.
3. 리턴 스키마에 ts_hour를 지금부터 넣어둔다. 지금은 시간축이 없어서 입력값과
   무관하게 항상 None을 돌려준다 — 나중에 시계열이 생기면 이 자리에 실제
   시간값이 들어가도 호출하는 쪽(API/프론트) 코드는 안 바뀌게 하기 위함이다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.common.config import CONFIG_DIR, SILVER_DIR
from src.common.logger import get_logger
from src.lion.silver import DIM_SEGMENT_PATH
from src.lion.traffic_score import DIM_SEGMENT_TRAFFIC_SCORE_PATH
from src.scoring.closure_penalty import OUT_SOURCE as CLOSURE_PENALTY_SOURCE

logger = get_logger(__name__, log_to_file=True, log_file_stem="scoring_traffic_score")

WEIGHTS_CONFIG_PATH = CONFIG_DIR / "traffic_score_weights.yaml"

# 컴포넌트 이름 -> 실제 데이터가 있는 컬럼명 매핑.
# 아직 구현 안 된 컴포넌트(tlc_volume, event_boost, closure_penalty)는 값이
# None이다 — 누군가 yaml에서 enabled: true로 켰는데 여기 매핑이 없으면
# _validate_weights()가 바로 에러를 낸다(조용히 무시하고 넘어가지 않음).
# 다른 조원이 새 컴포넌트를 구현하면 여기에 한 줄만 추가하면 된다.
COMPONENT_SOURCES: dict[str, str | None] = {
    "centrality": "demand_raw",
    "tlc_volume": None,
    "event_boost": None,
    "base_capacity": "capacity_per_hour",
    "closure_penalty": "closure_capacity_reduction",
}

_cache: dict[str, Any] = {}


def _latest_closure_penalty_path() -> Path | None:
    """dim_segment_closure_penalty의 dt= 파티션 중 가장 최근 것을 찾는다.

    ingest_daily가 매일 새로 만드는 테이블이라(construction 허가는 dt=today
    기준이라 매번 계산됨) dim_segment_traffic_score_v0(분기 1회)처럼 고정
    경로가 아니라 그날그날 파티션을 스스로 찾아야 한다.
    """
    partitions = sorted((SILVER_DIR / CLOSURE_PENALTY_SOURCE).glob("dt=*/data.parquet"))
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


def _load_base_data() -> pd.DataFrame:
    """dim_segment(geometry 등) + dim_segment_traffic_score_v0(컴포넌트 값)를 합친 조회용 테이블.

    API 요청마다 parquet을 다시 읽지 않도록 프로세스 안에서 한 번만 로드해 캐싱한다.
    """
    if "base_df" in _cache:
        return _cache["base_df"]

    dim = pd.read_parquet(
        DIM_SEGMENT_PATH,
        columns=["segment_id", "geometry", "road_class", "borough_code", "is_routable"],
    )
    score = pd.read_parquet(DIM_SEGMENT_TRAFFIC_SCORE_PATH)

    df = dim.merge(score, on="segment_id", how="inner")

    # closure_penalty(용량 감소량)는 매일 갱신되는 별도 테이블 — LEFT JOIN해서
    # 공사/통제가 없는 segment는 0(감소 없음)으로 채운다. 아직 한 번도 안
    # 만들어졌으면(파티션 없음) 전부 0으로 둔다 — closure_penalty가 아직
    # enabled: false인 상태에서도 이 함수 자체는 정상 동작해야 하기 때문.
    closure_path = _latest_closure_penalty_path()
    if closure_path is not None:
        closure = pd.read_parquet(closure_path, columns=["segment_id", "closure_capacity_reduction"])
        df = df.merge(closure, on="segment_id", how="left")
    else:
        df["closure_capacity_reduction"] = None
    df["closure_capacity_reduction"] = df["closure_capacity_reduction"].fillna(0.0)

    df = df.set_index("segment_id", drop=False)
    _cache["base_df"] = df
    logger.info(f"[scoring] 조회용 데이터 로드 완료: {len(df)}행")
    return df


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
    segment_id 하나의 traffic_score와 구성 요소별 세부값을 돌려주는 단일 조회 인터페이스.

    ts_hour: 지금은 받기만 하고 계산에 안 쓴다 (항상 같은 정적 값 리턴). 나중에
    segment_id x hour 테이블이 생기면 여기서 그 테이블을 조회하도록 내부 구현만
    바꾸면 되고, 이 함수를 호출하는 쪽(API, 대시보드)은 그대로 두면 된다.

    Raises:
        KeyError: segment_id가 dim_segment(+score)에 없을 때.
    """
    weights = load_weights()
    df = _load_base_data()

    if segment_id not in df.index:
        raise KeyError(f"segment_id를 찾을 수 없습니다: {segment_id}")
    row = df.loc[segment_id]

    demand_items = _enabled_items(weights["components"]["demand"], row)
    capacity_items = _enabled_items(weights["components"]["capacity"], row)

    demand_value = sum(i["contribution"] for i in demand_items)
    capacity_value = sum(i["contribution"] for i in capacity_items)
    traffic_score = (demand_value / capacity_value) if capacity_value else None

    return {
        "segment_id": segment_id,
        # 시간축이 아직 없어서 입력값과 무관하게 항상 None — 시계열이 생기면
        # 이 필드에 실제 시간(예: "2026-08-12T14:00")이 들어가도록 바뀔 자리.
        "ts_hour": None,
        "traffic_score": traffic_score,
        "components": {
            "demand": {"value": demand_value, "items": demand_items},
            "capacity": {"value": capacity_value, "items": capacity_items},
        },
    }


# LION borough_code(문자열) — 1=Manhattan, 2=Bronx, 3=Brooklyn, 4=Queens,
# 5=Staten Island (NYC 공식 자치구 코드, WEST 81 STREET/ARDEN STREET 등 알려진
# 맨해튼 도로로 실측 대조해서 확인함). 지금 프로젝트 범위가 맨해튼뿐이라
# 대시보드 지도에만 적용 — get_traffic_score()는 segment_id 단건 조회라 다른
# 자치구 segment_id가 들어와도 그대로 조회는 되게 두고, "지도에 뭘 그릴지"만
# 여기서 좁힌다. Bronze/Silver 자체는 필터링하지 않는다(요청에 따름).
DASHBOARD_BOROUGH_CODE = "1"


def get_map_data() -> pd.DataFrame:
    """지도 렌더링용 벌크 데이터 — segment_id, geometry, road_class, traffic_score.

    맨해튼(DASHBOARD_BOROUGH_CODE)만 반환한다 — 프로젝트 범위 자체가 맨해튼이라.
    """
    weights = load_weights()
    df = _load_base_data()
    df = df[df["borough_code"] == DASHBOARD_BOROUGH_CODE]

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
