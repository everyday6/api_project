"""
Event Silver -> LION segment mapping

Event의
- on_street
- from_street
- to_street

정보를 이용해 실제 도로 구간의 LION segment를 찾는다.
"""

import os
import re
import time
from collections import Counter
from datetime import date
from pathlib import Path

import networkx as nx
import pandas as pd

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import clean_street, save_parquet


logger = get_logger(__name__)

SOURCE = "event"

MANHATTAN_BOROUGH_CODE = "1"


# =========================================================
# 도로명 표기 정규화
# =========================================================
#
# Event와 LION의 표기가 달라 정확 일치로는 매칭되지 않는다.
# 실제 미매칭 사례에서 확인한 차이:
#   E 42ND STREET  / W. 34TH STREET  -> EAST 42 STREET / WEST 34 STREET
#   SECOND AVENUE  / FIRST AVENUE    -> 2 AVENUE / 1 AVENUE
#   GANSEVOORT ST  / FT WASHINGTON   -> ... STREET / FORT ...
#
# TODO: construction 매핑에서도 필요해지면 common.utils로 옮긴다.

ORDINAL_WORDS = {
    "FIRST": "1",
    "SECOND": "2",
    "THIRD": "3",
    "FOURTH": "4",
    "FIFTH": "5",
    "SIXTH": "6",
    "SEVENTH": "7",
    "EIGHTH": "8",
    "NINTH": "9",
    "TENTH": "10",
    "ELEVENTH": "11",
    "TWELFTH": "12",
}

ABBREVIATIONS = {
    r"\bST\b": "STREET",
    r"\bAVE\b": "AVENUE",
    r"\bBLVD\b": "BOULEVARD",
    r"\bPL\b": "PLACE",
    r"\bPKWY\b": "PARKWAY",
    r"\bDR\b": "DRIVE",
    r"\bFT\b": "FORT",
    r"\bSQ\b": "SQUARE",
}


def normalize_street(value):
    """
    도로명을 LION 표기 기준으로 정규화한다.

    clean_street(공백/대소문자) 이후의 추가 정규화를 담당한다.
    """

    name = clean_street(value)

    if not name:
        return None

    # W. 34 -> WEST 34 / E 42 -> EAST 42
    name = re.sub(r"^W\.?\s+(?=\d)", "WEST ", name)
    name = re.sub(r"^E\.?\s+(?=\d)", "EAST ", name)

    # 42ND -> 42 (원본에 32RD 같은 오타가 있어 RD도 포함)
    name = re.sub(r"(\d+)(ST|ND|RD|TH)\b", r"\1", name)

    # SECOND AVENUE -> 2 AVENUE
    for word, digit in ORDINAL_WORDS.items():
        name = re.sub(rf"\b{word}\b", digit, name)

    # 약어 확장
    for pattern, full in ABBREVIATIONS.items():
        name = re.sub(pattern, full, name)

    return re.sub(r"\s+", " ", name).strip() or None


# =========================================================
# 도로명 별칭
# =========================================================
#
# LION 실제 표기를 확인해서 넣는다 (JR 포함, DOUGLASS는 S 두 개).
# Event 쪽 변형/오타 표기도 같은 그룹에 넣어 양방향으로 확장한다.
#
# 주의: 할렘 대로 별칭은 원칙적으로 특정 구간에만 유효하지만,
# 실제 미매칭 사례가 전부 할렘(W 118/123/124/133/142) 이었기 때문에
# 전역 별칭으로 사용한다. 미드타운 구간에서 오매칭이 관찰되면 재검토.

ALIAS_GROUPS = [
    [
        "6 AVENUE",
        "AVENUE OF THE AMERICAS",
    ],
    [
        "7 AVENUE",
        "ADAM CLAYTON POWELL JR BOULEVARD",
        "ADAM CLAYTON POWELL BOULEVARD",
    ],
    [
        "8 AVENUE",
        "FREDERICK DOUGLASS BOULEVARD",
        "FREDRICK DOUGLAS BOULEVARD",
    ],
    [
        "LENOX AVENUE",
        "MALCOLM X BOULEVARD",
    ],
]

STREET_ALIASES = {
    normalize_street(name): [
        normalize_street(n) for n in group
    ]
    for group in ALIAS_GROUPS
    for name in group
}


def resolve_street(street):
    """도로명의 LION 별칭 후보를 반환한다."""

    if not street:
        return []

    return STREET_ALIASES.get(street, [street])


# =========================================================
# 경로
# =========================================================

def event_path(run_date: str) -> Path:
    return SILVER_DIR / SOURCE / f"dt={run_date}" / "data.parquet"


def lion_path() -> Path:
    return SILVER_DIR / "dim_segment.parquet"


def output_dir(run_date: str) -> Path:
    return SILVER_DIR / "mapping" / "event_lion" / f"dt={run_date}"


# =========================================================
# Load
# =========================================================

def load_event(run_date: str) -> pd.DataFrame:

    path = event_path(run_date)

    if not path.exists():
        raise FileNotFoundError(f"Event Silver 파일 없음: {path}")

    df = pd.read_parquet(path)

    logger.info("Event Silver 로드: rows=%d path=%s", len(df), path)

    return df


def load_lion() -> pd.DataFrame:

    path = lion_path()

    if not path.exists():
        raise FileNotFoundError(f"LION dim_segment 없음: {path}")

    df = pd.read_parquet(path)

    logger.info("LION 로드: rows=%d path=%s", len(df), path)

    return df


# =========================================================
# 입력 검증 / 정제
# =========================================================

STREET_COLS = ["on_street", "from_street", "to_street"]


def prepare_event(df: pd.DataFrame) -> pd.DataFrame:

    required = [
        "event_id",
        "start_ts",
        "end_ts",
        "closure_type",
        *STREET_COLS,
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(f"Event 필수 컬럼 없음: {missing}")

    work = df.copy()

    for col in STREET_COLS:
        work[col] = work[col].map(normalize_street)

    return work


def prepare_lion(df: pd.DataFrame) -> pd.DataFrame:

    required = [
        "segment_id",
        "street_name",
        "node_from",
        "node_to",
        "borough_code",
        "length_ft",
        "is_routable",
    ]

    missing = [c for c in required if c not in df.columns]

    if missing:
        raise ValueError(f"LION 필수 컬럼 없음: {missing}")

    # Event가 맨해튼만이므로 LION도 맨해튼만 사용한다.
    #
    # is_routable=False는 여기서 제거하지 않는다.
    # 타임스퀘어 브로드웨이처럼 실제 Event 위치가
    # non_routable로 분류된 사례가 있어 결과 컬럼으로만 남긴다.
    work = df[df["borough_code"] == MANHATTAN_BOROUGH_CODE].copy()

    work["street_name"] = work["street_name"].map(normalize_street)

    work["length_ft"] = pd.to_numeric(
        work["length_ft"], errors="coerce"
    )

    logger.info(
        "맨해튼 LION: rows=%d routable=%d non_routable=%d",
        len(work),
        int(work["is_routable"].sum()),
        int((~work["is_routable"]).sum()),
    )

    return work


# =========================================================
# 교차점 찾기
# =========================================================

def build_street_nodes(lion: pd.DataFrame) -> dict:
    """
    도로명별 node 집합을 미리 만들어둔다.

    위치마다 24만 행을 isin으로 스캔하는 대신
    한 번만 groupby 하고 이후에는 dict 조회로 끝낸다.
    """

    nodes = {}

    for name, group in lion.groupby("street_name"):
        nodes[name] = (
            set(group["node_from"].dropna())
            | set(group["node_to"].dropna())
        )

    return nodes


def nodes_of(street_nodes: dict, street: str) -> set:
    """별칭 후보를 모두 합친 node 집합."""

    result = set()

    for candidate in resolve_street(street):
        result |= street_nodes.get(candidate, set())

    return result


# =========================================================
# on_street 그래프
# =========================================================

def build_street_graph(segments: pd.DataFrame) -> nx.MultiGraph:

    graph = nx.MultiGraph()

    for row in segments.itertuples():

        if not row.node_from or not row.node_to:
            continue

        length = (
            row.length_ft if pd.notna(row.length_ft) else 1.0
        )

        graph.add_edge(
            row.node_from,
            row.node_to,
            segment_id=row.segment_id,
            weight=float(length),
            is_routable=bool(row.is_routable),
        )

    return graph


# =========================================================
# 가장 짧은 구간 찾기
# =========================================================

def find_segment_path(
    lion: pd.DataFrame,
    street_nodes: dict,
    graph_cache: dict,
    on_street: str,
    from_street: str,
    to_street: str,
):
    """
    on_street에서 from_street ~ to_street 사이의 LION segment를 찾는다.

    반환:
        성공 -> ([{segment_id, is_routable}, ...], None)
        실패 -> (None, 실패원인)
    """

    if not on_street or not from_street or not to_street:
        return None, "missing_from_to"

    on_candidates = resolve_street(on_street)

    on_nodes = nodes_of(street_nodes, on_street)

    if not on_nodes:
        return None, "street_not_found"

    from_nodes = on_nodes & nodes_of(street_nodes, from_street)

    if not from_nodes:
        return None, "from_intersection_not_found"

    to_nodes = on_nodes & nodes_of(street_nodes, to_street)

    if not to_nodes:
        return None, "to_intersection_not_found"

    # 같은 도로가 여러 구간에 반복 등장하므로 그래프를 캐싱한다.
    cache_key = tuple(on_candidates)

    if cache_key not in graph_cache:
        graph_cache[cache_key] = build_street_graph(
            lion[lion["street_name"].isin(on_candidates)]
        )

    graph = graph_cache[cache_key]

    best_path = None
    best_length = None

    for start in from_nodes:

        for end in to_nodes:

            if start not in graph or end not in graph:
                continue

            try:
                path = nx.shortest_path(
                    graph, start, end, weight="weight"
                )
                length = nx.shortest_path_length(
                    graph, start, end, weight="weight"
                )

            except (nx.NetworkXNoPath, nx.NodeNotFound):
                continue

            if best_length is None or length < best_length:
                best_length = length
                best_path = path

    if not best_path:
        return None, "no_path"

    segments = []

    for node_a, node_b in zip(best_path[:-1], best_path[1:]):

        edge_data = graph.get_edge_data(node_a, node_b)

        if not edge_data:
            continue

        # 같은 node 사이에 여러 segment가 있으면 가장 짧은 것 선택
        best_edge = min(
            edge_data.values(), key=lambda x: x["weight"]
        )

        segments.append({
            "segment_id": best_edge["segment_id"],
            "is_routable": best_edge["is_routable"],
        })

    if not segments:
        return None, "no_segment"

    return segments, None


# =========================================================
# Event -> LION mapping
# =========================================================

def map_event_to_lion(
    event_df: pd.DataFrame,
    lion_df: pd.DataFrame,
) -> pd.DataFrame:

    street_nodes = build_street_nodes(lion_df)
    graph_cache = {}

    failure_reasons = Counter()

    # 같은 도로 구간이 여러 날짜에 반복되므로 위치 조합은 한 번만 계산한다.
    locations = event_df[STREET_COLS].drop_duplicates()

    logger.info("고유 Event 도로 구간: %d", len(locations))

    location_mapping = {}

    for row in locations.itertuples(index=False):

        key = (row.on_street, row.from_street, row.to_street)

        # 위치 단위로 실패를 격리한다.
        # 특정 구간의 오류로 전체 배치를 죽이지 않는다.
        try:
            segments, reason = find_segment_path(
                lion_df,
                street_nodes,
                graph_cache,
                row.on_street,
                row.from_street,
                row.to_street,
            )

        except Exception as exc:
            logger.warning(
                "위치 매핑 예외: on=%s from=%s to=%s error=%s",
                row.on_street, row.from_street, row.to_street, exc,
            )
            segments, reason = None, "unexpected_error"

        location_mapping[key] = (segments, reason)

        if reason:
            failure_reasons[reason] += 1

    results = []

    for row in event_df.itertuples(index=False):

        key = (row.on_street, row.from_street, row.to_street)

        segments, reason = location_mapping.get(
            key, (None, "mapping_not_found")
        )

        base = {
            "event_id": row.event_id,
            "start_ts": row.start_ts,
            "end_ts": row.end_ts,
            "closure_type": row.closure_type,
            "on_street": row.on_street,
            "from_street": row.from_street,
            "to_street": row.to_street,
        }

        if not segments:
            results.append({
                **base,
                "segment_id": None,
                "is_routable": None,
                "mapping_status": "unmatched",
                "unmatched_reason": reason,
            })
            continue

        for segment in segments:
            results.append({
                **base,
                "segment_id": segment["segment_id"],
                "is_routable": segment["is_routable"],
                "mapping_status": "matched",
                "unmatched_reason": None,
            })

    if failure_reasons:
        logger.warning(
            "미매칭 원인(위치 단위): %s", dict(failure_reasons)
        )

    return pd.DataFrame(results)


# =========================================================
# Validation
# =========================================================

def validate_mapping(df: pd.DataFrame):

    if df.empty:
        raise ValueError("Event-LION 매핑 결과가 비었습니다.")

    event_key = ["event_id", "start_ts"]

    total_events = df[event_key].drop_duplicates().shape[0]

    matched_events = (
        df.loc[df["mapping_status"] == "matched", event_key]
        .drop_duplicates()
        .shape[0]
    )

    unmatched_events = total_events - matched_events

    match_rate = (
        matched_events / total_events * 100 if total_events else 0
    )

    logger.info(
        "Event-LION 검증: events=%d matched=%d unmatched=%d "
        "match_rate=%.1f%% mapping_rows=%d",
        total_events,
        matched_events,
        unmatched_events,
        match_rate,
        len(df),
    )

    unmatched = df[df["mapping_status"] == "unmatched"]

    if not unmatched.empty:
        logger.warning(
            "unmatched 원인 분포:\n%s",
            unmatched["unmatched_reason"]
            .value_counts(dropna=False)
            .to_string(),
        )

    # 같은 event 발생에 같은 segment가 두 번 붙으면 Gold에서 중복 집계된다.
    dup = df[df["segment_id"].notna()].duplicated(
        subset=["event_id", "start_ts", "segment_id"]
    )

    if dup.any():
        raise ValueError(
            "(event_id, start_ts, segment_id) 중복 "
            f"{int(dup.sum())}건 발생"
        )


# =========================================================
# Pipeline
# =========================================================

def build_event_lion_mapping(run_date: str) -> str:
    """load -> map -> save만 한다(validate 없음)."""

    started = time.perf_counter()

    logger.info("Event-LION 매핑 시작: run_date=%s", run_date)

    event_df = prepare_event(load_event(run_date))
    lion_df = prepare_lion(load_lion())

    result = map_event_to_lion(event_df, lion_df)

    path = save_parquet(result, output_dir(run_date))

    logger.info(
        "Event-LION 매핑 빌드 완료: rows=%d elapsed=%.2fs path=%s",
        len(result),
        time.perf_counter() - started,
        path,
    )

    return str(path)


def validate_output(path: str) -> str:
    """build_event_lion_mapping()이 저장한 결과를 다시 읽어 validate_mapping()을 돌린다."""
    df = pd.read_parquet(path)
    validate_mapping(df)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    path = build_event_lion_mapping(run_date)
    validate_output(path)
    return path


if __name__ == "__main__":
    main()