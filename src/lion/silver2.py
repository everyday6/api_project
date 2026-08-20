"""
Silver2 변환: LION bronze -> graph_segment_adjacency

두 세그먼트가 교차로 노드(LION의 NodeIDFrom/NodeIDTo)를 공유하면 "인접"으로
본다. 무방향 관계지만 (segment_id, neighbor_segment_id)와 그 반대 방향을
둘 다 저장해서 양방향 조회가 바로 되게 한다.

lion 도메인 자기 자신의 데이터끼리 맺는 구조적 조인이라 교차도메인이 아니므로
공용 silver2/ 폴더가 아니라 이 도메인 폴더 안에 둔다.

대상은 is_routable=True인 세그먼트만(dim_segment/map_zone_segment와 범위를
맞춤) — 157,153건. is_routable은 gold2가 계산하므로, 이 모듈은 gold2가 완성한
dim_segment를 읽는다(레이어 순서상 Silver2가 Gold2 산출물에 의존하는 역전이
있지만, "인접관계 자체는 구조적 조인"이라는 성격은 그대로다). 실제로 계산해본
규모는 약 68만 행, 노드 111,565개, 세그먼트당 평균 이웃 2.8개 수준이라 pandas로
충분하고 Spark는 필요 없다.

지오메트리 연산이 전혀 필요 없다 — "같은 노드 ID를 공유하는가"라는 순수 속성
조인이라 dim_segment/map_zone_segment보다 오히려 훨씬 단순하다.

구현 방식: (node_id, segment_id) 형태의 롱 포맷 테이블을 만든 뒤, node_id
기준으로 자기 자신과 merge하면 같은 노드를 공유하는 모든 세그먼트 쌍이
한 번에 나온다 (자기 자신과의 쌍만 제외하면 됨). 세그먼트 하나가 같은 노드를
두 번 참조하는 경우(자기 자신에게 돌아오는 self-loop 세그먼트, 실제로 3건
확인됨)도 이 필터링으로 자연스럽게 처리된다.
"""

from __future__ import annotations

import subprocess
import tempfile
from pathlib import Path

import pandas as pd

from src.common import db
from src.common.config import SILVER2_DIR, TMP_DIR
from src.common.logger import get_logger
from src.lion.gold2 import DIM_SEGMENT_PATH
from src.lion.silver1 import (
    LION_BRONZE_ROOT,
    _find_gdb,
    _latest_bronze_version,
    _stage_gdb_locally,
)

logger = get_logger(__name__, log_to_file=True, log_file_stem="graph_segment_adjacency")

GRAPH_SEGMENT_ADJACENCY_PATH = SILVER2_DIR / "graph_segment_adjacency.parquet"

NODE_COLUMNS = ["SegmentID", "NodeIDFrom", "NodeIDTo"]


def _gdb_to_node_csv(gdb_path: Path, out_path: Path) -> Path:
    """LION에서 세그먼트-노드 연결 정보만(geometry 없이) CSV로 뽑는다."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "ogr2ogr",
        "-f", "CSV",
        str(out_path),
        str(gdb_path),
        "lion",
        "-select", ",".join(NODE_COLUMNS),
    ]
    logger.info(f"[graph_segment_adjacency] ogr2ogr 실행: {' '.join(cmd)}")
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"[graph_segment_adjacency] ogr2ogr 실패: {result.stderr}")
        raise RuntimeError(f"ogr2ogr 변환 실패: {result.stderr}")
    return out_path


def build_graph_segment_adjacency(
    bronze_root: Path = LION_BRONZE_ROOT,
    dim_segment_path: Path = DIM_SEGMENT_PATH,
    silver_root: Path = SILVER2_DIR,
) -> str:
    """dim_segment(routable만) 기준으로 세그먼트 인접 관계 그래프를 만든다."""

    version_dir = _latest_bronze_version(bronze_root)
    gdb_path = _find_gdb(version_dir)
    logger.info(f"[graph_segment_adjacency] 입력 bronze: {gdb_path}")

    # ogr2ogr는 s3:// 경로의 File Geodatabase를 직접 읽을 수 없으므로
    # Silver1과 동일하게 .gdb를 로컬로 받은 뒤 노드 CSV를 추출한다.
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="lion_silver2_", dir=TMP_DIR) as tmp:
        work_dir = Path(tmp)
        local_gdb_path = _stage_gdb_locally(gdb_path, work_dir)
        tmp_csv = work_dir / "lion_nodes.csv"
        _gdb_to_node_csv(local_gdb_path, tmp_csv)

        nodes = pd.read_csv(tmp_csv, dtype=str, keep_default_na=False)
    # dim_segment와 동일한 dedupe (중복 원인은 lion/silver1.py 문서 참고 — 순수 중복 행)
    nodes = nodes.drop_duplicates(subset="SegmentID", keep="first")

    dim = pd.read_parquet(str(dim_segment_path), columns=["segment_id", "is_routable"])
    routable_ids = set(dim.loc[dim["is_routable"], "segment_id"])
    nodes = nodes[nodes["SegmentID"].isin(routable_ids)]
    logger.info(f"[graph_segment_adjacency] 대상(is_routable=True) 세그먼트: {len(nodes)}건")

    # (node_id, segment_id) 롱 포맷 — 세그먼트 하나당 from/to 두 행
    long_from = nodes[["NodeIDFrom", "SegmentID"]].rename(columns={"NodeIDFrom": "node_id"})
    long_to = nodes[["NodeIDTo", "SegmentID"]].rename(columns={"NodeIDTo": "node_id"})
    long_df = pd.concat([long_from, long_to], ignore_index=True)

    # 같은 node_id를 공유하는 모든 세그먼트 쌍을 self-merge로 한 번에 생성
    pairs = long_df.merge(long_df, on="node_id", suffixes=("", "_neighbor"))
    pairs = pairs[pairs["SegmentID"] != pairs["SegmentID_neighbor"]]

    graph = pairs.rename(
        columns={"SegmentID": "segment_id", "SegmentID_neighbor": "neighbor_segment_id"}
    )[["segment_id", "neighbor_segment_id", "node_id"]].rename(columns={"node_id": "shared_node_id"})

    graph_path = silver_root / "graph_segment_adjacency.parquet"
    silver_root.mkdir(parents=True, exist_ok=True)
    graph.to_parquet(str(graph_path), index=False)

    # 서빙 API(gold2/closure_penalty.py의 load_adjacency)가 RDS에서 읽으므로
    # 서빙 테이블도 같이 갱신한다.
    db.write_table(graph, "graph_segment_adjacency")

    logger.info(f"[graph_segment_adjacency] {len(graph)}행 저장 -> {graph_path} (+ RDS)")

    return str(graph_path)


def validate_graph_segment_adjacency(path: str, dim_segment_path: Path = DIM_SEGMENT_PATH) -> str:
    """graph_segment_adjacency.parquet의 최소 불변식을 확인한다."""
    df = pd.read_parquet(str(path))

    assert (df["segment_id"] != df["neighbor_segment_id"]).all(), "자기 자신과의 인접 쌍 발견"

    # 무방향 관계이므로 (A,B)가 있으면 (B,A)도 반드시 있어야 한다(양방향 저장 보장).
    forward = set(zip(df["segment_id"], df["neighbor_segment_id"], df["shared_node_id"]))
    reverse_missing = [
        (a, b, n) for (a, b, n) in forward if (b, a, n) not in forward
    ]
    assert not reverse_missing, f"반대 방향 쌍이 없는 행 {len(reverse_missing)}개 (예: {reverse_missing[:3]})"

    # 모든 segment_id/neighbor_segment_id가 dim_segment의 routable 세그먼트 안에 있는지
    dim = pd.read_parquet(str(dim_segment_path), columns=["segment_id", "is_routable"])
    routable_ids = set(dim.loc[dim["is_routable"], "segment_id"])
    unknown = (set(df["segment_id"]) | set(df["neighbor_segment_id"])) - routable_ids
    assert not unknown, f"routable 대상 밖의 segment_id가 섞여 있음: {len(unknown)}건"

    logger.info(f"[graph_segment_adjacency] 검증 통과 ({len(df)}행)")
    return path


if __name__ == "__main__":
    out = build_graph_segment_adjacency()
    validate_graph_segment_adjacency(out)
