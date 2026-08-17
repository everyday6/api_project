"""
Silver 매핑: road_control_events(other_road_control) x dim_segment(LION) -> map_road_closure_segment

other_road_control(road_closures 출신, construction과 안 겹치는 별개 도로 통제)는
geometry(WKT)가 99.99% 있어서, construction과 달리 도로명이 아니라 **공간
조인**(가장 가까운 세그먼트 찾기)으로 segment_id를 매핑한다 — map_zone_segment.py
가 dim_segment x Taxi Zone을 매핑할 때 쓴 것과 같은 shapely STRtree 방식.

dim_segment(LION)와 road_closures WKT 둘 다 EPSG:2263(NAD83 / New York Long
Island, US feet) 좌표계라 재투영 불필요(map_zone_segment.py에서 이미 확인한
사실 재사용).

거리 임계값: 실측 샘플(2,000건) 기준 거리 분포가 중앙값 0, 90%ile도 0.0001ft로
사실상 대부분 세그먼트 위에 정확히 겹친다. 0.1%만 878~1,627ft씩 떨어진 이상치
(데이터 오류 또는 LION 도로망 밖 위치로 추정). MATCH_DISTANCE_THRESHOLD_FT=100
으로 잡아서 이런 이상치만 걸러낸다(대부분의 정상 매칭엔 전혀 영향 없음).

other_road_control 행에는 permit_id 같은 고유 키가 없어서(road_closures 원본
자체에 없음), 이 함수가 처리하는 동안만 쓰는 임시 행 번호(row_id)를 부여한다.
"""

from __future__ import annotations

import os
from datetime import date
from pathlib import Path

import pandas as pd
from shapely import wkt
from shapely.strtree import STRtree

from src.common.config import SILVER_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.lion.silver import DIM_SEGMENT_PATH

logger = get_logger(__name__, log_to_file=True, log_file_stem="map_road_closure_segment")

OUT_SOURCE = "map_road_closure_segment"
ROAD_CONTROL_EVENTS_DIR = SILVER_DIR / "road_control_events"

MATCH_DISTANCE_THRESHOLD_FT = 100


def load_dim_segment_geoms(dim_segment_path: Path = DIM_SEGMENT_PATH) -> tuple[pd.DataFrame, list]:
    """routable 세그먼트만(map_zone_segment와 동일 범위) geometry를 shapely 객체로 파싱."""
    dim = pd.read_parquet(dim_segment_path, columns=["segment_id", "geometry", "is_routable"])
    dim = dim.loc[dim["is_routable"], ["segment_id", "geometry"]].reset_index(drop=True)
    geoms = dim["geometry"].map(wkt.loads).tolist()
    return dim, geoms


def load_other_road_control(run_date: str) -> pd.DataFrame:
    path = ROAD_CONTROL_EVENTS_DIR / f"dt={run_date}" / "data.parquet"
    df = pd.read_parquet(path)
    df = df[df["control_type"] == "other_road_control"].drop(columns=["control_type"])
    return df.reset_index(drop=True).reset_index(names="row_id")


def match(
    other_road_control: pd.DataFrame,
    dim_segment: pd.DataFrame,
    tree: STRtree,
) -> pd.DataFrame:
    events = other_road_control.copy()

    has_geom = events["geom_wkt"].notna()
    parsed = events.loc[has_geom, "geom_wkt"].map(wkt.loads)

    idx = tree.nearest(parsed.tolist())
    nearest_segment_ids = dim_segment["segment_id"].to_numpy()[idx]
    nearest_geoms = tree.geometries[idx]
    distances = [g1.distance(g2) for g1, g2 in zip(parsed.tolist(), nearest_geoms)]

    match_df = pd.DataFrame(
        {"segment_id": nearest_segment_ids, "match_distance_ft": distances},
        index=parsed.index,
    )
    match_df.loc[match_df["match_distance_ft"] > MATCH_DISTANCE_THRESHOLD_FT, "segment_id"] = None

    events = events.join(match_df)
    return events.drop(columns=["row_id"])


def validate(df: pd.DataFrame, total_rows: int) -> None:
    if df.empty:
        raise ValueError("map_road_closure_segment 결과가 비었습니다.")

    matched = df["segment_id"].notna().sum()
    logger.info(
        "map_road_closure_segment 검증 완료: 원본 other_road_control 행수=%d, 매칭된 행=%d(%.1f%%)",
        total_rows, matched, matched / total_rows * 100 if total_rows else 0.0,
    )

    far = df["match_distance_ft"].dropna()
    far = far[far > MATCH_DISTANCE_THRESHOLD_FT]
    if len(far):
        logger.warning(
            "거리 임계값(%dft) 초과로 제외된 행: %d건",
            MATCH_DISTANCE_THRESHOLD_FT, len(far),
        )


def build(run_date: str | None = None) -> str:
    """load -> match -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("map_road_closure_segment 변환 시작: run_date=%s", run_date)

    dim_segment, geoms = load_dim_segment_geoms()
    tree = STRtree(geoms)
    other_road_control = load_other_road_control(run_date)

    df = match(other_road_control, dim_segment, tree)

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "map_road_closure_segment 빌드 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


def validate_output(path: str, run_date: str) -> str:
    """build()가 저장한 결과를 다시 읽어, 그 run_date의 원본 other_road_control
    행수와 비교하며 validate()를 돌린다."""
    df = pd.read_parquet(path)
    total_rows = len(load_other_road_control(run_date))
    validate(df, total_rows=total_rows)
    return path


def main(run_date: str | None = None) -> str:
    """build + validate를 순서대로 실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())
    path = build(run_date)
    validate_output(path, run_date)
    return path


if __name__ == "__main__":
    main()
