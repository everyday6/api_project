"""index.html에 박아넣을 DATA(JSON) 블록을 생성한다.

맨하튼(borough_code=="1") routable 세그먼트만 쓴다 - 기존 정적 데모의
background_coords 세그먼트 수(19,981)와 정확히 일치해서, 원래도 이
필터로 만들어졌던 것으로 보인다.

background_coords: 세그먼트별 지도 렌더링용 좌표(회색 배경 도로망,
기존 포맷 그대로 유지 - JS 렌더링 코드를 안 건드리기 위함).

graph: 브라우저에서 클릭 두 점 사이 실시간 경로 탐색(Dijkstra)에 쓸
그래프 - 노드 좌표 + 엣지(구간 길이/제한속도/segment_id). 맨하튼만이면
19,981 세그먼트 규모라 브라우저에서 실시간 계산해도 충분히 빠르다
(Python networkx 기준 실측 평균 7ms, 최대 16ms).

사용법: python demo/build_route_map_data.py > /tmp/data.json 로 확인하거나,
--write로 index.html의 DATA 블록을 직접 갱신한다.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path

import pandas as pd
from shapely import wkt

DIM_SEGMENT_PATH = Path(__file__).resolve().parent.parent / "data" / "gold2" / "dim_segment.parquet"
DEMO_HTML_PATH = Path(__file__).resolve().parent / "index.html"


def _flatten_coords(geometry_wkt: str) -> list[float]:
    """MULTILINESTRING WKT를 [x1,y1,x2,y2,...] 평탄 배열로 바꾼다.

    여러 LineString이 이어져 있으면 순서대로 이어 붙인다 - 렌더링만
    할 거라 조각 사이 연속성은 상관없다(addCoordsToPath가 두 점씩
    끊어서 선분으로 그림)."""

    geom = wkt.loads(geometry_wkt)
    coords: list[float] = []
    for line in geom.geoms:
        for x, y in line.coords:
            coords.append(round(x, 3))
            coords.append(round(y, 3))
    return coords


def build_data() -> dict:
    df = pd.read_parquet(DIM_SEGMENT_PATH)
    manhattan = df[(df["borough_code"] == "1") & (df["is_routable"])].copy()

    background_coords: dict[str, list[float]] = {}
    node_coords: dict[str, list[float]] = {}
    edges: list[list] = []

    for row in manhattan.itertuples():
        coords = _flatten_coords(row.geometry)
        if len(coords) < 4:
            continue
        background_coords[row.segment_id] = coords

        # node_from/node_to 좌표는 이 세그먼트 선의 첫/끝 점이다(파일 전체
        # 훑어서 같은 노드가 여러 세그먼트에 나올 때도 항상 같은 좌표라
        # 마지막에 쓴 값으로 덮어써도 무방함을 확인함).
        node_coords[row.node_from] = coords[:2]
        node_coords[row.node_to] = coords[-2:]

        length_ft = float(row.length_ft) if row.length_ft and row.length_ft > 0 else 1.0
        speed_mph = float(row.speed_limit_mph) if row.speed_limit_mph and row.speed_limit_mph > 0 else 25.0
        edges.append([row.node_from, row.node_to, round(length_ft, 1), speed_mph, row.segment_id])

    return {
        "background_coords": background_coords,
        "graph": {
            "nodes": node_coords,
            "edges": edges,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="index.html의 DATA 블록을 직접 갱신")
    args = parser.parse_args()

    data = build_data()
    print(
        f"[build_route_map_data] background_coords={len(data['background_coords'])}개 "
        f"nodes={len(data['graph']['nodes'])}개 edges={len(data['graph']['edges'])}개"
    )

    if not args.write:
        print(json.dumps(data, ensure_ascii=False)[:500] + " ...")
        return

    html = DEMO_HTML_PATH.read_text(encoding="utf-8")
    new_line = "const DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":")) + ";"
    updated = re.sub(r"^const DATA = .*;$", lambda _: new_line, html, count=1, flags=re.MULTILINE)
    if updated == html:
        raise RuntimeError("const DATA = ...; 줄을 못 찾았습니다")
    DEMO_HTML_PATH.write_text(updated, encoding="utf-8")
    print(f"[build_route_map_data] {DEMO_HTML_PATH} 갱신 완료 ({len(new_line)} bytes)")


if __name__ == "__main__":
    main()
