"""
Traffic Score 조회 API.

프론트(대시보드)는 이 API만 호출하고 parquet을 직접 읽지 않는다. 실제 조회
로직은 전부 src/scoring/traffic_score.py의 get_traffic_score()/get_map_data()에
있고, 여기서는 그걸 HTTP로 감싸기만 한다.

로컬 실행:
    uvicorn src.scoring.api:app --reload --port 8000
그리고 http://localhost:8000 접속하면 대시보드가 뜬다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from shapely import wkt

from src.scoring.traffic_score import get_map_data, get_traffic_score

app = FastAPI(title="Traffic Score API")

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "dashboard"


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_DIR / "index.html")


@app.get("/api/traffic_score/{segment_id}")
def api_get_traffic_score(segment_id: str, ts_hour: Optional[int] = None):
    """
    단건 조회. ts_hour는 지금은 받기만 하고 계산엔 안 쓴다 (get_traffic_score 참고).
    """
    try:
        return get_traffic_score(segment_id, ts_hour=ts_hour)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


def _geometry_to_flat_coords(wkt_str: str) -> list[float]:
    """WKT LineString/MultiLineString -> [x1,y1,x2,y2,...] (가장 긴 라인만, 소수점 반올림)."""
    geom = wkt.loads(wkt_str)
    lines = geom.geoms if geom.geom_type == "MultiLineString" else [geom]
    longest = max(lines, key=lambda l: l.length)
    return [round(c) for xy in longest.coords for c in xy]


_map_records_cache: list[dict] | None = None


@app.get("/api/segments/map")
def api_get_map_data():
    """
    지도 초기 렌더링용 벌크 데이터. segment_id별 traffic_score 하나 값만 필요해서
    단건 조회 API를 15만 번 부르는 대신 get_map_data()로 한 번에 벡터화 계산한다
    (컴포넌트 결합 로직 자체는 get_traffic_score와 동일한 가중치 설정을 공유함).

    geometry는 WKT 문자열 대신 좌표 배열로 압축해서 내려준다 (페이로드 크기 절감,
    프론트에서 WKT 파싱 라이브러리 안 붙여도 되게).

    WKT 파싱(15.7만 건)이 요청당 6초 넘게 걸려서 프로세스 안에 캐싱한다 — 실측
    2번째 요청부터 수 ms로 떨어짐. weights.yaml을 바꾸면 서버를 재시작해야
    반영되는데, 이건 다른 캐시(_load_base_data 등)와 동일한 트레이드오프다.
    """
    global _map_records_cache
    if _map_records_cache is not None:
        return _map_records_cache

    df = get_map_data()
    records = []
    for row in df.itertuples(index=False):
        score = None if pd.isna(row.traffic_score) else float(row.traffic_score)
        records.append({
            "segment_id": row.segment_id,
            "road_class": row.road_class,
            "traffic_score": score,
            "coords": _geometry_to_flat_coords(row.geometry),
        })
    _map_records_cache = records
    return records
