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

from src.scoring.traffic_score import (
    get_map_data,
    get_nearby_closures,
    get_traffic_score,
    get_traffic_score_hourly,
)

app = FastAPI(title="Traffic Score API")

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "dashboard"


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_DIR / "index.html")


@app.get("/api/traffic_score/{segment_id}")
def api_get_traffic_score(segment_id: str, ts_hour: Optional[int] = None):
    """
    단건 조회. ts_hour 생략 시 get_traffic_score()가 현재 시각(America/New_York)으로
    자동 대체한다.
    """
    try:
        return get_traffic_score(segment_id, ts_hour=ts_hour)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


@app.get("/api/traffic_score/{segment_id}/hourly")
def api_get_traffic_score_hourly(segment_id: str):
    """
    segment_id 하나의 0~23시 전체 프로파일 — "하루 전체를 한눈에" 보여주는
    대시보드 막대 그래프용. get_traffic_score()를 24번 재사용할 뿐이라 별도
    계산 로직은 없다.
    """
    try:
        return get_traffic_score_hourly(segment_id)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


@app.get("/api/traffic_score/{segment_id}/nearby_closures")
def api_get_nearby_closures(segment_id: str, ts_hour: Optional[int] = None):
    """
    segment_id 기준 인접 구간(최대 3홉)에서 현재 활성인 공사/통제 목록 —
    대시보드 "현재 영향받는 공사" 상세 패널용.
    """
    try:
        return get_nearby_closures(segment_id, ts_hour=ts_hour)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


def _geometry_to_flat_coords(wkt_str: str) -> list[float]:
    """WKT LineString/MultiLineString -> [x1,y1,x2,y2,...] (가장 긴 라인만, 소수점 반올림)."""
    geom = wkt.loads(wkt_str)
    lines = geom.geoms if geom.geom_type == "MultiLineString" else [geom]
    longest = max(lines, key=lambda l: l.length)
    return [round(c) for xy in longest.coords for c in xy]


_geometry_cache: dict[str, list[float]] | None = None


def _get_geometry_cache() -> dict[str, list[float]]:
    """segment_id -> 좌표 배열. geometry는 시간에 안 따라 바뀌니 한 번만 파싱해서 캐싱한다.

    WKT 파싱이 비용의 대부분(15.7만 건 기준 요청당 6초 넘게 걸림, 지금은 맨해튼만이라
    더 적음)이라 이것만 캐싱하고, traffic_score는 시간대별로 달라지므로 매 요청 새로
    계산한다(get_map_data() 자체는 이미 캐싱된 base_df/closure 테이블에 대한 가벼운
    산술이라 빠르다).
    """
    global _geometry_cache
    if _geometry_cache is None:
        df = get_map_data()
        _geometry_cache = {
            row.segment_id: _geometry_to_flat_coords(row.geometry)
            for row in df.itertuples(index=False)
        }
    return _geometry_cache


@app.get("/api/segments/map")
def api_get_map_data(ts_hour: Optional[int] = None):
    """
    지도 렌더링용 벌크 데이터. segment_id별 traffic_score 하나 값만 필요해서
    단건 조회 API를 반복 호출하는 대신 get_map_data()로 한 번에 벡터화 계산한다
    (컴포넌트 결합 로직 자체는 get_traffic_score와 동일한 가중치 설정을 공유함).

    ts_hour 생략 시 현재 시각(America/New_York) 기준 — closure_penalty가
    segment_id x hour 단위라 시간마다 결과가 달라질 수 있어 geometry(좌표)만
    캐싱하고 점수는 매 요청 새로 계산한다.

    geometry는 WKT 문자열 대신 좌표 배열로 압축해서 내려준다 (페이로드 크기 절감,
    프론트에서 WKT 파싱 라이브러리 안 붙여도 되게).
    """
    df = get_map_data(ts_hour=ts_hour)
    geometry_cache = _get_geometry_cache()

    records = []
    for row in df.itertuples(index=False):
        score = None if pd.isna(row.traffic_score) else float(row.traffic_score)
        records.append({
            "segment_id": row.segment_id,
            "road_class": row.road_class,
            "traffic_score": score,
            "coords": geometry_cache.get(row.segment_id, []),
        })
    return records
