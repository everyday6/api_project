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
    get_active_closures,
    get_closure_data_date_range,
    get_map_data,
    get_nearby_closures,
    get_newly_issued_closures,
    get_segment_geometries,
    get_traffic_score,
    get_traffic_score_hourly,
)

app = FastAPI(title="Traffic Score API")

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "dashboard"


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_DIR / "index.html")


@app.get("/api/traffic_score/{segment_id}")
def api_get_traffic_score(segment_id: str, ts_hour: Optional[int] = None, ts_date: Optional[str] = None):
    """
    단건 조회. ts_hour/ts_date 생략 시 get_traffic_score()가 현재 시각/날짜
    (America/New_York)로 자동 대체한다.
    """
    try:
        return get_traffic_score(segment_id, ts_hour=ts_hour, ts_date=ts_date)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


@app.get("/api/traffic_score/{segment_id}/hourly")
def api_get_traffic_score_hourly(segment_id: str, ts_date: Optional[str] = None):
    """
    segment_id 하나의 (ts_date 기준) 0~23시 전체 프로파일 — "하루 전체를 한눈에"
    보여주는 대시보드 막대 그래프용. get_traffic_score()를 24번 재사용할 뿐이라
    별도 계산 로직은 없다.
    """
    try:
        return get_traffic_score_hourly(segment_id, ts_date=ts_date)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


@app.get("/api/traffic_score/{segment_id}/nearby_closures")
def api_get_nearby_closures(segment_id: str, ts_hour: Optional[int] = None, ts_date: Optional[str] = None):
    """
    segment_id 기준 인접 구간(최대 3홉)에서 (ts_date, ts_hour) 시점에 활성인
    공사/통제 목록 — 대시보드 "현재 영향받는 공사" 상세 패널용.
    """
    try:
        return get_nearby_closures(segment_id, ts_hour=ts_hour, ts_date=ts_date)
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


@app.get("/api/active_closures")
def api_get_active_closures(ts_hour: Optional[int] = None, ts_date: Optional[str] = None):
    """
    (ts_date, ts_hour) 시점에 맨해튼 전체에서 활성인 공사/통제 목록 —
    get_nearby_closures()와 달리 특정 segment에 anchor되지 않는다. 대시보드
    "이 날짜에 활성인 공사" 목록(클릭하면 그 segment로 지도 이동+점수 조회)용.
    """
    return get_active_closures(ts_hour=ts_hour, ts_date=ts_date)


@app.get("/api/newly_issued_closures")
def api_get_newly_issued_closures(ts_date: Optional[str] = None):
    """
    ts_date에 새로 발급된 공사 permit 목록 — /api/active_closures가 "그
    날짜에 진행 중인지"를 보는 것과 달리 "그 날짜에 허가가 올라왔는지"가
    기준이다. road_closures는 발급일 개념이 없어 대상에서 자연히 빠진다.
    """
    return get_newly_issued_closures(ts_date=ts_date)


@app.get("/api/closure_data_range")
def api_get_closure_data_range():
    """
    현재 매핑 스냅샷에 있는 공사/통제 permit들의 전체 날짜 범위 — 대시보드
    날짜 선택기의 min/max 힌트용. 이 범위를 벗어난 날짜는 항상 영향 없음(0)으로
    나온다.
    """
    min_date, max_date = get_closure_data_date_range()
    return {"min_date": min_date, "max_date": max_date}


def _geometry_to_flat_coords(wkt_str: str) -> list[float]:
    """WKT LineString/MultiLineString -> [x1,y1,x2,y2,...] (가장 긴 라인만, 소수점 반올림)."""
    geom = wkt.loads(wkt_str)
    lines = geom.geoms if geom.geom_type == "MultiLineString" else [geom]
    longest = max(lines, key=lambda l: l.length)
    return [round(c) for xy in longest.coords for c in xy]


_geometry_cache: dict[str, list[float]] | None = None


def _get_geometry_cache() -> dict[str, list[float]]:
    """segment_id -> 좌표 배열. geometry는 시간/날짜와 무관하니 한 번만 파싱해서 캐싱한다.

    WKT 파싱이 비용의 대부분(15.7만 건 기준 요청당 6초 넘게 걸림, 지금은 맨해튼만이라
    더 적음)이라 이것만 캐싱한다. get_segment_geometries()는 closure_penalty 계산이
    딸려오지 않는 가벼운 조회라 geometry만 뽑을 때 get_map_data() 대신 쓴다 —
    traffic_score(시간대/날짜별로 달라짐)는 매 요청 새로 계산한다.
    """
    global _geometry_cache
    if _geometry_cache is None:
        df = get_segment_geometries()
        _geometry_cache = {
            row.segment_id: _geometry_to_flat_coords(row.geometry)
            for row in df.itertuples(index=False)
        }
    return _geometry_cache


@app.get("/api/segments/map")
def api_get_map_data(ts_hour: Optional[int] = None, ts_date: Optional[str] = None):
    """
    지도 렌더링용 벌크 데이터. segment_id별 traffic_score 하나 값만 필요해서
    단건 조회 API를 반복 호출하는 대신 get_map_data()로 한 번에 벡터화 계산한다
    (컴포넌트 결합 로직 자체는 get_traffic_score와 동일한 가중치 설정을 공유함).

    ts_hour/ts_date 생략 시 현재 시각/날짜(America/New_York) 기준 — closure_penalty가
    segment_id x hour 단위고 그 활성 여부가 날짜에도 달려 있어 매 요청 새로
    계산한다(geometry(좌표)만 별도로 캐싱).

    geometry는 WKT 문자열 대신 좌표 배열로 압축해서 내려준다 (페이로드 크기 절감,
    프론트에서 WKT 파싱 라이브러리 안 붙여도 되게).
    """
    df = get_map_data(ts_hour=ts_hour, ts_date=ts_date)
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
