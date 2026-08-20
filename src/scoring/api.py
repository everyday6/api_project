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
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from shapely import wkt

from src.common.logger import get_logger
from src.scoring.traffic_score import (
    get_active_closures,
    get_closure_data_date_range,
    get_map_data,
    get_nearby_closures,
    get_nearby_segment_scores,
    get_newly_issued_closures,
    get_segment_geometries,
    get_traffic_score,
    get_traffic_score_hourly,
)

logger = get_logger(__name__, log_to_file=True, log_file_stem="scoring_api")

app = FastAPI(title="Traffic Score API")

DASHBOARD_DIR = Path(__file__).resolve().parents[2] / "dashboard"


@app.exception_handler(Exception)
async def log_unexpected_exception(request: Request, exc: Exception):
    """개별 엔드포인트에서 처리하지 않은 예외만 여기로 온다.

    KeyError -> HTTPException(404) 같이 각 엔드포인트가 이미 처리하는
    경우는 FastAPI가 그쪽 핸들러로 먼저 보내므로 여기까지 안 온다.
    여기 걸리는 건 전부 예상하지 못한 버그/데이터 이상이라는 뜻이라
    500으로 감춰지기 전에 반드시 기록해 둔다.
    """
    logger.error(
        "처리되지 않은 예외: %s %s",
        request.method,
        request.url.path,
        exc_info=exc,
    )
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal Server Error"},
    )


@app.get("/")
def dashboard():
    return FileResponse(DASHBOARD_DIR / "index.html")


class ExcludeSiteQuery:
    """단건/시간대별 두 엔드포인트가 똑같이 받는 exclude_* 6개 쿼리 파라미터를
    한 곳에서만 선언하기 위한 묶음. FastAPI가 이 클래스의 __init__ 시그니처를
    그대로 쿼리 파라미터로 풀어서 받아준다(Depends()). 필드를 추가/변경할
    일이 생기면 여기 한 곳만 고치면 된다 — 예전엔 두 엔드포인트 함수 시그니처에
    6개씩 그대로 복붙돼 있어서 하나만 고치고 하나를 빠뜨릴 위험이 있었다."""

    def __init__(
        self,
        exclude_on_street: Optional[str] = None,
        exclude_from_street: Optional[str] = None,
        exclude_to_street: Optional[str] = None,
        exclude_work_start_ts: Optional[str] = None,
        exclude_work_end_ts: Optional[str] = None,
        exclude_segment_id: Optional[str] = None,
    ):
        self.on_street = exclude_on_street
        self.from_street = exclude_from_street
        self.to_street = exclude_to_street
        self.work_start_ts = exclude_work_start_ts
        self.work_end_ts = exclude_work_end_ts
        self.segment_id = exclude_segment_id


def _build_exclude_site(q: ExcludeSiteQuery) -> Optional[dict]:
    """6개 필드가 전부 와야만 exclude_site를 만든다 — 일부만 오면(프론트
    버그 등) 조용히 무시하지 않고 그냥 None으로 둔다("모든 공사 없을 때"도
    아니고 "이 현장 없을 때"도 아닌 어중간한 필터가 조용히 걸리는 걸 막기
    위함). get_traffic_score()의 exclude_site 참고."""
    fields = [q.on_street, q.from_street, q.to_street, q.work_start_ts, q.work_end_ts, q.segment_id]
    if all(f is None for f in fields):
        return None
    if any(f is None for f in fields):
        raise HTTPException(
            status_code=400,
            detail="exclude_* 파라미터는 6개(on_street, from_street, to_street, "
                   "work_start_ts, work_end_ts, segment_id)를 전부 같이 보내야 합니다.",
        )
    return {
        "on_street": q.on_street,
        "from_street": q.from_street,
        "to_street": q.to_street,
        "work_start_ts": q.work_start_ts,
        "work_end_ts": q.work_end_ts,
        "segment_id": q.segment_id,
    }


@app.get("/api/traffic_score/{segment_id}")
def api_get_traffic_score(
    segment_id: str,
    ts_hour: Optional[int] = None,
    ts_date: Optional[str] = None,
    include_closure_penalty: bool = True,
    exclude: ExcludeSiteQuery = Depends(),
):
    """
    단건 조회. ts_hour/ts_date 생략 시 get_traffic_score()가 현재 시각/날짜
    (America/New_York)로 자동 대체한다.

    공사 전/후 비교 토글용으로 계산 기준이 다른 두 가지를 지원한다 — 화면에
    같이 노출되는 값이라 라벨을 서로 다르게 붙여야 한다(get_traffic_score()
    exclude_site 참고):
    - include_closure_penalty=false: "모든 공사 없을 때" (지도/검색으로 세그먼트를
      직접 볼 때 — 그 세그먼트에 영향 주는 공사/통제를 전부 제거)
    - exclude_* 6개(ExcludeSiteQuery): "이 현장 없을 때" (특정 공사 카드에서
      들어왔을 때 — 그 현장 하나의 기여분만 제거, 다른 공사 영향은 그대로 유지)
    """
    exclude_site = _build_exclude_site(exclude)
    try:
        return get_traffic_score(
            segment_id,
            ts_hour=ts_hour,
            ts_date=ts_date,
            include_closure_penalty=include_closure_penalty,
            exclude_site=exclude_site,
        )
    except KeyError:
        raise HTTPException(status_code=404, detail=f"segment_id를 찾을 수 없습니다: {segment_id}")


@app.get("/api/traffic_score/{segment_id}/hourly")
def api_get_traffic_score_hourly(
    segment_id: str,
    ts_date: Optional[str] = None,
    include_closure_penalty: bool = True,
    exclude: ExcludeSiteQuery = Depends(),
):
    """
    segment_id 하나의 (ts_date 기준) 0~23시 전체 프로파일 — "하루 전체를 한눈에"
    보여주는 대시보드 막대 그래프용. get_traffic_score()를 24번 재사용할 뿐이라
    별도 계산 로직은 없다. include_closure_penalty/exclude_* 파라미터는
    api_get_traffic_score() 참고.
    """
    exclude_site = _build_exclude_site(exclude)
    try:
        return get_traffic_score_hourly(
            segment_id,
            ts_date=ts_date,
            include_closure_penalty=include_closure_penalty,
            exclude_site=exclude_site,
        )
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


@app.get("/api/traffic_score/{segment_id}/nearby_segments")
def api_get_nearby_segment_scores(
    segment_id: str,
    ts_hour: Optional[int] = None,
    ts_date: Optional[str] = None,
    max_hops: int = 3,
):
    """
    대시보드에서 segment를 선택했을 때 "주변 도로만 지도에서 강조" 표시하는
    기능용 데이터. segment_id를 중심으로 인접 도로를 hop 트리로 묶어 각각의
    traffic_score(공사 후)/traffic_score_before(공사 영향 없다고 가정한
    가상값)와 함께 돌려준다 — 프론트는 이 트리에서 segment_id 집합만 뽑아
    지도 하이라이트에 쓰고, 호버 시 두 점수를 비교해서 보여준다
    (get_nearby_segment_scores() 참고).
    """
    try:
        return get_nearby_segment_scores(segment_id, ts_hour=ts_hour, ts_date=ts_date, max_hops=max_hops)
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
