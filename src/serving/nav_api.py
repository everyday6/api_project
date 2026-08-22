"""
서빙 API — 세그먼트 지표 조회

라우팅은 얇게 두고, 실제 조회/fallback 로직은 src/serving/nav_lookup.py에
위임한다.

로컬 실행: uvicorn src.serving.nav_api:app --reload --port 8001
"""

from __future__ import annotations

from typing import Literal

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.common.logger import get_logger
from src.serving.nav_lookup import resolve_segment_values

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_api")

app = FastAPI(title="Segment Metrics API")


class SegmentValuesRequest(BaseModel):
    segment_ids: list[str] = Field(
        min_length=1,
        description=(
            "경로를 순서대로 나열한 세그먼트 ID 목록. type=1(소요시간)일 때는 "
            "이 순서가 의미를 가진다 - 첫 세그먼트는 요청 시각 그대로, 이후 "
            "세그먼트는 앞 세그먼트들의 누적 소요시간만큼 시각이 이동된 "
            "상태로 조회된다(nav_lookup._resolve_time_values 참고). "
            "type=2(길이)는 시간과 무관해 순서/중복에 영향받지 않는다."
        ),
    )
    type: Literal[1, 2]
    time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")


class SegmentValuesResponse(BaseModel):
    values: list[int]


@app.exception_handler(Exception)
async def log_unexpected_exception(request: Request, exc: Exception):
    """개별 엔드포인트가 처리 못한 예외만 여기로 옴 — 500으로 감춰지기 전에 로그.

    fallback 체인 자체는 예외를 던지지 않으므로, 이 핸들러가 동작한다는 건
    설계된 장애 대응 범위를 벗어난 진짜 버그라는 뜻이다.
    """
    logger.error("처리되지 않은 예외: %s %s", request.method, request.url.path, exc_info=exc)
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})


@app.post("/segments/values", response_model=SegmentValuesResponse)
def get_segment_values(request: SegmentValuesRequest) -> SegmentValuesResponse:
    values = resolve_segment_values(request.segment_ids, request.type, request.time)
    return SegmentValuesResponse(values=values)
