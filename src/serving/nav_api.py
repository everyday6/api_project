"""
서빙 API — 세그먼트 지표 조회

라우팅은 얇게 두고, 실제 조회/fallback 로직은 src/serving/nav_lookup.py에
위임한다.

로컬 실행: uvicorn src.serving.nav_api:app --reload --port 8001
"""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from src.common.logger import get_logger
from src.serving.api import get_type3_values
from src.serving.nav_lookup import resolve_segment_values
from src.toll.serving import get_toll_values

logger = get_logger(__name__, log_to_file=True, log_file_stem="nav_api")

app = FastAPI(title="Segment Metrics API")

# API Gateway가 이 Lambda 앞에 catch-all 라우트(ANY /{proxy+} 등)로 붙어있으면,
# API Gateway 콘솔의 CORS 설정만으로는 preflight(OPTIONS) 요청이 API Gateway
# 선에서 자동 처리되지 않고 그대로 Lambda까지 넘어온다 - FastAPI가 OPTIONS를
# 모르면 200이 아닌 응답을 내서 브라우저가 "preflight가 OK 상태가 아니다"로
# 막아버린다(S3 정적 페이지에서 실제로 겪음). 그래서 애플리케이션 레벨에서도
# CORS를 직접 처리한다 - API Gateway 설정 여부와 무관하게 항상 동작한다.
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


class SegmentValuesRequest(BaseModel):
    segment_ids: list[str] = Field(
        min_length=1,
        # type=1은 세그먼트마다 순차로 RDS를 조회한다(누적시각 때문에
        # 배치 불가) - 상한이 없으면 요청 하나가 임의로 많은 순차 호출을
        # 유발할 수 있다. 500은 NYC 전역을 가로지르는 경로도 넉넉히 담을
        # 정성적 초안이다(TODO, 팀 검토 필요).
        max_length=500,
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


class NavigationValuesRequest(BaseModel):
    segment_ids: list[str] = Field(min_length=1, max_length=500)
    type: Literal[1, 2, 3, 4]
    date: str = Field(pattern=r"^\d{4}-\d{2}-\d{2}$")
    time: str = Field(pattern=r"^([01]\d|2[0-3]):[0-5]\d$")


class NavigationValuesResponse(BaseModel):
    value: list[float]


def _resolve_navigation_values(
    segment_ids: list[str], type_: int, date: str, time: str
) -> list[float]:
    if type_ == 3:
        requested_at = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
        return get_type3_values(segment_ids, requested_at)
    if type_ == 4:
        return get_toll_values(segment_ids)
    return [float(v) for v in resolve_segment_values(segment_ids, type_, time)]


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


@app.post("/api/navigation/values", response_model=NavigationValuesResponse)
def get_navigation_values(request: NavigationValuesRequest) -> NavigationValuesResponse:
    values = _resolve_navigation_values(
        request.segment_ids, request.type, request.date, request.time
    )
    return NavigationValuesResponse(value=values)
