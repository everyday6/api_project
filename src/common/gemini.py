"""
Gemini API를 이용한 construction stipulation 텍스트(work_hours, WORK EMBARGO)
파싱 폴백 — 저수준 호출 전용.

src/construction_stipulations/silver.py의 정규식(_parse_work_hours,
_parse_work_embargo)이 못 잡는 오탈자/포맷 변형 문구를 이걸로 한 번 더
시도한다. 정규식 실패 건만 호출하고(전체 재처리 안 함), 고유 문구 기준으로
결과를 캐싱해서(src/construction_stipulations/llm_pipeline.py 참고) 같은
문구를 반복 호출하지 않는다.

이 모듈은 API 호출 + 재시도/429/타임아웃 처리 + JSON 디코드까지만 한다.
파싱 결과가 실제로 의미상 맞는지(evidence 일치, 필드 유효성 등)는 검증하지
않는다 — 그건 llm_pipeline.py의 validate_*_llm_output()의 몫이다(정규식
파서와 마찬가지로 "이 모듈은 텍스트→구조화 변환만, 검증은 별도 계층"이라는
원칙을 유지하기 위해 여기서 내용 판단을 하지 않는다).

배치(Airflow 태스크) 안에서만 호출한다 — API 요청 경로에서 동기 호출하면
사용자 응답이 느려지고 요청마다 과금되므로, 온디맨드 조회(traffic_score.py)는
항상 이 모듈이 미리 계산해 둔 결과(Silver 파티션)만 읽는다.
"""

from __future__ import annotations

import json
import time

import requests

from src.common.config import GEMINI_API_KEY
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="gemini")


class GeminiUnavailable(Exception):
    """Gemini 호출 자체가 안 되는 상황(키 미설정/네트워크 오류/응답 형식이 아예
    깨짐) — "이 문구를 모델이 못 알아봤다"(진짜 파싱 실패, status=uncertain)와는
    다르다. 호출부는 이걸 "문구가 나쁘다"가 아니라 "이번엔 시도조차 못 했다"는
    신호로 써서, 캐시에 영구 실패로 남기지 않고 다음 실행에서 다시 시도하게
    한다(키를 나중에 설정하는 경우가 실제로 있음)."""


GEMINI_MODEL = "gemini-3.1-flash-lite"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
# 실측 결과 같은 모델/문구로도 응답이 1~2초 걸릴 때도, 60초 타임아웃까지 그냥
# 멈춰버릴 때도 있다(동시성/부하와 무관 — 순차 호출로도 재현됨. 실측 약 절반
# 정도가 이렇게 멈춘다). 재시도하면 대부분 몇 초 안에 성공하는 걸 보면 요청
# 자체가 서버 쪽에서 가끔 걸리는 것으로 보여, 짧게 여러 번 재시도한다.
GEMINI_TIMEOUT = 60
GEMINI_MAX_ATTEMPTS = 3
GEMINI_RETRY_DELAY = 2

# 공통 응답 규칙 — 두 카테고리 프롬프트 모두 이 문단을 붙인다. evidence가
# "원문 그대로 복사한 부분 문자열"이어야 한다는 게 핵심 — 이래야 나중에
# llm_pipeline.py가 evidence를 원문에서 재검색해서 값과 일치하는지(모델이
# 지어낸 값은 아닌지) 결정론적으로 재확인할 수 있다.
_COMMON_RULES = """
각 필드를 뽑을 때, 그 값을 어디서 읽었는지 원문에서 그대로 복사한 부분
문자열을 evidence로 같이 반환해라(재구성/의역 금지, 원문에 있는 그대로).
오탈자나 이상한 형식이 섞여 있을 수 있으니 문맥으로 최대한 추론하되, 도저히
알 수 없으면(핵심 정보 자체가 없거나 앞뒤가 안 맞아 복구 불가능하면)
status를 "uncertain"으로만 응답하고 나머지 필드는 비워라. 확신이 없는데
값을 지어내지 마라.
"""

WORK_HOURS_PROMPT_TEMPLATE = (
    """다음은 NYC 공사 허가 stipulation(조건문) 텍스트 중 작업 가능 시간대를
지정하는 "WORK ..." 문구다. 이 문구에서 매일 반복되는 시작 시각(0~23 정수),
종료 시각(0~23 정수), 요일 조건을 뽑아라. 요일 조건이 다음 중 하나로 명확히
분류되면 day_code에 그 값을, 아니면 "OTHER"를 써라: DAILY(매일/요일 명시
없음), WEEKDAY(월~금), WEEKEND(토~일), SATURDAY, SUNDAY, EXCEPT_SUNDAY.
"""
    + _COMMON_RULES
    + """
텍스트: {text}
"""
)

WORK_HOURS_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["parsed", "uncertain"]},
        "start_hour": {"type": "integer"},
        "start_hour_evidence": {"type": "string"},
        "end_hour": {"type": "integer"},
        "end_hour_evidence": {"type": "string"},
        "day_code": {
            "type": "string",
            "enum": ["DAILY", "WEEKDAY", "WEEKEND", "SATURDAY", "SUNDAY", "EXCEPT_SUNDAY", "OTHER"],
        },
        "day_evidence": {"type": "string"},
    },
    "required": ["status"],
}

EMBARGO_PROMPT_TEMPLATE = (
    """다음은 NYC 공사 허가 stipulation(조건문) 텍스트 중 "WORK EMBARGO:"로
시작하는 문구다. 이 문구에서 공사가 임시 중단되는 기간의 시작 날짜(YYYY-MM-DD),
종료 날짜(YYYY-MM-DD), 매일 반복되는 시작 시각(0~23 정수), 종료 시각(0~23
정수), 중단 사유를 뽑아라.
"""
    + _COMMON_RULES
    + """
텍스트: {text}
"""
)

EMBARGO_RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string", "enum": ["parsed", "uncertain"]},
        "start_date": {"type": "string", "description": "YYYY-MM-DD"},
        "start_date_evidence": {"type": "string"},
        "end_date": {"type": "string", "description": "YYYY-MM-DD"},
        "end_date_evidence": {"type": "string"},
        "start_hour": {"type": "integer"},
        "start_hour_evidence": {"type": "string"},
        "end_hour": {"type": "integer"},
        "end_hour_evidence": {"type": "string"},
        "reason": {"type": "string"},
    },
    "required": ["status"],
}


def call_gemini_structured(prompt: str, schema: dict) -> dict:
    """Gemini에게 구조화된 JSON 응답을 요청하는 저수준 호출.

    반환값 계약: 항상 dict(모델이 반환한 JSON을 그대로 디코드한 것 — 내용
    검증은 하지 않는다). 호출 자체가 안 되면(키 미설정/네트워크 오류/429/응답
    형식이 아예 깨짐) GeminiUnavailable을 던진다 — 이 경우는 문구 문제가
    아니라 이번 시도 자체가 실패한 것이므로, 호출부가 캐시에 영구 실패로
    남기면 안 된다.
    """
    if not GEMINI_API_KEY:
        raise GeminiUnavailable("GEMINI_API_KEY가 설정되지 않음")

    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": schema,
            # 이 모델은 기본적으로 내부 추론("thinking")에 시간을 꽤 쓴다(단순
            # "hello" 응답도 안 걸어두면 수십 초 걸림) — 문구 하나 추출하는
            # 데는 필요 없는 비용/시간이라 작은 값으로 제한한다.
            "thinkingConfig": {"thinkingBudget": 256},
        },
    }

    for attempt in range(1, GEMINI_MAX_ATTEMPTS + 1):
        try:
            res = requests.post(GEMINI_URL, params={"key": GEMINI_API_KEY}, json=body, timeout=GEMINI_TIMEOUT)
            if res.status_code == 429:
                # 쿼터/레이트리밋 초과 — 같은 요청을 바로 다시 보내봐야 소용없으니
                # (오히려 다른 문구 처리분 쿼터까지 더 깎아먹는다) 재시도 없이 바로 포기.
                logger.warning("Gemini 요청 한도 초과(429): 응답=%r", res.text[:200])
                raise GeminiUnavailable(f"Gemini 요청 한도 초과(429): {res.text[:200]}")
            res.raise_for_status()
            data = res.json()
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(raw_text)
        except GeminiUnavailable:
            raise
        except Exception as e:
            if attempt >= GEMINI_MAX_ATTEMPTS:
                logger.exception("Gemini 호출/응답 파싱 실패(%d회 재시도 후 포기): prompt=%r", GEMINI_MAX_ATTEMPTS, prompt[-200:])
                raise GeminiUnavailable(f"Gemini 호출/응답 파싱 실패({GEMINI_MAX_ATTEMPTS}회 재시도 후): {e}") from e
            logger.warning("Gemini 호출 실패(시도 %d/%d, 재시도함): error=%s", attempt, GEMINI_MAX_ATTEMPTS, e)
            time.sleep(GEMINI_RETRY_DELAY)

    raise AssertionError("unreachable")  # for 루프가 break/raise/return 없이 끝날 수 없음


def parse_work_hours_text_with_llm(text: str) -> dict:
    """work_hours 문구를 Gemini로 파싱. 반환값은 WORK_HOURS_RESPONSE_SCHEMA
    그대로(dict, 항상 반환) — status/필드 유효성 판단은 llm_pipeline.py의
    validate_work_hours_llm_output()이 한다."""
    return call_gemini_structured(WORK_HOURS_PROMPT_TEMPLATE.format(text=text), WORK_HOURS_RESPONSE_SCHEMA)


def parse_embargo_text_with_llm(text: str) -> dict:
    """WORK EMBARGO 문구를 Gemini로 파싱. 반환값은 EMBARGO_RESPONSE_SCHEMA
    그대로(dict, 항상 반환) — status/필드 유효성 판단은 llm_pipeline.py의
    validate_embargo_llm_output()이 한다."""
    return call_gemini_structured(EMBARGO_PROMPT_TEMPLATE.format(text=text), EMBARGO_RESPONSE_SCHEMA)
