"""
Gemini API를 이용한 WORK EMBARGO stipulation 텍스트 파싱 폴백.

src/construction_stipulations/silver.py의 정규식(_parse_work_embargo)이 못
잡는 오탈자/포맷 변형 문구("034/26/2025", "10700PM" 같은)를 이걸로 한 번 더
시도한다. 정규식 실패 건만 호출하고(전체 재처리 안 함), 고유 문구 기준으로
결과를 캐싱해서(build_embargoes() 참고) 같은 문구를 반복 호출하지 않는다.

배치(Airflow 태스크) 안에서만 호출한다 — API 요청 경로에서 동기 호출하면
사용자 응답이 느려지고 요청마다 과금되므로, 온디맨드 조회(traffic_score.py)는
항상 이 모듈이 미리 계산해 둔 결과(Silver 파티션)만 읽는다.
"""

from __future__ import annotations

import json
import time
from datetime import date

import requests

from src.common.config import GEMINI_API_KEY
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="gemini")


class GeminiUnavailable(Exception):
    """Gemini 호출 자체가 안 되는 상황(키 미설정/네트워크 오류/응답 형식이 아예
    깨짐) — "이 문구를 모델이 못 알아봤다"(진짜 파싱 실패, 반환값 None)와는
    다르다. 호출부(build_embargoes)는 이걸 "문구가 나쁘다"가 아니라 "이번엔
    시도조차 못 했다"는 신호로 써서, 캐시에 영구 실패로 남기지 않고 다음
    실행에서 다시 시도하게 한다(키를 나중에 설정하는 경우가 실제로 있음)."""

GEMINI_MODEL = "gemini-3.1-flash-lite"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
# 실측 결과 같은 모델/문구로도 응답이 1~2초 걸릴 때도, 60초 타임아웃까지 그냥
# 멈춰버릴 때도 있다(동시성/부하와 무관 — 순차 호출로도 재현됨. 실측 약 절반
# 정도가 이렇게 멈춘다). 재시도하면 대부분 몇 초 안에 성공하는 걸 보면 요청
# 자체가 서버 쪽에서 가끔 걸리는 것으로 보여, 짧게 여러 번 재시도한다.
GEMINI_TIMEOUT = 60
GEMINI_MAX_ATTEMPTS = 3
GEMINI_RETRY_DELAY = 2

PROMPT_TEMPLATE = """다음은 NYC 공사 허가 stipulation(조건문) 텍스트 중 "WORK EMBARGO:"로 시작하는 문구다.
이 문구에서 공사가 임시 중단되는 기간의 시작 날짜, 종료 날짜, 매일 반복되는 시작 시각(0~23 정수),
종료 시각(0~23 정수), 중단 사유를 뽑아라. 오탈자나 이상한 형식이 섞여 있을 수 있으니 문맥으로 최대한
추론하되, 도저히 알 수 없으면(날짜/시간 정보 자체가 없거나 앞뒤가 안 맞아 복구 불가능하면)
parseable을 false로만 응답하고 나머지 필드는 비워라.

텍스트: {text}
"""

RESPONSE_SCHEMA = {
    "type": "object",
    "properties": {
        "parseable": {"type": "boolean"},
        "start_date": {"type": "string", "description": "YYYY-MM-DD"},
        "end_date": {"type": "string", "description": "YYYY-MM-DD"},
        "start_hour": {"type": "integer"},
        "end_hour": {"type": "integer"},
        "reason": {"type": "string"},
    },
    "required": ["parseable"],
}


def parse_embargo_text_with_llm(text: str) -> dict | None:
    """Gemini에게 embargo 문구 파싱을 시켜본다.

    반환값 계약(호출부인 build_embargoes()가 이 구분에 따라 캐시 여부를
    결정한다):
    - dict: 파싱 성공. extract_work_embargoes()와 동일한 키
      ({embargo_start_date, embargo_end_date, embargo_start_hour,
      embargo_end_hour, embargo_reason}).
    - None: 모델이 이 문구를 "못 알아보겠다"고 명시적으로 응답했거나
      (parseable=false), parseable=true라면서도 필드가 이상한 경우 — 문구
      자체가 진짜 파싱 불가능하다는 뜻이라 영구 캐시해도 된다.
    - GeminiUnavailable 예외: 키 미설정/네트워크 오류/응답 형식 자체가 깨짐 —
      문구 문제가 아니라 이번 호출을 아예 못 한 것이므로 캐시하면 안 된다.
    """
    if not GEMINI_API_KEY:
        raise GeminiUnavailable("GEMINI_API_KEY가 설정되지 않음")

    body = {
        "contents": [{"parts": [{"text": PROMPT_TEMPLATE.format(text=text)}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": RESPONSE_SCHEMA,
            # gemini-3.6-flash는 기본적으로 내부 추론("thinking")에 시간을 꽤
            # 쓴다(단순 "hello" 응답도 안 걸어두면 수십 초 걸림 — 이 문구 하나
            # 추출하는 데는 필요 없는 비용/시간이라 작은 값으로 제한한다.
            "thinkingConfig": {"thinkingBudget": 256},
        },
    }

    for attempt in range(1, GEMINI_MAX_ATTEMPTS + 1):
        try:
            res = requests.post(GEMINI_URL, params={"key": GEMINI_API_KEY}, json=body, timeout=GEMINI_TIMEOUT)
            if res.status_code == 429:
                # 쿼터/레이트리밋 초과 — 같은 요청을 바로 다시 보내봐야 소용없으니
                # (오히려 다른 문구 처리분 쿼터까지 더 깎아먹는다) 재시도 없이 바로 포기.
                logger.warning("Gemini 요청 한도 초과(429): text=%r 응답=%r", text[:120], res.text[:200])
                raise GeminiUnavailable(f"Gemini 요청 한도 초과(429): {res.text[:200]}")
            res.raise_for_status()
            data = res.json()
            raw_text = data["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(raw_text)
            break
        except GeminiUnavailable:
            raise
        except Exception as e:
            if attempt >= GEMINI_MAX_ATTEMPTS:
                logger.exception("Gemini 호출/응답 파싱 실패(%d회 재시도 후 포기): text=%r", GEMINI_MAX_ATTEMPTS, text[:120])
                raise GeminiUnavailable(f"Gemini 호출/응답 파싱 실패({GEMINI_MAX_ATTEMPTS}회 재시도 후): {e}") from e
            logger.warning("Gemini 호출 실패(시도 %d/%d, 재시도함): text=%r error=%s", attempt, GEMINI_MAX_ATTEMPTS, text[:120], e)
            time.sleep(GEMINI_RETRY_DELAY)

    if not parsed.get("parseable"):
        return None

    try:
        start_date = date.fromisoformat(parsed["start_date"])
        end_date = date.fromisoformat(parsed["end_date"])
        start_hour = int(parsed["start_hour"])
        end_hour = int(parsed["end_hour"])
        if not (0 <= start_hour <= 23 and 0 <= end_hour <= 23):
            raise ValueError(f"시각 범위 벗어남: {start_hour}~{end_hour}")
    except (KeyError, ValueError, TypeError):
        logger.warning("Gemini가 parseable=true라고 했는데 필드가 이상함: %r (원문: %r)", parsed, text[:120])
        return None

    return {
        "embargo_start_date": start_date,
        "embargo_end_date": end_date,
        "embargo_start_hour": start_hour,
        "embargo_end_hour": end_hour,
        "embargo_reason": str(parsed.get("reason") or "")[:200],
    }
