"""
Silver — 공사 허가 시간대 제약 (construction_work_hours) + 공사 임시 중단 기간
(work_embargoes)

construction Gold(manhattan_construction_events, Traffic Score에 필요한 permit만
남은 상태) + construction_stipulations Bronze(조건/유의사항 텍스트)에서 두 종류의
시간대 관련 stipulation을 뽑는다:

1. work_hours — "WORK 9AM - 4PM, MONDAY TO FRIDAY" 류. "이 시간대에**만** 작업
   허용"이라는 뜻. extract_work_hours() 참고.
2. work_embargoes — "WORK EMBARGO: 07/19/2025 8AM - 5PM FOR SPECIAL EVENT Back
   to School Bash" 류. work_hours와 **반대 의미**로, "이 특정 날짜/시간대**에는**
   다른 행사(퍼레이드·블록파티 등) 때문에 작업이 일시 중단"이라는 뜻이다.
   extract_work_embargoes() 참고.

차선 유지/폭 관련 stipulation은 둘 다 파싱하지 않는다(요청 범위 밖 — 너무 복잡함).

전체 stipulation 텍스트 1,160만 건 중 고유 문구는 12,682개다. work_hours 패턴은
27개 고유 문구 · 52만여 행(4.5%)뿐이지만, WORK EMBARGO 패턴은 12,434개 고유
문구 · 56.7만여 행(4.9%)으로 훨씬 크다 — 처음엔 이 카테고리가 파싱 대상이 아니었는데,
실제로는 work_hours보다 큰 신호였다. 나머지 90%+는 소음 저감 인증, 자전거 랙
손상 금지 같은 법적/행정 보일러플레이트라 둘 다 아니라서 노이즈로 보고 버린다.

주의: 허가 하나가 요일별로 다른 시간대 제약을 여러 개 가질 수 있다(예: 평일은
"10AM-4PM", 토요일은 "8AM-4PM"을 별도 stipulation으로 둘 다 가짐 — 실제로 전체
매칭 허가의 약 28%가 이런 경우). 그래서 work_hours 결과는 "허가 하나당 한 행"이
아니라 "허가 x 시간대 규칙 하나당 한 행"이다 — 시간대 제약이 없는 허가는 정확히
한 행(전부 null), 여러 개 있는 허가는 그만큼 여러 행으로 나온다. work_embargoes도
같은 이유로 허가 하나가 여러 embargo 기간(행사가 여러 번 있으면)을 가질 수 있다.

Bronze 파티션(수백 개 날짜별 파일)을 한꺼번에 pandas로 합쳐 읽으면 컨테이너
메모리 한도에서 OOM이 나서(정확한 원인 미확인 — permitnumber 컬럼을 함께 읽을 때만
발생), 파티션 파일을 하나씩 순회하며 매칭되는 행만 누적하는 방식으로 우회한다.
전체 593개 파티션 기준 각각 몇 초 안쪽으로 끝나 성능 문제는 없다.

두 카테고리 모두 정규식만으로는 소수(work_hours 고유문구 중 1개, WORK EMBARGO
고유문구 중 414개, 실측 기준)가 안 잡힌다. extract_work_hours()/
extract_work_embargoes()는 각각 정규식 전용으로 순수하게 유지한다(온디맨드
API 경로에서도 안전하게 쓰기 위해 — LLM 호출 없음). build_work_hours_rules()/
build_embargoes()가 정규식 실패 문구를 Gemini로 한 번 더 시도하고
(src/construction_stipulations/llm_pipeline.py), 그래도 실패한 건(rule+LLM
둘 다 실패)은 버리지 않고 quarantine에 남겨 사람이 나중에 조회해서 패턴을
찾아 rule로 승격시킬 수 있게 한다. 매 실행 끝에는 고유 문구 기준
rule/LLM/quarantine 비율을 로그로 남기고(품질 리포트), 직전 실행 대비 크게
나빠지면 예외를 던져 기존 Slack 알림(on_failure_callback)이 울리게 한다 —
자세한 아키텍처는 llm_pipeline.py의 모듈 docstring 참고.
"""

from __future__ import annotations

import glob
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, GOLD_DIR, SILVER_DIR
from src.common.gemini import GEMINI_MODEL, parse_embargo_text_with_llm, parse_work_hours_text_with_llm
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.construction_stipulations.bronze import SOURCE as STIPULATIONS_SOURCE
from src.construction_stipulations.llm_pipeline import (
    DAY_MAP,
    _parse_embargo_date,
    _parse_embargo_time,
    _to_hour24,
    compute_and_log_quality_report,
    load_llm_cache,
    run_llm_fallback_batch,
    write_quarantine,
)

logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_stipulations_silver")

OUT_SOURCE = "construction_work_hours"

# construction Silver가 아니라 Gold(manhattan_construction_events)를 읽는다 —
# Manhattan/상태/시리즈로 이미 걸러진, Traffic Score에 실제로 필요한 permit만
# 대상으로 작업시간 stipulation을 매칭하기 위함 (src/construction/gold.py 참고).
CONSTRUCTION_GOLD_DIR = GOLD_DIR / "construction"

# "WORK 9AM - 4PM, MONDAY TO FRIDAY" / "WORK 10PM - 6AM NIGHTLY. SECTION 24-224..." 류를
# 매칭해서 시작/종료 시각 + 요일 구절(raw)을 뽑는다. 요일 구절 뒤에 붙는
# "SECTION 24-224 ADMINISTRATIVE CODE..." 보일러플레이트는 버린다.
WORK_HOUR_RE = re.compile(
    r"^WORK\s+(\d{1,2})\s*(AM|PM)\s*-\s*(\d{1,2})\s*(AM|PM)\s*,?\s*"
    r"([^.]*?)\s*\.?\s*(?:SECTION\s+24-224.*)?$",
    re.IGNORECASE,
)

# 요일 구절(raw, 대문자 정규화 후) -> 요일 코드는 DAY_MAP(llm_pipeline.py로
# 이설 — evidence 재검증에도 재사용하기 위해서다. 순환 임포트 방지를 위해
# llm_pipeline.py는 이 모듈을 import하지 않는다).


def _parse_work_hours(text: str) -> tuple[int, int, str, str] | None:
    m = WORK_HOUR_RE.match(text)
    if not m:
        return None

    start_h, start_ap, end_h, end_ap, days_raw = m.groups()
    days_raw = days_raw.strip().upper()
    day_code = DAY_MAP.get(days_raw, "OTHER")

    return _to_hour24(start_h, start_ap), _to_hour24(end_h, end_ap), day_code, days_raw


def _rule_parse_work_hours_with_lineage(text: str) -> dict | None:
    """_parse_work_hours()에 lineage(어떤 방식/규칙으로, 언제 처리됐는지)를
    덧붙인다 — 정규식 자체는 손대지 않는다."""
    parsed = _parse_work_hours(text)
    if parsed is None:
        return None
    start_hour, end_hour, day_code, days_raw = parsed
    return {
        "work_start_hour": start_hour,
        "work_end_hour": end_hour,
        "work_days_code": day_code,
        "work_days_raw": days_raw,
        "parse_method": "RULE",
        "parse_source": "WORK_HOUR_RE",
        "parsed_at": datetime.now(timezone.utc),
    }


WORK_HOURS_COLUMNS = [
    "permitnumber", "work_start_hour", "work_end_hour", "work_days_code", "work_days_raw",
    "parse_method", "parse_source", "parsed_at",
]
# lineage(parsed_at)는 호출마다 값이 달라져서 이 컬럼까지 포함해 dedup하면
# 사실상 아무것도 안 지워진다 — 의미 있는 값 컬럼만 기준으로 중복 제거한다.
_WORK_HOURS_DEDUP_SUBSET = ["permitnumber", "work_start_hour", "work_end_hour", "work_days_code", "work_days_raw"]
# _rule_parse_work_hours_with_lineage()가 반환하는 dict의 키(permitnumber
# 제외). pd.DataFrame(list_of_dicts, columns=이거)로 만들 때 명시적으로
# 넘긴다 — 모든 행이 정규식에 실패하면 list_of_dicts가 빈 리스트가 되는데,
# columns 없이 만들면 pandas가 컬럼 자체를 못 만들어서(0열) 이후
# raw[WORK_HOURS_COLUMNS] 선택에서 KeyError로 죽는다.
_WORK_HOURS_PARSE_KEYS = ["work_start_hour", "work_end_hour", "work_days_code", "work_days_raw", "parse_method", "parse_source", "parsed_at"]


def _load_raw_work_hours_rows(bronze_root: Path) -> pd.DataFrame:
    """stipulations Bronze 전체 파티션을 순회하며 "WORK ..." 시간대 문구가
    있는 (permitnumber, stipulationfulltext) 행만 골라 중복 제거해 반환한다
    (파싱은 안 함). _load_raw_embargo_rows()와 동일한 이유(OOM 회피)로
    파티션별로 순회한다."""
    files = sorted(glob.glob(str(bronze_root / "dt=*" / "data.parquet")))

    matched = []
    for f in files:
        day_df = pd.read_parquet(f, columns=["permitnumber", "stipulationfulltext"])
        mask = day_df["stipulationfulltext"].str.match(
            r"^WORK\s+\d{1,2}\s*[AP]M\s*-\s*\d{1,2}\s*[AP]M", case=False, na=False
        )
        if mask.any():
            matched.append(day_df[mask])

    if not matched:
        return pd.DataFrame(columns=["permitnumber", "stipulationfulltext"])

    return pd.concat(matched, ignore_index=True).drop_duplicates()


def extract_work_hours(bronze_root: Path = BRONZE_DIR / STIPULATIONS_SOURCE) -> pd.DataFrame:
    """
    stipulations Bronze 전체 파티션에서 작업 시간대 제약만 정규식으로 뽑는다
    (LLM 폴백 없음 — 온디맨드 API 경로에서도 안전하게 쓰라고 순수 함수로
    남겨둔다). LLM까지 포함한 결과는 build_work_hours_rules()로 미리 만들어
    둔 Silver 출력을 읽어라(load_built_work_hours_rules() 참고).
    """
    raw = _load_raw_work_hours_rows(bronze_root)
    if raw.empty:
        return pd.DataFrame(columns=WORK_HOURS_COLUMNS)

    parsed = raw["stipulationfulltext"].map(_rule_parse_work_hours_with_lineage)
    raw = raw[parsed.notna()].copy()
    parsed = parsed[parsed.notna()]

    parsed_df = pd.DataFrame(parsed.tolist(), index=raw.index, columns=_WORK_HOURS_PARSE_KEYS)
    for col in parsed_df.columns:
        raw[col] = parsed_df[col]

    return (
        raw[WORK_HOURS_COLUMNS]
        .drop_duplicates(subset=_WORK_HOURS_DEDUP_SUBSET)
        .reset_index(drop=True)
    )


# ─────────────────────────────────────────────────────────────
# work_embargoes — 공사 임시 중단 기간(work_hours와 반대 의미, 모듈 docstring 참고)
# ─────────────────────────────────────────────────────────────
#
# 날짜/시각 표기가 자유 텍스트라 변형이 많은데, 실측으로 확인한 주요 패턴은
# 세 가지다:
#   1) "DATE TIME - DATE TIME for REASON"  (기간 하나, 시작~끝 각각 날짜+시각)
#   2) "DATE TIME - TIME for REASON"       (같은 날 안에서 시작~끝, 끝 날짜 생략)
#   3) "DATE to DATE TIME-TIME for REASON" (기간 동안 매일 같은 시간대 반복 —
#      Open Street류. "07/22 12:01AM - 07/28/2025 11:59PM"처럼 사실상 하루 종일인
#      경우도 이 스키마로 표현하면 "그 기간 매일 00~24시 전부"가 되어 자연스럽다)
#
# 연도가 생략된 날짜(예: "03/28 12:01AM")는 쌍을 이루는 다른 쪽 날짜의 연도를
# 그대로 쓴다. 3)번은 "기간 동안 매일 반복되는 시간대"라는 점에서 work_hours의
# 스키마(work_start_ts~work_end_ts 기간 + 매일 반복되는 hour)와 구조가 같고,
# 1)/2)번도 시작일=종료일인 특수 케이스로 보면 동일하게 표현된다 — 그래서 결과
# 컬럼을 embargo_start_date/embargo_end_date/embargo_start_hour/embargo_end_hour로
# work_hours와 맞춰서, 나중에 closure_penalty에서 "work_hours 기준 활성인데 이
# embargo 기간에도 걸치면 그 시간만 제외" 식으로 조합하기 쉽게 했다(다음 단계 —
# 아직 closure_penalty에는 연결 안 함).
#
# 정규식 매칭률(전체 이력 기준 실측): 고유 문구 97.1%, 행 기준 86.1%, permit
# 기준 92.8%. 나머지는 "034/26/2025"(오탈자 날짜), "10700PM"(시각 오탈자),
# "00/00 - 00/00/2016"(더미 날짜), "for"가 아예 없는 경우 등 실제 데이터
# 품질 문제라 정규식을 더 다듬어도 수익이 적다고 판단해 work_hours와 동일하게
# 매칭 안 되면 결측 처리한다(Bronze 원칙 — 정제하지 않음).
EMBARGO_DATE = r"(\d{1,2}/\d{1,2}(?:/\d{2,4})?)"
EMBARGO_TIME = r"(\d{1,2}(?::\d{2})?\s*[AP]M)"

# 1)+2)
EMBARGO_RE_SINGLE = re.compile(
    rf"^WORK EMBARGO:\s*{EMBARGO_DATE}\s+{EMBARGO_TIME}\s*(?:-|to)\s*"
    rf"(?:{EMBARGO_DATE}\s+)?{EMBARGO_TIME}\s+for\s+(.+)",
    re.IGNORECASE,
)
# 3)
EMBARGO_RE_RECURRING = re.compile(
    rf"^WORK EMBARGO:\s*{EMBARGO_DATE}\s+to\s+{EMBARGO_DATE}\s+{EMBARGO_TIME}\s*-\s*{EMBARGO_TIME}\s+for\s+(.+)",
    re.IGNORECASE,
)
# reason 뒤에 붙는 "*NYC DOT REQUIRES FULL RESTORATION..." 보일러플레이트를 잘라낸다.
EMBARGO_REASON_BOILERPLATE_RE = re.compile(r"\s*\*?\s*NYC DOT REQUIRES.*", re.IGNORECASE)

# _parse_embargo_time/_parse_embargo_date는 llm_pipeline.py로 이설(evidence
# 재검증에도 재사용하기 위해서 — 순환 임포트 방지를 위해 llm_pipeline.py는
# 이 모듈을 import하지 않는다).


def _clean_embargo_reason(reason: str) -> str:
    reason = EMBARGO_REASON_BOILERPLATE_RE.sub("", reason).strip()
    return reason.rstrip(".").strip()


def _parse_work_embargo(text: str) -> dict | None:
    """WORK EMBARGO 문구 하나를 {embargo_start_date, embargo_end_date,
    embargo_start_hour, embargo_end_hour, embargo_reason}으로 파싱한다.
    실패하면(오탈자 등) None — 반복(3번) 패턴을 먼저 시도하고 안 되면 단일
    기간(1/2번) 패턴을 시도한다."""
    m = EMBARGO_RE_RECURRING.match(text)
    if m:
        start_date_str, end_date_str, start_time_str, end_time_str, reason = m.groups()
        end_date = _parse_embargo_date(end_date_str, None)
        if end_date is None:
            return None
        start_date = _parse_embargo_date(start_date_str, end_date.year)
        if start_date is None:
            return None
        return {
            "embargo_start_date": start_date,
            "embargo_end_date": end_date,
            "embargo_start_hour": _parse_embargo_time(start_time_str),
            "embargo_end_hour": _parse_embargo_time(end_time_str),
            "embargo_reason": _clean_embargo_reason(reason),
        }

    m = EMBARGO_RE_SINGLE.match(text)
    if m:
        start_date_str, start_time_str, end_date_str, end_time_str, reason = m.groups()
        if end_date_str:
            end_date = _parse_embargo_date(end_date_str, None)
            start_date = _parse_embargo_date(start_date_str, end_date.year) if end_date else None
        else:
            start_date = _parse_embargo_date(start_date_str, None)
            end_date = start_date
        if start_date is None or end_date is None:
            return None
        return {
            "embargo_start_date": start_date,
            "embargo_end_date": end_date,
            "embargo_start_hour": _parse_embargo_time(start_time_str),
            "embargo_end_hour": _parse_embargo_time(end_time_str),
            "embargo_reason": _clean_embargo_reason(reason),
        }

    return None


def _rule_parse_embargo_with_lineage(text: str) -> dict | None:
    """_parse_work_embargo()에 lineage(어떤 정규식으로, 언제 처리됐는지)를
    덧붙인다 — 정규식 자체는 손대지 않는다."""
    parsed = _parse_work_embargo(text)
    if parsed is None:
        return None
    parse_source = "EMBARGO_RE_RECURRING" if EMBARGO_RE_RECURRING.match(text) else "EMBARGO_RE_SINGLE"
    return {
        **parsed,
        "parse_method": "RULE",
        "parse_source": parse_source,
        "parsed_at": datetime.now(timezone.utc),
    }


EMBARGO_COLUMNS = [
    "permitnumber", "embargo_start_date", "embargo_end_date",
    "embargo_start_hour", "embargo_end_hour", "embargo_reason",
    "parse_method", "parse_source", "parsed_at",
]
# _rule_parse_embargo_with_lineage()가 반환하는 dict의 키(permitnumber
# 제외) — WORK_HOURS와 동일한 이유로 pd.DataFrame(...) 생성 시 명시적으로
# 넘긴다(모든 행이 정규식 실패 시 빈 리스트가 되어 컬럼이 안 생기는 문제 방지).
_EMBARGO_PARSE_KEYS = [
    "embargo_start_date", "embargo_end_date", "embargo_start_hour", "embargo_end_hour",
    "embargo_reason", "parse_method", "parse_source", "parsed_at",
]
# lineage(parsed_at)는 호출마다 값이 달라져서 이 컬럼까지 포함해 dedup하면
# 사실상 아무것도 안 지워진다 — 의미 있는 값 컬럼만 기준으로 중복 제거한다.
_EMBARGO_DEDUP_SUBSET = [
    "permitnumber", "embargo_start_date", "embargo_end_date",
    "embargo_start_hour", "embargo_end_hour", "embargo_reason",
]


_work_embargoes_cache: dict[str, pd.DataFrame] = {}


def _load_raw_embargo_rows(bronze_root: Path) -> pd.DataFrame:
    """stipulations Bronze 전체 파티션을 순회하며 "WORK EMBARGO:" 문구가 있는
    (permitnumber, stipulationfulltext) 행만 골라 중복 제거해 반환한다(파싱은
    안 함). extract_work_embargoes()(정규식 전용)와 build_embargoes()(정규식
    +LLM 폴백)가 이 스캔 결과를 공유하기 위한 헬퍼 — 둘 다 매번 593개 파티션을
    도는 건 낭비라 분리했다.

    extract_work_hours()와 동일한 이유(OOM 회피)로 파티션별로 순회하며 매칭
    행만 누적한다.
    """
    files = sorted(glob.glob(str(bronze_root / "dt=*" / "data.parquet")))

    matched = []
    for f in files:
        day_df = pd.read_parquet(f, columns=["permitnumber", "stipulationfulltext"])
        mask = day_df["stipulationfulltext"].str.startswith("WORK EMBARGO", na=False)
        if mask.any():
            matched.append(day_df[mask])

    if not matched:
        return pd.DataFrame(columns=["permitnumber", "stipulationfulltext"])

    return pd.concat(matched, ignore_index=True).drop_duplicates()


def extract_work_embargoes(bronze_root: Path = BRONZE_DIR / STIPULATIONS_SOURCE) -> pd.DataFrame:
    """
    stipulations Bronze 전체 파티션에서 "WORK EMBARGO:" 문구만 골라 정규식으로
    파싱한다(LLM 폴백 없음 — 온디맨드 API 경로에서 안전하게 쓰라고 순수 함수로
    남겨둔다). extract_work_hours()와 반대 의미(허용 시간대가 아니라 임시 중단
    시간대)라 별도 함수/결과로 둔다.

    LLM까지 포함한 결과가 필요하면 build_embargoes()로 미리 만들어 둔
    construction_work_embargoes Silver 출력을 읽어라(closure_penalty.py의
    load_built_embargoes() 참고).

    전체 파티션을 매번 순회하면 몇 초 걸려서(실측 ~9초), closure_penalty.py의
    load_embargoes_by_permit()처럼 여러 곳에서 같은 프로세스 안에 호출될 수
    있는 경우를 위해 bronze_root별로 캐싱한다.
    """
    cache_key = str(bronze_root)
    if cache_key in _work_embargoes_cache:
        return _work_embargoes_cache[cache_key]

    raw = _load_raw_embargo_rows(bronze_root)

    if raw.empty:
        result = pd.DataFrame(columns=EMBARGO_COLUMNS)
        _work_embargoes_cache[cache_key] = result
        return result

    parsed = raw["stipulationfulltext"].map(_rule_parse_embargo_with_lineage)
    raw = raw[parsed.notna()].copy()
    parsed = parsed[parsed.notna()]

    parsed_df = pd.DataFrame(parsed.tolist(), index=raw.index, columns=_EMBARGO_PARSE_KEYS)
    for col in parsed_df.columns:
        raw[col] = parsed_df[col]

    result = raw[EMBARGO_COLUMNS].drop_duplicates(subset=_EMBARGO_DEDUP_SUBSET).reset_index(drop=True)
    _work_embargoes_cache[cache_key] = result
    return result


EMBARGO_OUT_SOURCE = "construction_work_embargoes"

# 정규식+LLM 둘 다 실패한 "신규"(오늘 처음 본) 고유 문구가 이 개수를 넘으면
# validate_embargoes_output()이 예외를 던진다. 이미 예전부터 실패로 확정된
# 캐시 항목은 매일 다시 세지 않는다(그건 못 고치는 기존 오탈자라 매번
# 알림오면 무시하게 될 뿐) — 그래서 임계값을 낮게(사실상 "0개면 정상, 몇 개만
# 나와도 이상 신호") 잡아도 노이즈가 안 된다.
EMBARGO_NEW_FAILURE_ALERT_THRESHOLD = 5


def build_embargoes(run_date: str | None = None) -> str:
    """extract_work_embargoes()가 정규식으로 못 잡는 문구를 Gemini로 한 번 더
    시도해서(src/common/gemini.py, llm_pipeline.run_llm_fallback_batch) 최종
    embargo 결과를 만든다.

    정규식 실패 문구 중 LLM 캐시에 아직 없는 "신규" 고유 문구만 실제로
    Gemini를 호출한다. 정규식 성공분 + LLM 성공분을 합쳐
    construction_work_embargoes Silver 파티션으로 저장한다 — 이 저장은 LLM
    호출이 도중에 막혀도 항상 일어난다. rule+LLM 둘 다 실패한 현재 전체
    후보는 quarantine에 기록하고(사람이 나중에 조회해서 rule로 승격할지
    판단), 고유 문구 기준 rule/LLM/quarantine 비율을 품질 리포트로 남긴다
    (llm_pipeline.compute_and_log_quality_report).

    LLM도 실패한 문구가 이번 실행에서 몇 개나 새로 나왔는지는
    validate_embargoes_output()이 캐시의 resolved_date로 판단한다. Gemini
    호출이 아예 막혀서 신규 문구를 하나도 못 시도했거나, 직전 실행 대비
    품질이 급락했으면 이 함수 자체가 (Silver 저장은 끝낸 뒤) RuntimeError를
    던져 별도로 알린다.
    """
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("construction_work_embargoes Silver(LLM 폴백 포함) 빌드 시작: run_date=%s", run_date)

    raw = _load_raw_embargo_rows(BRONZE_DIR / STIPULATIONS_SOURCE)
    if raw.empty:
        path = save_parquet(pd.DataFrame(columns=EMBARGO_COLUMNS), SILVER_DIR / EMBARGO_OUT_SOURCE / f"dt={run_date}")
        logger.info("WORK EMBARGO 문구 자체가 없음: path=%s", path)
        return str(path)

    parsed = raw["stipulationfulltext"].map(_rule_parse_embargo_with_lineage)
    regex_ok = raw[parsed.notna()].copy()
    parsed_df = pd.DataFrame(parsed[parsed.notna()].tolist(), index=regex_ok.index, columns=_EMBARGO_PARSE_KEYS)
    for col in parsed_df.columns:
        regex_ok[col] = parsed_df[col]

    regex_failed = raw[parsed.isna()]
    failed_texts = regex_failed["stipulationfulltext"].unique().tolist()

    cache = load_llm_cache("embargo")
    known_texts = set(cache["stipulationfulltext"]) if not cache.empty else set()
    new_texts = [t for t in failed_texts if t not in known_texts]

    logger.info(
        "정규식 실패 고유 문구=%d개, 이 중 LLM 캐시에 없는 신규 문구=%d개",
        len(failed_texts), len(new_texts),
    )

    unavailable_count, last_unavailable_error = run_llm_fallback_batch(
        "embargo", new_texts, run_date, parse_embargo_text_with_llm,
    )

    cache = load_llm_cache("embargo")
    llm_ok = cache[cache["parseable"]] if not cache.empty else cache
    llm_resolved = regex_failed.merge(
        llm_ok[["stipulationfulltext", "embargo_start_date", "embargo_end_date", "embargo_start_hour", "embargo_end_hour", "embargo_reason"]],
        on="stipulationfulltext",
        how="inner",
    ).copy()
    llm_resolved["parse_method"] = "LLM"
    llm_resolved["parse_source"] = GEMINI_MODEL
    llm_resolved["parsed_at"] = datetime.now(timezone.utc)

    combined = pd.concat([regex_ok[EMBARGO_COLUMNS], llm_resolved[EMBARGO_COLUMNS]], ignore_index=True)
    combined = combined.drop_duplicates(subset=_EMBARGO_DEDUP_SUBSET).reset_index(drop=True)

    path = save_parquet(combined, SILVER_DIR / EMBARGO_OUT_SOURCE / f"dt={run_date}")
    logger.info(
        "construction_work_embargoes Silver 빌드 완료: rows=%d(정규식=%d, LLM 보강=%d) path=%s",
        len(combined), len(regex_ok), len(llm_resolved), path,
    )

    # quarantine: 지금 시점에 rule+LLM 둘 다 실패한 전체 후보(캐시에
    # parseable=False로 남은 문구)를 permitnumber와 조인해서 기록한다.
    # write_quarantine()은 이미 있는 키를 건드리지 않으니 매번 전체 백로그를
    # 넘겨도 안전하다(idempotent).
    llm_failed = cache[~cache["parseable"]] if not cache.empty else cache
    if not llm_failed.empty:
        quarantine_candidates = regex_failed.merge(
            llm_failed[["stipulationfulltext", "llm_status", "llm_output_raw", "validation_failure_reason"]],
            on="stipulationfulltext",
            how="inner",
        ).copy()
        quarantine_candidates["rule_failure_reason"] = "EMBARGO_RE_RECURRING/EMBARGO_RE_SINGLE 둘다 미매칭"
        write_quarantine(
            "embargo",
            quarantine_candidates[[
                "permitnumber", "stipulationfulltext", "rule_failure_reason",
                "llm_status", "llm_output_raw", "validation_failure_reason",
            ]],
            run_date,
        )

    # 품질 리포트(고유 문구 기준) + 직전 실행 대비 급락 감지.
    # llm_parsed_count는 llm_ok(캐시 전체, 예전에 LLM으로 풀렸던 문구까지
    # 다 포함)가 아니라 llm_resolved(지금 이 실행에서 실제로 regex_failed와
    # 합쳐진 것)로 세야 한다 — 안 그러면 나중에 rule 정규식이 개선돼서
    # 예전에 LLM으로 풀렸던 문구를 rule이 잡게 됐을 때 그 문구가 rule_parsed_count와
    # llm_parsed_count 양쪽에 다 잡혀서 합계가 100%를 넘고 quarantine_count가
    # 0으로 뭉개져 drift 감지 신호가 왜곡된다.
    total_unique_texts = raw["stipulationfulltext"].nunique()
    rule_parsed_count = regex_ok["stipulationfulltext"].nunique()
    llm_parsed_count = llm_resolved["stipulationfulltext"].nunique() if not llm_resolved.empty else 0
    drift_message = compute_and_log_quality_report(
        "embargo", run_date, total_unique_texts, rule_parsed_count, llm_parsed_count,
    )

    # 신규 문구 중 절반 넘게 호출 자체가 안 됐으면(키 미설정, 또는 타임아웃/오류가
    # 산발적 몇 건이 아니라 대다수) "이번 실행은 LLM 폴백이 사실상 안 돌았다"로
    # 보고 알린다. 한두 건의 개별 타임아웃은 정상 변동 범위라 알리지 않는다.
    # 정규식 결과는 이미 정상 저장했으니(위 save_parquet) 아래에서 실패해도
    # 데이터 자체는 안 깨진다 — 이 예외들은 전부 Slack 알림용 신호일 뿐이다.
    if new_texts and unavailable_count / len(new_texts) > 0.5:
        raise RuntimeError(
            f"Gemini LLM 폴백 호출 실패(정규식 결과는 정상 저장됨): "
            f"신규 문구 {len(new_texts)}개 중 {unavailable_count}개 호출 불가 — {last_unavailable_error}"
        )
    if drift_message:
        raise RuntimeError(drift_message)

    return str(path)


def validate_embargoes_output(path: str, run_date: str) -> str:
    """build_embargoes()가 저장한 결과를 검증한다. 오늘 새로 LLM에 물어본
    문구 중 정규식+LLM 둘 다 실패한("진짜 복구 불가능한 쓰레기") 건이
    EMBARGO_NEW_FAILURE_ALERT_THRESHOLD를 넘으면 예외를 던진다 — 이 예외가
    Airflow task를 실패시켜 construction_pipeline.py의 on_failure_callback
    (notify_slack_failure)이 그대로 Slack 알림을 보낸다."""
    df = pd.read_parquet(path)

    cache = load_llm_cache("embargo")
    if cache.empty:
        logger.info("construction_work_embargoes 검증 완료: rows=%d, 오늘 신규 LLM 호출 없음", len(df))
        return path

    new_failures = cache[(cache["resolved_date"] == run_date) & (~cache["parseable"])]
    if len(new_failures) > EMBARGO_NEW_FAILURE_ALERT_THRESHOLD:
        raise ValueError(
            f"WORK EMBARGO 문구 중 정규식+LLM 둘 다 파싱 실패한 신규 고유 문구가 "
            f"{len(new_failures)}개(임계값 {EMBARGO_NEW_FAILURE_ALERT_THRESHOLD}) 발생 — "
            f"NYC 쪽 문구 포맷이 바뀌었을 수 있음. 샘플: {new_failures['stipulationfulltext'].head(3).tolist()}"
        )

    logger.info(
        "construction_work_embargoes 검증 완료: rows=%d, 오늘 신규 LLM 실패=%d건(임계값 %d)",
        len(df), len(new_failures), EMBARGO_NEW_FAILURE_ALERT_THRESHOLD,
    )
    return path


def main_embargoes(run_date: str | None = None) -> str:
    """build_embargoes + validate_embargoes_output을 순서대로 실행 — Airflow
    밖에서 스크립트로 직접 돌릴 때용."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())
    path = build_embargoes(run_date)
    validate_embargoes_output(path, run_date)
    return path


def load_built_embargoes() -> pd.DataFrame:
    """build_embargoes()가 저장한 construction_work_embargoes Silver의 가장
    최근 파티션을 읽는다(정규식+LLM 폴백 다 반영된 결과). build_embargoes()는
    매번 전체 이력을 다시 스캔해 저장하므로 최신 파티션이 곧 최신 전체
    데이터다. 아직 한 번도 안 돌아서 파티션이 없으면(최초 배포 직후 등)
    extract_work_embargoes()(정규식 전용)로 폴백한다 — 온디맨드 API 경로가
    빈 결과를 주는 것보다는 낫다.
    """
    partitions = sorted(glob.glob(str(SILVER_DIR / EMBARGO_OUT_SOURCE / "dt=*" / "data.parquet")))
    if not partitions:
        return extract_work_embargoes()
    return pd.read_parquet(partitions[-1])


def load_construction_gold(run_date: str) -> pd.DataFrame:
    path = CONSTRUCTION_GOLD_DIR / f"dt={run_date}" / "data.parquet"
    return pd.read_parquet(path)


WORK_HOURS_OUT_SOURCE = "construction_work_hours_rules"

# work_hours는 역사상 정규식 실패 문구가 1개뿐이라(embargo의 414개에 비해
# 극히 적음) 임계값을 embargo보다 낮게 잡는다 — 신규 실패가 조금만 나와도
# 이상 신호로 본다.
WORK_HOURS_NEW_FAILURE_ALERT_THRESHOLD = 3


def build_work_hours_rules(run_date: str | None = None) -> str:
    """extract_work_hours()가 정규식으로 못 잡는 문구를 Gemini로 한 번 더
    시도해서 최종 work_hours 결과를 만든다 — build_embargoes()와 동일한
    구조(rule -> LLM 폴백 -> quarantine -> 품질 리포트)를 work_hours
    카테고리에 그대로 적용한다(llm_pipeline.py 참고)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("construction_work_hours_rules Silver(LLM 폴백 포함) 빌드 시작: run_date=%s", run_date)

    raw = _load_raw_work_hours_rows(BRONZE_DIR / STIPULATIONS_SOURCE)
    if raw.empty:
        path = save_parquet(
            pd.DataFrame(columns=WORK_HOURS_COLUMNS), SILVER_DIR / WORK_HOURS_OUT_SOURCE / f"dt={run_date}"
        )
        logger.info("work_hours 문구 자체가 없음: path=%s", path)
        return str(path)

    parsed = raw["stipulationfulltext"].map(_rule_parse_work_hours_with_lineage)
    regex_ok = raw[parsed.notna()].copy()
    parsed_df = pd.DataFrame(parsed[parsed.notna()].tolist(), index=regex_ok.index)
    for col in parsed_df.columns:
        regex_ok[col] = parsed_df[col]

    regex_failed = raw[parsed.isna()]
    failed_texts = regex_failed["stipulationfulltext"].unique().tolist()

    cache = load_llm_cache("work_hours")
    known_texts = set(cache["stipulationfulltext"]) if not cache.empty else set()
    new_texts = [t for t in failed_texts if t not in known_texts]

    logger.info(
        "[work_hours] 정규식 실패 고유 문구=%d개, 이 중 LLM 캐시에 없는 신규 문구=%d개",
        len(failed_texts), len(new_texts),
    )

    unavailable_count, last_unavailable_error = run_llm_fallback_batch(
        "work_hours", new_texts, run_date, parse_work_hours_text_with_llm,
    )

    cache = load_llm_cache("work_hours")
    llm_ok = cache[cache["parseable"]] if not cache.empty else cache
    llm_resolved = regex_failed.merge(
        llm_ok[["stipulationfulltext", "work_start_hour", "work_end_hour", "work_days_code", "work_days_raw"]],
        on="stipulationfulltext",
        how="inner",
    ).copy()
    llm_resolved["parse_method"] = "LLM"
    llm_resolved["parse_source"] = GEMINI_MODEL
    llm_resolved["parsed_at"] = datetime.now(timezone.utc)

    combined = pd.concat([regex_ok[WORK_HOURS_COLUMNS], llm_resolved[WORK_HOURS_COLUMNS]], ignore_index=True)
    combined = combined.drop_duplicates(subset=_WORK_HOURS_DEDUP_SUBSET).reset_index(drop=True)

    path = save_parquet(combined, SILVER_DIR / WORK_HOURS_OUT_SOURCE / f"dt={run_date}")
    logger.info(
        "construction_work_hours_rules Silver 빌드 완료: rows=%d(정규식=%d, LLM 보강=%d) path=%s",
        len(combined), len(regex_ok), len(llm_resolved), path,
    )

    llm_failed = cache[~cache["parseable"]] if not cache.empty else cache
    if not llm_failed.empty:
        quarantine_candidates = regex_failed.merge(
            llm_failed[["stipulationfulltext", "llm_status", "llm_output_raw", "validation_failure_reason"]],
            on="stipulationfulltext",
            how="inner",
        ).copy()
        quarantine_candidates["rule_failure_reason"] = "WORK_HOUR_RE 미매칭"
        write_quarantine(
            "work_hours",
            quarantine_candidates[[
                "permitnumber", "stipulationfulltext", "rule_failure_reason",
                "llm_status", "llm_output_raw", "validation_failure_reason",
            ]],
            run_date,
        )

    # llm_parsed_count는 llm_ok(캐시 전체)가 아니라 llm_resolved(이번 실행의
    # regex_failed와 실제로 합쳐진 것)로 센다 — build_embargoes()의 동일한
    # 수정 사항 참고(리뷰에서 발견된 이중 카운팅 문제).
    total_unique_texts = raw["stipulationfulltext"].nunique()
    rule_parsed_count = regex_ok["stipulationfulltext"].nunique()
    llm_parsed_count = llm_resolved["stipulationfulltext"].nunique() if not llm_resolved.empty else 0
    drift_message = compute_and_log_quality_report(
        "work_hours", run_date, total_unique_texts, rule_parsed_count, llm_parsed_count,
    )

    if new_texts and unavailable_count / len(new_texts) > 0.5:
        raise RuntimeError(
            f"Gemini LLM 폴백 호출 실패(정규식 결과는 정상 저장됨): "
            f"신규 문구 {len(new_texts)}개 중 {unavailable_count}개 호출 불가 — {last_unavailable_error}"
        )
    if drift_message:
        raise RuntimeError(drift_message)

    return str(path)


def validate_work_hours_rules_output(path: str, run_date: str) -> str:
    """build_work_hours_rules()가 저장한 결과를 검증한다 —
    validate_embargoes_output()과 동일한 구조."""
    df = pd.read_parquet(path)

    cache = load_llm_cache("work_hours")
    if cache.empty:
        logger.info("construction_work_hours_rules 검증 완료: rows=%d, 오늘 신규 LLM 호출 없음", len(df))
        return path

    new_failures = cache[(cache["resolved_date"] == run_date) & (~cache["parseable"])]
    if len(new_failures) > WORK_HOURS_NEW_FAILURE_ALERT_THRESHOLD:
        raise ValueError(
            f"work_hours 문구 중 정규식+LLM 둘 다 파싱 실패한 신규 고유 문구가 "
            f"{len(new_failures)}개(임계값 {WORK_HOURS_NEW_FAILURE_ALERT_THRESHOLD}) 발생 — "
            f"NYC 쪽 문구 포맷이 바뀌었을 수 있음. 샘플: {new_failures['stipulationfulltext'].head(3).tolist()}"
        )

    logger.info(
        "construction_work_hours_rules 검증 완료: rows=%d, 오늘 신규 LLM 실패=%d건(임계값 %d)",
        len(df), len(new_failures), WORK_HOURS_NEW_FAILURE_ALERT_THRESHOLD,
    )
    return path


def main_work_hours_rules(run_date: str | None = None) -> str:
    """build_work_hours_rules + validate_work_hours_rules_output을 순서대로
    실행 — Airflow 밖에서 스크립트로 직접 돌릴 때용."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())
    path = build_work_hours_rules(run_date)
    validate_work_hours_rules_output(path, run_date)
    return path


def load_built_work_hours_rules() -> pd.DataFrame:
    """build_work_hours_rules()가 저장한 construction_work_hours_rules
    Silver의 가장 최근 파티션을 읽는다(정규식+LLM 폴백 다 반영된 결과).
    아직 한 번도 안 돌아서 파티션이 없으면(최초 배포 직후 등)
    extract_work_hours()(정규식 전용)로 폴백한다 — load_built_embargoes()와
    동일한 패턴."""
    partitions = sorted(glob.glob(str(SILVER_DIR / WORK_HOURS_OUT_SOURCE / "dt=*" / "data.parquet")))
    if not partitions:
        return extract_work_hours()
    return pd.read_parquet(partitions[-1])


def _merge_work_hours(construction: pd.DataFrame) -> pd.DataFrame:
    work_hours = load_built_work_hours_rules()

    return construction.merge(
        work_hours,
        left_on="permit_id",
        right_on="permitnumber",
        how="left",
    ).drop(columns=["permitnumber"])


def validate(df: pd.DataFrame, construction_rows: int) -> None:
    if df.empty:
        raise ValueError("construction_work_hours Silver 결과가 비었습니다.")

    if df["permit_id"].isna().any():
        raise ValueError("permit_id NULL 발생")

    # LEFT JOIN이라 원본(construction) 행수보다 적을 수 없다 (여러 시간대 규칙이면 더 늘어남).
    if len(df) < construction_rows:
        raise ValueError(
            f"조인 후 행수({len(df)})가 원본 construction 행수({construction_rows})보다 적음 — LEFT JOIN 오류 가능성"
        )

    has_rule = df["work_start_hour"].notna().sum()
    logger.info(
        "construction_work_hours Silver 검증 완료: rows=%d (원본 construction=%d), 시간대 제약 있는 행=%d (%.1f%%)",
        len(df), construction_rows, has_rule, has_rule / len(df) * 100,
    )
    logger.info("work_days_code 분포:\n%s", df["work_days_code"].value_counts(dropna=False).to_string())


def build(run_date: str | None = None) -> str:
    """load -> merge -> save만 한다(validate 없음)."""
    if run_date is None:
        run_date = os.getenv("RUN_DATE", date.today().isoformat())

    logger.info("construction_work_hours Silver 변환 시작: run_date=%s", run_date)

    construction = load_construction_gold(run_date)
    df = _merge_work_hours(construction)

    path = save_parquet(df, SILVER_DIR / OUT_SOURCE / f"dt={run_date}")

    logger.info(
        "construction_work_hours Silver 빌드 완료: rows=%d columns=%d path=%s",
        len(df), len(df.columns), path,
    )
    return str(path)


def validate_output(path: str, run_date: str) -> str:
    """build()가 저장한 결과를 다시 읽어, 그 run_date의 construction Gold
    행수와 비교하며 validate()를 돌린다."""
    df = pd.read_parquet(path)
    construction_rows = len(load_construction_gold(run_date))
    validate(df, construction_rows=construction_rows)
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
