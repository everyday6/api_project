"""
construction stipulation 텍스트(work_hours, WORK EMBARGO) 파싱의 "Rule 실패 →
LLM 폴백 → 결정론적 검증 → Quarantine → 품질 리포트" 공통 엔진.

카테고리(work_hours/embargo)에 무관한 부분만 여기 둔다 — LLM 호출 자체는
src/common/gemini.py, 정규식 rule 파서는 src/construction_stipulations/
silver.py에 그대로 남아있고, 이 모듈은 그 사이/뒤에서 캐싱·검증·quarantine·
리포트를 담당한다. 두 카테고리는 스키마 모양이 달라서(day_code enum vs
날짜 쌍) 범용 "카테고리 플러그인" 추상화 대신, 이 모듈이 제공하는 함수들을
category별 얇은 함수(silver.py의 build_embargoes/build_work_hours_rules)가
호출하는 방식으로 공유한다.

시간/날짜 관련 최소 파싱 primitive(_to_hour24, _parse_embargo_time,
_parse_embargo_date, DAY_MAP)는 원래 silver.py에 있던 것을 여기로 옮겼다 —
evidence 재검증(값이 원문과 실제로 일치하는지)에 그대로 재사용하기 위해서고,
silver.py의 rule 파서도 여기서 import해서 쓴다(순환 임포트 방지 — 이 모듈은
silver.py를 import하지 않는다).
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timezone

import pandas as pd

from src.common.config import GEMINI_API_KEY, SILVER1_DIR
from src.common.gemini import GeminiUnavailable
from src.common.logger import get_logger
from src.common.utils import save_parquet

logger = get_logger(__name__, log_to_file=True, log_file_stem="construction_stipulations_llm_pipeline")

# ─────────────────────────────────────────────────────────────
# 공유 파싱 primitive (silver.py에서 이설 — evidence 재검증에도 재사용)
# ─────────────────────────────────────────────────────────────

_EMBARGO_YEAR_MIN = 2000
_EMBARGO_YEAR_MAX = 2100

# 요일 구절(raw, 대문자 정규화 후) -> 요일 코드. 매칭 안 되는 구절은 OTHER.
DAY_MAP = {
    "": "DAILY",
    "NIGHTLY": "DAILY",
    "MONDAY TO FRIDAY": "WEEKDAY",
    "MONDAY THROUGH FRIDAY": "WEEKDAY",
    "WEEKNIGHTS": "WEEKDAY",
    "WEEKNIGHTS, NO WEEKENDS": "WEEKDAY",
    "SATURDAY AND SUNDAY": "WEEKEND",
    "SATURDAY": "SATURDAY",
    "SUNDAY": "SUNDAY",
    "EXCEPT SUNDAY": "EXCEPT_SUNDAY",
}
DAY_CODES = {"DAILY", "WEEKDAY", "WEEKEND", "SATURDAY", "SUNDAY", "EXCEPT_SUNDAY", "OTHER"}


def _to_hour24(hour: str, ampm: str) -> int:
    h = int(hour)
    if ampm.upper() == "AM":
        return 0 if h == 12 else h
    return 12 if h == 12 else h + 12


def _parse_embargo_time(time_str: str) -> int:
    """"8AM", "9:30PM", "12:01AM" -> 0~23시(정수). 분은 버린다."""
    m = re.match(r"(\d{1,2})(?::\d{2})?\s*([AP])M", time_str, re.IGNORECASE)
    hour = int(m.group(1))
    is_am = m.group(2).upper() == "A"
    if is_am:
        return 0 if hour == 12 else hour
    return 12 if hour == 12 else hour + 12


def _parse_embargo_date(date_str: str, reference_year: int | None) -> date | None:
    """"7/19/2025", "7/19/25", "7/19"(연도 생략 -> reference_year) -> date.
    오탈자로 연도/날짜 자체가 이상하면 None."""
    parts = date_str.split("/")
    try:
        month, day = int(parts[0]), int(parts[1])
        if len(parts) == 3:
            year = int(parts[2])
            if year < 100:
                year += 2000
        elif reference_year is not None:
            year = reference_year
        else:
            return None
        if not (_EMBARGO_YEAR_MIN <= year <= _EMBARGO_YEAR_MAX):
            return None
        return date(year, month, day)
    except (ValueError, IndexError):
        return None


def _normalize(s: str | None) -> str:
    return re.sub(r"\s+", " ", s or "").strip().upper()


def _evidence_in_text(evidence: str | None, raw_text: str) -> bool:
    if not evidence:
        return False
    return _normalize(evidence) in _normalize(raw_text)


def _hour_from_evidence(evidence: str | None) -> int | None:
    m = re.search(r"\d{1,2}(?::\d{2})?\s*[AP]M", evidence or "", re.IGNORECASE)
    if not m:
        return None
    return _parse_embargo_time(m.group(0))


def _date_evidence_matches(evidence: str | None, claimed: date) -> bool:
    """evidence에서 월/일만 뽑아 claimed(모델이 준 날짜)와 비교한다 — 연도는
    비교하지 않는다. 실제 실패 케이스 대부분이 "034/26/2025"·"205"처럼
    연도/자릿수가 깨진 오탈자라, evidence를 _parse_embargo_date로 그대로
    재파싱하면(연도 유효성 검사가 있어서) 정규식이 이미 실패한 바로 그
    이유로 evidence도 항상 실패한다 — 즉 "모델이 오탈자를 문맥으로 복구한
    것"과 "모델이 값을 지어낸 것"을 구분 못 하게 된다. 월/일은 문맥으로
    복구할 필요가 거의 없는(오탈자가 나는 부분은 거의 항상 연도) 값이라
    이것만 엄격히 재확인하고, 연도가 말이 되는지는 claimed 쪽에서 별도로
    범위 체크한다(_year_in_range)."""
    m = re.search(r"(\d{1,2})/(\d{1,2})", evidence or "")
    if not m:
        return False
    month, day = int(m.group(1)), int(m.group(2))
    return month == claimed.month and day == claimed.day


def _year_in_range(d: date) -> bool:
    return _EMBARGO_YEAR_MIN <= d.year <= _EMBARGO_YEAR_MAX


# ─────────────────────────────────────────────────────────────
# 결정론적 Validation — LLM 출력이 그대로 신뢰할 수 있는지 코드로 재확인한다.
# LLM에게 다시 판단시키지 않는다(evidence를 원문에서 재검색해서 재계산한
# 값과 비교하는 방식 — 재호출 없음).
# ─────────────────────────────────────────────────────────────


def validate_work_hours_llm_output(raw_text: str, llm_raw: dict) -> tuple[dict | None, str | None]:
    """work_hours LLM 출력 검증. 성공하면 (extract_work_hours()와 동일한 키를
    가진 dict, None), 실패하면 (None, 실패이유)."""
    if llm_raw.get("status") != "parsed":
        return None, "model_uncertain"

    try:
        start_hour = int(llm_raw["start_hour"])
        end_hour = int(llm_raw["end_hour"])
        day_code = llm_raw["day_code"]
    except (KeyError, TypeError, ValueError):
        return None, "missing_or_invalid_field"

    if not (0 <= start_hour <= 23 and 0 <= end_hour <= 23):
        return None, "hour_out_of_range"
    if day_code not in DAY_CODES:
        return None, "invalid_day_code"

    start_evidence = llm_raw.get("start_hour_evidence")
    end_evidence = llm_raw.get("end_hour_evidence")
    day_evidence = llm_raw.get("day_evidence")

    if not _evidence_in_text(start_evidence, raw_text):
        return None, "evidence_not_found:start_hour"
    if _hour_from_evidence(start_evidence) != start_hour:
        return None, "evidence_value_mismatch:start_hour"

    if not _evidence_in_text(end_evidence, raw_text):
        return None, "evidence_not_found:end_hour"
    if _hour_from_evidence(end_evidence) != end_hour:
        return None, "evidence_value_mismatch:end_hour"

    # day_evidence는 요일 언급이 아예 없는 "매일" 케이스에선 원문에 대응하는
    # 문구가 없을 수 있어(DAY_MAP[""]="DAILY") 없으면 그냥 통과시킨다 — 다만
    # 있는데 원문에 없는 문구를 지어냈으면(hallucination) 실패시킨다.
    # DAY_MAP은 알려진 고정 문구만 다루는 좁은 사전이라(그래서애초에 LLM
    # 폴백이 필요한 것), day_code 자체의 재계산-일치는 강제하지 않는다 —
    # evidence가 실존하는지만 결정론적으로 확인한다.
    if day_evidence and not _evidence_in_text(day_evidence, raw_text):
        return None, "evidence_not_found:day"

    return {
        "work_start_hour": start_hour,
        "work_end_hour": end_hour,
        "work_days_code": day_code,
        "work_days_raw": day_evidence or "",
    }, None


def validate_embargo_llm_output(raw_text: str, llm_raw: dict) -> tuple[dict | None, str | None]:
    """embargo LLM 출력 검증. 성공하면 (extract_work_embargoes()와 동일한
    키를 가진 dict, None), 실패하면 (None, 실패이유)."""
    if llm_raw.get("status") != "parsed":
        return None, "model_uncertain"

    try:
        start_hour = int(llm_raw["start_hour"])
        end_hour = int(llm_raw["end_hour"])
    except (KeyError, TypeError, ValueError):
        return None, "missing_or_invalid_field"

    try:
        end_date_val = date.fromisoformat(llm_raw["end_date"])
        start_date_val = date.fromisoformat(llm_raw["start_date"])
    except (KeyError, ValueError, TypeError):
        return None, "invalid_date_format"

    if not (0 <= start_hour <= 23 and 0 <= end_hour <= 23):
        return None, "hour_out_of_range"
    if not (_year_in_range(start_date_val) and _year_in_range(end_date_val)):
        return None, "date_year_out_of_range"
    # 날짜에는 start<=end를 적용한다(시각 필드에는 적용 안 함 — "10PM-6AM
    # NIGHTLY"처럼 자정을 넘기는 정상 케이스가 실제로 있다, 모듈 docstring 참고).
    if start_date_val > end_date_val:
        return None, "start_after_end_date"

    for field, evidence in [
        ("start_date", llm_raw.get("start_date_evidence")),
        ("end_date", llm_raw.get("end_date_evidence")),
        ("start_hour", llm_raw.get("start_hour_evidence")),
        ("end_hour", llm_raw.get("end_hour_evidence")),
    ]:
        if not _evidence_in_text(evidence, raw_text):
            return None, f"evidence_not_found:{field}"

    # 날짜는 월/일만 재확인한다(연도는 오탈자 복구가 LLM의 핵심 가치라 비교
    # 안 함 — _date_evidence_matches docstring 참고). 시각은 evidence에서
    # 그대로 재계산한 값과 정확히 일치해야 한다.
    if not _date_evidence_matches(llm_raw.get("start_date_evidence"), start_date_val):
        return None, "evidence_value_mismatch:start_date"
    if not _date_evidence_matches(llm_raw.get("end_date_evidence"), end_date_val):
        return None, "evidence_value_mismatch:end_date"
    if _hour_from_evidence(llm_raw.get("start_hour_evidence")) != start_hour:
        return None, "evidence_value_mismatch:start_hour"
    if _hour_from_evidence(llm_raw.get("end_hour_evidence")) != end_hour:
        return None, "evidence_value_mismatch:end_hour"

    return {
        "embargo_start_date": start_date_val,
        "embargo_end_date": end_date_val,
        "embargo_start_hour": start_hour,
        "embargo_end_hour": end_hour,
        "embargo_reason": str(llm_raw.get("reason") or "")[:200],
    }, None


VALIDATORS = {
    "work_hours": validate_work_hours_llm_output,
    "embargo": validate_embargo_llm_output,
}

# ─────────────────────────────────────────────────────────────
# LLM 캐시 — 문구 단위로 결과(성공/실패 모두)를 영구 저장해 같은 문구를
# 반복 호출하지 않는다. 카테고리별로 파일을 분리한다(embargo는 기존 경로를
# 그대로 유지 — 이미 쌓인 프로덕션 데이터를 건드리는 마이그레이션을 피하기
# 위해서다).
# ─────────────────────────────────────────────────────────────

_CACHE_PATH_BY_CATEGORY = {
    "embargo": SILVER1_DIR / "embargo_llm_cache" / "data.parquet",
    "work_hours": SILVER1_DIR / "work_hours_llm_cache" / "data.parquet",
}

_CACHE_EXTRA_COLUMNS_BY_CATEGORY = {
    "work_hours": ["work_start_hour", "work_end_hour", "work_days_code", "work_days_raw"],
    "embargo": ["embargo_start_date", "embargo_end_date", "embargo_start_hour", "embargo_end_hour", "embargo_reason"],
}

_CACHE_COMMON_COLUMNS = [
    "stipulationfulltext", "parseable", "llm_status", "llm_output_raw",
    "validation_failure_reason", "resolved_date", "sampled_for_review",
]


def _cache_columns(category: str) -> list[str]:
    return ["stipulationfulltext", "parseable"] + _CACHE_EXTRA_COLUMNS_BY_CATEGORY[category] + [
        "llm_status", "llm_output_raw", "validation_failure_reason", "resolved_date", "sampled_for_review",
    ]


def load_llm_cache(category: str) -> pd.DataFrame:
    path = _CACHE_PATH_BY_CATEGORY[category]
    if not path.exists():
        return pd.DataFrame(columns=_cache_columns(category))
    df = pd.read_parquet(str(path))
    # 컬럼이 빠져 있을 수 있는 두 가지 경우 모두 기본값으로 채운다:
    # (1) 이 기능 이전에 쌓인 기존 embargo_llm_cache(마이그레이션 스크립트 없음),
    # (2) 이번 실행에서 새로 쓴 배치가 전부 검증 실패라서(성공한 행이 하나도
    #     없으면) pandas가 DataFrame 만들 때 카테고리 전용 값 컬럼(work_start_hour
    #     등) 자체를 안 만드는 경우 — run_llm_fallback_batch가 실패만 있는
    #     배치를 저장했을 때 실제로 겪은 문제.
    defaults = {"llm_status": None, "llm_output_raw": None, "validation_failure_reason": None, "sampled_for_review": False}
    for col in _CACHE_EXTRA_COLUMNS_BY_CATEGORY[category]:
        defaults[col] = None
    for col, default in defaults.items():
        if col not in df.columns:
            df[col] = default
    return df


def save_llm_cache(category: str, df: pd.DataFrame) -> None:
    # stipulationfulltext는 캐시의 유니크 키다 — 중복이 생기면(이론상 없어야
    # 하지만) 이후 merge(on="stipulationfulltext")가 fan-out되어 Silver
    # 출력/quarantine에 중복 행을 만든다. 마지막 값을 우선해 안전망으로
    # 제거한다.
    df = df.drop_duplicates(subset=["stipulationfulltext"], keep="last")
    path = _CACHE_PATH_BY_CATEGORY[category]
    save_parquet(df, path.parent, filename=path.name)


# ─────────────────────────────────────────────────────────────
# LLM 폴백 배치 실행기 — 기존 build_embargoes()의 동시성+재시도+중간저장
# 로직을 그대로 이설(카테고리 무관하게 일반화).
# ─────────────────────────────────────────────────────────────

LLM_MAX_WORKERS = 4
LLM_CACHE_FLUSH_EVERY = 10
LLM_SAMPLE_REVIEW_COUNT = 5


def run_llm_fallback_batch(
    category: str,
    new_texts: list[str],
    run_date: str,
    llm_call_fn: Callable[[str], dict],
) -> tuple[int, Exception | None]:
    """정규식 실패 신규 문구를 LLM으로 처리해서 캐시에 누적 저장한다(성공/
    실패 모두 캐시 — 문구 문제로 실패한 것과 호출 자체가 안 된 것은 구분해서
    후자만 캐시하지 않는다). 반환값: (호출 자체가 안 된 건수, 마지막
    GeminiUnavailable 예외 또는 None) — 호출부가 이 비율로 "이번 실행은
    LLM이 사실상 안 돌았다"를 판단한다.

    캐시는 EMBARGO_LLM_CACHE_FLUSH_EVERY마다, 그리고 끝나고 한 번 더
    저장한다 — 신규 문구가 몇백 개 규모라 중간에 죽어도 유실을 최소화하기
    위해서다.
    """
    if not new_texts:
        return 0, None

    validate_fn = VALIDATORS[category]
    cache = load_llm_cache(category)
    new_rows: list[dict] = []
    unavailable_count = 0
    last_unavailable_error: Exception | None = None

    if not GEMINI_API_KEY:
        logger.warning("[%s] GEMINI_API_KEY가 설정되지 않아 신규 문구 %d개를 이번 실행에서 건너뜁니다.", category, len(new_texts))
        return len(new_texts), GeminiUnavailable("GEMINI_API_KEY가 설정되지 않음")

    def _process(text: str):
        llm_raw = llm_call_fn(text)  # GeminiUnavailable이면 그대로 전파됨
        parsed, failure_reason = validate_fn(text, llm_raw)
        return text, llm_raw, parsed, failure_reason

    with ThreadPoolExecutor(max_workers=LLM_MAX_WORKERS) as executor:
        future_to_text = {executor.submit(_process, text): text for text in new_texts}
        for future in as_completed(future_to_text):
            try:
                text, llm_raw, parsed, failure_reason = future.result()
            except GeminiUnavailable as e:
                unavailable_count += 1
                last_unavailable_error = e
                continue

            row = {
                "stipulationfulltext": text,
                "parseable": parsed is not None,
                "llm_status": llm_raw.get("status"),
                "llm_output_raw": json.dumps(llm_raw, ensure_ascii=False, default=str),
                "validation_failure_reason": failure_reason,
                "resolved_date": run_date,
                "sampled_for_review": False,
            }
            if parsed is not None:
                row.update(parsed)
            new_rows.append(row)

            if len(new_rows) % LLM_CACHE_FLUSH_EVERY == 0:
                cache = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True) if not cache.empty else pd.DataFrame(new_rows)
                save_llm_cache(category, cache)
                new_rows = []
                logger.info("[%s] LLM 캐시 중간 저장: 누적 %d행", category, len(cache))

    if new_rows:
        cache = pd.concat([cache, pd.DataFrame(new_rows)], ignore_index=True) if not cache.empty else pd.DataFrame(new_rows)
        save_llm_cache(category, cache)

    if unavailable_count:
        logger.warning(
            "[%s] Gemini 호출 불가/타임아웃 %d건(신규 문구 %d개 중) — 그 문구들은 캐시 안 하고 다음 실행에 재시도",
            category, unavailable_count, len(new_texts),
        )

    # 이번 실행에서 새로 성공 처리된 것 중 최대 LLM_SAMPLE_REVIEW_COUNT개를
    # 사람 검수용으로 표시한다(요구사항 7 — 최소 구현, 별도 리뷰 UI 없이
    # 캐시 parquet을 pandas로 읽어서 sampled_for_review==True만 걸러보는
    # 방식).
    cache = load_llm_cache(category)
    this_run_success = cache[(cache["resolved_date"] == run_date) & (cache["parseable"])]
    if not this_run_success.empty:
        sample_idx = this_run_success.sample(n=min(LLM_SAMPLE_REVIEW_COUNT, len(this_run_success))).index
        cache.loc[sample_idx, "sampled_for_review"] = True
        save_llm_cache(category, cache)
        logger.info("[%s] 사람 검수용 샘플 %d건 표시(resolved_date=%s)", category, len(sample_idx), run_date)

    return unavailable_count, last_unavailable_error


# ─────────────────────────────────────────────────────────────
# Quarantine — rule+LLM 둘 다 실패한 레코드를 사람이 나중에 조회해서
# 분석/패턴 발견 후 rule로 승격시킬 수 있게 보존한다(버리지 않음).
# ─────────────────────────────────────────────────────────────

# 카테고리별로 파일을 분리한다(LLM 캐시와 동일한 이유) — build_work_hours_rules와
# build_embargoes는 서로 의존 관계가 없어 Airflow가 동시에 실행할 수 있는데,
# 두 태스크가 quarantine을 "전체 읽기 -> 수정 -> 전체 쓰기"로 갱신하다 보니
# 하나의 공유 파일이었다면 서로의 신규 항목을 덮어써 유실시킬 수 있었다
# (리뷰에서 발견된 실제 경합 조건). 카테고리별 파일 분리로 각 태스크가 자기
# 파일만 건드리게 해서 이 경합을 없앤다.
QUARANTINE_DIR = SILVER1_DIR / "stipulation_parse_quarantine"
QUARANTINE_COLUMNS = [
    "category", "permitnumber", "stipulationfulltext", "rule_failure_reason",
    "llm_status", "llm_output_raw", "validation_failure_reason",
    "batch_date", "created_at", "resolved",
]


def _quarantine_path(category: str) -> Path:
    return QUARANTINE_DIR / f"category={category}" / "data.parquet"


def _load_quarantine(category: str) -> pd.DataFrame:
    path = _quarantine_path(category)
    if not path.exists():
        return pd.DataFrame(columns=QUARANTINE_COLUMNS)
    return pd.read_parquet(str(path))


def write_quarantine(category: str, candidates: pd.DataFrame, run_date: str) -> None:
    """candidates: 지금 시점에 rule+LLM 둘 다 실패한 전체 후보(permitnumber,
    stipulationfulltext, rule_failure_reason, llm_status, llm_output_raw,
    validation_failure_reason 컬럼 포함) — 매 실행마다 전체 백로그를 다시
    넘겨도 안전하다: 이미 quarantine에 있는 키(category, permitnumber,
    stipulationfulltext)는 절대 덮어쓰지 않는다(사람이 resolved를 True로
    바꿔놨을 수 있어서). 신규 키만 추가한다."""
    if candidates.empty:
        return

    # candidates 자체에 (permitnumber, stipulationfulltext) 중복이 있으면(상류
    # merge 결과가 우연히 fan-out됐을 경우) is_new 체크가 existing만 보고
    # candidates끼리는 비교 안 해서 같은 배치 안에서 중복 삽입될 수 있다 —
    # 먼저 자체 중복부터 제거한다.
    candidates = candidates.drop_duplicates(subset=["permitnumber", "stipulationfulltext"])

    existing = _load_quarantine(category)
    if not existing.empty:
        existing_keys = set(zip(existing["permitnumber"], existing["stipulationfulltext"]))
        is_new = candidates.apply(
            lambda r: (r["permitnumber"], r["stipulationfulltext"]) not in existing_keys, axis=1
        )
        new_rows = candidates[is_new].copy()
    else:
        new_rows = candidates.copy()

    if new_rows.empty:
        return

    new_rows["category"] = category
    new_rows["batch_date"] = run_date
    new_rows["created_at"] = datetime.now(timezone.utc)
    new_rows["resolved"] = False

    combined = (
        pd.concat([existing, new_rows[QUARANTINE_COLUMNS]], ignore_index=True)
        if not existing.empty else new_rows[QUARANTINE_COLUMNS]
    )
    path = _quarantine_path(category)
    save_parquet(combined, path.parent, filename=path.name)
    logger.info("[%s] quarantine 신규 %d건 추가(누적 %d건)", category, len(new_rows), len(combined))


def quarantined_texts(category: str) -> set[str]:
    """이미 quarantine에 들어간(resolved 여부 무관) 문구 집합. 한 번
    quarantine에 들어간 문구는 LLM이 다시 자동으로 건드리지 않는다 — 과거
    통째로 쌓여있던 백로그를 "사람이 직접 처리할 몫"으로 못박아 두고, 이후
    로직(build_embargoes/build_work_hours_rules)이 그 문구들을 신규 문구
    선정에서 제외하는 데 쓴다."""
    df = _load_quarantine(category)
    if df.empty:
        return set()
    return set(df["stipulationfulltext"])


def summarize_quarantine(category: str | None = None, top_n: int = 20) -> pd.DataFrame:
    """resolved==False인 quarantine 항목을 (category, stipulationfulltext)
    기준으로 그룹핑해서 영향받는 행수 기준 내림차순 정렬한다 — 사람이 반복
    패턴을 찾아 rule로 승격시킬지 판단하는 용도. 셸/노트북에서 직접 호출,
    DAG에는 연결하지 않는다(요구사항 5 Feedback Loop의 전체 구현 — 자동
    rule 승격은 하지 않는다)."""
    categories = [category] if category is not None else list(VALIDATORS.keys())
    df = pd.concat([_load_quarantine(c) for c in categories], ignore_index=True)
    if df.empty:
        return df
    df = df[~df["resolved"]]
    if df.empty:
        return df
    return (
        df.groupby(["category", "stipulationfulltext"])
        .agg(
            affected_permits=("permitnumber", "nunique"),
            rule_failure_reason=("rule_failure_reason", "first"),
            validation_failure_reason=("validation_failure_reason", "first"),
            first_seen=("batch_date", "min"),
        )
        .reset_index()
        .sort_values("affected_permits", ascending=False)
        .head(top_n)
    )


# ─────────────────────────────────────────────────────────────
# 품질 리포트 + Drift 감지 — 건별 알림이 아니라 배치 단위 집계로 로그를
# 남기고, 직전 실행 대비 성공률이 급락하면 예외를 던져 기존
# on_failure_callback(Slack 알림)이 그대로 전달하게 한다(새 알림 코드 없음).
# ─────────────────────────────────────────────────────────────

# quarantine과 동일한 이유(build_work_hours_rules/build_embargoes가 서로
# 의존관계 없이 동시에 실행될 수 있음)로 카테고리별 파일 분리 — 공유 파일
# 하나였다면 두 태스크가 서로의 신규 이력 행을 덮어쓸 수 있었다.
QUALITY_HISTORY_DIR = SILVER1_DIR / "stipulation_parse_quality_history"
QUALITY_HISTORY_COLUMNS = [
    "run_date", "category", "total_unique_texts", "rule_parsed_count", "rule_rate",
    "llm_parsed_count", "llm_rate", "quarantine_count", "quarantine_rate", "created_at",
]

# 하락폭 임계값(%p) — 직전 실행 대비 rule+LLM 합산 성공률이 이 이상 떨어지면
# 알림. 표본이 너무 작은 날(신규 문구 자체가 거의 없는 날)은 비율이 요동쳐도
# 의미가 없어서 DRIFT_MIN_SAMPLE_SIZE 미만이면 아예 건너뛴다.
DRIFT_ALERT_THRESHOLD_PCT_POINTS = 5.0
DRIFT_MIN_SAMPLE_SIZE = 30


def _quality_history_path(category: str) -> Path:
    return QUALITY_HISTORY_DIR / f"category={category}" / "data.parquet"


def _load_quality_history(category: str) -> pd.DataFrame:
    path = _quality_history_path(category)
    if not path.exists():
        return pd.DataFrame(columns=QUALITY_HISTORY_COLUMNS)
    return pd.read_parquet(str(path))


def compute_and_log_quality_report(
    category: str,
    run_date: str,
    total_unique_texts: int,
    rule_parsed_count: int,
    llm_parsed_count: int,
) -> str | None:
    """이번 실행의 rule/LLM/quarantine 비율(고유 문구 기준)을 로그로 남기고
    이력 테이블에 추가한다. 직전 실행 대비 rule+LLM 합산 성공률이
    DRIFT_ALERT_THRESHOLD_PCT_POINTS 넘게 떨어졌으면 알림 메시지를
    반환한다(없으면 None) — 호출부가 이 메시지로 예외를 던지는 방식으로
    알린다."""
    quarantine_count = max(total_unique_texts - rule_parsed_count - llm_parsed_count, 0)
    rule_rate = rule_parsed_count / total_unique_texts * 100 if total_unique_texts else 0.0
    llm_rate = llm_parsed_count / total_unique_texts * 100 if total_unique_texts else 0.0
    quarantine_rate = quarantine_count / total_unique_texts * 100 if total_unique_texts else 0.0
    success_rate = rule_rate + llm_rate

    logger.info(
        "[%s] 파싱 품질 리포트: 총 고유문구=%d, Rule=%d(%.1f%%), LLM=%d(%.1f%%), Quarantine=%d(%.1f%%)",
        category, total_unique_texts, rule_parsed_count, rule_rate, llm_parsed_count, llm_rate,
        quarantine_count, quarantine_rate,
    )

    history = _load_quality_history(category)
    prior = history.sort_values("run_date")

    alert_message = None
    if not prior.empty and total_unique_texts >= DRIFT_MIN_SAMPLE_SIZE:
        prior_row = prior.iloc[-1]
        prior_success_rate = prior_row["rule_rate"] + prior_row["llm_rate"]
        drop = prior_success_rate - success_rate
        if drop > DRIFT_ALERT_THRESHOLD_PCT_POINTS:
            alert_message = (
                f"[{category}] 파싱 품질 급락 감지: 직전 실행({prior_row['run_date']}) 성공률 "
                f"{prior_success_rate:.1f}% -> 이번 실행({run_date}) {success_rate:.1f}% "
                f"({drop:.1f}%p 하락, 임계값 {DRIFT_ALERT_THRESHOLD_PCT_POINTS}%p)"
            )
            logger.warning(alert_message)

    new_row = pd.DataFrame([{
        "run_date": run_date, "category": category, "total_unique_texts": total_unique_texts,
        "rule_parsed_count": rule_parsed_count, "rule_rate": rule_rate,
        "llm_parsed_count": llm_parsed_count, "llm_rate": llm_rate,
        "quarantine_count": quarantine_count, "quarantine_rate": quarantine_rate,
        "created_at": datetime.now(timezone.utc),
    }])
    # 같은 (run_date, category)로 재시도하면 이전 행을 지우고 새로 남긴다 —
    # 안 그러면 재시도 때마다 이력에 같은 날짜가 중복으로 쌓인다.
    history = history[history["run_date"] != run_date]
    combined = pd.concat([history, new_row], ignore_index=True) if not history.empty else new_row
    path = _quality_history_path(category)
    save_parquet(combined, path.parent, filename=path.name)

    return alert_message
