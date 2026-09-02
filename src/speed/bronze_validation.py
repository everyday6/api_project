"""speed Bronze 데이터 품질 검증 (Great Expectations 기반).

30분마다 수집되는 배치 하나를 pandas로 검증한다 - 데이터가 작아서
(수백~수천 행) Spark 세션 없이 pandas로 충분하다. TLC(taxi_type × 월,
여러 파일)와 달리 speed는 사이클당 배치가 하나뿐이라 "파일 제외" 대신
"이번 사이클 스킵"으로 critical 실패에 대응한다.

collect_speed_data()가 Bronze에 저장하기 직전, 메모리에 있는 DataFrame을
바로 검증한다(validate_bronze_df/_validate_and_decide_df) - 저장 후 다시
읽어서 검증하지 않는다. validate_bronze_file은 이미 저장된 파일을 나중에
따로 검증하고 싶을 때 쓰는 경로 기반 유틸리티다.
"""

from __future__ import annotations

import pandas as pd

from src.common.alerts import notify_slack_message
from src.common.gx import validate_pandas_dataframe
from src.common.logger import get_logger
from src.common.suspect import flag_suspect_pandas, log_quality_gate, suspect_ratio
from src.speed.expectations import (
    _DATA_AS_OF_MIN,
    _REQUIRED_COLUMNS,
    _SPEED_MAX_MPH,
    _SPEED_MIN_MPH,
    MAX_SUSPECT_RATIO,
    _data_as_of_max,
    critical_expectations,
    log_only_expectations,
)

logger = get_logger(__name__, log_to_file=True, log_file_stem="speed_bronze_validation")


class CriticalValidationError(Exception):
    """speed Bronze의 critical 검증(필수 컬럼 존재)이 실패했을 때 발생한다."""


def _cast_for_validation(df: pd.DataFrame) -> pd.DataFrame:
    """speed/data_as_of는 Bronze에 문자열로 저장되어 있다(Socrata가 모든
    필드를 문자열로 주기 때문). ExpectColumnValuesToBeBetween을 문자열
    컬럼에 그대로 돌리면 GX가 타입 불일치 예외를 내부적으로 삼켜서
    success=False, result={}만 남긴다(실제로 재현 확인됨). 그래서 검증
    직전에 복사본에서만 캐스팅한다 - 원본 df는 그대로 둔다.

    errors="coerce"로 파싱 안 되는 값은 예외 대신 null로 만든다 - 그러면
    not-null 체크에서 자연스럽게 잡힌다.
    """

    validation_df = df.copy()
    validation_df["speed"] = pd.to_numeric(validation_df["speed"], errors="coerce")
    validation_df["data_as_of"] = pd.to_datetime(validation_df["data_as_of"], errors="coerce")
    return validation_df


def validate_bronze_df(df: pd.DataFrame) -> list[dict]:
    """speed Bronze DataFrame 하나를 검증한다.

    critical 검증(다운스트림이 의존하는 컬럼 존재 여부)이 실패하면
    CriticalValidationError를 던진다. 통과하면 log-only 검증 중 실패한
    항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).
    """

    critical_results = validate_pandas_dataframe(
        df,
        critical_expectations(),
        datasource_name="speed_bronze_critical",
        asset_name="speed_bronze_critical",
    )
    failed_critical = [r for r in critical_results if not r["success"]]
    if failed_critical:
        missing_columns = [r["kwargs"].get("column") for r in failed_critical]
        raise CriticalValidationError(f"필수 컬럼 없음: {missing_columns}")

    validation_df = _cast_for_validation(df)
    log_results = validate_pandas_dataframe(
        validation_df,
        log_only_expectations(),
        datasource_name="speed_bronze_logonly",
        asset_name="speed_bronze_logonly",
    )
    return [r for r in log_results if not r["success"]]


def validate_bronze_file(bronze_path: str) -> list[dict]:
    """speed Bronze 파일 하나를 검증한다 - 저장된 parquet을 읽어서
    validate_bronze_df에 위임한다."""

    return validate_bronze_df(pd.read_parquet(bronze_path))


def mark_suspect_rows(df: pd.DataFrame) -> pd.DataFrame:
    """log_only_expectations()가 검사하는 조건 중 행 단위로 판정 가능한
    것만 재적용해 `is_suspect` 컬럼을 추가한 복사본을 반환한다.

    validate_bronze_df()는 배치 전체 단위로 "이 검증이 실패했는가"만
    판단하고(Slack 알림용), 어떤 행이 원인인지는 남기지 않는다. 이 함수는
    그 행을 원본 df에 표시해, 저장된 데이터를 읽는 쪽이 신뢰도 낮은 행을
    구분할 수 있게 한다 — 전면적인 quarantine(격리) 대신 쓰는 최소
    버전이다(RELIABILITY_PRINCIPLES.md 원칙 0-1 — 신뢰도가 낮은 값도
    "낮다는 사실이 보이면" 허용된다).

    ExpectTableRowCountToBeBetween·ExpectColumnUniqueValueCountToBeBetween
    처럼 배치 전체를 보는 검증은 특정 행을 지목할 수 없어 대상이 아니다.
    범위·필수 컬럼·허용 날짜 상한을 전부 expectations.py의 상수/함수에서
    그대로 가져다 써서(_REQUIRED_COLUMNS/_SPEED_*/_DATA_AS_OF_MIN/
    _data_as_of_max), log_only_expectations()와 기준이 어긋나는 일을 막는다.
    복사본 생성·bool 확정·컬럼명은 src.common.suspect로 위임한다.
    """
    validation_df = _cast_for_validation(df)

    suspect = pd.Series(False, index=df.index)
    for column in _REQUIRED_COLUMNS:
        suspect |= validation_df[column].isna()
    suspect |= ~validation_df["speed"].between(_SPEED_MIN_MPH, _SPEED_MAX_MPH)
    suspect |= ~validation_df["data_as_of"].between(_DATA_AS_OF_MIN, _data_as_of_max())

    return flag_suspect_pandas(df, suspect)


def suspect_ratio_ok(df: pd.DataFrame, context: str) -> bool:
    """mark_suspect_rows()로 표시된 `is_suspect` 비율이 평소 수준이면 True.
    임계치(MAX_SUSPECT_RATIO)를 넘으면 log-only 이상치를 critical로 승격해
    False + Slack을 낸다 - 개별 센서의 산발적 이상은 넘기되, 값이 뭉텅이로
    이상하면(스키마 드리프트, 피드 포맷 변경 등) 오염된 배치를 저장하지
    않는다(RELIABILITY_PRINCIPLES.md 열린 질문 - "비율 급증 시 critical 승격").

    collect_speed_data()가 mark_suspect_rows() 직후 호출한다 - 그 시점엔
    critical 검증(필수 컬럼 존재)이 이미 통과해 `is_suspect` 컬럼이 있는 게
    보장된다.
    """
    ratio = suspect_ratio(df)
    log_quality_gate(
        logger,
        domain="speed",
        metric="suspect_ratio",
        value=ratio,
        threshold=MAX_SUSPECT_RATIO,
        passed=ratio <= MAX_SUSPECT_RATIO,
        context=context,
    )
    if ratio > MAX_SUSPECT_RATIO:
        logger.error(
            f"speed Bronze 의심 행 비율 초과: {ratio:.1%} > {MAX_SUSPECT_RATIO:.1%}"
        )
        notify_slack_message(
            f":red_circle: speed Bronze 의심 행 비율 {ratio:.1%} > {MAX_SUSPECT_RATIO:.1%} "
            f"- log-only 이상치가 평소보다 급증, 저장하지 않고 이번 사이클 스킵\n"
            f"*배치*: `{context}`"
        )
        return False

    logger.info(
        f"speed Bronze 의심 행 비율 {ratio:.1%} (임계 {MAX_SUSPECT_RATIO:.1%} 이내)"
    )
    return True


def _validate_and_decide_df(df: pd.DataFrame, context: str) -> bool:
    """실제 결정 로직 - critical 실패시 False+Slack, log_only 실패시
    True+Slack+로그, 전부 통과시 True.

    collect_speed_data()가 Bronze에 저장하기 직전, 메모리에 있는 df를
    그대로 검증한다(저장 후 다시 읽는 대신 - 2026-08-26 순서 변경: API
    응답이 이미 메모리에 다 있는데 굳이 저장했다 다시 읽을 이유가 없고,
    이래야 critical 실패시 애초에 Bronze에 저장 자체를 안 할 수 있다).
    context는 아직 저장된 파일이 없으므로 Slack 메시지에 어느 배치인지
    표시할 식별자(예: "batch_end=..., rows=N")."""

    try:
        failed_log_only = validate_bronze_df(df)
    except CriticalValidationError as error:
        logger.error(f"speed Bronze critical 검증 실패: {error}")
        notify_slack_message(
            f":red_circle: speed Bronze critical 검증 실패 - 저장하지 않고 이번 사이클 스킵\n"
            f"*배치*: `{context}`\n{error}"
        )
        return False

    if failed_log_only:
        for check in failed_log_only:
            logger.warning(
                f"speed Bronze 검증 실패(로그만): {check['expectation_type']} "
                f"{check['kwargs']} -> {check['result']} "
                f"-> exception_info: {check['exception_info']}"
            )
        failed_columns = [check["kwargs"].get("column") for check in failed_log_only]
        notify_slack_message(
            f":warning: speed Bronze log_only 검증 실패 {len(failed_log_only)}건 "
            f"(저장은 계속됨)\n"
            f"*배치*: `{context}`\n"
            f"*실패 컬럼*: {failed_columns}"
        )

    return True
