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
from src.speed.expectations import critical_expectations, log_only_expectations

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
