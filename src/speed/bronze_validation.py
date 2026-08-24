"""speed Bronze 데이터 품질 검증 (Great Expectations 기반).

30분마다 수집되는 Bronze 파일 하나를 pandas로 검증한다 - 파일이 작아서
(수백~수천 행) Spark 세션 없이 pandas로 충분하다. TLC(taxi_type × 월,
여러 파일)와 달리 speed는 사이클당 파일이 하나뿐이라 "파일 제외" 대신
"이번 사이클 스킵"으로 critical 실패에 대응한다(DAG 쪽 처리는
Task 3 참고).
"""

from __future__ import annotations

import pandas as pd

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


def validate_bronze_file(bronze_path: str) -> list[dict]:
    """speed Bronze 파일 하나를 검증한다.

    critical 검증(다운스트림이 의존하는 컬럼 존재 여부)이 실패하면
    CriticalValidationError를 던진다. 통과하면 log-only 검증 중 실패한
    항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).
    """

    df = pd.read_parquet(bronze_path)

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
