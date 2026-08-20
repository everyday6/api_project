"""GX(Great Expectations) 공통 러너.

Spark/pandas DataFrame과 Expectation 목록을 받아 검증을 실행하고, 결과를 dict
리스트로 반환하는 것까지만 책임진다. 검증 실패 시 어떻게 반응할지
(파일 제외/로그/알림)는 호출하는 도메인 코드가 결정한다.

반환하는 각 dict는 success/expectation_type/kwargs/result에 더해
exception_info도 포함한다 — GX가 메트릭 계산 중 내부적으로 예외를 잡은
경우(예: 컬럼 타입 불일치) success=False에 result={}만 남고 실제 원인은
exception_info에만 담기므로, 이걸 버리면 구조적 실패의 진짜 원인을 알 수
없다.
"""

import great_expectations as gx
import pandas as pd
from pyspark.sql import DataFrame


def _run_batch_validation(batch, expectations: list) -> list[dict]:
    """이미 만들어진 GX batch에 대해 Expectation 목록을 순서대로 실행한다."""

    results = []
    for expectation in expectations:
        validation_result = batch.validate(expectation)
        results.append({
            "success": validation_result.success,
            "expectation_type": validation_result.expectation_config.type,
            "kwargs": dict(validation_result.expectation_config.kwargs),
            "result": dict(validation_result.result),
            "exception_info": dict(validation_result.exception_info or {}),
        })
    return results


def validate_spark_dataframe(
    df: DataFrame,
    expectations: list,
    datasource_name: str,
    asset_name: str,
) -> list[dict]:
    """Spark DataFrame을 Expectation 목록으로 검증한다.

    매 호출마다 새 ephemeral GX Context를 만들어 쓰고 버리므로,
    datasource_name/asset_name은 이 호출 안에서만 고유하면 된다.
    """

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(name=datasource_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{asset_name}_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    return _run_batch_validation(batch, expectations)


def validate_pandas_dataframe(
    df: pd.DataFrame,
    expectations: list,
    datasource_name: str,
    asset_name: str,
) -> list[dict]:
    """pandas DataFrame을 Expectation 목록으로 검증한다.

    validate_spark_dataframe()와 실행 엔진(add_pandas vs add_spark)만
    다르고 나머지 동작은 동일하다.
    """

    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(name=datasource_name)
    data_asset = data_source.add_dataframe_asset(name=asset_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(
        f"{asset_name}_batch"
    )
    batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

    return _run_batch_validation(batch, expectations)
