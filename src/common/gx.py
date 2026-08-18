"""GX(Great Expectations) 공통 러너.

Spark DataFrame과 Expectation 목록을 받아 검증을 실행하고, 결과를 dict
리스트로 반환하는 것까지만 책임진다. 검증 실패 시 어떻게 반응할지
(파일 제외/로그/알림)는 호출하는 도메인 코드가 결정한다.
"""

import great_expectations as gx
from pyspark.sql import DataFrame


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

    results = []
    for expectation in expectations:
        validation_result = batch.validate(expectation)
        results.append({
            "success": validation_result.success,
            "expectation_type": validation_result.expectation_config.type,
            "kwargs": dict(validation_result.expectation_config.kwargs),
            "result": dict(validation_result.result),
        })
    return results
