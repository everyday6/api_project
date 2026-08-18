"""TLC Bronze 데이터 품질 검증 (Great Expectations 기반).

같은 taxi_type 파일들을 Spark 세션 하나로 순회하며 검증하되(build_silver와
동일한 세션 재사용 패턴), 파일 하나의 검증 실패가 같은 청크의 다른 파일까지
막지 않도록 파일 단위로 개별 판정한다.
"""

from pathlib import Path

from airflow.decorators import task

from src.common.alerts import notify_slack_message
from src.common.gx import validate_spark_dataframe
from src.common.logger import get_logger
from src.common.spark import get_spark

from src.tlc.expectations import critical_expectations, log_only_expectations


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_bronze_validation")


class CriticalValidationError(Exception):
    """Bronze 파일의 critical 검증(필수 컬럼 존재)이 실패했을 때 발생한다."""


def validate_bronze_file(spark, bronze_path: str, taxi_type: str) -> list[dict]:
    """Bronze 파일 하나를 검증한다.

    critical 검증(dropoff_datetime/dropoff_location_id 원본 컬럼 존재)이
    실패하면 CriticalValidationError를 던진다. 통과하면 log-only 검증 중
    실패한 항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).
    """

    df = spark.read.parquet(str(bronze_path))
    asset_id = Path(bronze_path).stem

    critical_results = validate_spark_dataframe(
        df,
        critical_expectations(taxi_type),
        datasource_name=f"tlc_bronze_critical_{asset_id}",
        asset_name=f"tlc_bronze_critical_{asset_id}",
    )
    failed_critical = [r for r in critical_results if not r["success"]]
    if failed_critical:
        missing_columns = [r["kwargs"].get("column") for r in failed_critical]
        raise CriticalValidationError(
            f"필수 컬럼 없음: {missing_columns} (taxi_type={taxi_type})"
        )

    log_results = validate_spark_dataframe(
        df,
        log_only_expectations(taxi_type),
        datasource_name=f"tlc_bronze_logonly_{asset_id}",
        asset_name=f"tlc_bronze_logonly_{asset_id}",
    )
    return [r for r in log_results if not r["success"]]


def _validate_chunk_files(spark, bronze_chunk: list[dict]) -> list[dict]:
    """청크(taxi_type 하나) 안 파일을 순회하며 개별 판정한다.

    한 파일의 예외가 루프 밖으로 전파되지 않게 파일마다 개별 try/except로
    감싸서, critical 실패 파일만 결과에서 제외되고 나머지는 계속 처리된다.
    """

    passed = []

    for bronze_result in bronze_chunk:
        filename = bronze_result["filename"]
        taxi_type = bronze_result["taxi_type"]
        bronze_path = bronze_result["bronze_path"]

        try:
            failed_checks = validate_bronze_file(spark, bronze_path, taxi_type)

            for check in failed_checks:
                logger.warning(
                    f"검증 실패(로그만) - {filename} : "
                    f"{check['expectation_type']} {check['kwargs']} → {check['result']}"
                )

            passed.append(bronze_result)

        except CriticalValidationError as error:
            logger.error(f"Critical 검증 실패 - {filename} : {error}")
            notify_slack_message(
                f":warning: TLC Bronze 검증 실패로 파일 제외\n"
                f"*파일*: `{filename}`\n*사유*: {error}"
            )

        except Exception as error:
            logger.error(f"Bronze 파일 검증 중 오류 - {filename} : {error}")
            notify_slack_message(
                f":warning: TLC Bronze 검증 중 오류로 파일 제외\n"
                f"*파일*: `{filename}`\n*사유*: {error}"
            )

    return passed


@task(pool="silver_pool")
def validate_bronze_quality(bronze_chunk: list[dict]) -> list[dict]:
    """청크(taxi_type 하나) 안 파일들을 검증하고, 통과한 파일만 반환한다.

    build_silver와 같은 이유로 taxi_type당 Spark 세션 하나를 재사용하고
    같은 silver_pool을 공유한다 — spark-worker가 1대(10코어)뿐인 유한
    자원이라, Bronze 검증과 Silver 변환이 각자 다른 풀로 동시에 실행되면
    풀 슬롯 상한(3개)과 무관하게 Spark 클러스터 코어가 초과 예약될 수 있다.
    """

    if not bronze_chunk:
        return []

    spark = get_spark()

    try:
        return _validate_chunk_files(spark, bronze_chunk)
    finally:
        spark.stop()
