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
from src.common.spark import to_spark_path

from src.tlc.emr import run_tlc_emr_operation
from src.tlc.expectations import critical_expectations, log_only_expectations


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_bronze_validation")

# 청크 실행 순서. 동시에 돌 수 있는 청크 수보다 taxi_type이 많으면 누군가는
# 대기하게 되므로, 제일 오래 걸리는 FHVHV를 맨 앞에 둬서 대기 없이 먼저
# 시작하게 한다.
TAXI_TYPE_PRIORITY = ["fhvhv", "yellow", "green", "fhv"]


@task
def chunk_bronze_files(bronze_files: list[dict]) -> list[list[dict]]:
    """taxi_type별로 묶는다 (yellow/green/fhv/fhvhv 청크 4개).

    validate_bronze_quality가 청크 하나당 Spark 세션 하나만 열게 하기 위한
    준비 단계. 같은 taxi_type끼리만 묶는 이유: 검증 대상 컬럼 구성이
    taxi_type마다 달라, 청크 안에 taxi_type이 섞이면 파일마다 다시
    분기해야 해서 복잡해진다.
    """

    grouped: dict[str, list[dict]] = {}
    for bronze_result in bronze_files:
        grouped.setdefault(bronze_result["taxi_type"], []).append(bronze_result)

    chunks = [
        grouped[taxi_type]
        for taxi_type in TAXI_TYPE_PRIORITY
        if taxi_type in grouped
    ]

    logger.info(f"Bronze 검증 청크 {len(chunks)}개 생성 (파일 {len(bronze_files)}개)")

    return chunks


class CriticalValidationError(Exception):
    """Bronze 파일의 critical 검증(필수 컬럼 존재)이 실패했을 때 발생한다."""


def validate_bronze_file(spark, bronze_path: str, taxi_type: str) -> list[dict]:
    """Bronze 파일 하나를 검증한다.

    critical 검증(taxi_type이 요구하는 원본 컬럼 전부의 존재 여부)이
    실패하면 CriticalValidationError를 던진다. 통과하면 log-only 검증 중
    실패한 항목들의 결과 dict 리스트를 반환한다(전부 통과면 빈 리스트).
    """

    df = spark.read.parquet(to_spark_path(bronze_path))
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


# 청크 하나에 대한 집계 Slack 메시지에 파일별 사유를 나열할 때, 목록이
# 지나치게 길어지지 않도록 여기까지만 나열하고 나머지는 "...외 N건"으로
# 줄인다. 초기 적재 기준 taxi_type 하나당 최대 파일 수(약 36개)를 넘게 잡아,
# 컬럼 하나가 통째로 사라지는 최악의 경우에도 전부 나열되게 한다.
MAX_EXCLUDED_FILES_IN_MESSAGE = 40


def _validate_chunk_files(spark, bronze_chunk: list[dict]) -> list[dict]:
    """청크(taxi_type 하나) 안 파일을 순회하며 개별 판정한다.

    데이터 스키마의 critical 실패만 해당 파일에서 제외한다. S3/Spark/GX 등
    예상하지 못한 실행 오류는 Airflow가 재시도하고 최종 알림을 보낼 수 있도록
    즉시 다시 발생시킨다.

    파일 하나가 제외될 때마다 Slack 메시지를 바로 보내지 않는다 — 초기
    적재처럼 청크 하나에 파일이 수십 개일 수 있는 상황에서, TLC가 컬럼
    형식을 바꾸면 같은 taxi_type의 모든 파일이 한꺼번에 critical 실패를
    낼 수 있다. Slack Incoming Webhook은 초당 약 1개로 제한되고
    _post_to_slack은 실패를 그대로 삼키므로, 파일마다 알림을 보내면 한도를
    넘는 순간부터 알림이 조용히 사라진다. 그래서 제외된 파일을 모아뒀다가
    청크가 끝난 뒤 하나로 합쳐서 보낸다.
    """

    passed = []
    excluded = []

    for bronze_result in bronze_chunk:
        filename = bronze_result["filename"]
        taxi_type = bronze_result["taxi_type"]
        bronze_path = bronze_result["bronze_path"]

        try:
            failed_checks = validate_bronze_file(spark, bronze_path, taxi_type)

            passed.append(bronze_result)

            for check in failed_checks:
                logger.warning(
                    f"검증 실패(로그만) - {filename} : "
                    f"{check['expectation_type']} {check['kwargs']} → {check['result']} "
                    f"→ exception_info: {check['exception_info']}"
                )

        except CriticalValidationError as error:
            logger.error(f"Critical 검증 실패 - {filename} : {error}")
            excluded.append({"filename": filename, "reason": str(error)})

        except Exception:
            logger.exception(
                "Bronze 파일 검증 시스템 오류 - %s; Airflow 재시도를 요청합니다",
                filename,
            )
            raise

    if excluded:
        notify_slack_message(_build_excluded_files_message(excluded))

    return passed


def _build_excluded_files_message(excluded: list[dict]) -> str:
    """제외된 파일 목록을 청크당 하나의 Slack 메시지로 합친다.

    파일마다 개별 알림을 보내지 않고 청크가 끝난 뒤 한 번만 보내기 위한
    메시지 포맷팅. 목록이 너무 길면 일부만 보여주고 나머지는 개수로
    요약한다.
    """

    lines = [
        ":warning: TLC Bronze 검증 실패로 파일 제외",
        f"*제외된 파일 수*: {len(excluded)}건",
    ]

    shown = excluded[:MAX_EXCLUDED_FILES_IN_MESSAGE]
    for item in shown:
        lines.append(f"- `{item['filename']}`: {item['reason']}")

    remaining = len(excluded) - len(shown)
    if remaining > 0:
        lines.append(f"...외 {remaining}건")

    return "\n".join(lines)


@task(pool="silver_pool")
def validate_bronze_quality(bronze_chunk: list[dict]) -> list[dict]:
    """EMR에서 Bronze 청크를 검증하고 통과한 파일만 반환한다."""

    if not bronze_chunk:
        return []

    result = run_tlc_emr_operation(
        "validate_bronze",
        {"bronze_chunk": bronze_chunk},
    )
    excluded = result.get("excluded", [])
    if excluded:
        notify_slack_message(_build_excluded_files_message(excluded))
    return result["passed"]
