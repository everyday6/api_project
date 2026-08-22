"""TLC type=3 Zone Gold2 기록과 Segment DynamoDB 서빙값 적재 태스크."""

from __future__ import annotations

import re
import shutil
from datetime import date
from pathlib import Path
from uuid import uuid4

import boto3
from airflow.decorators import task
from airflow.exceptions import AirflowSkipException
from pyspark import StorageLevel
from pyspark.sql.functions import (
    col,
    max as spark_max,
    min as spark_min,
    to_date,
)

from src.common.config import (
    AWS_REGION,
    DYNAMODB_NAV_TABLE,
    GOLD2_DIR,
    SILVER1_DIR,
    SILVER2_DIR,
    TAXI_TYPES,
    TLC_TYPE3_ROLLING_WEEKS,
)
from src.common.logger import get_logger
from src.common.downloader import get_recent_service_months
from src.common.spark import get_spark, to_spark_path
from src.tlc.gold2 import (
    TYPE_ID,
    TYPE3_META_SEGMENT_ID,
    TYPE3_META_SK,
    build_daily_zone_frame,
    build_weekday_rolling_frame,
    expand_zone_values_to_segments,
    select_latest_date_partitions,
    validate_daily_zone_month,
    validate_segment_values,
    write_type3_rolling_to_dynamodb,
)


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_type3")

TLC_SILVER1_ROOT = SILVER1_DIR / "tlc"
TYPE3_DAILY_ROOT = GOLD2_DIR / "tlc" / "type3_zone_daily"
MAP_ZONE_SEGMENT_PATH = SILVER2_DIR / "map_zone_segment.parquet"
TYPE3_STAGING_ROOT = TYPE3_DAILY_ROOT / "_staging"
TYPE3_MONTH_MARKER_ROOT = TYPE3_DAILY_ROOT / "_month_success"
RUN_ID_PATTERN = re.compile(r"^[0-9a-f]{32}$")


def _complete_silver_paths_for_month(
    month: str,
    silver_root=TLC_SILVER1_ROOT,
) -> tuple[list[str], list[str]]:
    """월별 네 taxi_type의 완료된 Silver1 경로와 누락 타입을 반환한다."""

    completed_paths = []
    missing_types = []
    for taxi_type in TAXI_TYPES:
        path = silver_root / f"{taxi_type}_tripdata_{month}"
        if (path / "_SUCCESS").exists():
            completed_paths.append(to_spark_path(path))
        else:
            missing_types.append(taxi_type)
    return completed_paths, missing_types


def _month_success_marker(month: str, marker_root=TYPE3_MONTH_MARKER_ROOT):
    return marker_root / f"month={month}" / "_SUCCESS"


def _staging_run_path(run_id: str, staging_root=TYPE3_STAGING_ROOT):
    if not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError(f"잘못된 Type 3 staging run_id입니다: {run_id}")
    return staging_root / f"run_id={run_id}"


def _find_pending_type3_months(
    service_months: list[str],
    silver_root=TLC_SILVER1_ROOT,
    marker_root=TYPE3_MONTH_MARKER_ROOT,
) -> list[str]:
    """Silver1 네 종류가 완성됐고 Zone Gold2 완료 마커가 없는 월을 찾는다."""

    pending = []
    for month in service_months:
        _, missing_types = _complete_silver_paths_for_month(month, silver_root)
        if not missing_types and not _month_success_marker(month, marker_root).exists():
            pending.append(month)
    return sorted(pending)


def _type3_metadata_is_current(item: dict, window_start: date, window_end: date) -> bool:
    """DynamoDB 메타데이터가 현재 S3 롤링 윈도우를 가리키는지 확인한다."""

    return (
        item.get("status") == "COMPLETED"
        and item.get("window_start") == window_start.isoformat()
        and item.get("window_end") == window_end.isoformat()
    )


def validate_type3_reference(map_zone_segment_path=MAP_ZONE_SEGMENT_PATH) -> str:
    """운영 Type 3에 필요한 Zone-Segment 매핑의 존재를 확인한다."""

    if not map_zone_segment_path.exists():
        raise FileNotFoundError(
            f"Type 3 필수 입력인 zone-segment 매핑이 없습니다: "
            f"{map_zone_segment_path}"
        )
    logger.info("Type 3 zone-segment 매핑 확인 완료: %s", map_zone_segment_path)
    return str(map_zone_segment_path)


@task(trigger_rule="none_failed")
def find_pending_type3_months(_silver_results=None) -> list[str]:
    """신규 Silver가 없어도 최근 Silver1/Zone Gold2 상태를 다시 맞춘다."""

    service_months = [value.strftime("%Y-%m") for value in get_recent_service_months()]
    pending = _find_pending_type3_months(service_months)
    logger.info("Type 3 처리 대기 월: %s", pending)
    return pending


@task(pool="silver_pool")
def build_type3_staged_records(months: list[str]) -> dict:
    """완료되지 않은 월의 Zone Gold2 결과를 실행별 임시 경로에 저장한다."""

    if not months:
        raise AirflowSkipException("처리할 Type 3 월이 없어 갱신을 건너뜁니다")

    run_id = uuid4().hex
    run_path = _staging_run_path(run_id)
    spark = get_spark()
    try:
        staged_months = []
        for month in months:
            silver_paths, missing_types = _complete_silver_paths_for_month(month)
            if missing_types:
                logger.info(
                    "Type 3 월 계산 보류: month=%s missing_taxi_types=%s",
                    month,
                    missing_types,
                )
                continue
            daily = build_daily_zone_frame(
                spark,
                silver_paths,
                service_month=month,
            )
            month_stage_path = run_path / f"month={month}"
            (
                daily.write
                .mode("overwrite")
                .partitionBy("date")
                .parquet(to_spark_path(month_stage_path))
            )
            staged_months.append({"month": month, "stage_path": str(month_stage_path)})
            logger.info("Type 3 staging 저장 완료: month=%s path=%s", month, month_stage_path)
        if not staged_months:
            raise AirflowSkipException(
                "네 taxi_type이 모두 준비된 신규 월이 없어 Type 3 갱신을 건너뜁니다"
            )
        return {"run_id": run_id, "months": staged_months}
    finally:
        spark.stop()


@task(pool="silver_pool")
def validate_type3_staged_records(stage_result: dict) -> dict:
    """월별 임시 결과를 검증하고 통과한 결과만 승격 계획으로 반환한다."""

    run_id = stage_result["run_id"]
    _staging_run_path(run_id)
    spark = get_spark()
    try:
        validations = []
        for item in stage_result["months"]:
            validations.append(
                validate_daily_zone_month(
                    spark,
                    item["stage_path"],
                    item["month"],
                )
            )
        if not validations:
            raise ValueError("검증할 Type 3 staging 결과가 없습니다")
        logger.info("Type 3 staging 검증 통과: run_id=%s stats=%s", run_id, validations)
        return {**stage_result, "validations": validations}
    finally:
        spark.stop()


@task(pool="silver_pool")
def publish_type3_daily_records(validated_stage: dict) -> dict:
    """검증된 월의 날짜 파티션만 운영 경로에 반영하고 완료 마커를 만든다."""

    run_id = validated_stage["run_id"]
    _staging_run_path(run_id)
    spark = get_spark()
    try:
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        published_months = []
        for item in validated_stage["months"]:
            staged = (
                spark.read
                .option("basePath", to_spark_path(item["stage_path"]))
                .parquet(to_spark_path(item["stage_path"]))
            )
            (
                staged.write
                .mode("overwrite")
                .partitionBy("date")
                .parquet(to_spark_path(TYPE3_DAILY_ROOT))
            )
            marker = _month_success_marker(item["month"])
            marker.parent.mkdir(parents=True, exist_ok=True)
            marker.write_text(item["month"], encoding="utf-8")
            published_months.append(item["month"])
            logger.info("Type 3 운영 파티션 승격 완료: month=%s", item["month"])
        return {
            "run_id": run_id,
            "daily_path": str(TYPE3_DAILY_ROOT),
            "months": published_months,
        }
    finally:
        spark.stop()


@task
def cleanup_type3_staging(published_result: dict) -> None:
    """운영 경로 승격에 성공한 실행의 임시 결과를 삭제한다."""

    run_path = _staging_run_path(published_result["run_id"])
    if not run_path.exists():
        return
    if isinstance(run_path, Path):
        shutil.rmtree(run_path)
    else:
        run_path.rmtree()
    logger.info("Type 3 staging 정리 완료: %s", run_path)


@task(trigger_rule="none_failed")
def check_type3_publish_needed(_published_result=None) -> dict:
    """S3 최신 N주와 DynamoDB 메타데이터를 비교해 적재 여부를 판단한다."""

    if not DYNAMODB_NAV_TABLE:
        raise ValueError("DYNAMODB_NAV_TABLE 환경변수가 필요합니다")

    _, window_start, window_end = select_latest_date_partitions(
        TYPE3_DAILY_ROOT.glob("date=*"),
        TLC_TYPE3_ROLLING_WEEKS * 7,
    )
    table = boto3.resource("dynamodb", region_name=AWS_REGION).Table(DYNAMODB_NAV_TABLE)
    response = table.get_item(
        Key={"segment_id": TYPE3_META_SEGMENT_ID, "sk": TYPE3_META_SK},
        ConsistentRead=True,
    )
    metadata = response.get("Item", {})
    if _type3_metadata_is_current(metadata, window_start, window_end):
        raise AirflowSkipException(
            f"DynamoDB Type 3가 이미 최신입니다: {window_start}~{window_end}"
        )

    logger.info(
        "DynamoDB Type 3 갱신 필요: current_window_end=%s target=%s~%s",
        metadata.get("window_end"),
        window_start,
        window_end,
    )
    return {
        "daily_path": str(TYPE3_DAILY_ROOT),
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }


@task(pool="silver_pool")
def publish_type3_rolling_values(publish_plan: dict) -> dict:
    """최근 N주의 요일별 평균을 ``3#DOW#HHMM`` 키에 덮어쓴다."""

    if not DYNAMODB_NAV_TABLE:
        raise ValueError("DYNAMODB_NAV_TABLE 환경변수가 필요합니다")

    spark = get_spark()
    try:
        daily_path = publish_plan["daily_path"]
        if daily_path != str(TYPE3_DAILY_ROOT):
            raise ValueError(f"예상하지 못한 Type 3 날짜별 경로입니다: {daily_path}")

        partition_paths, expected_start, expected_end = select_latest_date_partitions(
            TYPE3_DAILY_ROOT.glob("date=*"),
            TLC_TYPE3_ROLLING_WEEKS * 7,
        )
        daily = (
            spark.read
            .option("basePath", to_spark_path(TYPE3_DAILY_ROOT))
            .parquet(*(to_spark_path(path) for path in partition_paths))
            .withColumn("date", to_date(col("date")))
        )
        rolling_with_count, window_start, window_end = build_weekday_rolling_frame(
            daily,
            TLC_TYPE3_ROLLING_WEEKS,
        )
        if window_start != expected_start or window_end != expected_end:
            raise ValueError("선택한 S3 파티션과 실제 Type 3 날짜 범위가 다릅니다")
        if (
            publish_plan["window_start"] != window_start.isoformat()
            or publish_plan["window_end"] != window_end.isoformat()
        ):
            raise ValueError("DynamoDB 적재 계획 이후 Type 3 날짜 범위가 변경됐습니다")

        cached_zone_rolling = rolling_with_count.persist(StorageLevel.DISK_ONLY)
        try:
            sample_stats = cached_zone_rolling.agg(
                spark_min("sample_count").alias("min_count"),
                spark_max("sample_count").alias("max_count"),
            ).first()
            if (
                sample_stats["min_count"] != TLC_TYPE3_ROLLING_WEEKS
                or sample_stats["max_count"] != TLC_TYPE3_ROLLING_WEEKS
            ):
                raise ValueError(
                    f"모든 Type 3 버킷에 {TLC_TYPE3_ROLLING_WEEKS}개 "
                    "동일 요일 표본이 필요합니다"
                )

            zone_rolling = cached_zone_rolling.drop("sample_count")
            invalid = zone_rolling.filter(
                col("zone_id").isNull()
                | col("dow").isNull()
                | col("time").isNull()
                | col("value").isNull()
                | (col("value") < 0)
                | (col("type") != TYPE_ID)
            ).limit(1).count()
            if invalid:
                raise ValueError("Type 3 Zone 롤링값 검증 실패")

            mapping = spark.read.parquet(to_spark_path(MAP_ZONE_SEGMENT_PATH))
            segment_values = expand_zone_values_to_segments(
                zone_rolling,
                mapping,
            ).persist(StorageLevel.DISK_ONLY)
            try:
                final_stats = validate_segment_values(segment_values, mapping)
                table = boto3.resource(
                    "dynamodb",
                    region_name=AWS_REGION,
                ).Table(DYNAMODB_NAV_TABLE)
                written = write_type3_rolling_to_dynamodb(
                    table,
                    segment_values,
                    window_start,
                    window_end,
                    TLC_TYPE3_ROLLING_WEEKS,
                )
            finally:
                segment_values.unpersist()
        finally:
            cached_zone_rolling.unpersist()

        logger.info(
            "Type 3 DynamoDB 갱신 완료: table=%s items=%s segments=%s window=%s~%s",
            DYNAMODB_NAV_TABLE,
            written,
            final_stats["segments"],
            window_start,
            window_end,
        )
        return {
            "table": DYNAMODB_NAV_TABLE,
            "written_items": written,
            "segments": final_stats["segments"],
            "rolling_weeks": TLC_TYPE3_ROLLING_WEEKS,
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
        }
    finally:
        spark.stop()
