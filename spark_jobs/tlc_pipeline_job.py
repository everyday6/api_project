"""EMR Serverless 엔트리포인트 — TLC Spark 연산 실행.

Airflow DAG의 task 경계(Stage → Validate → Publish)는 그대로 유지하고,
각 task가 이 스크립트에 operation/payload를 넘겨 실제 Spark 연산만 EMR
Serverless에서 수행한다.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from cloudpathlib import S3Path
from pyspark import StorageLevel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, min as spark_min, to_date

from src.common.config import TLC_TYPE3_ROLLING_WEEKS
from src.common.gx import validate_spark_dataframe
from src.common.logger import get_logger
from src.common.spark import to_spark_path
from src.tlc.expectations import critical_expectations, log_only_expectations
from src.tlc.gold2 import (
    TYPE_ID,
    build_daily_zone_frame,
    build_weekday_rolling_frame,
    expand_zone_values_to_segments,
    select_latest_date_partitions,
    validate_daily_zone_month,
    validate_segment_values,
    write_type3_rolling_to_dynamodb,
)
from src.tlc.silver1_transform import transform


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_emr_job")


def _validate_bronze(spark: SparkSession, payload: dict) -> dict:
    passed: list[dict] = []
    excluded: list[dict] = []

    for item in payload["bronze_chunk"]:
        filename = item["filename"]
        taxi_type = item["taxi_type"]
        bronze_path = item["bronze_path"]
        df = spark.read.parquet(to_spark_path(bronze_path))
        asset_id = Path(bronze_path).stem

        critical = validate_spark_dataframe(
            df,
            critical_expectations(taxi_type),
            datasource_name=f"tlc_bronze_critical_{asset_id}",
            asset_name=f"tlc_bronze_critical_{asset_id}",
        )
        failed_critical = [result for result in critical if not result["success"]]
        if failed_critical:
            missing = [result["kwargs"].get("column") for result in failed_critical]
            reason = f"필수 컬럼 없음: {missing} (taxi_type={taxi_type})"
            logger.error("Critical 검증 실패 - %s: %s", filename, reason)
            excluded.append({"filename": filename, "reason": reason})
            continue

        log_results = validate_spark_dataframe(
            df,
            log_only_expectations(taxi_type),
            datasource_name=f"tlc_bronze_logonly_{asset_id}",
            asset_name=f"tlc_bronze_logonly_{asset_id}",
        )
        for result in log_results:
            if not result["success"]:
                logger.warning(
                    "검증 실패(로그만) - %s: %s %s -> %s",
                    filename,
                    result["expectation_type"],
                    result["kwargs"],
                    result["result"],
                )
        passed.append(item)

    return {"passed": passed, "excluded": excluded}


def _build_silver(spark: SparkSession, payload: dict) -> dict:
    results: list[dict] = []
    for item in payload["bronze_chunk"]:
        taxi_type = item["taxi_type"]
        filename = item["filename"]
        bronze_path = item["bronze_path"]
        silver_path = item["silver_path"]

        success_marker = S3Path(silver_path) / "_SUCCESS"
        if not success_marker.exists():
            bronze_df = spark.read.parquet(to_spark_path(bronze_path))
            transform(bronze_df, taxi_type).write.mode("overwrite").parquet(
                to_spark_path(silver_path)
            )
        results.append({
            "taxi_type": taxi_type,
            "filename": filename,
            "silver_path": silver_path,
        })
    return {"results": results}


def _build_type3_stage(spark: SparkSession, payload: dict) -> dict:
    staged_months: list[dict] = []
    run_path = payload["run_path"]
    for item in payload["months"]:
        month = item["month"]
        daily = build_daily_zone_frame(
            spark,
            item["silver_paths"],
            service_month=month,
        )
        stage_path = f"{run_path}/month={month}"
        (
            daily.write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(to_spark_path(stage_path))
        )
        staged_months.append({"month": month, "stage_path": stage_path})
    return {"run_id": payload["run_id"], "months": staged_months}


def _validate_type3_stage(spark: SparkSession, payload: dict) -> dict:
    stage_result = payload["stage_result"]
    validations = [
        validate_daily_zone_month(
            spark,
            item["stage_path"],
            item["month"],
        )
        for item in stage_result["months"]
    ]
    if not validations:
        raise ValueError("검증할 Type 3 staging 결과가 없습니다")
    return {**stage_result, "validations": validations}


def _publish_type3_daily(spark: SparkSession, payload: dict) -> dict:
    validated = payload["validated_stage"]
    daily_root = payload["daily_root"]
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    published_months: list[str] = []

    for item in validated["months"]:
        staged = (
            spark.read
            .option("basePath", to_spark_path(item["stage_path"]))
            .parquet(to_spark_path(item["stage_path"]))
        )
        (
            staged.write
            .mode("overwrite")
            .partitionBy("date")
            .parquet(to_spark_path(daily_root))
        )
        marker = S3Path(payload["marker_root"]) / f"month={item['month']}" / "_SUCCESS"
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text(item["month"], encoding="utf-8")
        published_months.append(item["month"])

    return {
        "run_id": validated["run_id"],
        "daily_path": daily_root,
        "months": published_months,
    }


def _publish_type3_rolling(spark: SparkSession, payload: dict) -> dict:
    plan = payload["publish_plan"]
    daily_root = S3Path(payload["daily_root"])
    rolling_weeks = int(payload.get("rolling_weeks", TLC_TYPE3_ROLLING_WEEKS))
    partition_paths, expected_start, expected_end = select_latest_date_partitions(
        daily_root.glob("date=*"),
        rolling_weeks * 7,
    )
    daily = (
        spark.read
        .option("basePath", to_spark_path(daily_root))
        .parquet(*(to_spark_path(path) for path in partition_paths))
        .withColumn("date", to_date(col("date")))
    )
    rolling_with_count, window_start, window_end = build_weekday_rolling_frame(
        daily,
        rolling_weeks,
    )
    if window_start != expected_start or window_end != expected_end:
        raise ValueError("선택한 S3 파티션과 실제 Type 3 날짜 범위가 다릅니다")
    if (
        plan["window_start"] != window_start.isoformat()
        or plan["window_end"] != window_end.isoformat()
    ):
        raise ValueError("DynamoDB 적재 계획 이후 Type 3 날짜 범위가 변경됐습니다")

    cached = rolling_with_count.persist(StorageLevel.DISK_ONLY)
    try:
        sample_stats = cached.agg(
            spark_min("sample_count").alias("min_count"),
            spark_max("sample_count").alias("max_count"),
        ).first()
        if (
            sample_stats["min_count"] != rolling_weeks
            or sample_stats["max_count"] != rolling_weeks
        ):
            raise ValueError(f"모든 Type 3 버킷에 {rolling_weeks}개 동일 요일 표본이 필요합니다")

        zone_rolling = cached.drop("sample_count")
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

        mapping = spark.read.parquet(to_spark_path(payload["mapping_path"]))
        segment_values = expand_zone_values_to_segments(zone_rolling, mapping).persist(
            StorageLevel.DISK_ONLY
        )
        try:
            final_stats = validate_segment_values(segment_values, mapping)
            written = write_type3_rolling_to_dynamodb(
                payload["table_name"],
                segment_values,
                window_start,
                window_end,
                rolling_weeks,
            )
        finally:
            segment_values.unpersist()
    finally:
        cached.unpersist()

    return {
        "table": payload["table_name"],
        "written_items": written,
        "segments": final_stats["segments"],
        "rolling_weeks": rolling_weeks,
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
    }


OPERATIONS = {
    "validate_bronze": _validate_bronze,
    "build_silver": _build_silver,
    "build_type3_stage": _build_type3_stage,
    "validate_type3_stage": _validate_type3_stage,
    "publish_type3_daily": _publish_type3_daily,
    "publish_type3_rolling": _publish_type3_rolling,
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation", choices=sorted(OPERATIONS), required=True)
    parser.add_argument("--payload-json", required=True)
    parser.add_argument("--output-s3", required=True)
    args = parser.parse_args()

    spark = SparkSession.builder.appName(f"tlc-{args.operation}").getOrCreate()
    try:
        result = OPERATIONS[args.operation](spark, json.loads(args.payload_json))
    finally:
        spark.stop()

    S3Path(args.output_s3).write_text(json.dumps(result, ensure_ascii=False))


if __name__ == "__main__":
    main()
