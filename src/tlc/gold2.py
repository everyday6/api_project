"""TLC Type 3의 Spark 롤링 계산과 DynamoDB Gold 적재 로직."""

from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    array,
    avg,
    broadcast,
    col,
    count,
    dayofweek,
    element_at,
    floor,
    format_string,
    hour,
    lit,
    max as spark_max,
    minute,
    to_date,
)

from src.common.config import TLC_TYPE3_DOW_NAMES, TLC_TYPE3_ID
from src.common.dynamodb import get_table
from src.common.spark import to_spark_path


TYPE_ID = TLC_TYPE3_ID
TIME_SLOT_MINUTES = 30
TIME_SLOTS = tuple(
    f"{hour_value:02d}{minute_value:02d}"
    for hour_value in range(24)
    for minute_value in range(0, 60, TIME_SLOT_MINUTES)
)
TYPE3_META_SEGMENT_ID = "__META__"
TYPE3_META_SK = "TYPE#3"
DATE_PARTITION_PATTERN = re.compile(r"^date=(\d{4}-\d{2}-\d{2})$")

# DynamoDB 쓰기 병렬도. executor마다 자기 파티션을 독립적으로 batch_writer로
# 쓰게 해서, driver가 toLocalIterator()로 한 줄씩 순차 처리할 때보다
# wall-clock을 파티션 수만큼 나눈다(Airflow heartbeat timeout 예방 — segment
# 수가 많으면 순차 처리가 5분을 넘겨 태스크가 강제 종료되는 사고가 있었다).
# 원래 32로 뒀었는데, PAY_PER_REQUEST(온디맨드) 테이블도 최근 트래픽 기준으로
# 순간 처리량 한도가 정해져 있어서 32-way로 한꺼번에 몰아치면
# RequestLimitExceeded로 죽는 사고가 실제로 있었다(get_dynamodb_resource의
# adaptive 재시도와 별개로, 애초에 순간 부하 자체를 낮춰서 두 겹으로 방어).
TYPE3_DYNAMODB_WRITE_PARTITIONS = 10
TAXI_ZONE_IDS = tuple(range(1, 264))
DOW_NAMES = TLC_TYPE3_DOW_NAMES
SPARK_DOW_NAMES = (DOW_NAMES[-1], *DOW_NAMES[:-1])


def _month_bounds(service_month: str) -> tuple[date, date]:
    """YYYY-MM의 시작일과 다음 달 시작일을 반환한다."""

    year, month = (int(value) for value in service_month.split("-"))
    start = date(year, month, 1)
    end = date(year + (month == 12), month % 12 + 1, 1)
    return start, end


def build_daily_zone_frame(
    spark: SparkSession,
    silver_paths: list[str],
    service_month: str | None = None,
    zone_ids: tuple[int, ...] = TAXI_ZONE_IDS,
) -> DataFrame:
    """TLC 운행을 Zone×날짜×30분 단위의 Gold2 승차 건수로 만든다."""

    if not silver_paths:
        raise ValueError("Type 3 날짜별 기록을 만들 Silver1 경로가 없습니다")
    if not zone_ids:
        raise ValueError("Type 3 집계 대상 Taxi Zone이 없습니다")

    source = spark.read.parquet(*silver_paths).select(
        "pickup_datetime",
        "pickup_location_id",
    )
    valid = source.filter(
        col("pickup_datetime").isNotNull()
        & col("pickup_location_id").between(1, 263)
    )
    if service_month is not None:
        month_start, next_month_start = _month_bounds(service_month)
        valid = valid.filter(
            (to_date(col("pickup_datetime")) >= lit(month_start))
            & (to_date(col("pickup_datetime")) < lit(next_month_start))
        )
    if not valid.limit(1).count():
        raise ValueError(f"Type 3 유효 승차 데이터가 없습니다: {service_month or '전체'}")

    prepared = (
        valid
        .withColumn("date", to_date(col("pickup_datetime")))
        .withColumn(
            "slot_minute",
            (floor(minute(col("pickup_datetime")) / 30) * 30).cast("int"),
        )
        .withColumn(
            "time",
            format_string(
                "%02d%02d",
                hour(col("pickup_datetime")),
                col("slot_minute"),
            ),
        )
    )
    zone_counts = (
        prepared
        .groupBy(
            col("pickup_location_id").cast("int").alias("zone_id"),
            "date",
            "time",
        )
        .agg(count("*").cast("double").alias("value"))
    )
    dates = prepared.select("date").distinct()
    slots = spark.createDataFrame([(slot,) for slot in TIME_SLOTS], ["time"])
    zones = spark.createDataFrame(
        [(int(zone_id),) for zone_id in zone_ids],
        ["zone_id"],
    )
    zone_grid = zones.crossJoin(dates).crossJoin(slots)
    return (
        zone_grid
        .join(zone_counts, on=["zone_id", "date", "time"], how="left")
        .fillna({"value": 0.0})
        .select(
            "zone_id",
            lit(TYPE_ID).cast("int").alias("type"),
            "date",
            "time",
            col("value").cast("double").alias("value"),
        )
    )


def validate_daily_zone_month(
    spark: SparkSession,
    stage_path,
    service_month: str,
    expected_zone_ids: tuple[int, ...] = TAXI_ZONE_IDS,
) -> dict:
    """운영 경로로 승격하기 전 월별 Zone Gold2 결과를 검증한다."""

    staged = (
        spark.read
        .option("basePath", to_spark_path(stage_path))
        .parquet(to_spark_path(stage_path))
        .withColumn("date", to_date(col("date")))
    )
    required_columns = {"zone_id", "type", "date", "time", "value"}
    missing_columns = required_columns - set(staged.columns)
    if missing_columns:
        raise ValueError(f"Type 3 staging 필수 컬럼 없음: {missing_columns}")

    month_start, next_month_start = _month_bounds(service_month)
    invalid = staged.filter(
        col("zone_id").isNull()
        | col("type").isNull()
        | col("date").isNull()
        | col("time").isNull()
        | col("value").isNull()
        | (col("type") != TYPE_ID)
        | (col("value") < 0)
        | ~col("time").isin(list(TIME_SLOTS))
        | (col("date") < lit(month_start))
        | (col("date") >= lit(next_month_start))
    ).limit(1).count()
    if invalid:
        raise ValueError(f"Type 3 staging 값 검증 실패: {service_month}")

    duplicate = (
        staged.groupBy("zone_id", "type", "date", "time")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError(f"Type 3 staging 복합 키 중복: {service_month}")

    expected_dates = {
        month_start + timedelta(days=offset)
        for offset in range((next_month_start - month_start).days)
    }
    actual_dates = {row["date"] for row in staged.select("date").distinct().collect()}
    if actual_dates != expected_dates:
        missing_dates = sorted(expected_dates - actual_dates)
        extra_dates = sorted(actual_dates - expected_dates)
        raise ValueError(
            f"Type 3 staging 날짜 불일치: month={service_month} "
            f"missing={missing_dates[:5]} extra={extra_dates[:5]}"
        )

    expected_zones = set(expected_zone_ids)
    actual_zones = {
        row["zone_id"]
        for row in staged.select("zone_id").distinct().collect()
    }
    if actual_zones != expected_zones:
        raise ValueError(
            f"Type 3 staging Zone coverage 불일치: "
            f"{len(actual_zones)}/{len(expected_zones)}"
        )

    actual_rows = staged.count()
    expected_rows = len(expected_zones) * len(expected_dates) * len(TIME_SLOTS)
    if actual_rows != expected_rows:
        raise ValueError(
            f"Type 3 staging 행 수 불일치: {actual_rows}/{expected_rows} "
            f"(month={service_month})"
        )

    return {
        "month": service_month,
        "rows": actual_rows,
        "zones": len(actual_zones),
        "dates": len(actual_dates),
    }


def select_latest_date_partitions(
    partition_paths,
    rolling_days: int = 28,
) -> tuple[list, date, date]:
    """date=YYYY-MM-DD 경로 중 최신 N개의 연속 파티션을 선택한다."""

    if rolling_days <= 0:
        raise ValueError("rolling_days는 1 이상이어야 합니다")

    dated_paths = {}
    for path in partition_paths:
        match = DATE_PARTITION_PATTERN.match(path.name)
        if not match:
            continue
        partition_date = date.fromisoformat(match.group(1))
        if partition_date in dated_paths:
            raise ValueError(f"Type 3 날짜 파티션 중복 발견: {partition_date}")
        dated_paths[partition_date] = path

    if len(dated_paths) < rolling_days:
        raise ValueError(
            f"최근 {rolling_days}일 계산에 필요한 날짜 파티션이 부족합니다: "
            f"{len(dated_paths)}개"
        )

    selected_dates = sorted(dated_paths)[-rolling_days:]
    window_start = selected_dates[0]
    window_end = selected_dates[-1]
    expected_dates = [
        window_start + timedelta(days=offset)
        for offset in range(rolling_days)
    ]
    if selected_dates != expected_dates:
        missing_dates = sorted(set(expected_dates) - set(selected_dates))
        raise ValueError(
            f"최근 {rolling_days}개 날짜 파티션이 연속적이지 않습니다. "
            f"누락 날짜: {missing_dates[:5]}"
        )

    return [dated_paths[value] for value in selected_dates], window_start, window_end


def build_weekday_rolling_frame(
    daily: DataFrame,
    rolling_weeks: int,
) -> tuple[DataFrame, date, date]:
    """최신 N주에서 같은 요일·시간대의 평균을 만든다."""

    if rolling_weeks <= 0:
        raise ValueError("rolling_weeks는 1 이상이어야 합니다")

    latest_date = daily.agg(spark_max("date").alias("latest_date")).first()["latest_date"]
    if latest_date is None:
        raise ValueError("Type 3 날짜별 S3 기록이 비어 있습니다")
    window_days = rolling_weeks * len(DOW_NAMES)
    window_start = latest_date - timedelta(days=window_days - 1)

    window = daily.filter(col("date").between(lit(window_start), lit(latest_date)))
    if not window.limit(1).count():
        raise ValueError("Type 3 롤링 윈도우에 데이터가 없습니다")

    actual_dates = {row["date"] for row in window.select("date").distinct().collect()}
    expected_dates = {
        window_start + timedelta(days=offset)
        for offset in range(window_days)
    }
    if actual_dates != expected_dates:
        missing_dates = sorted(expected_dates - actual_dates)
        raise ValueError(
            f"최근 {rolling_weeks}주 연속 데이터가 필요합니다. "
            f"누락 날짜: {missing_dates[:5]}"
        )

    rolling = (
        window
        .withColumn(
            "dow",
            element_at(
                array(*(lit(name) for name in SPARK_DOW_NAMES)),
                dayofweek(col("date")),
            ),
        )
        .groupBy("zone_id", "type", "dow", "time")
        .agg(
            avg("value").cast("double").alias("value"),
            count("*").alias("sample_count"),
        )
    )
    return rolling, window_start, latest_date


def expand_zone_values_to_segments(
    rolling: DataFrame,
    mapping: DataFrame,
) -> DataFrame:
    """작은 Zone 평균 결과를 마지막에 Segment 서빙 단위로 확장한다."""

    missing_rolling = {"zone_id", "type", "dow", "time", "value"} - set(
        rolling.columns
    )
    if missing_rolling:
        raise ValueError(f"Type 3 Zone 평균 필수 컬럼 없음: {missing_rolling}")
    missing_mapping = {"segment_id", "zone_id"} - set(mapping.columns)
    if missing_mapping:
        raise ValueError(f"Zone-Segment 매핑 필수 컬럼 없음: {missing_mapping}")

    segments = mapping.select(
        col("segment_id").cast("string").alias("segment_id"),
        col("zone_id").cast("int").alias("zone_id"),
    )
    return (
        segments
        .join(broadcast(rolling), on="zone_id", how="inner")
        .select("segment_id", "type", "dow", "time", "value")
    )


def validate_segment_values(
    segment_values: DataFrame,
    mapping: DataFrame,
) -> dict:
    """각 Segment에 요일 7개×시간 48개의 Type 3 값이 있는지 검증한다."""

    required = {"segment_id", "type", "dow", "time", "value"}
    missing = required - set(segment_values.columns)
    if missing:
        raise ValueError(f"Type 3 Segment Gold2 필수 컬럼 없음: {missing}")

    segments = mapping.select(
        col("segment_id").cast("string").alias("segment_id"),
        col("zone_id").cast("int").alias("zone_id"),
    )
    if segments.filter(col("segment_id").isNull() | col("zone_id").isNull()).limit(1).count():
        raise ValueError("Zone-Segment 매핑에 NULL이 있습니다")
    if segments.groupBy("segment_id").count().filter(col("count") > 1).limit(1).count():
        raise ValueError("Zone-Segment 매핑의 segment_id가 중복됩니다")

    invalid = segment_values.filter(
        col("segment_id").isNull()
        | col("type").isNull()
        | col("dow").isNull()
        | col("time").isNull()
        | col("value").isNull()
        | (col("type") != TYPE_ID)
        | (col("value") < 0)
        | ~col("dow").isin(list(DOW_NAMES))
        | ~col("time").isin(list(TIME_SLOTS))
    ).limit(1).count()
    if invalid:
        raise ValueError("DynamoDB 적재 전 Type 3 Segment 값 검증 실패")

    duplicate = (
        segment_values
        .groupBy("segment_id", "type", "dow", "time")
        .count()
        .filter(col("count") > 1)
        .limit(1)
        .count()
    )
    if duplicate:
        raise ValueError("Type 3 Segment Gold2 복합 키 중복 발견")

    expected_segments = segments.count()
    actual_segments = segment_values.select("segment_id").distinct().count()
    if actual_segments != expected_segments:
        raise ValueError(
            f"Type 3 Segment coverage 불일치: {actual_segments}/{expected_segments}"
        )

    actual_rows = segment_values.count()
    expected_rows = expected_segments * len(DOW_NAMES) * len(TIME_SLOTS)
    if actual_rows != expected_rows:
        raise ValueError(
            f"Type 3 Segment 행 수 불일치: {actual_rows}/{expected_rows}"
        )

    return {"segments": actual_segments, "rows": actual_rows}


def _write_type3_partition(table_name: str):
    """executor 파티션 하나를 자기만의 boto3 리소스로 DynamoDB에 쓴다.

    driver에서 만든 boto3 리소스는 executor로 직렬화해서 보낼 수 없으므로
    (네트워크 커넥션을 포함한 객체라 pickle 불가/안전하지 않음), 파티션마다
    executor 안에서 새로 만든다."""

    def _write(rows) -> None:
        table = get_table(table_name)
        with table.batch_writer(overwrite_by_pkeys=["segment_id", "sk"]) as batch:
            for row in rows:
                batch.put_item(Item={
                    "segment_id": str(row["segment_id"]),
                    "sk": f"{TYPE_ID}#{row['dow']}#{str(row['time']).zfill(4)}",
                    "value": Decimal(str(row["value"])),
                })

    return _write


def write_type3_rolling_to_dynamodb(
    table_name: str,
    rolling: DataFrame,
    window_start: date,
    window_end: date,
    rolling_weeks: int,
) -> int:
    """검증된 Spark 롤링 결과를 executor 병렬로 저장한 뒤 완료 메타데이터를 기록한다.

    이전엔 driver가 toLocalIterator()로 한 줄씩 순차로 batch_writer를
    호출했다 — segment 수가 많으면(zone 값이 segment마다 복제되므로 수만~
    수십만 건) 이 태스크 하나가 Airflow heartbeat timeout(기본 300초)을
    넘겨 강제 종료되는 사고가 실제로 있었다. foreachPartition으로
    executor마다 자기 파티션을 병렬로 쓰게 바꿔서 wall-clock을 파티션
    수만큼 나눈다.
    """

    to_write = rolling.select("segment_id", "dow", "time", "value").repartition(
        TYPE3_DYNAMODB_WRITE_PARTITIONS
    )
    written = to_write.count()
    to_write.foreachPartition(_write_type3_partition(table_name))

    # 값 저장 도중(어느 파티션에서든) 실패하면 예외가 여기까지 전파되어
    # COMPLETED가 안 남고, 다음 DAG 실행이 워터마크를 보고 다시 처리한다.
    get_table(table_name).put_item(Item={
        "segment_id": TYPE3_META_SEGMENT_ID,
        "sk": TYPE3_META_SK,
        "status": "COMPLETED",
        "window_start": window_start.isoformat(),
        "window_end": window_end.isoformat(),
        "rolling_weeks": rolling_weeks,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    })
    return written
