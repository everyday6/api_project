"""TLC Silver1의 순수 스키마 표준화 로직.

Yellow, Green, FHV, FHVHV 원본의 서로 다른 컬럼명을 공통 스키마로
통일한다. Silver1은 비즈니스 필터를 적용하지 않는다. 원천별로 존재하지
않는 선택 컬럼은 nullable 컬럼으로 보존하고, 실제 결측치는 삭제하지 않고
현황만 기록한다.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, count, lit, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_silver1")


SILVER_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
])

SILVER_COLUMNS = [field.name for field in SILVER_SCHEMA.fields]


COLUMN_MAPPING = {
    "yellow": {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
    },
    "green": {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
    },
    "fhv": {
        "pickup_datetime": "pickup_datetime",
        "dropOff_datetime": "dropoff_datetime",
        "PUlocationID": "pickup_location_id",
        "DOlocationID": "dropoff_location_id",
    },
    "fhvhv": {
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "trip_miles": "trip_distance",
    },
}


def rename_columns(df: DataFrame, taxi_type: str) -> DataFrame:
    """taxi_type별 원본 컬럼명을 Silver1 공통 컬럼명으로 바꾼다."""

    if taxi_type not in COLUMN_MAPPING:
        raise ValueError(f"지원하지 않는 택시 종류입니다 : {taxi_type}")

    for old_name, new_name in COLUMN_MAPPING[taxi_type].items():
        if old_name not in df.columns:
            raise ValueError(f"필수 컬럼이 존재하지 않습니다 : {old_name}")
        df = df.withColumnRenamed(old_name, new_name)

    return df


def add_missing_columns(df: DataFrame) -> DataFrame:
    """원천에 없는 Silver1 선택 컬럼을 스키마에 맞는 NULL로 추가한다."""

    for field in SILVER_SCHEMA.fields:
        if field.name not in df.columns:
            logger.warning("컬럼 없음 - %s → NULL로 추가", field.name)
            df = df.withColumn(field.name, lit(None).cast(field.dataType))

    return df


def cast_columns(df: DataFrame) -> DataFrame:
    """공통 컬럼을 Silver1 스키마의 타입으로 캐스팅한다."""

    for field in SILVER_SCHEMA.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df


def select_columns(df: DataFrame) -> DataFrame:
    """확장용 공통 컬럼 6개만 정해진 순서로 남긴다."""

    return df.select(*SILVER_COLUMNS)


def check_null(df: DataFrame) -> DataFrame:
    """컬럼별 결측 현황을 기록하되 행은 삭제하지 않는다."""

    summary = df.agg(
        count("*").alias("total_count"),
        *[
            count(when(col(column).isNull(), column)).alias(column)
            for column in SILVER_COLUMNS
        ],
    ).collect()[0]

    total_count = summary["total_count"]
    if total_count == 0:
        logger.warning("데이터가 존재하지 않습니다.")
        return df

    for column in SILVER_COLUMNS:
        null_count = summary[column]
        if null_count:
            logger.warning(
                "결측치 발견 - %s : %s건 (%.4f%%)",
                column,
                null_count,
                null_count / total_count * 100,
            )

    return df


def transform(df: DataFrame, taxi_type: str) -> DataFrame:
    """TLC 원본 DataFrame을 필터링 없이 Silver1 공통 스키마로 바꾼다."""

    renamed = rename_columns(df, taxi_type)
    completed = add_missing_columns(renamed)
    casted = cast_columns(completed)
    selected = select_columns(casted)
    return check_null(selected)
