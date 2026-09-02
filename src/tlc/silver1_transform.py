"""TLC Silver1의 순수 스키마 표준화 로직.

Yellow, Green, FHV, FHVHV 원본의 서로 다른 컬럼명을 공통 스키마로
통일한다. Silver1은 비즈니스 필터를 적용하지 않는다. 원천별로 존재하지
않는 선택 컬럼은 nullable 컬럼으로 보존하고, 실제 결측치는 삭제하지 않고
현황만 기록한다.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col, count, lit, when
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StructField,
    StructType,
    TimestampType,
)

from src.common.logger import get_logger
from src.common.suspect import IS_SUSPECT_COLUMN, flag_suspect_spark

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

# transform()이 실제로 내보내는 컬럼 = 공통 6개 + is_suspect 플래그.
# select_columns()와 테스트가 이 상수 하나를 참조해, "6개"와 "+1"이
# 코드/테스트 여러 곳에 따로 박히지 않게 한다.
SILVER_OUTPUT_COLUMNS = SILVER_COLUMNS + [IS_SUSPECT_COLUMN]

# TLC zone ID 유효 범위(1~263 실사용 + 264/265 unknown). src/tlc/expectations.py의
# log_only_expectations()가 import해서 같은 값을 본다 - 여기가 원본이다
# (COLUMN_MAPPING과 같은 이유로, silver1_transform.py를 단일 진실 공급원으로 둔다).
LOCATION_ID_MIN = 1
LOCATION_ID_MAX = 265

# spark_jobs/tlc_pipeline_job.py의 _validate_bronze가 suspect_fraction()으로
# 파일별 is_suspect 비율을 재고, 이 값을 넘으면 그 파일을 critical처럼 제외한다
# (RELIABILITY_PRINCIPLES.md 열린 질문 - "비율 급증 시 critical 승격").
# 개별 행의 이상은 log-only(is_suspect 표시)로 넘기되, 한 파일에서 값이
# 뭉텅이로 이상하면(스키마 드리프트, 원천 포맷 변경 등) Silver로 안 넘긴다.
# 월 단위 파일이라 speed(30분 배치, 0.20)보다는 조이되, 원천 노이즈(FHV 결측
# 등)를 고려해 lion(0.05)/silver2(0.10)보다는 넉넉하게.
#
# NOTE: placeholder. 실제 월별 파일들의 baseline suspect 비율 측정 후 조정 필요.
MAX_SUSPECT_RATIO = 0.15


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
    """확장용 공통 컬럼 6개 + is_suspect 플래그를 정해진 순서로 남긴다."""

    return df.select(*SILVER_OUTPUT_COLUMNS)


def mark_suspect_rows(df: DataFrame, taxi_type: str) -> DataFrame:
    """src/tlc/expectations.py의 log_only_expectations()가 검사하는 조건 중
    행 단위로 판정 가능한 것만 재적용해 `is_suspect` 컬럼을 추가한다.

    Bronze GX 검증(log-only)은 결과를 로그로만 남기고 이 df(실제로 저장되는
    Silver 데이터)에는 전혀 반영되지 않던 문제
    (RELIABILITY_PRINCIPLES.md "GX 적용 현황" 참고)를 해소한다.
    src/speed/bronze_validation.py의 mark_suspect_rows와 같은 목적·같은
    패턴이다 - 전면적인 quarantine(격리) 대신 표시만 남기는 최소 버전.

    cast_columns() 이후 select_columns() 이전에만 호출해야 한다 - 이미
    Silver 컬럼명·타입으로 표준화된 df를 전제로 한다.

    taxi_type마다 COLUMN_MAPPING에 없는 선택 컬럼(passenger_count,
    trip_distance)은 add_missing_columns()가 항상 NULL로 채운다. 이건
    "원천에 그 컬럼이 아예 없다"는 정상 상태이지 이상치가 아니므로, 그
    taxi_type에 실제로 매핑된 컬럼에 대해서만 범위 검사를 적용한다 -
    log_only_expectations()가 `if "passenger_count" in columns`로 조건부
    검사를 추가하던 것과 동일한 이유다.

    GX의 ExpectColumnValuesToBeBetween은 기본적으로 null을 검사 대상에서
    제외한다(불통과로 세지 않음) - 여기서도 같은 동작을 맞추기 위해
    passenger_count/trip_distance의 null은 별도로 suspect 처리하지
    않는다(범위 검사에서만 걸림). NULL로 새는 boolean 표현식을 False(정상)로
    확정하는 것과 컬럼명은 src.common.suspect.flag_suspect_spark로 위임한다.
    """
    expected = set(COLUMN_MAPPING[taxi_type].values())

    suspect = (
        col("pickup_datetime").isNull()
        | col("dropoff_datetime").isNull()
        | col("pickup_location_id").isNull()
        | col("dropoff_location_id").isNull()
        | (col("pickup_location_id") < LOCATION_ID_MIN)
        | (col("pickup_location_id") > LOCATION_ID_MAX)
        | (col("dropoff_location_id") < LOCATION_ID_MIN)
        | (col("dropoff_location_id") > LOCATION_ID_MAX)
    )
    if "passenger_count" in expected:
        suspect = suspect | (col("passenger_count") < 0)
    if "trip_distance" in expected:
        suspect = suspect | (col("trip_distance") < 0)

    return flag_suspect_spark(df, suspect)


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


def _standardize_and_flag(df: DataFrame, taxi_type: str) -> DataFrame:
    """rename → add_missing → cast → mark_suspect_rows까지. transform()과
    suspect_fraction()이 공유해, is_suspect를 붙이는 로직이 두 군데서
    갈라지지 않게 한다."""

    renamed = rename_columns(df, taxi_type)
    completed = add_missing_columns(renamed)
    casted = cast_columns(completed)
    return mark_suspect_rows(casted, taxi_type)


def transform(df: DataFrame, taxi_type: str) -> DataFrame:
    """TLC 원본 DataFrame을 필터링 없이 Silver1 공통 스키마로 바꾼다."""

    flagged = _standardize_and_flag(df, taxi_type)
    selected = select_columns(flagged)
    return check_null(selected)


def suspect_fraction(df: DataFrame, taxi_type: str) -> float:
    """raw Bronze df에서 transform()이 붙일 is_suspect 비율(0.0~1.0)을 계산한다.

    _validate_bronze가 저장 전에 이 비율을 보고 임계치(MAX_SUSPECT_RATIO)를
    넘는 파일을 critical처럼 제외한다. speed의 suspect_ratio_ok()와 같은
    목적이다(RELIABILITY_PRINCIPLES.md 열린 질문 - "비율 급증 시 critical 승격").

    critical 검증(필수 원본 컬럼 존재)이 이미 통과한 뒤에만 호출해야 한다 -
    그래야 rename_columns()가 ValueError를 던지지 않는다.
    """
    flagged = _standardize_and_flag(df, taxi_type)
    row = flagged.agg(
        avg(col(IS_SUSPECT_COLUMN).cast("double")).alias("frac")
    ).first()
    return float(row["frac"]) if row is not None and row["frac"] is not None else 0.0
