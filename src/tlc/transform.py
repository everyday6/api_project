"""
TLC 데이터 변환 모듈

TLC의 4종류의 택시 데이터를
하나의 공통된 Silver 데이터 형식으로 변환한다.

주요 작업:
1. 데이터별 컬럼명 통일
2. 원본에 없는 컬럼은 NULL로 생성
3. 데이터 타입을 Silver 스키마에 맞게 통일
4. Traffic 분석에 필요한 6개 컬럼만 선택
5. 컬럼별 결측치 개수와 비율을 확인하고 로그로 기록
6. 변환된 데이터를 하나의 DataFrame으로 병합

최종 Silver 컬럼:
- 승차 시각
- 하차 시각
- 승차 위치 ID
- 하차 위치 ID
- 승객 수
- 이동거리

※ 결측치가 존재하더라도 데이터를 삭제하지 않고
   결측치 현황만 로그로 기록한다.
"""

from functools import reduce

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


logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_silver")


# =========================================================
# Silver 공통 스키마
# =========================================================
# 모든 택시 데이터를 Silver에서 동일한 구조로 통일한다.
#
# 원본 데이터에 특정 컬럼이 존재하지 않는 경우에도
# Silver에서는 해당 컬럼을 유지하고 NULL 값을 넣는다.
#
# 예)
# FHV → passenger_count 컬럼 없음
#     → passenger_count 컬럼을 생성하고 NULL 저장
# =========================================================

SILVER_SCHEMA = StructType([
    StructField("pickup_datetime", TimestampType(), True),
    StructField("dropoff_datetime", TimestampType(), True),
    StructField("pickup_location_id", IntegerType(), True),
    StructField("dropoff_location_id", IntegerType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
])


# 스키마에서 컬럼 이름만 추출
# → select(), 결측치 검사 등에 공통으로 사용
SILVER_COLUMNS = [
    field.name
    for field in SILVER_SCHEMA.fields
]


# =========================================================
# 택시 종류별 원본 컬럼명 → Silver 공통 컬럼명
# =========================================================
#
# Yellow / Green / FHV / FHVHV는
# 같은 의미의 데이터라도 원본 컬럼명이 서로 다르다.
#
# 따라서 Silver로 저장하기 전에 컬럼명을 통일한다.
# =========================================================

COLUMN_MAPPING = {

    # Yellow Taxi
    "yellow": {
        "tpep_pickup_datetime": "pickup_datetime",
        "tpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
    },

    # Green Taxi
    "green": {
        "lpep_pickup_datetime": "pickup_datetime",
        "lpep_dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "passenger_count": "passenger_count",
        "trip_distance": "trip_distance",
    },

    # FHV
    #
    # FHV에는 passenger_count와 trip_distance가
    # 원본 컬럼으로 존재하지 않는다.
    # → 이후 add_missing_columns()에서 NULL 생성
    "fhv": {
        "pickup_datetime": "pickup_datetime",
        "dropOff_datetime": "dropoff_datetime",
        "PUlocationID": "pickup_location_id",
        "DOlocationID": "dropoff_location_id",
    },

    # High Volume FHV
    #
    # FHVHV의 trip_miles는 Silver의
    # trip_distance로 이름을 통일한다.
    #
    # passenger_count는 원본에 없으므로 NULL 생성
    "fhvhv": {
        "pickup_datetime": "pickup_datetime",
        "dropoff_datetime": "dropoff_datetime",
        "PULocationID": "pickup_location_id",
        "DOLocationID": "dropoff_location_id",
        "trip_miles": "trip_distance",
    },
}


# =========================================================
# 컬럼명 통일
# =========================================================

def rename_columns(
    df: DataFrame,
    taxi_type: str,
) -> DataFrame:
    """
    택시 종류별 원본 컬럼명을
    Silver 공통 컬럼명으로 변경한다.
    """

    # 지원하지 않는 택시 종류인지 확인
    if taxi_type not in COLUMN_MAPPING:
        raise ValueError(
            f"지원하지 않는 택시 종류입니다 : {taxi_type}"
        )

    mapping = COLUMN_MAPPING[taxi_type]

    # 매핑된 컬럼을 하나씩 이름 변경
    for old_name, new_name in mapping.items():

        # 원본에 필요한 컬럼이 있는지 확인
        if old_name not in df.columns:
            raise ValueError(
                f"필수 컬럼이 존재하지 않습니다 : {old_name}"
            )

        df = df.withColumnRenamed(
            old_name,
            new_name,
        )

    return df


# =========================================================
# 없는 컬럼 NULL 생성
# =========================================================

def add_missing_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Silver 스키마에는 존재하지만
    원본 데이터에는 없는 컬럼을 추가한다.

    추가되는 컬럼의 모든 값은 NULL이다.
    """

    for field in SILVER_SCHEMA.fields:

        column = field.name
        data_type = field.dataType

        # 원본 DataFrame에 해당 컬럼이 없는 경우
        if column not in df.columns:

            logger.warning(
                f"컬럼 없음 - {column} → NULL로 추가"
            )

            # Silver 스키마에 정의된 타입으로 NULL 컬럼 생성
            df = df.withColumn(
                column,
                lit(None).cast(data_type),
            )

    return df


# =========================================================
# 데이터 타입 통일
# =========================================================

def cast_columns(
    df: DataFrame,
) -> DataFrame:
    """
    모든 데이터의 컬럼 타입을
    Silver 공통 스키마에 맞게 변환한다.
    """

    for field in SILVER_SCHEMA.fields:

        df = df.withColumn(
            field.name,
            col(field.name).cast(field.dataType),
        )

    return df


# =========================================================
# 필요한 컬럼만 선택
# =========================================================

def select_columns(
    df: DataFrame,
) -> DataFrame:
    """
    Silver에서 사용할 6개 컬럼만 선택한다.
    """

    return df.select(
        *SILVER_COLUMNS,
    )


# =========================================================
# 결측치 확인
# =========================================================

def check_null(
    df: DataFrame,
) -> DataFrame:
    """
    각 컬럼의 결측치 개수와 비율을 계산한다.

    결측치가 있다고 해서 데이터를 삭제하지 않는다.
    결과만 로그로 기록한다.
    """

    result = df.agg(
        # 전체 데이터 건수
        count("*").alias("total_count"),

        # 각 컬럼별 NULL 개수
        *[
            count(
                when(
                    col(column).isNull(),
                    column,
                )
            ).alias(column)
            for column in SILVER_COLUMNS
        ],
    ).collect()[0]

    total_count = result["total_count"]

    # 데이터 자체가 없는 경우
    if total_count == 0:

        logger.warning(
            "데이터가 존재하지 않습니다."
        )

        return df

    # 컬럼별 NULL 개수 및 비율 출력
    for column in SILVER_COLUMNS:

        null_count = result[column]

        if null_count > 0:

            null_ratio = (
                null_count / total_count
            ) * 100

            logger.warning(
                f"결측치 발견 - "
                f"{column} : "
                f"{null_count}건 "
                f"({null_ratio:.4f}%)"
            )

    return df


# =========================================================
# 전체 변환 과정
# =========================================================

def transform(
    df: DataFrame,
    taxi_type: str,
) -> DataFrame:
    """
    TLC 원본 DataFrame을
    Silver 표준 형식으로 변환한다.

    처리 순서:
    1. 컬럼명 통일
    2. 없는 컬럼 NULL 생성
    3. 데이터 타입 통일
    4. 필요한 컬럼만 선택
    5. 결측치 확인
    """

    # 1. 택시 종류별 컬럼명을 공통 이름으로 변경
    df = rename_columns(
        df,
        taxi_type,
    )

    # 2. 원본에 없는 컬럼은 NULL로 생성
    df = add_missing_columns(
        df,
    )

    # 3. 데이터 타입 통일
    df = cast_columns(
        df,
    )

    # 4. Silver에서 사용할 컬럼만 선택
    df = select_columns(
        df,
    )

    # 5. 결측치 확인
    #    → 데이터는 삭제하지 않는다.
    df = check_null(
        df,
    )

    return df


# =========================================================
# 여러 DataFrame 병합
# =========================================================

def union_all(
    dfs: list[DataFrame],
) -> DataFrame:
    """
    여러 택시 종류의 DataFrame을 하나로 합친다.

    모든 DataFrame이 동일한 Silver 스키마를
    가지고 있기 때문에 unionByName을 사용할 수 있다.
    """

    if not dfs:
        raise ValueError(
            "병합할 DataFrame이 없습니다."
        )

    return reduce(
        lambda left, right: left.unionByName(
            right,
        ),
        dfs,
    )