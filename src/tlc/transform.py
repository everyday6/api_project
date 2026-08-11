"""
TLC 데이터 변환 모듈

역할
1. 컬럼명 통일
2. 컬럼 선택
3. 결측치 확인
4. DataFrame 병합
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    count,
    when,
)

from src.common.logger import get_logger

# Logger 생성
logger = get_logger(__name__)

# 컬럼 매핑
COLUMN_MAPPING = {
    "yellow": {
        "tpep_dropoff_datetime": "dropoff_datetime",
        "DOLocationID": "dropoff_location_id",
    },
    "green": {
        "lpep_dropoff_datetime": "dropoff_datetime",
        "DOLocationID": "dropoff_location_id",
    },
    "fhv": {
        "dropOff_datetime": "dropoff_datetime",
        "DOlocationID": "dropoff_location_id",
    },
    "hvfhv": {
        "dropoff_datetime": "dropoff_datetime",
        "DOLocationID": "dropoff_location_id",
    },
}

# Silver 컬럼
SILVER_COLUMNS = [
    "dropoff_datetime",
    "dropoff_location_id",
]


def rename_columns(
    df: DataFrame,
    taxi_type: str,
) -> DataFrame:
    """컬럼명 통일"""

    if taxi_type not in COLUMN_MAPPING:

        raise ValueError(
            f"지원하지 않는 택시 종류입니다 : {taxi_type}"
        )

    mapping = COLUMN_MAPPING[taxi_type]

    for old_name, new_name in mapping.items():

        # 컬럼 존재 여부 확인
        if old_name not in df.columns:

            raise ValueError(
                f"필수 컬럼이 존재하지 않습니다 : {old_name}"
            )

        df = df.withColumnRenamed(
            old_name,
            new_name,
        )

    return df


def select_columns(
    df: DataFrame,
) -> DataFrame:
    """필요한 컬럼 선택"""

    return df.select(
        *SILVER_COLUMNS,
    )


def check_null(
    df: DataFrame,
) -> DataFrame:
    """결측치 확인"""

    # 전체 건수 + 컬럼별 결측치 집계
    result = df.agg(
        count("*").alias("total_count"),
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

    if total_count == 0:

        logger.warning(
            "데이터가 존재하지 않습니다."
        )

        return df

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


def transform(
    df: DataFrame,
    taxi_type: str,
) -> DataFrame:
    """DataFrame 변환"""

    df = rename_columns(
        df,
        taxi_type,
    )

    df = select_columns(
        df,
    )

    df = check_null(
        df,
    )

    return df


def union_all(
    dfs: list[DataFrame],
) -> DataFrame:
    """DataFrame 병합"""

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