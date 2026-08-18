"""TLC Bronze taxi_type별 Great Expectations 정의.

컬럼 구성은 src.tlc.transform.COLUMN_MAPPING(원본 컬럼명 → Silver 컬럼명)을
뒤집어 재사용한다 — taxi_type별 컬럼 구성을 여기 따로 하드코딩하면
transform.py와 어긋날 위험이 있다.
"""

import great_expectations as gx

from src.tlc.transform import COLUMN_MAPPING


# dropoff_datetime/dropoff_location_id는 Silver의 traffic score 분석(세그먼트별
# 하차 위치·시각 집계)에 직접 쓰이는 핵심 값이라, 원본 컬럼 자체가 없으면
# critical로 다룬다. 그 외 컬럼이 없거나 값이 이상한 경우는 로그만 남긴다.
CRITICAL_COLUMNS = ["dropoff_datetime", "dropoff_location_id"]


def _raw_columns(taxi_type: str) -> dict:
    """taxi_type의 Silver 컬럼명 → 원본 컬럼명 매핑 (COLUMN_MAPPING의 역방향)."""

    return {
        silver_name: raw_name
        for raw_name, silver_name in COLUMN_MAPPING[taxi_type].items()
    }


def critical_expectations(taxi_type: str) -> list:
    """실패 시 파일을 Silver로 넘기지 않고 제외해야 하는 검증."""

    columns = _raw_columns(taxi_type)
    return [
        gx.expectations.ExpectColumnToExist(column=columns[name])
        for name in CRITICAL_COLUMNS
    ]


def log_only_expectations(taxi_type: str) -> list:
    """실패해도 로그만 남기고 파일은 계속 Silver로 진행하는 검증.

    passenger_count/trip_distance는 taxi_type에 따라 원본에 아예 없을 수
    있으므로(COLUMN_MAPPING 참고), 그 taxi_type에 실제로 존재하는 컬럼에
    대해서만 검증을 추가한다.
    """

    columns = _raw_columns(taxi_type)

    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
        gx.expectations.ExpectColumnToExist(column=columns["pickup_datetime"]),
        gx.expectations.ExpectColumnToExist(column=columns["pickup_location_id"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["pickup_datetime"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["dropoff_datetime"]),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=columns["pickup_location_id"], min_value=1, max_value=263,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=columns["dropoff_location_id"], min_value=1, max_value=263,
        ),
    ]

    if "passenger_count" in columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=columns["passenger_count"], min_value=0, max_value=None,
            )
        )

    if "trip_distance" in columns:
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=columns["trip_distance"], min_value=0, max_value=None,
            )
        )

    return expectations
