"""TLC Bronze taxi_type별 Great Expectations 정의.

컬럼 구성은 src.tlc.transform.COLUMN_MAPPING(원본 컬럼명 → Silver 컬럼명)을
뒤집어 재사용한다 — taxi_type별 컬럼 구성을 여기 따로 하드코딩하면
transform.py와 어긋날 위험이 있다.
"""

import great_expectations as gx

from src.tlc.transform import COLUMN_MAPPING


def _raw_columns(taxi_type: str) -> dict:
    """taxi_type의 Silver 컬럼명 → 원본 컬럼명 매핑 (COLUMN_MAPPING의 역방향)."""

    if taxi_type not in COLUMN_MAPPING:
        raise ValueError(f"지원하지 않는 택시 종류입니다 : {taxi_type}")

    return {
        silver_name: raw_name
        for raw_name, silver_name in COLUMN_MAPPING[taxi_type].items()
    }


def critical_expectations(taxi_type: str) -> list:
    """실패 시 파일을 Silver로 넘기지 않고 제외해야 하는 검증.

    이 taxi_type이 요구하는 모든 원본 컬럼(COLUMN_MAPPING 기준)의 존재
    여부를 검사한다. rename_columns()는 이 중 하나라도 없으면 ValueError를
    던지고, build_silver는 청크 전체를 실패 처리하므로 — 컬럼 존재 여부는
    "일부만 critical"이 아니라 요구되는 컬럼 전부가 critical이어야 한다.
    """

    columns = _raw_columns(taxi_type)
    return [
        gx.expectations.ExpectColumnToExist(column=raw_name)
        for raw_name in columns.values()
    ]


def log_only_expectations(taxi_type: str) -> list:
    """실패해도 로그만 남기고 파일은 계속 Silver로 진행하는 검증.

    컬럼 존재 여부는 더 이상 여기서 다루지 않는다(critical_expectations로
    이동) — 값 범위/결측치 등 "컬럼은 있지만 값이 이상한" 경우만 다룬다.

    passenger_count/trip_distance는 taxi_type에 따라 원본에 아예 없을 수
    있으므로(COLUMN_MAPPING 참고), 그 taxi_type에 실제로 존재하는 컬럼에
    대해서만 검증을 추가한다.
    """

    columns = _raw_columns(taxi_type)

    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["pickup_datetime"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["dropoff_datetime"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["pickup_location_id"]),
        gx.expectations.ExpectColumnValuesToNotBeNull(column=columns["dropoff_location_id"]),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=columns["pickup_location_id"], min_value=1, max_value=265,
        ),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column=columns["dropoff_location_id"], min_value=1, max_value=265,
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
