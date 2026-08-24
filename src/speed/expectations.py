"""speed Bronze Great Expectations 정의.

TLC(`src/tlc/expectations.py`)와 같은 critical/log_only 2단 구조를
따르되, speed는 taxi_type 같은 분기가 없어서 목록이 고정이다.
"""

from datetime import datetime, timedelta

import great_expectations as gx

# 다운스트림(clean_speed_silver1)이 실제로 참조하는 컬럼만 critical로
# 잡는다 - API가 주는 나머지 9개 컬럼(status/owner/borough 등)은 우리
# 파이프라인이 안 쓰므로 사라져도 critical이 아니다.
_REQUIRED_COLUMNS = ["speed", "link_points", "data_as_of", "link_id"]

# 개별 센서의 산발적 결측(노이즈)은 넘기고, 스키마 드리프트처럼 값이
# 뭉텅이로 비는 경우만 잡기 위한 허용치.
_NULL_TOLERANCE = 0.90

# 이 데이터셋 생성일자(Socrata 메타데이터 createdAt=2017-04-17) 기준.
_DATA_AS_OF_MIN = datetime(2017, 1, 1)


def critical_expectations() -> list:
    """실패 시 이번 파이프라인 사이클을 스킵해야 하는 검증.

    컬럼이 실제로 사라졌는지(존재 여부)만 본다 - 값 이상은
    log_only_expectations로 다룬다.
    """

    return [
        gx.expectations.ExpectColumnToExist(column=column)
        for column in _REQUIRED_COLUMNS
    ]


def log_only_expectations() -> list:
    """실패해도 배치 처리는 계속하되 Slack 알림을 보내는 검증.

    컬럼은 있지만 값이 이상한 경우(null 급증, 범위 이탈)를 잡는다.
    """

    return [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
        *[
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column, mostly=_NULL_TOLERANCE)
            for column in _REQUIRED_COLUMNS
        ],
        gx.expectations.ExpectColumnValuesToBeBetween(column="speed", min_value=0, max_value=150),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="data_as_of",
            min_value=_DATA_AS_OF_MIN,
            max_value=datetime.now() + timedelta(days=1),
        ),
    ]
