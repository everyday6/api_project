"""Construction Bronze Expectation 정의.

필수 컬럼 목록은 src.construction.silver1.READ_COLS를 그대로 재사용한다 —
Silver가 실제로 무엇을 요구하는지와 어긋나지 않게 하기 위해서다.

컬럼 존재 여부는 전부 critical이다. Silver의 load_bronze()가 READ_COLS를
명시해서 읽고, rename/validate 과정에서 permitnumber의 null/중복도 각각
raise하므로 — "일부만 critical, 나머지는 log-only"로 나누면 log-only로
통과시킨 파일이 결국 Silver에서 죽는 모순이 생긴다(TLC에서 실제로 겪은
문제). row count > 0도 같은 이유로 critical이다: 행이 0개면 Silver의
validate()가 빈 결과에 raise한다.
"""

import great_expectations as gx

from src.construction.silver1 import READ_COLS


def critical_expectations() -> list:
    """실패 시 Bronze 검증을 실패로 처리해야 하는 검증."""

    expectations = [
        gx.expectations.ExpectTableRowCountToBeBetween(min_value=1, max_value=None),
    ]

    expectations.extend(
        gx.expectations.ExpectColumnToExist(column=column)
        for column in READ_COLS
    )

    expectations.append(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="permitnumber")
    )
    expectations.append(
        gx.expectations.ExpectColumnValuesToBeUnique(column="permitnumber")
    )

    return expectations


def log_only_expectations() -> list:
    """실패해도 로그만 남기고 파이프라인은 계속 진행하는 검증."""

    return [
        gx.expectations.ExpectColumnValuesToBeDateutilParseable(
            column="issuedworkstartdate"
        ),
        gx.expectations.ExpectColumnValuesToBeDateutilParseable(
            column="issuedworkenddate"
        ),
        # 실측 기준(2026-08-18 스냅샷) permitlinearfeet의 non-null 비율은
        # 약 41%다 — 이 필드가 원래도 자주 비어 있는 정부 데이터라, null
        # 자체를 100% 기준으로 걸면 매 실행마다 걸려 로그가 무의미해진다.
        # 그 비율이 30% 밑으로 떨어지는 경우만(=이례적으로 더 나빠졌을 때만)
        # 잡아낸다.
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="permitlinearfeet", mostly=0.3
        ),
    ]
