"""검증이 잡아낸 행 단위 이상치를 저장되는 데이터에 표시하는 공통 인터페이스.

RELIABILITY_PRINCIPLES.md "처음 잡을 때 할 일" 3번 - "검증 실행과 그 결과의
영속화를 분리하면(TLC 사례) 검증이 사실상 무의미해진다"에 대한 최소 구현이다.
각 도메인(speed/tlc/lion)의 `mark_suspect_rows()`는 어떤 컬럼·어떤 범위를
이상치로 볼지(도메인마다 다르고, 각 도메인 expectations.py의 조건을 그대로
반영한다)만 정하고, "복사본에 bool 컬럼을 붙인다 / NULL은 정상으로 확정한다 /
컬럼 이름은 무엇이다"라는 기계적인 부분은 여기로 모은다.

`is_suspect`는 "이 행은 틀렸다"가 아니라 "이 행은 신뢰도가 낮으니 downstream이
구분해서 다뤄라"는 표시다(원칙 0-1). 전면적인 quarantine(격리)이 아니라
표시만 남기는 단계다.
"""

from __future__ import annotations

import json
import logging

import pandas as pd

# 저장되는 데이터에 붙는 표준 컬럼명. speed/tlc/lion과 그 테스트가 모두
# 이 상수를 참조해, 리터럴 문자열이 여기저기 흩어지지 않게 한다.
IS_SUSPECT_COLUMN = "is_suspect"


def flag_suspect_pandas(df: pd.DataFrame, suspect_mask: pd.Series) -> pd.DataFrame:
    """원본을 건드리지 않고 `is_suspect`(bool) 컬럼을 붙인 복사본을 반환한다.

    suspect_mask의 NA는 False(정상)로 확정한다 - GX의
    ExpectColumnValuesToBeBetween이 null을 불통과로 세지 않는 것과 같은 취급.
    """
    result = df.copy()
    result[IS_SUSPECT_COLUMN] = (
        suspect_mask.reindex(df.index).fillna(False).astype(bool)
    )
    return result


def flag_suspect_spark(df, suspect_condition):
    """Spark 버전. NULL로 새는 boolean 표현식을 False(정상)로 확정한다
    (flag_suspect_pandas와 동일 취급). pyspark는 이 함수가 실제로 호출되는
    EMR Spark 잡 안에서만 import된다 - pandas만 쓰는 speed/lion 경로에
    pyspark 의존을 강제하지 않으려고 지연 import한다."""
    from pyspark.sql import functions as F

    return df.withColumn(IS_SUSPECT_COLUMN, F.coalesce(suspect_condition, F.lit(False)))


def suspect_ratio(df: pd.DataFrame) -> float:
    """pandas df에서 `is_suspect`=True인 행의 비율(0.0~1.0).

    비율 기반 publish 게이트(예: src/lion/silver1.py의
    validate_dim_segment_base)에서 쓴다. 컬럼이 없으면 조용히 0을 주는 대신
    명시적으로 실패한다 - mark_suspect_rows를 안 거친 데이터를 통과시키는
    사고를 막는다.
    """
    if IS_SUSPECT_COLUMN not in df.columns:
        raise KeyError(
            f"{IS_SUSPECT_COLUMN!r} 컬럼이 없습니다 - mark_suspect_rows를 먼저 적용해야 합니다"
        )
    if len(df) == 0:
        return 0.0
    return float(df[IS_SUSPECT_COLUMN].mean())


def log_quality_gate(
    logger: logging.Logger,
    *,
    domain: str,
    metric: str,
    value: float,
    threshold: float,
    passed: bool,
    **extra: object,
) -> None:
    """데이터 품질 게이트의 판정을 통과/차단 상관없이 한 줄 구조화 로그로 남긴다.

    지금까지 게이트는 **차단할 때만** 비율을 로그에 남겼다. 그러면 사후에
    "평소 1%인데 30%로 튄 것"과 "평소 19%인데 21%로 튄 것"을 구분할 수
    없다 - 정상 판정의 분포 자체가 baseline이라, 매 판정을 남겨야 임계값
    (MAX_SUSPECT_RATIO 등)을 실측으로 조정할 근거가 쌓인다
    (RELIABILITY_PRINCIPLES.md Tier 1 #3, 열린 질문의 "임계값 baseline" 항목).

    차단 경로의 사람용 error/raise/Slack은 각 호출부에 그대로 둔다 - 이건
    그 위에 얹는 기계 판독용(Grafana/Loki 집계) 한 줄이다. `event` 키가
    고정 토큰이라 로그에서 바로 필터할 수 있다.
    """
    payload = {
        "event": "data_quality_gate",
        "domain": domain,
        "metric": metric,
        "value": round(float(value), 6),
        "threshold": threshold,
        "decision": "pass" if passed else "block",
        **extra,
    }
    logger.info("data_quality_gate %s", json.dumps(payload, sort_keys=True, ensure_ascii=False))
