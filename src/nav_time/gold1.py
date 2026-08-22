"""
Gold1 — 유효 속도만 남긴다.

type1(시간) 버킷 평균 계산의 입력을 좁힌다: 0 이하(또는 비정상적으로 낮은)
속도 판독값은 제외한다.

(과거엔 "최근 N일" 롤링 윈도우 필터도 있었지만, 실제로 한 버킷 계산 시점엔
그 30분 동안 들어온 판독값 파일 하나만 입력으로 들어와서 날짜 필터는
죽은 코드였다 — 최종 리뷰 C4로 제거했다.)
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql.functions import col

from src.common.config import MIN_VALID_SPEED_MPH


def filter_valid_speed(df: DataFrame) -> DataFrame:
    return df.filter(col("speed") >= MIN_VALID_SPEED_MPH)
