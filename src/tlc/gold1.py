"""
Gold1 — TLC 관련성 필터

tlc Silver1(전체 요일, zone_id 결측 포함)에서 통행량 집계에 쓸 "평일 트립"과
"zone_id를 아는 트립"만 남긴다. 평일 필터는 집계 전 원본 트립(Spark
DataFrame) 단계에서, zone_id 결측 필터는 집계(zone x hour 카운트) 이후
(pandas DataFrame) 단계에서 적용된다 — 필터 대상 데이터의 형태가 서로
달라 두 함수로 나뉜다. 실제 집계(groupBy)는 새 지표(카운트)를 만드는
연산이라 src/tlc/gold2.py의 몫이다.
"""

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofweek

import pandas as pd

from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_gold")


def filter_weekday(df: DataFrame) -> DataFrame:
    """평일(월~금) 트립만 남긴다. Spark의 dayofweek: 일요일=1 ~ 토요일=7."""
    return df.filter(dayofweek(col("dropoff_datetime")).between(2, 6))


def drop_null_zone(result: pd.DataFrame) -> pd.DataFrame:
    """zone_id(dropoff_location_id) 결측 행을 제외한다.

    실 데이터의 dropoff_location_id는 nullable(src/tlc/silver1.py의
    SILVER_SCHEMA)이고 Silver1은 결측치를 삭제하지 않는다. Spark groupBy는
    NULL도 자기 그룹으로 유지하므로, 여기서 걸러내지 않으면 zone_id 컬럼에
    NaN이 남아 이후 int64 캐스팅이 깨진다.
    """
    null_zone = result["zone_id"].isna()
    if null_zone.any():
        dropped = int(result.loc[null_zone, "dropoff_count"].sum())
        logger.warning(f"[tlc_gold] dropoff_location_id 결측으로 제외: {dropped}건")
        result = result.loc[~null_zone].copy()
    return result
