"""
DAG: tlc_gold_volume

TLC 하차(dropoff) 데이터를 세그먼트x평일시간대(0~23시) 단위로 집계한
dim_segment_tlc_volume Gold 테이블을 만든다. 맨해튼 세그먼트만 대상이며,
실행할 때마다 그 시점에 존재하는 TLC silver 파일 전부를 다시 읽어 처음부터
계산한다(증분 아님).

실제 로직은 src/tlc/gold.py(집계 + 빌드 + 검증)에 있고, 이 파일은 그 함수들을
언제/어떤 순서로 실행할지만 정의한다.

collect_zone_hour_counts(3년치 Silver 전체를 스캔하는 무거운 부분)와
build_dim_segment_tlc_volume(집계 결과를 세그먼트로 펼치고 정규화해 저장하는
가벼운 후처리)를 별도 태스크로 나눴다. 후자가 실패해도(예: 저장 경로 문제)
전자를 다시 돌릴 필요가 없다. 전자의 결과(zone x hour 집계, 최대 수천 행)는
작아서 XCom으로 그대로 넘긴다.

의존성: map_zone_segment.parquet(ingest_quarterly DAG 산출물)이 먼저 있어야 한다
(없으면 이 태스크가 바로 실패해서 알 수 있음). TLC silver 파일도 tlc_pipeline /
ingest_daily / ingest_weekly로 이미 적재돼 있어야 한다.

지금은 검증 목적의 수동 트리거만 지원한다(schedule=None). 운영 주기는 확정되면
추가한다.
"""

from datetime import timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context

from src.common.alerts import notify_slack_failure
from src.common.spark import get_spark
from src.tlc.gold import (
    build_dim_segment_tlc_volume,
    collect_zone_hour_counts,
    validate_dim_segment_tlc_volume,
)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

# collect_zone_hour_counts가 반환하는 DataFrame의 컬럼 순서.
# XCom으로는 레코드(list[dict])로 오가는데, 다음 태스크에서 DataFrame으로
# 복원할 때 이 순서를 그대로 쓴다.
ZONE_HOUR_COUNTS_COLUMNS = ["zone_id", "hour", "dropoff_count"]


def _collect_zone_hour_counts():
    """PythonOperator에서 Spark 세션 생명주기를 관리하며 집계 함수를 호출한다.

    반환값(zone x hour, 최대 수천 행)은 작아서 XCom으로 그대로 넘긴다.
    """

    spark = get_spark()
    try:
        df = collect_zone_hour_counts(spark)
    finally:
        spark.stop()

    return df.to_dict(orient="records")


def _build_dim_segment_tlc_volume():
    """앞 태스크가 XCom으로 넘긴 zone x hour 집계 결과를 받아 Gold를 만든다."""

    context = get_current_context()
    records = context["ti"].xcom_pull(task_ids="collect_zone_hour_counts")

    zone_hour_counts = pd.DataFrame(records, columns=ZONE_HOUR_COUNTS_COLUMNS)

    return build_dim_segment_tlc_volume(zone_hour_counts)


with DAG(
    dag_id="tlc_gold_volume",
    description="TLC 세그먼트x평일시간대 통행량 Gold 테이블 생성 (맨해튼 한정)",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gold", "tlc", "manual"],
) as dag:

    task_collect_zone_hour_counts = PythonOperator(
        task_id="collect_zone_hour_counts",
        python_callable=_collect_zone_hour_counts,
    )

    task_build_dim_segment_tlc_volume = PythonOperator(
        task_id="build_dim_segment_tlc_volume",
        python_callable=_build_dim_segment_tlc_volume,
    )

    task_validate_dim_segment_tlc_volume = PythonOperator(
        task_id="validate_dim_segment_tlc_volume",
        python_callable=validate_dim_segment_tlc_volume,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='build_dim_segment_tlc_volume') }}",
        },
    )

    (
        task_collect_zone_hour_counts
        >> task_build_dim_segment_tlc_volume
        >> task_validate_dim_segment_tlc_volume
    )
