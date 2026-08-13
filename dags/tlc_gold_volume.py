"""
DAG: tlc_gold_volume

TLC 하차(dropoff) 데이터를 세그먼트x평일시간대(0~23시) 단위로 집계한
dim_segment_tlc_volume Gold 테이블을 만든다. 맨해튼 세그먼트만 대상이며,
실행할 때마다 그 시점에 존재하는 TLC silver 파일 전부를 다시 읽어 처음부터
계산한다(증분 아님).

실제 로직은 src/tlc/gold.py(집계 + 빌드 + 검증)에 있고, 이 파일은 그 함수들을
언제/어떤 순서로 실행할지만 정의한다.

의존성: map_zone_segment.parquet(ingest_quarterly DAG 산출물)이 먼저 있어야 한다
(없으면 이 태스크가 바로 실패해서 알 수 있음). TLC silver 파일도 tlc_pipeline /
ingest_daily / ingest_weekly로 이미 적재돼 있어야 한다.

지금은 검증 목적의 수동 트리거만 지원한다(schedule=None). 운영 주기는 확정되면
추가한다.
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.spark import get_spark
from src.tlc.gold import build_dim_segment_tlc_volume, validate_dim_segment_tlc_volume

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def _build_dim_segment_tlc_volume():
    """PythonOperator에서 Spark 세션 생명주기를 관리하며 빌드 함수를 호출한다."""

    spark = get_spark()
    try:
        return build_dim_segment_tlc_volume(spark)
    finally:
        spark.stop()


with DAG(
    dag_id="tlc_gold_volume",
    description="TLC 세그먼트x평일시간대 통행량 Gold 테이블 생성 (맨해튼 한정)",
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["gold", "tlc", "manual"],
) as dag:

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

    task_build_dim_segment_tlc_volume >> task_validate_dim_segment_tlc_volume
