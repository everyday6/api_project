"""
DAG: lion_pipeline

NYC DCP LION(도로망) Bronze ingestion만 담당하는 도메인 파이프라인.
분기마다 새 릴리즈가 나오는 전체 스냅샷 데이터라, 증분 개념 없이 매번
통째로 받는다.

실제 로직은 src/lion/bronze.py(적재)에 있고, 이 파일은 언제 실행할지만
정의한다. dim_segment 등 파생 Silver/Gold 산출물은 nav 골드 데이터셋
재설계 범위에서 다시 만든다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.alerts import notify_slack_failure
from src.lion.bronze import ingest_lion

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

with DAG(
    dag_id="lion_pipeline",
    description="LION(도로망) 분기 Bronze",
    schedule="0 5 1 1,4,7,10 *",     # 1/4/7/10월 1일 새벽 5시
    start_date=datetime(2025, 1, 1),
    catchup=False,                    # 과거 분기 버전은 지금 굳이 안 채움 (최신 버전이면 충분)
    default_args=default_args,
    tags=["lion", "quarterly"],
) as dag:

    task_ingest_lion = PythonOperator(
        task_id="ingest_lion",
        python_callable=ingest_lion,
        op_kwargs={
            # 실행일을 그대로 버전 태그로 사용 (파일명이 아니라 "언제 받았는지" 기준)
            "version_date": "{{ ds }}",
        },
    )
