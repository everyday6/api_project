"""
DAG: ingest_weekly

도로 통제(road_closures) ingestion. 매주 월요일 새벽에 실행되며,
Airflow가 자동으로 계산해주는 "이번 주 구간(data_interval)"을
ingest_road_closures(start_date, end_date)에 그대로 전달한다.

실제 ingestion 로직은 src/road_closures/bronze.py에 있고, 이 파일은
언제/어떤 파라미터로 그 함수를 실행할지만 정의한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.road_closures.bronze import ingest_road_closures

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_weekly",
    description="도로 통제(road_closures) 주간 ingestion",
    schedule="0 4 * * 1",          # 매주 월요일 새벽 4시
    start_date=datetime(2025, 1, 1),
    catchup=False,                  # 과거분은 backfill_road_closures.py로 이미 채웠으므로 여기선 밀린 것 자동 실행 안 함
    default_args=default_args,
    tags=["bronze", "weekly"],
) as dag:

    task_ingest_road_closures = PythonOperator(
        task_id="ingest_road_closures",
        python_callable=ingest_road_closures,
        op_kwargs={
            # Airflow가 이번 실행이 담당하는 구간(지난 월요일~이번 월요일)을 자동으로 채워줌
            "start_date": "{{ data_interval_start | ds }}",
            "end_date": "{{ data_interval_end | ds }}",
        },
    )