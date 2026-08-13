"""
DAG — Join LION

역할
- source별 Silver 데이터를 LION segment와 매핑
- 현재는 Ticketmaster -> LION 매핑 실행
- 추후 construction / event 매핑 추가 가능
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.mapping.ticketmaster_lion import (
    build_ticketmaster_lion_mapping,
)


DEFAULT_ARGS = {
    "owner": "jiwon",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def run_ticketmaster_lion(**context):
    """
    Airflow logical date를 run_date로 넘겨
    Ticketmaster-LION 매핑을 실행한다.
    """

    run_date = context["ds"]

    return build_ticketmaster_lion_mapping(
        run_date
    )


with DAG(
    dag_id="join_lion",
    description="Silver 데이터를 LION segment와 매핑",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=[
        "lion",
        "mapping",
    ],
) as dag:

    ticketmaster_lion = PythonOperator(
        task_id="ticketmaster_lion_mapping",
        python_callable=run_ticketmaster_lion,
    )

    ticketmaster_lion