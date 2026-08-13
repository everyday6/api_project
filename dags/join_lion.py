from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.mapping.ticketmaster_lion import (
    build_ticketmaster_lion_mapping,
)
from src.mapping.event_lion import (
    build_event_lion_mapping,
)


DEFAULT_ARGS = {
    "owner": "jiwon",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


def run_ticketmaster_lion(**context):
    run_date = context["ds"]

    return build_ticketmaster_lion_mapping(
        run_date
    )


def run_event_lion(**context):
    run_date = context["ds"]

    return build_event_lion_mapping(
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

    event_lion = PythonOperator(
        task_id="event_lion_mapping",
        python_callable=run_event_lion,
    )

    [ticketmaster_lion, event_lion]