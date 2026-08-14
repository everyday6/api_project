"""
DAG: ingest_static

정적/저빈도 참조 테이블(Taxi Zone lookup + shapefile) ingestion.
스케줄 없음 (schedule=None) — 필요할 때 Airflow UI에서 수동으로 Trigger.

실제 ingestion/검증 로직은 src/taxi_zone/bronze.py에 있고, 이 파일은 그 함수들을
언제/어떤 task로 실행할지만 정의한다. lookup/shapefile 각각 build >> validate로
분리해서, validate 실패 시 재시도할 때 다운로드부터 다시 하지 않아도 되게 한다.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.taxi_zone.bronze import (
    ingest_taxi_zone_lookup,
    ingest_taxi_zone_shapefile,
    validate_taxi_zone_lookup,
    validate_taxi_zone_shapefile,
)

default_args = {
    "retries": 2,
}

with DAG(
    dag_id="ingest_static",
    description="정적/저빈도 참조 테이블 (Taxi Zone lookup, shapefile) 수동 실행용",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bronze", "static"],
) as dag:

    task_lookup = PythonOperator(
        task_id="ingest_taxi_zone_lookup",
        python_callable=ingest_taxi_zone_lookup,
    )

    task_validate_lookup = PythonOperator(
        task_id="validate_taxi_zone_lookup",
        python_callable=validate_taxi_zone_lookup,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='ingest_taxi_zone_lookup') }}",
        },
    )

    task_shapefile = PythonOperator(
        task_id="ingest_taxi_zone_shapefile",
        python_callable=ingest_taxi_zone_shapefile,
    )

    task_validate_shapefile = PythonOperator(
        task_id="validate_taxi_zone_shapefile",
        python_callable=validate_taxi_zone_shapefile,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='ingest_taxi_zone_shapefile') }}",
        },
    )

    # lookup/shapefile 두 체인은 서로 의존관계 없음 -> 병렬 실행
    task_lookup >> task_validate_lookup
    task_shapefile >> task_validate_shapefile