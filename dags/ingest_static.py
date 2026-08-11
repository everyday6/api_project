"""
DAG: ingest_static

정적/저빈도 참조 테이블(Taxi Zone lookup + shapefile, Parks Properties) ingestion.
스케줄 없음 (schedule=None) — 필요할 때 Airflow UI에서 수동으로 Trigger.

실제 ingestion 로직은 각각 src/taxi_zone/bronze.py, src/parks_properties/bronze.py에
있고, 이 파일은 그 함수들을 언제/어떤 task로 실행할지만 정의한다.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.taxi_zone.bronze import ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile
from src.parks_properties.bronze import ingest_parks_properties

default_args = {
    "retries": 2,
}

with DAG(
    dag_id="ingest_static",
    description="정적/저빈도 참조 테이블 (Taxi Zone lookup, shapefile, Parks Properties) 수동 실행용",
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

    task_shapefile = PythonOperator(
        task_id="ingest_taxi_zone_shapefile",
        python_callable=ingest_taxi_zone_shapefile,
    )

    task_parks_properties = PythonOperator(
        task_id="ingest_parks_properties",
        python_callable=ingest_parks_properties,
        op_kwargs={
            # 실행일을 그대로 버전 태그로 사용 (lion DAG와 동일한 방식)
            "version_date": "{{ ds }}",
        },
    )

    # 셋 다 서로 의존관계 없음 -> 병렬 실행
    [task_lookup, task_shapefile, task_parks_properties]