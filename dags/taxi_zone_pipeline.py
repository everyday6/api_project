"""TLC Taxi Zone Bronze/Silver1 적재 DAG.

원래 schedule=None(수동 트리거) "1회성" DAG였는데, ingest_taxi_zone_shapefile이
ETag 기반 변경 감지를 갖추면서(src/taxi_zone/bronze.py 참고) 매달 다시
돌려도 안전해졌다 — 원본이 그대로면 다운로드도, Silver1 재생성도,
Asset emit도 전부 스킵한다. Wayback Machine으로 실측한 실제 변경 주기가
1~2년에 한 번이라(2024-03~2024-10 무변경, 다음 변경 2026-02), 매달 확인하면
탐지 지연 최대 1개월로 충분히 빠르면서 불필요한 재계산은 없다.

build_taxi_zone_silver1은 Asset("taxi_zone_silver1_updated")를 outlet으로
내보낸다 — zone_segment_pipeline이 lion_pipeline의 lion_dim_segment_ready와
함께 이 Asset을 구독해서, 둘 중 하나만 갱신돼도 자동으로 재실행된다
(TriggerDagRunOperator로 DAG 이름을 직접 지정하던 방식에서 전환 — 두 소스가
같은 날 겹치면 중복 실행되던 문제가 있었음). build_taxi_zone_silver1이
변경 없음으로 스킵되면 이 Asset도 emit되지 않아 zone_segment_pipeline이
매달 헛돌지 않는다.
"""

from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.taxi_zone.bronze import ingest_taxi_zone_shapefile
from src.taxi_zone.silver1 import build as build_taxi_zone_silver1

default_args = {
    "retries": 2,
    "on_failure_callback": notify_slack_failure,
}

TAXI_ZONE_SILVER1_UPDATED = Asset("taxi_zone_silver1_updated")

with DAG(
    dag_id="taxi_zone_pipeline",
    description="TLC Taxi Zone 정적 참조 데이터 S3 Bronze/Silver1 적재 (월 1회 변경 확인)",
    schedule="0 4 1 * *",          # 매월 1일 새벽 4시
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["taxi_zone", "monthly"],
) as dag:
    shapefile = PythonOperator(
        task_id="ingest_taxi_zone_shapefile",
        python_callable=ingest_taxi_zone_shapefile,
    )
    silver1 = PythonOperator(
        task_id="build_taxi_zone_silver1",
        python_callable=build_taxi_zone_silver1,
        op_kwargs={
            "shapefile_result": "{{ ti.xcom_pull(task_ids='ingest_taxi_zone_shapefile') }}",
        },
        outlets=[TAXI_ZONE_SILVER1_UPDATED],
    )

    shapefile >> silver1
