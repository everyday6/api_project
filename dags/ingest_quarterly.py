"""
DAG: ingest_quarterly

NYC DCP LION(도로망) ingestion + dim_segment Silver 변환. 분기마다 새 릴리즈가
나오는 전체 스냅샷 데이터라, 증분 개념 없이 매번 통째로 받는다.

실제 로직은 src/lion/bronze.py(적재), src/lion/silver.py(dim_segment 변환 +
검증), src/mapping/zone_segment.py(dim_segment x Taxi Zone 매핑 + 검증)에
있고, 이 파일은 언제/어떤 순서로 그 함수들을 실행할지만 정의한다.

map_zone_segment는 Taxi Zone(정적 참조 데이터, ingest_static DAG)도 필요하다.
Taxi Zone은 거의 안 바뀌는 데이터라 별도 DAG 의존성 연결 없이, 이미 Bronze에
적재돼 있다고 가정한다 (없으면 이 태스크가 바로 실패해서 알 수 있음).
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.lion.bronze import ingest_lion
from src.lion.silver import build_dim_segment, validate_dim_segment
from src.mapping.zone_segment import build_map_zone_segment, validate_map_zone_segment

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="ingest_quarterly",
    description="LION(도로망) 분기 ingestion + dim_segment 변환",
    schedule="0 5 1 1,4,7,10 *",     # 1/4/7/10월 1일 새벽 5시
    start_date=datetime(2025, 1, 1),
    catchup=False,                    # 과거 분기 버전은 지금 굳이 안 채움 (최신 버전이면 충분)
    default_args=default_args,
    tags=["bronze", "quarterly"],
) as dag:

    task_ingest_lion = PythonOperator(
        task_id="ingest_lion",
        python_callable=ingest_lion,
        op_kwargs={
            # 실행일을 그대로 버전 태그로 사용 (파일명이 아니라 "언제 받았는지" 기준)
            "version_date": "{{ ds }}",
        },
    )

    task_build_dim_segment = PythonOperator(
        task_id="build_dim_segment",
        python_callable=build_dim_segment,
        # bronze_root/silver_root 둘 다 기본값(common.config 기준) 사용.
        # 최신 version_date 파티션을 스스로 찾아서 읽으므로 XCom 연결 불필요.
    )

    task_validate_dim_segment = PythonOperator(
        task_id="validate_dim_segment",
        python_callable=validate_dim_segment,
        op_kwargs={
            # build_dim_segment의 리턴값(저장 경로)을 XCom으로 받아서 그대로 검증한다.
            "path": "{{ ti.xcom_pull(task_ids='build_dim_segment') }}",
        },
    )

    task_build_map_zone_segment = PythonOperator(
        task_id="build_map_zone_segment",
        python_callable=build_map_zone_segment,
        # dim_segment_root/zone_shapefile_path 둘 다 기본값(common.config 기준) 사용.
    )

    task_validate_map_zone_segment = PythonOperator(
        task_id="validate_map_zone_segment",
        python_callable=validate_map_zone_segment,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='build_map_zone_segment') }}",
        },
    )

    (
        task_ingest_lion
        >> task_build_dim_segment
        >> task_validate_dim_segment
        >> task_build_map_zone_segment
        >> task_validate_map_zone_segment
    )