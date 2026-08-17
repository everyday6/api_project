"""
DAG: lion_pipeline

NYC DCP LION(도로망) ingestion + dim_segment Silver 변환 + 파생 테이블(존
매핑, 인접 그래프, traffic_score 기초값)까지 담당하는 도메인 파이프라인.
분기마다 새 릴리즈가 나오는 전체 스냅샷 데이터라, 증분 개념 없이 매번
통째로 받는다.

실제 로직은 src/lion/bronze.py(적재), src/lion/silver.py(dim_segment 변환 +
검증), src/mapping/zone_segment.py(dim_segment x Taxi Zone 매핑 + 검증),
src/lion/segment_adjacency.py(세그먼트 인접 그래프 + 검증),
src/lion/traffic_score.py(매개중심성 기반 traffic_score_v0 + 검증)에 있고,
이 파일은 언제/어떤 순서로 그 함수들을 실행할지만 정의한다.

build_dim_segment_traffic_score는 매개중심성 근사 계산 때문에 k=1000 기준
약 8~9분 걸린다(직접 측정함) — 분기 1회 배치라 문제없는 수준이다. 이 값이
물리적으로는 Silver 폴더에 저장되지만 매개중심성 계산+정규화까지 끝난
산출물이라 사실상 Gold(traffic_score 컴포넌트) — 다른 파이프라인이 필요로
하는 게 아니라 이 안에서만 쓰이는 값이라 별도 DAG로 안 뺐다.

map_zone_segment는 Taxi Zone(정적 참조 데이터, taxi_zone_pipeline DAG)도
필요하다. Taxi Zone은 거의 안 바뀌는 데이터라 별도 DAG 의존성 연결 없이,
이미 Bronze에 적재돼 있다고 가정한다(없으면 이 태스크가 바로 실패해서
알 수 있음).

dim_segment/graph_segment_adjacency/map_zone_segment를 Asset으로 내보낸다
(construction_pipeline/event_pipeline/ticketmaster_pipeline/gold_tlc_volume이
이 파일들을 쓴다는 걸 Airflow UI에서 계보로 볼 수 있게). 다만 이 파이프라인이
분기 1회라 daily로 도는 소비자들이 매번 기다리게 만들 순 없어서, 소비자
쪽에서는 Asset 트리거가 아니라 그냥 최신 파일을 읽는 방식을 그대로 쓴다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Asset

from src.lion.bronze import ingest_lion
from src.lion.segment_adjacency import build_graph_segment_adjacency, validate_graph_segment_adjacency
from src.lion.silver import build_dim_segment, validate_dim_segment
from src.lion.traffic_score import build_dim_segment_traffic_score, validate_dim_segment_traffic_score
from src.mapping.zone_segment import build_map_zone_segment, validate_map_zone_segment

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="lion_pipeline",
    description="LION(도로망) 분기 Bronze/Silver/Mapping + traffic_score 기초값",
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

    task_build_dim_segment = PythonOperator(
        task_id="build_dim_segment",
        python_callable=build_dim_segment,
        outlets=[Asset("dim_segment")],
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
        outlets=[Asset("map_zone_segment")],
        # dim_segment_root/zone_shapefile_path 둘 다 기본값(common.config 기준) 사용.
    )

    task_validate_map_zone_segment = PythonOperator(
        task_id="validate_map_zone_segment",
        python_callable=validate_map_zone_segment,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='build_map_zone_segment') }}",
        },
    )

    task_build_graph_segment_adjacency = PythonOperator(
        task_id="build_graph_segment_adjacency",
        python_callable=build_graph_segment_adjacency,
        outlets=[Asset("graph_segment_adjacency")],
        # dim_segment의 is_routable만 필요 — map_zone_segment와는 서로 독립적이라 병렬 실행됨.
    )

    task_validate_graph_segment_adjacency = PythonOperator(
        task_id="validate_graph_segment_adjacency",
        python_callable=validate_graph_segment_adjacency,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='build_graph_segment_adjacency') }}",
        },
    )

    task_build_traffic_score = PythonOperator(
        task_id="build_dim_segment_traffic_score",
        python_callable=build_dim_segment_traffic_score,
        # dim_segment_path/graph_path 둘 다 기본값(common.config 기준) 사용.
    )

    task_validate_traffic_score = PythonOperator(
        task_id="validate_dim_segment_traffic_score",
        python_callable=validate_dim_segment_traffic_score,
        op_kwargs={
            "path": "{{ ti.xcom_pull(task_ids='build_dim_segment_traffic_score') }}",
        },
    )

    task_ingest_lion >> task_build_dim_segment >> task_validate_dim_segment

    task_validate_dim_segment >> task_build_map_zone_segment >> task_validate_map_zone_segment
    task_validate_dim_segment >> task_build_graph_segment_adjacency >> task_validate_graph_segment_adjacency
    task_validate_graph_segment_adjacency >> task_build_traffic_score >> task_validate_traffic_score
