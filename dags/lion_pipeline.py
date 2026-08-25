"""
DAG: lion_pipeline

NYC DCP LION(도로망) Bronze와 Silver1을 담당하는 도메인 파이프라인.
분기마다 새 릴리즈가 나오는 전체 스냅샷 데이터라, 증분 개념 없이 매번
통째로 받는다.

ingest_lion이 ETag 기반 변경 감지를 갖추고 있다(src/lion/bronze.py 참고) -
같은 분기에 재시도/수동 재실행이 겹쳐도 원본이 그대로면 다운로드,
build_dim_segment_staged 이후 전체(validate/publish/cleanup), 두 Asset
emit까지 전부 스킵된다.

ingest_lion은 Asset("lion_bronze_updated")를 outlet으로 내보낸다 —
toll_silver_gold_pipeline이 이 Asset을 구독해서, 분기 LION 갱신 때도
(요금표가 안 바뀌어도) segment 매핑을 다시 계산하도록 하기 위함이다.

publish_dim_segment는 별도로 Asset("lion_dim_segment_ready")를 outlet으로
내보낸다 — segment_length_pipeline(nav type2)이 이 Asset을 구독해서,
자체적으로 LION을 다시 받지 않고 여기서 발행한 Silver1 dim_segment를 그대로
읽어 Gold2(is_routable)만 계산한다. lion_bronze_updated가 아니라 이 Asset을
쓰는 이유: Bronze 다운로드 직후가 아니라 Silver1 검증·발행까지 끝난
뒤여야 dim_segment.parquet가 실제로 최신 상태이기 때문.

예전엔 publish_dim_segment 뒤에 TriggerDagRunOperator로 zone_segment_pipeline을
직접 호출했는데, taxi_zone_pipeline도 같은 걸 호출하다 보니 둘이 같은 날
겹치면 zone_segment_pipeline이 두 번 도는 문제가 있었다. zone_segment_pipeline이
Asset("lion_dim_segment_ready") | Asset("taxi_zone_silver1_updated")를
직접 구독하도록 바꿔, TriggerDagRunOperator 대신 Asset 이벤트를 기준으로 실행한다.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sdk import Asset

from src.common.alerts import notify_slack_failure
from src.lion.bronze import ingest_lion
from src.lion.silver1 import (
    build_dim_segment_staged,
    cleanup_dim_segment_staging,
    publish_dim_segment,
    validate_staged_dim_segment,
)

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}

LION_BRONZE_UPDATED = Asset("lion_bronze_updated")
LION_DIM_SEGMENT_READY = Asset("lion_dim_segment_ready")

with DAG(
    dag_id="lion_pipeline",
    description="LION(도로망) 분기 Bronze/Silver1",
    schedule="0 5 1 1,4,7,10 *",     # 1/4/7/10월 1일 새벽 5시
    start_date=datetime(2025, 1, 1),
    catchup=False,                    # 과거 분기 버전은 지금 굳이 안 채움 (최신 버전이면 충분)
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args=default_args,
    tags=["lion", "quarterly"],
) as dag:

    task_ingest_lion = PythonOperator(
        task_id="ingest_lion",
        # {{ ds }}를 op_kwargs로 넘기지 않는다 - 수동 트리거(logical_date
        # 없음)에서 Jinja가 UndefinedError로 죽는다. ingest_lion()은 인자
        # 없으면 실행 시점의 실제 날짜로 태깅한다.
        python_callable=ingest_lion,
        outlets=[LION_BRONZE_UPDATED],
    )

    task_build_dim_segment_staged = PythonOperator(
        task_id="build_dim_segment_staged",
        python_callable=build_dim_segment_staged,
        op_kwargs={
            "bronze_version_result": "{{ ti.xcom_pull(task_ids='ingest_lion') }}",
        },
    )

    task_validate_dim_segment = PythonOperator(
        task_id="validate_staged_dim_segment",
        python_callable=validate_staged_dim_segment,
        op_kwargs={
            "stage_result": "{{ ti.xcom_pull(task_ids='build_dim_segment_staged') }}",
        },
    )

    task_publish_dim_segment = PythonOperator(
        task_id="publish_dim_segment",
        python_callable=publish_dim_segment,
        op_kwargs={
            "validated_stage": "{{ ti.xcom_pull(task_ids='validate_staged_dim_segment') }}",
        },
        outlets=[LION_DIM_SEGMENT_READY],
    )

    task_cleanup_dim_segment_staging = PythonOperator(
        task_id="cleanup_dim_segment_staging",
        python_callable=cleanup_dim_segment_staging,
        op_kwargs={
            "published_result": "{{ ti.xcom_pull(task_ids='publish_dim_segment') }}",
        },
    )

    (
        task_ingest_lion
        >> task_build_dim_segment_staged
        >> task_validate_dim_segment
        >> task_publish_dim_segment
        >> task_cleanup_dim_segment_staging
    )
