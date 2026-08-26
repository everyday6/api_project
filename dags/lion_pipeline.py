"""
DAG: lion_pipeline

NYC DCP LION(도로망) Bronze와 Silver1을 담당하는 도메인 파이프라인.
분기 단위로 나오는 새 릴리즈를 놓치지 않도록 매주 변경 여부를 확인한다
(taxi_zone_pipeline과 동일한 논리 - 실제 변경 주기 대비 충분히 촘촘한
간격으로 확인해서, 공식 발표 주기에서 며칠~몇 주 슬립이 나도 놓치지
않는다. 분기=90일 기준으로 주 1회면 약 13배 안전마진). 전체 스냅샷
데이터라 변경이 감지되면 증분 적재 없이 통째로 받는다.

ingest_lion이 ETag 기반 변경 감지를 갖추고 있다(src/lion/bronze.py 참고) -
주간 확인이나 재시도/수동 재실행이 겹쳐도 원본이 그대로면 다운로드,
build_dim_segment_staged 이후 전체(validate/publish/cleanup), 두 Asset
emit까지 전부 스킵된다.

ETag 마커(_latest_etag.txt) 자체는 ingest_lion이 아니라 맨 끝의
mark_lion_etag가 publish_dim_segment 성공 뒤에만 쓴다(2026-08-26 수정) -
예전엔 ingest_lion이 다운로드 직후 바로 마커를 갱신해서, 그 뒤 Silver1
(validate/publish)이 실패해도 마커는 이미 새 버전을 가리켜 다음 스케줄
실행이 "원본 그대로"로 보고 재시도를 영원히 건너뛰는 사고가 있었다
(정기 확인이 주 1회라 사람이 실패한 run을 수동으로 찾아 clear하지 않으면
최대 한 주 동안 복구가 늦어질 수 있음). 마커 갱신을 파이프라인 끝으로 옮기면,
중간에 실패했을 때 마커가 그대로 "예전 버전"을 가리켜서 다음 스케줄
실행이 자동으로 처음부터 다시 시도한다.

publish_dim_segment는 Asset("lion_bronze_updated")도 함께 내보낸다 —
toll_silver_gold_pipeline이 이 Asset을 구독해서, 실제 LION 갱신 때만
(요금표가 안 바뀌어도) segment 매핑을 다시 계산하도록 하기 위함이다.
ingest_lion에 outlet을 두지 않는 이유는 ETag가 동일한 주간 확인까지
downstream 갱신 이벤트로 오인하는 것을 막기 위해서다.

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
from src.lion.bronze import ingest_lion, mark_lion_etag
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
    description="LION(도로망) 주간 변경 확인 및 Bronze/Silver1",
    schedule="0 5 * * 1",            # 매주 월요일 새벽 5시(UTC) - 프로젝트 내 다른 DAG와
                                       # 동일하게 타임존 명시 없이 암묵적 UTC를 그대로 따름
    start_date=datetime(2025, 1, 1),
    catchup=False,                    # 과거 주간 확인은 채우지 않음 (최신 버전이면 충분)
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args=default_args,
    tags=["lion", "quarterly-source", "weekly-check"],
) as dag:

    task_ingest_lion = PythonOperator(
        task_id="ingest_lion",
        # {{ ds }}를 op_kwargs로 넘기지 않는다 - 수동 트리거(logical_date
        # 없음)에서 Jinja가 UndefinedError로 죽는다. ingest_lion()은 인자
        # 없으면 실행 시점의 실제 날짜로 태깅한다.
        python_callable=ingest_lion,
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
        # ETag 변경이 확인되고 Silver1 검증·발행까지 성공한 경우에만 두
        # downstream Asset을 발행한다. 변경 없음이면 앞 단계가 skip되어
        # 이 태스크와 Asset 발행도 함께 skip된다.
        outlets=[LION_DIM_SEGMENT_READY, LION_BRONZE_UPDATED],
    )

    task_cleanup_dim_segment_staging = PythonOperator(
        task_id="cleanup_dim_segment_staging",
        python_callable=cleanup_dim_segment_staging,
        op_kwargs={
            "published_result": "{{ ti.xcom_pull(task_ids='publish_dim_segment') }}",
        },
    )

    # publish_dim_segment가 성공했을 때만(기본 trigger_rule=all_success) 돈다 -
    # 그래야 ETag 마커가 "Silver1까지 전부 끝났다"는 사실과 실제로 일치한다.
    task_mark_lion_etag = PythonOperator(
        task_id="mark_lion_etag",
        python_callable=mark_lion_etag,
        op_kwargs={
            "bronze_version_result": "{{ ti.xcom_pull(task_ids='ingest_lion') }}",
        },
    )

    (
        task_ingest_lion
        >> task_build_dim_segment_staged
        >> task_validate_dim_segment
        >> task_publish_dim_segment
        >> [task_cleanup_dim_segment_staging, task_mark_lion_etag]
    )
