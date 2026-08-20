"""
DAG: ticketmaster_pipeline

Ticketmaster Bronze/Silver + LION segment 매핑까지 담당하는 도메인
파이프라인. event_pipeline과 동일한 이유로, 예전엔 수동(schedule=None)이던
join_lion의 ticketmaster_lion_mapping을 여기로 흡수해서 daily cron에
자동으로 편입시켰다.

매핑 단계는 dim_segment.parquet(lion_pipeline 산출물)의 최신 파일을 그냥
읽는다(이유는 event_pipeline과 동일 — 이 파이프라인 자체가 daily cron으로
매일 돌 이유가 있음).

emits: Asset(map_ticketmaster_lion) — 지금은 구독하는 Gold가 없지만, 나중에
필요해지면 여기서 바로 트리거받을 수 있게 미리 emit.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

from src.common.alerts import notify_slack_failure

LOCAL_TZ = pendulum.timezone("America/New_York")

default_args = {
    "owner": "jiwon",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
    "on_failure_callback": notify_slack_failure,
}

MAP_TICKETMASTER_LION = Asset("map_ticketmaster_lion")


@dag(
    dag_id="ticketmaster_pipeline",
    description="Ticketmaster Bronze/Silver/Mapping 파이프라인",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["ticketmaster", "daily"],
)
def ticketmaster_pipeline():

    @task(task_id="fetch_ticketmaster")
    def fetch_ticketmaster():
        import os
        from src.ticketmaster.bronze import build

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]
        return build()

    @task(task_id="validate_ticketmaster_bronze")
    def validate_ticketmaster_bronze(path: str):
        from src.ticketmaster.bronze import validate_output
        return validate_output(path)

    @task(task_id="build_ticketmaster")
    def build_ticketmaster():
        from src.ticketmaster.silver1 import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_ticketmaster")
    def validate_ticketmaster(path: str):
        from src.ticketmaster.silver1 import validate_output
        return validate_output(path)

    @task(task_id="build_ticketmaster_gold1")
    def build_ticketmaster_gold1():
        from src.ticketmaster.gold1 import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_ticketmaster_gold1")
    def validate_ticketmaster_gold1(path: str):
        from src.ticketmaster.gold1 import validate_output
        return validate_output(path)

    @task(task_id="map_ticketmaster_lion", outlets=[MAP_TICKETMASTER_LION])
    def map_ticketmaster_lion():
        from src.silver2.ticketmaster_lion import build_ticketmaster_lion_mapping
        context = get_current_context()
        return build_ticketmaster_lion_mapping(context["ds"])

    @task(task_id="validate_map_ticketmaster_lion")
    def validate_map_ticketmaster_lion(path: str):
        from src.silver2.ticketmaster_lion import validate_output
        context = get_current_context()
        return validate_output(path, context["ds"])

    bronze_path = fetch_ticketmaster()
    bronze_validated = validate_ticketmaster_bronze(bronze_path)

    silver_path = build_ticketmaster()
    bronze_validated >> silver_path
    silver_validated = validate_ticketmaster(silver_path)

    # map_ticketmaster_lion은 지역/활성기간 필터 이전의 전체 ticketmaster
    # Silver1을 그대로 매칭 대상으로 삼는다(Silver2는 전 지역을 유지한다는
    # 원칙) — gold1은 별도 병렬 분기로 둔다.
    mapping_path = map_ticketmaster_lion()
    silver_validated >> mapping_path
    validate_map_ticketmaster_lion(mapping_path)

    gold1_path = build_ticketmaster_gold1()
    silver_validated >> gold1_path
    validate_ticketmaster_gold1(gold1_path)


ticketmaster_pipeline()
