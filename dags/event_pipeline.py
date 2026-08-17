"""
DAG: event_pipeline

NYC Permitted Event Bronze/Silver + LION segment 매핑까지 담당하는 도메인
파이프라인. 예전엔 Bronze/Silver가 ingest_daily에, 매핑(event_lion_mapping)이
별도의 수동(schedule=None) join_lion DAG에 나뉘어 있었는데, 매핑이 수동이라
아무도 안 돌리면 계속 stale하던 문제가 있었다. 이 파이프라인으로 합쳐서
daily cron에 자동으로 편입시켰다.

매핑 단계는 dim_segment.parquet(lion_pipeline 산출물)의 최신 파일을 그냥
읽는다 — Asset으로 안 묶은 건, 이 파이프라인 자체가 daily cron으로 매일 돌
이유가 있어서 dim_segment이 갱신될 때마다 따로 안 기다려도 되기 때문이다.

emits: Asset(map_event_lion) — 지금은 구독하는 Gold가 없지만, 나중에
event_boost 같은 컴포넌트를 만들 때 여기서 바로 트리거받을 수 있게 미리 emit.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, get_current_context, task

LOCAL_TZ = pendulum.timezone("America/New_York")

default_args = {
    "owner": "jiwon",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}

MAP_EVENT_LION = Asset("map_event_lion")


@dag(
    dag_id="event_pipeline",
    description="NYC Permitted Event Bronze/Silver/Mapping 파이프라인",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["event", "daily"],
)
def event_pipeline():

    @task(task_id="fetch_event")
    def fetch_event():
        from src.event.bronze import build
        context = get_current_context()
        return build(run_date=context["ds"])

    @task(task_id="validate_event_bronze")
    def validate_event_bronze(path: str):
        from src.event.bronze import validate_output
        return validate_output(path)

    @task(task_id="build_event")
    def build_event():
        from src.event.silver import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_event")
    def validate_event(path: str):
        from src.event.silver import validate_output
        return validate_output(path)

    @task(task_id="map_event_lion", outlets=[MAP_EVENT_LION])
    def map_event_lion():
        from src.mapping.event_lion import build_event_lion_mapping
        context = get_current_context()
        return build_event_lion_mapping(context["ds"])

    @task(task_id="validate_map_event_lion")
    def validate_map_event_lion(path: str):
        from src.mapping.event_lion import validate_output
        return validate_output(path)

    bronze_path = fetch_event()
    bronze_validated = validate_event_bronze(bronze_path)

    silver_path = build_event()
    bronze_validated >> silver_path
    silver_validated = validate_event(silver_path)

    mapping_path = map_event_lion()
    silver_validated >> mapping_path
    validate_map_event_lion(mapping_path)


event_pipeline()
