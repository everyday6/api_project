"""
DAG: construction_pipeline

공사 허가(construction) + 스티퓰레이션(work_hours) + road_closures를 합친
road_control_events, 그리고 그걸 segment_id에 매핑하는 것까지 담당하는
도메인 파이프라인(Bronze -> Silver -> Mapping). 예전엔 ingest_daily 하나에
event/ticketmaster와 섞여 있었는데, "독립적으로 실패/재시도/스케줄될 필요가
있는가" 기준으로 도메인별로 쪼갰다.

끝에서 map_road_control_segment/map_road_closure_segment를 Asset으로
내보낸다 — gold_closure_penalty가 이 두 Asset을 구독해서, cron 추측 없이
정확히 이 파이프라인이 끝난 뒤에만 다시 계산된다.

road_control_events_silver는 road_closures(road_closure_pipeline, 주 1회)의
최신 Bronze 파일을 그냥 읽는다 — Asset으로 안 묶은 건, 이 파이프라인 자체가
daily cron으로 매일 돌 이유가 이미 있어서 road_closures가 갱신될 때마다
따로 안 기다려도 되기 때문이다(과거 ingest_daily의 동일한 팀 결정 유지).

build/validate를 별도 태스크로 나눠서, validate 실패로 재시도할 때 무거운
fetch/transform을 처음부터 다시 안 해도 되게 했다.
"""

from datetime import date as _date, timedelta

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

MAP_ROAD_CONTROL_SEGMENT = Asset("map_road_control_segment")
MAP_ROAD_CLOSURE_SEGMENT = Asset("map_road_closure_segment")


@dag(
    dag_id="construction_pipeline",
    description="공사 허가 Bronze/Silver/Mapping 파이프라인",
    schedule="0 4 * * *",
    start_date=pendulum.datetime(2026, 8, 1, tz=LOCAL_TZ),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["construction", "daily"],
)
def construction_pipeline():

    # ───────────────────────────
    # Bronze
    # ───────────────────────────

    @task(task_id="fetch_construction")
    def fetch_construction():
        from src.construction.bronze import build

        context = get_current_context()
        import os
        os.environ["RUN_DATE"] = context["ds"]
        return build()

    @task(task_id="validate_construction_bronze")
    def validate_construction_bronze(path: str):
        from src.construction.bronze import validate_output
        return validate_output(path)

    @task(task_id="fetch_stipulations")
    def fetch_stipulations():
        from src.construction_stipulations.backfill import backfill_construction_stipulations

        context = get_current_context()
        return backfill_construction_stipulations(end=_date.fromisoformat(context["ds"]))

    @task(task_id="validate_stipulations")
    def validate_stipulations(path: str | None):
        from src.construction_stipulations.bronze import validate_output
        return validate_output(path)

    # ───────────────────────────
    # Silver
    # ───────────────────────────

    @task(task_id="build_construction")
    def build_construction():
        from src.construction.silver import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_construction")
    def validate_construction(path: str):
        from src.construction.silver import validate_output
        return validate_output(path)

    @task(task_id="build_work_hours")
    def build_work_hours():
        from src.construction_stipulations.silver import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_work_hours")
    def validate_work_hours(path: str):
        from src.construction_stipulations.silver import validate_output
        context = get_current_context()
        return validate_output(path, context["ds"])

    @task(task_id="build_road_control_events")
    def build_road_control_events():
        from src.road_closures.silver import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_road_control_events")
    def validate_road_control_events(path: str):
        from src.road_closures.silver import validate_output
        context = get_current_context()
        return validate_output(path, context["ds"])

    # ───────────────────────────
    # Mapping
    # ───────────────────────────

    @task(task_id="map_road_control_segment", outlets=[MAP_ROAD_CONTROL_SEGMENT])
    def map_road_control_segment():
        from src.mapping.road_control_segment import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_map_road_control_segment")
    def validate_map_road_control_segment(path: str):
        from src.mapping.road_control_segment import validate_output
        context = get_current_context()
        return validate_output(path, context["ds"])

    @task(task_id="map_road_closure_segment", outlets=[MAP_ROAD_CLOSURE_SEGMENT])
    def map_road_closure_segment():
        from src.mapping.road_closure_segment import build
        context = get_current_context()
        return build(context["ds"])

    @task(task_id="validate_map_road_closure_segment")
    def validate_map_road_closure_segment(path: str):
        from src.mapping.road_closure_segment import validate_output
        context = get_current_context()
        return validate_output(path, context["ds"])

    # ───────────────────────────
    # 의존 관계
    # ───────────────────────────

    construction_bronze_path = fetch_construction()
    construction_bronze_validated = validate_construction_bronze(construction_bronze_path)

    stipulations_path = fetch_stipulations()
    stipulations_validated = validate_stipulations(stipulations_path)

    construction_silver_path = build_construction()
    construction_bronze_validated >> construction_silver_path
    construction_silver_validated = validate_construction(construction_silver_path)

    work_hours_path = build_work_hours()
    [construction_silver_validated, stipulations_validated] >> work_hours_path
    work_hours_validated = validate_work_hours(work_hours_path)

    road_control_events_path = build_road_control_events()
    work_hours_validated >> road_control_events_path
    road_control_events_validated = validate_road_control_events(road_control_events_path)

    map_rcs_path = map_road_control_segment()
    road_control_events_validated >> map_rcs_path
    validate_map_road_control_segment(map_rcs_path)

    map_rclose_path = map_road_closure_segment()
    road_control_events_validated >> map_rclose_path
    validate_map_road_closure_segment(map_rclose_path)


construction_pipeline()
