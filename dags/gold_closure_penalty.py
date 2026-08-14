"""
DAG: gold_closure_penalty

closure_penalty(traffic_score의 용량 감소 컴포넌트)를 계산한다. 이 DAG는
자기 자신만의 스케줄 이유가 없다 — "construction_pipeline의 매핑 결과가
갱신됐을 때"만 다시 계산하면 되므로, cron 대신 Asset 트리거만 쓴다.

construction_pipeline이 map_road_control_segment/map_road_closure_segment를
둘 다 새로 만들어야 이 DAG가 실행된다(AssetAll — 둘 다 갱신될 때까지 기다림).
예전에 ingest_daily 한 DAG 안에서 `[map_road_control_segment_t,
map_road_closure_segment_t] >> closure_penalty_t`로 보장하던 것과 동일한
관계를, DAG를 쪼갠 뒤에도 Asset으로 그대로 유지한다.

주의: Asset 트리거 실행은 cron 스케줄과 달리 logical_date가 없어서
context["ds"]가 KeyError를 낸다(실제로 겪음 — schedule=None DAG를 로지컬
데이트 없이 수동 트리거할 때와 동일한 함정). 그래서 run_date를 context에서
안 뽑고 build()가 알아서 오늘 날짜(America/New_York 관례상 date.today())로
기본값을 채우게 둔다 — 어차피 이 DAG는 "지금 시점 기준으로 다시 계산"이
목적이라 오늘 날짜가 맞다.
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

default_args = {
    "owner": "jiwon",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="gold_closure_penalty",
    description="공사/통제 closure_penalty 계산 (construction_pipeline Asset 트리거)",
    schedule=[Asset("map_road_control_segment"), Asset("map_road_closure_segment")],
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["gold", "asset-triggered"],
)
def gold_closure_penalty():

    @task(task_id="build_closure_penalty")
    def build_closure_penalty():
        from src.scoring.closure_penalty import build
        return build()

    @task(task_id="validate_closure_penalty")
    def validate_closure_penalty(path: str):
        from src.scoring.closure_penalty import validate_output
        return validate_output(path)

    validate_closure_penalty(build_closure_penalty())


gold_closure_penalty()
