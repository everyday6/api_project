"""
DAG: toll_gold_pipeline

toll_bronze_pipeline이 요금표/시설목록/CBD 폴리곤을 갱신할 때마다
(Asset("toll_bronze_updated") 트리거) Silver2 매핑을 다시 만들고 Gold
값을 재계산해서 DynamoDB에 적재한다. 요금표가 1년에 한 번 정도만
바뀌므로 cron 스케줄 없이 Asset 트리거만 쓴다(gold_closure_penalty와
동일한 패턴).
"""

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, dag, task

from src.common.alerts import notify_slack_failure

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": notify_slack_failure,
}


@dag(
    dag_id="toll_gold_pipeline",
    description="통행료 Silver2 매핑 + Gold 계산 (toll_bronze_pipeline Asset 트리거)",
    schedule=[Asset("toll_bronze_updated")],
    start_date=pendulum.datetime(2026, 8, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["toll", "asset-triggered"],
)
def toll_gold_pipeline():

    @task(task_id="find_latest_lion_gdb")
    def find_latest_lion_gdb() -> str:
        from src.common.config import BRONZE_DIR

        gdb_candidates = sorted((BRONZE_DIR / "lion").glob("version_date=*/lion/lion.gdb"))
        if not gdb_candidates:
            raise FileNotFoundError("LION Bronze GDB를 찾을 수 없습니다 — lion_pipeline DAG를 먼저 실행하세요.")
        return str(gdb_candidates[-1])

    @task(task_id="build_facility_mapping")
    def build_facility_mapping(gdb_path: str) -> str:
        from pathlib import Path

        from src.toll.silver2 import build_map_toll_facility_segment

        return build_map_toll_facility_segment(gdb_path=Path(gdb_path))

    @task(task_id="build_zone_mapping")
    def build_zone_mapping(gdb_path: str) -> str:
        from pathlib import Path

        from src.toll.silver2 import build_map_cbd_zone_segment

        return build_map_cbd_zone_segment(gdb_path=Path(gdb_path))

    @task(task_id="build_and_write_gold")
    def build_and_write_gold(facility_map_path: str, zone_map_path: str) -> int:
        from pathlib import Path

        from src.toll.gold import build_and_write

        return build_and_write(
            facility_map_path=Path(facility_map_path),
            zone_map_path=Path(zone_map_path),
        )

    gdb_path = find_latest_lion_gdb()
    facility_map = build_facility_mapping(gdb_path)
    zone_map = build_zone_mapping(gdb_path)
    build_and_write_gold(facility_map, zone_map)


toll_gold_pipeline()
