"""
ingest_daily — 공사 · 행사 · TicketMaster 일일 파이프라인

네 소스는 매일 갱신한다.

- construction: 공사 허가 전체 최신 스냅샷 수집
- construction_stipulations: 공사 허가 스티퓰레이션(조건/유의사항) 증분 수집
  (createdon 기준 실행일 하루치만)
- event: NYC Permitted Event 최신 상태 수집
- ticketmaster: 앞으로 120일 이벤트 수집

각 소스의 Bronze → Silver는 순차 실행하고,
소스끼리는 서로 독립적으로 병렬 실행한다.

단, construction_work_hours_silver는 예외 — construction Silver(허가)와
construction_stipulations Bronze(조건 텍스트에서 뽑은 작업 시간대 제약)를 조인한
결과라 두 소스가 모두 끝난 뒤에 실행된다. 매일 도는 조인이라 construction은 항상
그날 최신, stipulations는 그 시점까지 누적된 전체 이력을 반영한다.

road_control_events_silver 이후에는 closure_penalty(traffic_score의 용량 감소
컴포넌트)를 만드는 체인이 이어진다:
  road_control_events_silver
    -> map_road_control_segment (construction, 도로명 기반 매핑)
    -> map_road_closure_segment (other_road_control, 공간 조인 기반 매핑)
    -> closure_penalty (둘을 합쳐서 graph_segment_adjacency로 3홉 감쇠 확산)

실패 시 최대 3회 자동 재시도하며,
동일 RUN_DATE 재실행 시 동일 날짜 파티션을 사용한다.
"""

import sys
import os
from datetime import timedelta
from pathlib import Path

import pendulum
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# 프로젝트 루트를 import 경로에 추가

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))


# ==========================
# Airflow 공통 설정
# ==========================

LOCAL_TZ = pendulum.timezone("America/New_York")

default_args = {
    "owner": "de5",

    # Task 실패 시 최대 3회 자동 재시도
    "retries": 3,

    # 최초 재시도 대기 시간
    "retry_delay": timedelta(minutes=5),

    # 재시도할수록 대기 시간을 증가시킴
    "retry_exponential_backoff": True,

    # Task가 비정상적으로 오래 실행되는 것을 방지
    "execution_timeout": timedelta(hours=1),
}


@dag(
    dag_id="ingest_daily",
    description="공사 · 행사 · TicketMaster Bronze/Silver 일일 파이프라인",

    # 매일 New York 시간 기준 오전 4시 실행
    schedule="0 4 * * *",

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz=LOCAL_TZ,
    ),

    # 과거 미실행 날짜를 자동으로 소급 실행하지 않음
    catchup=False,

    # 동일 DAG가 동시에 여러 번 실행되는 것을 방지
    max_active_runs=1,

    default_args=default_args,

    tags=[
        "daily",
        "bronze",
        "silver",
    ],
)
def ingest_daily():

    # ───────────────────────────
    # Construction
    # ───────────────────────────

    @task(task_id="construction_bronze")
    def construction_bronze():
        from src.construction.bronze import main

        context = get_current_context()

        # Airflow 논리 실행일을 각 스크립트에 전달
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="construction_silver")
    def construction_silver():
        from src.construction.silver import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    # ───────────────────────────
    # Construction Stipulations
    # ───────────────────────────

    @task(task_id="construction_stipulations_bronze")
    def construction_stipulations_bronze():
        """
        하루치(main())만 받는 대신 backfill_construction_stipulations()를 매일
        돌린다 — 이미 받아둔 날짜는 스킵(resumable)하니 평소엔 오늘 하루치만
        새로 받는 것과 비용이 같지만, DAG가 하루라도 안 돌았을 때도 다음 실행에서
        자동으로 빠진 날짜를 채운다(construction의 "매번 전체 범위가 항상
        채워져 있음"과 동일한 효과를 증분 방식 비용으로 얻음).
        """
        from datetime import date as _date

        from src.construction_stipulations.backfill import backfill_construction_stipulations

        context = get_current_context()
        backfill_construction_stipulations(end=_date.fromisoformat(context["ds"]))


    @task(task_id="construction_work_hours_silver")
    def construction_work_hours_silver():
        """
        construction Silver(허가) + construction_stipulations Bronze(조건 텍스트에서
        뽑은 작업 시간대 제약)를 permit_id 기준으로 조인한다. construction_silver의
        오늘자 출력과 construction_stipulations_bronze의 오늘자 증분이 모두 있어야
        의미가 있어서 둘 다 끝난 뒤에 실행한다(아래 의존 관계 참고).
        """
        from src.construction_stipulations.silver import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="road_control_events_silver")
    def road_control_events_silver():
        """
        construction_work_hours + road_closures(가장 최근 주간 스냅샷)를 도로명
        구간(on/from/to_street) + 기간 겹침으로 대조해서 합친다. road_closures는
        ingest_weekly(별도 DAG)에서 갱신되는 소스라 여기서는 그 시점 기준 가장
        최근 파일을 그냥 읽는다 — cross-DAG 의존 없이, construction 쪽만 매일
        최신으로 반영된다.
        """
        from src.road_closures.silver import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="map_road_control_segment")
    def map_road_control_segment():
        """
        road_control_events의 construction 쪽(geometry 없음)을 도로명 +
        graph_segment_adjacency 기반 매칭으로 segment_id에 매핑한다.
        """
        from src.mapping.road_control_segment import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="map_road_closure_segment")
    def map_road_closure_segment():
        """
        road_control_events의 other_road_control 쪽(geometry 99.99% 있음)을
        공간 조인(가장 가까운 세그먼트)으로 segment_id에 매핑한다.
        """
        from src.mapping.road_closure_segment import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="closure_penalty")
    def closure_penalty():
        """
        construction + road_closures 진앙 segment을 합쳐서 graph_segment_adjacency로
        3홉까지 감쇠 확산시킨 뒤, segment별 용량 감소량(closure_capacity_reduction)을
        계산한다. traffic_score의 closure_penalty 컴포넌트가 이 결과를 참조한다.
        """
        from src.scoring.closure_penalty import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    # ───────────────────────────
    # NYC Event
    # ───────────────────────────

    @task(task_id="event_bronze")
    def event_bronze(**context):
        from src.event.bronze import main
        main(run_date=context["ds"])


    @task(task_id="event_silver")
    def event_silver(**context):
        from src.event.silver import main
        main(run_date=context["ds"])


    # ───────────────────────────
    # TicketMaster
    # ───────────────────────────

    @task(task_id="ticketmaster_bronze")
    def ticketmaster_bronze():
        from src.ticketmaster.bronze import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    @task(task_id="ticketmaster_silver")
    def ticketmaster_silver():
        from src.ticketmaster.silver import main

        context = get_current_context()
        os.environ["RUN_DATE"] = context["ds"]

        main()


    # ───────────────────────────
    # Task 생성
    # ───────────────────────────

    construction_b = construction_bronze()
    construction_s = construction_silver()

    construction_stipulations_b = construction_stipulations_bronze()
    construction_work_hours_s = construction_work_hours_silver()
    road_control_events_s = road_control_events_silver()
    map_road_control_segment_t = map_road_control_segment()
    map_road_closure_segment_t = map_road_closure_segment()
    closure_penalty_t = closure_penalty()

    event_b = event_bronze()
    event_s = event_silver()

    ticketmaster_b = ticketmaster_bronze()
    ticketmaster_s = ticketmaster_silver()


    # ───────────────────────────
    # 의존 관계
    # ───────────────────────────

    # 각 Bronze가 성공해야 해당 Silver 실행
    construction_b >> construction_s
    event_b >> event_s
    ticketmaster_b >> ticketmaster_s

    # 조인 태스크는 construction Silver(오늘자 허가)와 stipulations Bronze(오늘자
    # 증분)가 둘 다 끝난 뒤 실행 — 매일 도는 조인이라 매번 최신 construction을
    # 반영하고, stipulations 쪽은 그 시점까지 누적된 전체 이력을 읽는다.
    [construction_s, construction_stipulations_b] >> construction_work_hours_s

    # road_closures는 별도 DAG(ingest_weekly)에서 갱신되므로 여기선 의존관계를
    # 안 걸고, construction_work_hours만 끝나면 바로 실행(그 시점 road_closures
    # 최신 스냅샷을 그냥 읽음).
    construction_work_hours_s >> road_control_events_s

    # road_control_events(construction/other_road_control 둘 다)를 각각 다른
    # 방식(도로명 / 공간 조인)으로 segment_id에 매핑한 뒤, 둘을 합쳐서
    # closure_penalty를 계산한다.
    road_control_events_s >> map_road_control_segment_t
    road_control_events_s >> map_road_closure_segment_t
    [map_road_control_segment_t, map_road_closure_segment_t] >> closure_penalty_t


ingest_daily()