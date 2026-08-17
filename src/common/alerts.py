"""
Slack 장애 알림

태스크가 재시도를 전부 소진하고 최종 실패했을 때만 호출된다(Airflow의
on_failure_callback은 매 시도가 아니라 "이 태스크 인스턴스가 더 이상 재시도
안 하고 최종 실패로 확정된 시점"에 한 번만 불린다).

DAG의 default_args에 이렇게 걸어서 쓴다:

    from src.common.alerts import notify_slack_failure

    default_args = {
        ...
        "on_failure_callback": notify_slack_failure,
    }

알림 자체가 실패해도(webhook 오류, 네트워크 문제 등) 원래 태스크 실패를
가리면 안 되므로, 여기서 발생하는 예외는 밖으로 던지지 않고 로그만 남긴다.
"""

from __future__ import annotations

import requests

from src.common.config import SLACK_WEBHOOK_URL
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="alerts")

SLACK_TIMEOUT = 10


def _build_message(context: dict) -> str:
    """Airflow 콜백 context에서 필요한 정보만 뽑아 Slack 메시지를 만든다."""

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")

    dag_id = (
        task_instance.dag_id if task_instance
        else getattr(dag, "dag_id", "?")
    )
    task_id = task_instance.task_id if task_instance else "?"
    try_number = getattr(task_instance, "try_number", "?")
    log_url = getattr(task_instance, "log_url", None)

    # Airflow 3.x는 execution_date 대신 logical_date를 쓴다 — 버전 차이에
    # 안전하게 둘 다 확인한다.
    run_time = context.get("logical_date") or context.get("execution_date")

    lines = [
        ":red_circle: *Airflow 태스크 실패*",
        f"*DAG*: `{dag_id}`",
        f"*Task*: `{task_id}` (시도 {try_number}회 모두 소진)",
        f"*실행 시각*: {run_time}",
        f"*에러*: `{exception}`",
    ]

    if log_url:
        lines.append(f"<{log_url}|로그 보기>")

    return "\n".join(lines)


def notify_slack_failure(context: dict) -> None:
    """on_failure_callback으로 등록해서 쓰는 함수."""

    if not SLACK_WEBHOOK_URL:
        logger.warning(
            "SLACK_WEBHOOK_URL이 없어서 알림을 건너뜁니다 — .env 확인"
        )
        return

    try:
        message = _build_message(context)

        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": message},
            timeout=SLACK_TIMEOUT,
        )
        response.raise_for_status()

        logger.info("Slack 실패 알림 전송 완료")

    except Exception:
        # 알림 실패가 원래 태스크 실패를 가리면 안 되므로 여기서 삼킨다.
        logger.exception("Slack 실패 알림 전송 실패")
