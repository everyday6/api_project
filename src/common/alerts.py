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

Task 자체는 성공하지만 특정 파일만 제외하는 등, Task 최종 실패가 아닌
상황에서 즉시 알림이 필요하면 notify_slack_message(text)를 직접 호출한다.
"""

from __future__ import annotations

import requests

from src.common.config import SLACK_WEBHOOK_URL
from src.common.logger import get_logger

logger = get_logger(__name__, log_to_file=True, log_file_stem="alerts")

SLACK_TIMEOUT = 10
MAX_ERROR_SUMMARY_CHARS = 500
MAX_ERROR_SUMMARY_LINES = 3


def _summarize_exception(exception: object) -> tuple[str, str]:
    """긴 Spark/Java 스택 트레이스에서 Slack에 보낼 핵심 내용만 추린다."""

    if exception is None:
        # 태스크 코드가 예외를 던진 게 아니라, Airflow가 하트비트 끊김(zombie)을
        # 감지해서 강제로 실패 처리한 경우 context에 실제 예외가 없다. 이
        # 서비스는 Airflow 컴포넌트/Spark/navigation-api가 전부 vCPU 2개짜리
        # EC2 하나에서 같이 도는 구조라, 무거운 태스크가 몰리면 워커가
        # OOM 등으로 죽었다가 restart:unless-stopped로 되살아나는 사이에
        # 이 상태가 자주 나온다 — "이 태스크 코드의 버그"보다 "그 시점에
        # 워커/스케줄러가 죽었었는가"부터 의심하는 게 순서다.
        return "UnknownError", (
            "실제 예외 없이 실패 처리됨 — 태스크 실행 중 워커/스케줄러가 하트비트를 "
            "못 보내 zombie로 판정됐을 가능성이 높습니다(코드 버그가 아니라 EC2 "
            "자원 부족으로 컨테이너가 죽었다 재기동됐을 수 있음). 같은 시간대에 "
            "컨테이너 재시작 알림이 있었는지 먼저 확인하고, 없으면 Airflow 로그를 "
            "확인하세요."
        )

    error_type = type(exception).__name__
    raw_lines = [line.strip() for line in str(exception).splitlines() if line.strip()]

    # Java/Spark 예외는 가장 마지막 Caused by가 실제 근본 원인인 경우가 많다.
    caused_by_indexes = [
        index for index, line in enumerate(raw_lines)
        if "Caused by:" in line
    ]
    start_index = caused_by_indexes[-1] if caused_by_indexes else 0

    meaningful_lines = []
    for line in raw_lines[start_index:]:
        if (
            line.startswith("at ")
            or line.startswith("Traceback ")
            or line.startswith("File ")
            or (line.startswith("...") and line.endswith("more"))
        ):
            continue

        meaningful_lines.append(line)
        if len(meaningful_lines) >= MAX_ERROR_SUMMARY_LINES:
            break

    if not meaningful_lines:
        meaningful_lines = [raw_lines[0]] if raw_lines else [error_type]

    summary = "\n".join(meaningful_lines)
    if len(summary) > MAX_ERROR_SUMMARY_CHARS:
        summary = summary[: MAX_ERROR_SUMMARY_CHARS - 3].rstrip() + "..."

    return error_type, summary


def _build_message(context: dict) -> str:
    """Airflow 콜백 context에서 필요한 정보만 뽑아 Slack 메시지를 만든다."""

    task_instance = context.get("task_instance")
    dag = context.get("dag")
    exception = context.get("exception")
    error_type, error_summary = _summarize_exception(exception)

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
        f"*에러 타입*: `{error_type}`",
        f"*핵심 내용*: {error_summary}",
    ]

    if log_url:
        lines.append(f"<{log_url}|로그 보기>")

    return "\n".join(lines)


def _post_to_slack(text: str) -> None:
    """Slack Webhook으로 텍스트를 전송한다.

    알림 자체가 실패해도(webhook 오류, 네트워크 문제 등) 호출부의 원래
    처리 흐름을 가리면 안 되므로, 여기서 발생하는 예외는 밖으로 던지지
    않고 로그만 남긴다.
    """

    if not SLACK_WEBHOOK_URL:
        logger.warning(
            "SLACK_WEBHOOK_URL이 없어서 알림을 건너뜁니다 — .env 확인"
        )
        return

    try:
        response = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": text},
            timeout=SLACK_TIMEOUT,
        )
        response.raise_for_status()

        logger.info("Slack 메시지 전송 완료")

    except Exception:
        logger.exception("Slack 메시지 전송 실패")


def notify_slack_failure(context: dict) -> None:
    """on_failure_callback으로 등록해서 쓰는 함수."""

    try:
        message = _build_message(context)
    except Exception:
        logger.exception("Slack 실패 알림 메시지 생성 실패")
        return

    _post_to_slack(message)


def notify_slack_message(text: str) -> None:
    """Airflow 실패 콜백과 무관하게, 임의의 텍스트를 Slack으로 즉시 전송한다.

    Bronze 검증처럼 Task 자체는 성공하지만 특정 파일을 제외했다는 걸
    바로 알려야 할 때 쓴다 — on_failure_callback은 Task가 최종 실패로
    확정될 때만 호출되므로 이런 경우엔 발동하지 않는다.
    """

    _post_to_slack(text)
