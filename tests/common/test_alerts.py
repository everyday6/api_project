from unittest.mock import MagicMock, patch

from src.common import alerts


def test_notify_slack_message_posts_to_webhook(monkeypatch):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    with patch.object(alerts.requests, "post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        alerts.notify_slack_message("테스트 메시지")

    mock_post.assert_called_once_with(
        "https://hooks.slack.test/webhook",
        json={"text": "테스트 메시지"},
        timeout=alerts.SLACK_TIMEOUT,
    )


def test_notify_slack_message_skips_when_webhook_missing(monkeypatch, caplog):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", None)

    with patch.object(alerts.requests, "post") as mock_post:
        with caplog.at_level("WARNING"):
            alerts.notify_slack_message("테스트 메시지")

    mock_post.assert_not_called()
    assert any("SLACK_WEBHOOK_URL" in rec.message for rec in caplog.records)


def test_notify_slack_message_swallows_request_exception(monkeypatch, caplog):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    with patch.object(
        alerts.requests, "post",
        side_effect=alerts.requests.exceptions.ConnectionError("boom"),
    ):
        with caplog.at_level("ERROR"):
            alerts.notify_slack_message("테스트 메시지")  # 예외를 던지면 안 됨

    assert any("전송 실패" in rec.message for rec in caplog.records)


def test_notify_slack_failure_still_works_after_refactor(monkeypatch):
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    context = {
        "task_instance": MagicMock(
            dag_id="tlc_pipeline", task_id="store_bronze",
            try_number=3, log_url="http://example.com/log",
        ),
        "exception": ValueError("boom"),
        "logical_date": "2026-08-18",
    }

    with patch.object(alerts.requests, "post") as mock_post:
        mock_post.return_value = MagicMock(status_code=200)
        alerts.notify_slack_failure(context)

    assert mock_post.call_count == 1
    sent_text = mock_post.call_args.kwargs["json"]["text"]
    assert "tlc_pipeline" in sent_text
    assert "store_bronze" in sent_text


def test_notify_slack_failure_swallows_build_message_exception(monkeypatch, caplog):
    """notify_slack_failure는 메시지 생성 실패도 삼켜야 한다."""
    monkeypatch.setattr(alerts, "SLACK_WEBHOOK_URL", "https://hooks.slack.test/webhook")

    # _build_message를 패치해서 예외를 던지게 한다
    with patch.object(alerts, "_build_message", side_effect=ValueError("메시지 생성 오류")):
        with caplog.at_level("ERROR"):
            # 메시지 생성이 실패해도 예외를 던지면 안 된다
            alerts.notify_slack_failure({"task_instance": MagicMock()})

    # 메시지 생성 실패가 로그되었는지 확인
    assert any("메시지 생성 실패" in rec.message for rec in caplog.records)
