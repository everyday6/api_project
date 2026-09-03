import logging
from unittest.mock import patch

import pytest

from src.common import tier_metrics
from src.common.tier_metrics import log_tier_summary

_KNOWN = ["rds", "snapshot", "hardcoded"]


@pytest.fixture(autouse=True)
def _reset_alert_state():
    tier_metrics._last_alert_at.clear()
    yield
    tier_metrics._last_alert_at.clear()


def _tiers(hardcoded: int, rds: int) -> list[str]:
    return ["hardcoded"] * hardcoded + ["rds"] * rds


def test_log_tier_summary_still_logs_one_summary_line(caplog):
    logger = logging.getLogger("t")
    with caplog.at_level("INFO"):
        log_tier_summary(logger, "type3_fallback_tier_summary", _tiers(1, 4), _KNOWN)

    assert "[type3_fallback_tier_summary] rds=4 snapshot=0 hardcoded=1 total=5" in caplog.text


def test_alerts_when_hardcoded_ratio_exceeds_threshold():
    logger = logging.getLogger("t")
    with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
        log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(15, 5), _KNOWN)  # 75%

    mock_notify.assert_called_once()
    msg = mock_notify.call_args.args[0]
    assert "type2_fallback_tier_summary" in msg
    assert "hardcoded=75%" in msg


def test_silent_when_hardcoded_ratio_within_threshold():
    logger = logging.getLogger("t")
    with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
        log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(1, 99), _KNOWN)  # 1%

    mock_notify.assert_not_called()


def test_silent_for_small_requests_even_if_all_hardcoded():
    logger = logging.getLogger("t")
    with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
        log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(5, 0), _KNOWN)  # total 5 < 20

    mock_notify.assert_not_called()


def test_rate_limited_within_cooldown():
    logger = logging.getLogger("t")
    with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
        log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(20, 0), _KNOWN)
        log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(20, 0), _KNOWN)

    assert mock_notify.call_count == 1


def test_cooldown_is_per_tag():
    logger = logging.getLogger("t")
    with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
        log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(20, 0), _KNOWN)
        log_tier_summary(logger, "type3_fallback_tier_summary", _tiers(20, 0), _KNOWN)

    assert mock_notify.call_count == 2


def test_terminal_tier_is_last_of_known_tiers_not_avg():
    # type1은 fresh/avg/hardcoded 순 - 새벽에 avg가 100%여도 알림 대상이 아니다.
    logger = logging.getLogger("t")
    type1_known = ["fresh", "avg", "hardcoded"]
    with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
        log_tier_summary(logger, "fallback_tier_summary", ["avg"] * 50, type1_known, extra="type=1")

    mock_notify.assert_not_called()


def test_first_alert_fires_even_when_monotonic_clock_is_small():
    # time.monotonic()은 부팅 후 경과 초 - 갓 뜬 런너/Lambda에서는 쿨다운
    # (600s)보다 작을 수 있다. "한 번도 안 보냄"을 0.0으로 취급하면 그 창
    # 동안 첫 알림이 삼켜지므로(콜드 스타트 = 장애 상황일 때 특히 나쁨),
    # 미기록 tag의 첫 알림은 monotonic 값과 무관하게 발화해야 한다.
    logger = logging.getLogger("t")
    with patch.object(tier_metrics.time, "monotonic", return_value=5.0):
        with patch.object(tier_metrics, "notify_slack_message") as mock_notify:
            log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(20, 0), _KNOWN)

    mock_notify.assert_called_once()


def test_alert_send_failure_does_not_propagate(caplog):
    logger = logging.getLogger("t")
    with patch.object(tier_metrics, "notify_slack_message", side_effect=RuntimeError("slack down")):
        with caplog.at_level("ERROR"):
            log_tier_summary(logger, "type2_fallback_tier_summary", _tiers(20, 0), _KNOWN)

    assert "Slack 알림 전송 실패" in caplog.text
