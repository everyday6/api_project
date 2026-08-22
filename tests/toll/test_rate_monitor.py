from src.toll.rate_monitor import build_reminder_message


def test_build_reminder_message_includes_all_urls():
    message = build_reminder_message(urls=["https://example.com/a", "https://example.com/b"])

    assert "https://example.com/a" in message
    assert "https://example.com/b" in message


def test_build_reminder_message_mentions_config_file():
    message = build_reminder_message(urls=["https://example.com/a"])

    assert "toll_rates.yaml" in message
