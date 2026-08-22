import logging

from src.common.logger import get_logger


def test_get_logger_returns_working_logger_without_file_handler():
    logger = get_logger("test.no_file_logging")

    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO


def test_get_logger_falls_back_silently_when_log_dir_is_read_only(monkeypatch, tmp_path):
    read_only_dir = tmp_path / "readonly-logs"
    read_only_dir.mkdir()
    read_only_dir.chmod(0o500)  # 쓰기 금지 — Lambda의 /var/task 흉내

    monkeypatch.setattr("src.common.logger.LOG_DIR", read_only_dir / "nested")

    logger = get_logger("test.read_only_log_dir", log_to_file=True, log_file_stem="ro")

    assert isinstance(logger, logging.Logger)
    assert not any(
        isinstance(h, logging.handlers.RotatingFileHandler) for h in logger.handlers
    )
