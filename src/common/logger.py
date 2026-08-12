"""
프로젝트 공통 Logger
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

LOG_DIR = Path("logs")


def get_logger(name: str, log_to_file: bool = False, log_file_stem: str | None = None) -> logging.Logger:
    """
    Logger 생성

    Args:
        name: Logger 이름
        log_to_file: True면 logs/{log_file_stem or name}.log 파일에도 기록
        log_file_stem: 로그 파일명 (지정 안 하면 name 사용)

    Returns:
        logging.Logger
    """

    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "[%(asctime)s] %(levelname)s - %(message)s"
    )

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    if log_to_file:
        LOG_DIR.mkdir(exist_ok=True)
        file_handler = logging.FileHandler(LOG_DIR / f"{log_file_stem or name}.log")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger