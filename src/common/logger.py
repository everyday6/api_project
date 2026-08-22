"""
프로젝트 공통 Logger
"""

from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from .config import LOG_DIR
from pathlib import Path

# Airflow가 run_id/attempt별로 이미 태스크 로그를 남기므로, 여기 파일은
# "도메인별로 최근 실행을 빠르게 grep"하기 위한 보조 로그다. 여러 run이
# 계속 이어 붙는 성격상 무한정 커지지 않게 용량 기준으로 회전시킨다.
MAX_BYTES = 10 * 1024 * 1024  # 파일당 10MB
BACKUP_COUNT = 5  # {stem}.log.1 ~ .5 까지 최대 5개 보관


def get_logger(
    name: str,
    log_to_file: bool = False,
    log_file_stem: str | None = None,
    level: int = logging.INFO,
) -> logging.Logger:
    """Logger 생성. 핸들러는 실행 환경(Airflow 등)에 맡긴다.

    로거 자체의 레벨을 명시적으로 지정한다 — 안 그러면 NOTSET 상태로
    root logger 레벨을 그대로 물려받는데, root 레벨은 실행 환경마다
    다르다(Airflow는 태스크 실행 시 INFO로 맞추지만, `__main__`으로
    로컬 단독 실행하면 기본값인 WARNING이라 info() 호출이 전부
    조용히 씹힌다). 그래서 실행 환경과 무관하게 항상 INFO가 남도록
    여기서 고정한다.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if log_to_file:
        log_path = (LOG_DIR / f"{log_file_stem or name}.log").resolve()

        # 핸들러 타입이 아니라 실제로 이 파일을 가리키고 있는지로 확인한다.
        # 타입만 보면, 같은 로거가 다른 log_file_stem으로 다시 호출될 때
        # "이미 FileHandler가 있다"고 착각해서 새 파일에는 안 붙는다.
        already_attached = any(
            isinstance(h, RotatingFileHandler)
            and Path(h.baseFilename) == log_path
            for h in logger.handlers
        )

        if not already_attached:
            try:
                LOG_DIR.mkdir(exist_ok=True, parents=True)
                file_handler = RotatingFileHandler(
                    log_path,
                    maxBytes=MAX_BYTES,
                    backupCount=BACKUP_COUNT,
                    encoding="utf-8",
                )
                file_handler.setFormatter(
                    logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
                )
                logger.addHandler(file_handler)
            except OSError:
                # 읽기 전용 파일시스템(예: AWS Lambda의 /var/task)에서는 파일
                # 핸들러를 못 붙인다 — 표준 출력(→ CloudWatch Logs 등)으로만
                # 로깅을 계속한다.
                pass

    return logger