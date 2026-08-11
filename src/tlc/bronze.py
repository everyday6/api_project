"""
TLC Bronze 적재 모듈

역할:
1. 검증이 완료된 TLC 파일을 Bronze 폴더에 저장
2. 이미 존재하는 파일은 중복 저장하지 않음
3. 저장이 완료되면 다음 Task에서 사용할 파일 정보를 dict로 전달

Airflow Task 간 데이터 전달은
커스텀 객체가 아닌 dict를 사용한다.
"""

import shutil

from airflow.decorators import task

from src.common.config import BRONZE_DIR
from src.common.logger import get_logger


# Logger 생성
logger = get_logger(__name__)


@task
def store_bronze(
    downloaded_file: dict,
) -> dict:
    """
    검증이 완료된 파일을 Bronze 영역에 저장한다.

    입력:
        {
            "taxi_type": "...",
            "filename": "...",
            "tmp_path": "..."
        }

    반환:
        {
            "taxi_type": "...",
            "filename": "...",
            "bronze_path": "...",
            "is_new": True/False
        }
    """

    # -------------------------------------------------
    # 파일 정보 추출
    # -------------------------------------------------

    taxi_type = downloaded_file["taxi_type"]
    filename = downloaded_file["filename"]

    # downloader.py에서 tmp_path를 문자열로 전달했으므로
    # 파일 시스템 작업을 위해 Path 객체로 변환
    from pathlib import Path

    tmp_path = Path(
        downloaded_file["tmp_path"]
    )

    # -------------------------------------------------
    # Bronze 폴더 생성
    # -------------------------------------------------

    BRONZE_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # Bronze에 저장될 최종 경로
    bronze_path = BRONZE_DIR / filename

    # -------------------------------------------------
    # 이미 존재하는 파일 확인
    # -------------------------------------------------

    if bronze_path.exists():

        logger.warning(
            f"이미 존재하는 파일입니다. "
            f"건너뜁니다 : {filename}"
        )

        # 이미 Bronze에 저장되어 있으므로
        # 임시 파일은 삭제한다.
        if tmp_path.exists():
            tmp_path.unlink()

        return {
            "taxi_type": taxi_type,
            "filename": filename,
            "bronze_path": str(bronze_path),
            "is_new": False,
        }

    # -------------------------------------------------
    # Bronze 저장
    # -------------------------------------------------

    try:

        shutil.move(
            tmp_path,
            bronze_path,
        )

        logger.info(
            f"Bronze 저장 완료 : {filename}"
        )

        return {
            "taxi_type": taxi_type,
            "filename": filename,
            "bronze_path": str(bronze_path),
            "is_new": True,
        }

    except Exception as error:

        logger.error(
            f"Bronze 저장 실패 : "
            f"{filename} ({error})"
        )

        raise