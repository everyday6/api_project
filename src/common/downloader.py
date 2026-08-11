"""
공통 다운로드 모듈

역할
1. 다운로드 목록 생성
2. 파일 다운로드
"""

from __future__ import annotations

import requests
from dateutil.relativedelta import relativedelta
from datetime import datetime

from airflow.decorators import task

from src.common.config import (
    BASE_URL,
    INITIAL_START_DATE,
    INITIAL_END_DATE,
    BRONZE_DIR,
    TAXI_TYPES,
    TMP_DIR,
    HTTP_TIMEOUT,
    CHUNK_SIZE,
    USER_AGENT,
)

from src.common.logger import get_logger

from src.common.models import (
    DownloadFile,
    DownloadResult,
)

# Logger 생성
logger = get_logger(__name__)

# HTTP Session 생성
session = requests.Session()

# 공통 Header 설정
session.headers.update(USER_AGENT)


def build_filename(
    taxi_type: str,
    year: int,
    month: int,
) -> str:
    """다운로드 파일명 생성"""

    return (
        f"{taxi_type}_tripdata_"
        f"{year}-{month:02d}.parquet"
    )


def build_url(filename: str) -> str:
    """다운로드 URL 생성"""

    return f"{BASE_URL}/{filename}"


def check_file_exists(url: str) -> bool:
    """파일 존재 여부 확인"""

    try:

        response = session.head(
            url=url,
            timeout=HTTP_TIMEOUT,
        )

        # 파일 존재
        if response.status_code == 200:
            return True

        # 파일 없음
        if response.status_code == 404:
            return False

        raise RuntimeError(
            f"HEAD Request Failed ({response.status_code}) : {url}"
        )

    except requests.RequestException as error:

        logger.error(
            f"HEAD Request Failed : {url} ({error})"
        )

        raise


@task
def generate_download_list() -> list[DownloadFile]:
    """다운로드 대상 목록 생성"""

    download_list: list[DownloadFile] = []

    current = INITIAL_START_DATE

    while current <= INITIAL_END_DATE:

        year = current.year
        month = current.month

        for taxi_type in TAXI_TYPES:

            # 파일명 생성
            filename = build_filename(
                taxi_type,
                year,
                month,
            )

            # URL 생성
            url = build_url(filename)

            # 파일 존재 여부 확인
            if check_file_exists(url):

                logger.info(f"Found : {filename}")

                download_list.append(
                    DownloadFile(
                        taxi_type=taxi_type,
                        filename=filename,
                        url=url,
                    )
                )

            else:

                logger.warning(f"Skip : {filename}")

        # 다음 달
        current += relativedelta(months=1)

    logger.info(
        f"Total Files : {len(download_list)}"
    )

    return download_list


@task
def download_file(
    file_info: DownloadFile,
) -> DownloadResult:
    """파일 다운로드"""

    # tmp 폴더 생성
    TMP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # 저장 경로
    save_path = TMP_DIR / file_info.filename

    try:

        logger.info(
            f"Download Start : {file_info.filename}"
        )

        # 파일 다운로드
        response = session.get(
            url=file_info.url,
            timeout=HTTP_TIMEOUT,
            stream=True,
        )

        response.raise_for_status()

        # 파일 저장
        with open(save_path, "wb") as file:

            for chunk in response.iter_content(
                chunk_size=CHUNK_SIZE
            ):

                if chunk:
                    file.write(chunk)

        logger.info(
            f"Download Complete : {file_info.filename}"
        )

        return DownloadResult(
            taxi_type=file_info.taxi_type,
            filename=file_info.filename,
            tmp_path=save_path,
        )

    except requests.RequestException as error:

        logger.error(
            f"Download Failed : "
            f"{file_info.filename} ({error})"
        )

        # 실패 파일 삭제
        if save_path.exists():
            save_path.unlink()

        raise

    except Exception as error:

        logger.error(
            f"Unexpected Error : "
            f"{file_info.filename} ({error})"
        )

        # 실패 파일 삭제
        if save_path.exists():
            save_path.unlink()

        raise

def get_start_date() -> datetime:
    """다운로드 시작 월 반환"""