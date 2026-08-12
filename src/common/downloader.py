"""
TLC 데이터 다운로드 모듈

TLC 데이터의 다운로드 대상 파일을 생성하고,
각 파일을 실제로 다운로드하여 임시 저장소에 저장한다.

주요 작업:
1. 기간과 택시 종류를 기준으로 다운로드 파일 목록 생성
2. TLC 서버에 파일이 존재하는지 확인
3. 파일 다운로드
4. 다운로드한 파일을 임시 디렉터리에 저장

Airflow Task 간 데이터 전달은
커스텀 객체가 아닌 dict를 사용한다.
"""

from __future__ import annotations

import time
from datetime import datetime, timedelta

import requests
from dateutil.relativedelta import relativedelta

from airflow.decorators import task

from src.common.config import (
    BASE_URL,
    INITIAL_START_DATE,
    INITIAL_END_DATE,
    TMP_DIR,
    TAXI_TYPES,
    HTTP_TIMEOUT,
    CHUNK_SIZE,
    USER_AGENT,
)

from src.common.logger import get_logger


# =========================================================
# Logger
# =========================================================

logger = get_logger(__name__)


# =========================================================
# HTTP Session
# =========================================================

# 여러 파일을 다운로드할 때 HTTP 연결을 재사용하기 위해
# 하나의 Session을 사용한다.
session = requests.Session()

# TLC 서버에 요청할 때 사용할 공통 Header
session.headers.update(USER_AGENT)


# =========================================================
# 파일명 생성
# =========================================================

def build_filename(
    taxi_type: str,
    year: int,
    month: int,
) -> str:
    """
    TLC 데이터 파일명을 생성한다.

    예:
    yellow + 2022 + 09
    → yellow_tripdata_2022-09.parquet
    """

    return (
        f"{taxi_type}_tripdata_"
        f"{year}-{month:02d}.parquet"
    )


# =========================================================
# URL 생성
# =========================================================

def build_url(
    filename: str,
) -> str:
    """
    생성된 파일명을 이용하여
    TLC 데이터 다운로드 URL을 만든다.
    """

    return f"{BASE_URL}/{filename}"


# =========================================================
# 파일 존재 여부 확인
# =========================================================

def check_file_exists(
    url: str,
    max_attempts: int = 3,
) -> bool:
    """
    TLC 서버에 해당 파일이 존재하는지 확인한다.

    반환값:
    - 200 → True
    - 404 → False
    - 그 외 → CloudFront 쪽의 일시적인 오류/제한일 수 있으므로
      잠깐 대기 후 재시도, 그래도 계속 실패하면 오류 발생
    """

    for attempt in range(1, max_attempts + 1):

        try:

            # 파일 자체를 다운로드하지 않고
            # HEAD 요청으로 존재 여부만 확인한다.
            response = session.head(
                url=url,
                timeout=HTTP_TIMEOUT,
            )

            # 파일이 존재함
            if response.status_code == 200:
                return True

            # 파일이 존재하지 않음
            if response.status_code == 404:
                return False

            logger.warning(
                f"HEAD Request Unexpected Status "
                f"({response.status_code}, attempt {attempt}/{max_attempts}) : {url}"
            )

        except requests.RequestException as error:

            logger.warning(
                f"HEAD Request Failed "
                f"(attempt {attempt}/{max_attempts}) : {url} ({error})"
            )

        # 마지막 시도가 아니면 잠깐 대기 후 재시도
        if attempt < max_attempts:
            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(
        f"HEAD Request Failed after {max_attempts} attempts : {url}"
    )


# =========================================================
# 다운로드 목록 생성
# =========================================================

@task(
    retries=2,
    retry_delay=timedelta(minutes=1),
)
def generate_download_list() -> list[dict]:
    """
    지정된 기간 동안 존재하는 TLC 데이터의
    다운로드 목록을 생성한다.

    Airflow XCom으로 전달하기 위해
    DownloadFile 객체 대신 dict를 반환한다.
    """

    # 다운로드 대상 파일 목록
    download_list: list[dict] = []

    # 시작 날짜
    current = INITIAL_START_DATE

    # 설정된 종료 날짜까지 월 단위로 반복
    while current <= INITIAL_END_DATE:

        year = current.year
        month = current.month

        # Yellow / Green / FHV / FHVHV 순회
        for taxi_type in TAXI_TYPES:

            # -------------------------------------------------
            # 1. 파일명 생성
            # -------------------------------------------------

            filename = build_filename(
                taxi_type=taxi_type,
                year=year,
                month=month,
            )

            # -------------------------------------------------
            # 2. 다운로드 URL 생성
            # -------------------------------------------------

            url = build_url(
                filename=filename,
            )

            # -------------------------------------------------
            # 3. 파일 존재 여부 확인
            # -------------------------------------------------

            if check_file_exists(url):

                logger.info(
                    f"Found : {filename}"
                )

                # 커스텀 객체 대신 dict 사용
                # → Airflow XCom에서 안전하게 전달 가능
                download_list.append(
                    {
                        "taxi_type": taxi_type,
                        "filename": filename,
                        "url": url,
                    }
                )

            else:

                logger.warning(
                    f"Skip : {filename}"
                )

        # 다음 달로 이동
        current += relativedelta(
            months=1
        )

    logger.info(
        f"Total Files : {len(download_list)}"
    )

    return download_list


# =========================================================
# 파일 다운로드
# =========================================================

@task(
    retries=3,
    retry_delay=timedelta(minutes=1),
    # 동시 다운로드 개수를 4개로 제한해서 대용량 파일(fhvhv 등)이
    # 네트워크 대역폭을 너무 잘게 나눠쓰지 않게 함
    pool="downloads",
)
def download_file(
    file_info: dict,
) -> dict:
    """
    다운로드 목록에 있는 파일 하나를 다운로드한다.

    입력:
        file_info
        {
            "taxi_type": "...",
            "filename": "...",
            "url": "..."
        }

    반환:
        {
            "taxi_type": "...",
            "filename": "...",
            "tmp_path": "..."
        }
    """

    # -------------------------------------------------
    # 1. 임시 디렉터리 생성
    # -------------------------------------------------

    TMP_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    # -------------------------------------------------
    # 2. 임시 저장 경로 생성
    # -------------------------------------------------

    filename = file_info["filename"]

    save_path = TMP_DIR / filename

    try:

        logger.info(
            f"Download Start : {filename}"
        )

        # -------------------------------------------------
        # 3. 파일 다운로드
        # -------------------------------------------------

        response = session.get(
            url=file_info["url"],
            timeout=HTTP_TIMEOUT,
            stream=True,
        )

        # HTTP 오류가 발생하면 예외 발생
        response.raise_for_status()

        # -------------------------------------------------
        # 4. 파일 저장
        # -------------------------------------------------

        with open(
            save_path,
            "wb",
        ) as file:

            for chunk in response.iter_content(
                chunk_size=CHUNK_SIZE
            ):

                if chunk:
                    file.write(chunk)

        logger.info(
            f"Download Complete : {filename}"
        )

        # -------------------------------------------------
        # 5. 다음 Task로 결과 전달
        # -------------------------------------------------
        #
        # DownloadResult 객체가 아니라 dict를 반환한다.
        #

        return {
            "taxi_type": file_info["taxi_type"],
            "filename": filename,
            "tmp_path": str(save_path),
        }

    except requests.RequestException as error:

        logger.error(
            f"Download Failed : "
            f"{filename} ({error})"
        )

        # 다운로드 실패한 파일 삭제
        if save_path.exists():
            save_path.unlink()

        raise

    except Exception as error:

        logger.error(
            f"Unexpected Error : "
            f"{filename} ({error})"
        )

        # 예상하지 못한 오류가 발생한 경우에도
        # 불완전하게 저장된 파일을 삭제한다.
        if save_path.exists():
            save_path.unlink()

        raise


# =========================================================
# 시작 날짜 반환
# =========================================================

def get_start_date() -> datetime:
    """
    다운로드 시작 날짜를 반환한다.
    """

    return INITIAL_START_DATE