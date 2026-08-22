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
from zoneinfo import ZoneInfo

import requests
from dateutil.relativedelta import relativedelta

from airflow.decorators import task

from src.common.config import (
    BASE_URL,
    TMP_DIR,
    TAXI_TYPES,
    HTTP_TIMEOUT,
    CHUNK_SIZE,
    USER_AGENT,
    TLC_PUBLISH_LAG_MONTHS,
    RECENT_MONTHS_WINDOW,
    TLC_TIMEZONE,
)

from src.common.logger import get_logger
from src.tlc.bronze import BRONZE_ROOT


# =========================================================
# Logger
# =========================================================

logger = get_logger(__name__, log_to_file=True, log_file_stem="tlc_bronze")


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
    treat_403_as_missing: bool = False,
) -> bool:
    """
    TLC 서버에 해당 파일이 존재하는지 확인한다.

    이 버킷은 공개 목록 조회(ListBucket) 권한이 없어서,
    파일이 없을 때 404가 아니라 403(Access Denied)을 준다.
    근데 실제로 존재하는 파일에서도 CloudFront 쪽 일시적인
    오류/제한으로 403이 뜰 때가 있어서, 둘을 구분해야 한다.

    반환값:
    - 200 → True
    - 404 → False
    - 403 →
        treat_403_as_missing=True면 "그냥 아직 없는 파일"로 보고
        바로 False (예: 매일 신규 데이터 확인할 때 사용).
        False(기본값)면 이미 존재해야 하는 파일인데 403이 뜬 것으로
        보고, 일시적인 오류일 수 있으니 재시도 후에도 계속 403이면
        오류 발생 (예: 존재를 이미 알고 있는 과거 데이터 백필용).
    - 그 외(5xx 등) → CloudFront 쪽의 일시적인 오류일 수 있으므로
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

            # 아직 안 올라온 파일을 확인하는 상황이면
            # 403도 "없음"으로 바로 처리한다.
            if response.status_code == 403 and treat_403_as_missing:
                return False

            logger.warning(
                f"HEAD 요청 응답 이상 "
                f"(상태 코드 {response.status_code}, {attempt}/{max_attempts}번째 시도) : {url}"
            )

        except requests.RequestException as error:

            logger.warning(
                f"HEAD 요청 실패 "
                f"({attempt}/{max_attempts}번째 시도) : {url} ({error})"
            )

        # 마지막 시도가 아니면 잠깐 대기 후 재시도
        if attempt < max_attempts:
            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(
        f"HEAD 요청이 {max_attempts}번 시도 후에도 계속 실패했습니다 : {url}"
    )


# =========================================================
# 신규 데이터 확인 (운영 중 매일 실행)
# =========================================================

def get_recent_service_months(reference_time: datetime | None = None) -> list[datetime]:
    """다음 공개 후보 1개월과 최근 완료 3개월의 월 시작일을 반환한다."""

    current_time = reference_time or datetime.now(ZoneInfo(TLC_TIMEZONE))
    return [
        (current_time - relativedelta(months=offset)).replace(
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )
        for offset in range(
            TLC_PUBLISH_LAG_MONTHS,
            TLC_PUBLISH_LAG_MONTHS + RECENT_MONTHS_WINDOW,
        )
    ]


@task(
    retries=2,
    retry_delay=timedelta(minutes=1),
)
def generate_incremental_download_list() -> list[dict]:
    """
    오늘 날짜 기준으로 새로 올라왔을 만한 TLC 데이터를 확인한다.

    다음 공개 후보 1개월과 최근 완료 3개월을 매일 다시 확인한다.

    이미 Bronze에 저장된 파일은 로컬에 존재하는지만 확인해서 건너뛰고
    (서버에 다시 물어보지 않음), 아직 없는 파일만 서버에 존재 여부를 확인한다.
    """

    download_list: list[dict] = []

    for target in get_recent_service_months():

        for taxi_type in TAXI_TYPES:

            filename = build_filename(
                taxi_type=taxi_type,
                year=target.year,
                month=target.month,
            )

            # 이미 Bronze에 있으면 서버에 물어볼 필요 없이 건너뛴다.
            if (BRONZE_ROOT / filename).exists():
                continue

            url = build_url(
                filename=filename,
            )

            if check_file_exists(url, treat_403_as_missing=True):

                logger.info(
                    f"신규 파일 확인됨 : {filename}"
                )

                download_list.append(
                    {
                        "taxi_type": taxi_type,
                        "filename": filename,
                        "url": url,
                    }
                )

            else:

                logger.info(
                    f"아직 올라오지 않음 : {filename}"
                )

    logger.info(
        f"신규 다운로드 대상 파일 수 : {len(download_list)}"
    )

    return download_list


# =========================================================
# 파일 다운로드
# =========================================================

@task(
    retries=5,
    retry_delay=timedelta(minutes=1),
    # TLC 서버가 몇 분 이상 일시적으로 403/5xx를 뱉는 경우까지
    # 버티도록 재시도 간격을 지수적으로 늘린다 (1분 → 2분 → ... → 최대 10분).
    retry_exponential_backoff=True,
    max_retry_delay=timedelta(minutes=10),
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
            f"다운로드 시작 : {filename}"
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
            f"다운로드 완료 : {filename}"
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
            f"다운로드 실패 : "
            f"{filename} ({error})"
        )

        # 다운로드 실패한 파일 삭제
        if save_path.exists():
            save_path.unlink()

        raise

    except Exception as error:

        logger.error(
            f"예상치 못한 오류 : "
            f"{filename} ({error})"
        )

        # 예상하지 못한 오류가 발생한 경우에도
        # 불완전하게 저장된 파일을 삭제한다.
        if save_path.exists():
            save_path.unlink()

        raise
