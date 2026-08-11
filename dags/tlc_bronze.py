"""
TLC Bronze DAG

역할:
1. TLC 데이터 다운로드 목록 생성
2. TLC 데이터 파일 다운로드
3. 다운로드 파일 검증
4. 검증된 파일을 Bronze 영역에 저장

모든 Task 간 데이터 전달은
dict 기반으로 처리한다.
"""

from datetime import datetime

from airflow.decorators import dag

from src.common.downloader import (
    generate_download_list,
    download_file,
)

from src.common.validator import (
    validate_download,
)

from src.tlc.bronze import (
    store_bronze,
)


@dag(
    dag_id="tlc_bronze",

    start_date=datetime(
        2025,
        8,
        1,
    ),

    schedule="@monthly",

    catchup=False,

    tags=[
        "bronze",
        "tlc",
    ],
)
def tlc_bronze():

    # -------------------------------------------------
    # 1. 다운로드 목록 생성
    # -------------------------------------------------
    download_list = generate_download_list()

    # -------------------------------------------------
    # 2. 파일 다운로드
    #
    # expand()를 사용하기 때문에
    # download_list에 들어있는 파일들을
    # 각각 독립적인 Task Instance로 실행한다.
    #
    # 예:
    # download_file[0]
    # download_file[1]
    # download_file[2]
    # ...
    # -------------------------------------------------
    downloaded_files = download_file.expand(
        file_info=download_list,
    )

    # -------------------------------------------------
    # 3. 다운로드 파일 검증
    #
    # 각 download_file의 결과를 받아서
    # 파일 존재 여부와 크기를 검사한다.
    # -------------------------------------------------
    validated_files = validate_download.expand(
        download_result=downloaded_files,
    )

    # -------------------------------------------------
    # 4. Bronze 저장
    #
    # 검증된 파일을 Bronze 영역으로 이동한다.
    # -------------------------------------------------
    store_bronze.expand(
        downloaded_file=validated_files,
    )


dag = tlc_bronze()