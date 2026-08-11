"""
TLC Bronze DAG

역할
1. TLC 데이터 다운로드
2. 다운로드 검증
3. Bronze 적재
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

    # 다운로드 목록 생성
    download_list = generate_download_list()

    # 파일 다운로드 (병렬)
    downloaded_files = download_file.expand(
        file_info=download_list,
    )

    # 다운로드 검증
    validated_files = validate_download(
        file_info=downloaded_files,
    )

    # Bronze 저장
    store_bronze(
        file_info=validated_files,
    )


dag = tlc_bronze()