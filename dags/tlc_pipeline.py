"""
TLC 전체 ETL Pipeline

Download
    ↓
Validate
    ↓
Bronze
    ↓
Silver

의 전체 파이프라인을 단계별로 구성한다.

파일별로 독립적으로 흘러가는 방식이 아니라,
한 단계(예: Download)가 전체 파일에 대해 다 끝나야
다음 단계(예: Validate)가 전체적으로 시작된다.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag

from src.common.alerts import notify_slack_failure
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

from src.tlc.silver import (
    build_silver,
    chunk_bronze_files,
)

# download_file.expand()가 파일 개수만큼 태스크 인스턴스를 만드는 mapped
# task라서, default_args에 on_failure_callback을 넣으면 실패한 인덱스마다
# Slack이 따로 온다. retries만 여기 걸고, on_failure_callback은 아래
# @dag()에 DAG 레벨로 따로 걸어서 이 DAG run 전체가 실패로 확정될 때
# 딱 한 번만 오게 한다.
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# =========================================================
# DAG
# =========================================================

@dag(
    dag_id="tlc_pipeline",
    start_date=datetime(2025, 8, 1),
    # 과거~현재 데이터를 한 번에 받아오는 초기 적재용 — AWS에 올린 뒤 1회만
    # 트리거하고, 이후 신규 데이터 확인은 tlc_daily(@daily)가 전담한다.
    schedule=None,
    catchup=False,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["TLC"],
)
def tlc_pipeline():

    # -----------------------------------------
    # 1. 다운로드 대상 파일 목록 생성
    # -----------------------------------------

    download_list = generate_download_list()

    # -----------------------------------------
    # 2. 전체 다운로드
    # -----------------------------------------

    downloaded_files = download_file.expand(
        file_info=download_list,
    )

    # -----------------------------------------
    # 3. 전체 검증
    # -----------------------------------------

    validated_files = validate_download.expand(
        download_result=downloaded_files,
    )

    # -----------------------------------------
    # 4. 전체 Bronze 저장
    # -----------------------------------------

    bronze_files = store_bronze.expand(
        downloaded_file=validated_files,
    )

    # -----------------------------------------
    # 5. taxi_type별 청크로 묶기
    # -----------------------------------------

    bronze_chunks = chunk_bronze_files(
        bronze_files=bronze_files,
    )

    # -----------------------------------------
    # 6. 청크별 Silver 변환 (청크당 Spark 세션 1개)
    # -----------------------------------------

    build_silver.expand(
        bronze_chunk=bronze_chunks,
    )


# DAG 생성
tlc_pipeline()
