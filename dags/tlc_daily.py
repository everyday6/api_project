"""
TLC 신규 데이터 확인 (운영 중)

tlc_pipeline이 과거~현재 데이터를 한 번에 받아오는
초기 적재용이라면, 이 DAG는 서비스 운영 중에
매일 자정 신규 데이터가 올라왔는지 확인하는 용도다.

TLC 데이터는 정확히 한 달 뒤가 아니라 몇 달씩 지연을 두고
불규칙하게 올라오기 때문에, 특정 날짜를 기다리는 대신
매일 최근 몇 달치를 다시 확인해서 새로 생긴 파일만 처리한다.
(이미 Bronze에 있는 파일은 건너뜀)

Download
    ↓
Validate
    ↓
Bronze
    ↓
Validate (Great Expectations)
    ↓
Silver

신규 파일이 없는 날은 각 단계가 빈 목록에 대해 실행되어
아무 일도 하지 않고 정상 종료된다.
"""

from datetime import datetime, timedelta

from airflow.decorators import dag

from src.common.alerts import notify_slack_failure
from src.common.downloader import (
    generate_incremental_download_list,
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

from src.tlc.bronze_validation import (
    validate_bronze_quality,
)

# download_file.expand()가 파일 개수만큼 태스크 인스턴스를 만드는 mapped
# task라서, default_args에 on_failure_callback을 넣으면 실패한 인덱스마다
# Slack이 따로 온다(예: 120개 중 20개 실패 -> 알림 20개). retries만 여기
# 걸고, on_failure_callback은 아래 @dag()에 DAG 레벨로 따로 걸어서 이 DAG
# run 전체가 실패로 확정될 때 딱 한 번만 오게 한다.
default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}


# =========================================================
# DAG
# =========================================================

@dag(
    dag_id="tlc_daily",
    start_date=datetime(2025, 8, 1),
    schedule="@daily",
    catchup=False,
    # 이전 실행이 아직 안 끝났는데 다음 실행이 겹쳐서 시작되면
    # 같은 파일(tmp 경로가 run별로 안 나뉘어 있음)을 두고 충돌할 수 있어서
    # 동시에 1개만 실행되게 제한
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_slack_failure,
    tags=["TLC", "daily"],
)
def tlc_daily():

    # -----------------------------------------
    # 1. 신규 데이터 확인
    # -----------------------------------------

    download_list = generate_incremental_download_list()

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
    # 6. 청크별 Bronze 데이터 품질 검증 (Great Expectations)
    # -----------------------------------------

    validated_chunks = validate_bronze_quality.expand(
        bronze_chunk=bronze_chunks,
    )

    # -----------------------------------------
    # 7. 청크별 Silver 변환 (청크당 Spark 세션 1개)
    # -----------------------------------------

    build_silver.expand(
        bronze_chunk=validated_chunks,
    )


# DAG 생성
tlc_daily()
