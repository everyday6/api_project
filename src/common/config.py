"""
프로젝트 공통 설정 파일

프로젝트 전반에서 사용하는 설정값을 관리한다.
환경이 변경되더라도 이 파일만 수정하면 된다.
"""

from datetime import datetime
from pathlib import Path

# ==========================
# TLC 데이터 설정
# ==========================

# TLC 원본 데이터 URL
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

# 다운로드 대상 기간
# 최초 적재 시작 월
INITIAL_START_DATE = datetime(2022, 9, 1)
# 최초 적재 종료 월
INITIAL_END_DATE = datetime(2025, 8, 1)

# 다운로드할 택시 종류
TAXI_TYPES = [
    "yellow",
    "green",
    "fhv",
    "fhvhv",
]

# ==========================
# 저장 경로
# ==========================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

DATA_DIR = PROJECT_ROOT / "data"

TMP_DIR = DATA_DIR / "tmp"

BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"

# ==========================
# HTTP 설정
# ==========================

# HEAD / GET Timeout
HTTP_TIMEOUT = 30

# 다운로드 Chunk 크기
CHUNK_SIZE = 8192

# User-Agent
USER_AGENT = {
    "User-Agent": "Traffic-Score-Project/1.0"
}