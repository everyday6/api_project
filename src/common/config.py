"""내비게이션 데이터 파이프라인 공통 설정."""

import os
from pathlib import Path

from cloudpathlib import S3Path
from dotenv import load_dotenv


load_dotenv()

# TLC 원본 데이터
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
TAXI_TYPES = ["yellow", "green", "fhv", "fhvhv"]

# 매일 다음 공개 후보 1개월과 최근 완료 3개월을 확인한다.
TLC_PUBLISH_LAG_MONTHS = 2
RECENT_MONTHS_WINDOW = 4
TLC_TIMEZONE = "America/New_York"
TLC_TYPE3_ID = 3
TLC_TYPE3_DOW_NAMES = ("MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN")
TLC_TYPE3_ROLLING_WEEKS = int(os.getenv("TLC_TYPE3_ROLLING_WEEKS", "12"))

# 로컬 경로
PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = PROJECT_ROOT / "config"
LOG_DIR = PROJECT_ROOT / "logs"
TMP_DIR = PROJECT_ROOT / "data" / "tmp"

# "local"은 로컬 디스크와 Spark local[*]를 사용한다. 운영 기본값은 "aws"다.
APP_ENV = os.getenv("APP_ENV", "aws")

# AWS에서는 정적 키 대신 EC2 IAM Role로 인증한다.
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_DATA = os.getenv("S3_BUCKET_DATA")
DYNAMODB_NAV_TABLE = os.getenv("DYNAMODB_NAV_TABLE")

if APP_ENV == "local":
    BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
    SILVER1_DIR = PROJECT_ROOT / "data" / "silver1"
    SILVER2_DIR = PROJECT_ROOT / "data" / "silver2"
    GOLD1_DIR = PROJECT_ROOT / "data" / "gold1"
    GOLD2_DIR = PROJECT_ROOT / "data" / "gold2"
else:
    BRONZE_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/bronze")
    SILVER1_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/silver1")
    SILVER2_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/silver2")
    GOLD1_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/gold1")
    GOLD2_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/gold2")

# TLC HTTP 다운로드
HTTP_TIMEOUT = 60
CHUNK_SIZE = 8192
USER_AGENT = {"User-Agent": "Navigation-Data-Project/1.0"}

# Airflow 장애 알림
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
