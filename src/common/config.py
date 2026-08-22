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

# ==========================
# RDS (Gold 서빙 테이블) 설정
# ==========================

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT", "5432")
RDS_DB = os.getenv("RDS_DB")
RDS_USER = os.getenv("RDS_USER")
RDS_PASSWORD = os.getenv("RDS_PASSWORD")

# ==========================
# DynamoDB (nav 골드 데이터셋 서빙) 설정
# ==========================

# nav 골드 데이터셋(segment_id x type 조회)은 RDS가 아니라 DynamoDB로
# 서빙한다 — 접근 패턴이 key-value 조회(BatchGetItem)뿐이고, 타입별로
# 갱신 주기가 달라 RDS의 write_table() 전체 replace 방식이 안 맞기
# 때문이다(자세한 배경은 docs/superpowers/specs/2026-08-21-navigation-gold-pipeline-design.md).
DYNAMO_REGION = os.getenv("AWS_REGION", "us-east-1")

# APP_ENV=local이면 docker-compose의 dynamodb-local(호스트 포트 8002)에
# 붙는다. 컨테이너 안에서 도는 스크립트/DAG는 LOCAL_RDS_HOST와 동일한
# 이유로 서비스명("dynamodb-local")을 써야 하므로 환경변수로 덮어쓸 수
# 있게 둔다.
DYNAMO_LOCAL_ENDPOINT = os.getenv("DYNAMO_LOCAL_ENDPOINT", "http://localhost:8002")

NAV_GOLD_TABLE = "nav_gold_values"

# ==========================
# EMR Serverless (Spark 잡 실행) 설정
# ==========================
#
# TLC Spark 잡(build_silver 등)을 Airflow worker 안에서 SparkSession으로
# 직접 여는 대신 EMR Serverless에 제출한다 — spark-master/worker 컨테이너를
# EC2에 상주시키지 않고, 무거운 컴퓨트를 온디맨드로 분리하기 위함
# (src/common/emr_serverless.py 참고). APP_ENV=local 로컬 개발 모드는 아직
# 이 경로를 지원하지 않는다 — EMR Serverless는 실제 AWS 계정이 있어야
# 제출 가능해서 로컬 대체 수단이 없다.

EMR_APPLICATION_ID = os.getenv("EMR_APPLICATION_ID")
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN")

EMR_JOBS_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/emr-jobs")

# ==========================
# HTTP 설정
# ==========================

# HEAD / GET Timeout
HTTP_TIMEOUT = 60
CHUNK_SIZE = 8192
USER_AGENT = {"User-Agent": "Navigation-Data-Project/1.0"}

# NYC Open Data(Socrata) 페이지당 최대 조회 건수
SOCRATA_PAGE_SIZE = 50_000

# Airflow 장애 알림
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ==========================
# 2016 하차 위경도 Hotspot 설정
# ==========================

# BigQuery로 받은 2016년 하차 위경도 grid(bq-results.csv)의 좌표계.
# TLC가 2017년부터 정확한 위경도 대신 zone_id만 제공하므로, 위경도 기준으로
# zone 내부 분포를 볼 수 있는 마지막 해 데이터다.
BQ_HOTSPOT_CRS = "EPSG:4326"

# zone 내부 세그먼트별 spatial_weight 계산 시, grid point가 0건 매칭된
# 세그먼트도 완전히 0이 되지 않게 하는 라플라스 스무딩 상수. 정성적 초안이다
# (TODO, 팀 검토 필요) — docs/superpowers/specs/2026-08-19-segment-spatial-weight-design.md 참고.
LAPLACE_SMOOTHING_ALPHA = 1.0

# grid point 하나(8~11m 셀)가 세그먼트에 매칭될 때, 이 반경(feet) 이내 세그먼트
# 전부를 후보로 삼아 거리 역가중으로 나눠 배분한다. venue-도로 매핑에 쓴
# TICKETMASTER_LION_BUFFER_FT(200ft)보다 좁게 잡은 이유는 grid 셀 자체가 훨씬
# 작기 때문이다. 정성적 초안이다(TODO, 팀 검토 필요).
HOTSPOT_SEGMENT_BUFFER_FT = 100

# 반경 안 세그먼트에 거리 역가중(1/(distance+epsilon))을 매길 때, point가 세그먼트
# 위에 정확히 있어 distance=0이 되는 경우의 0-division만 막는 최소 상수. 정성적
# 초안이다(TODO, 팀 검토 필요).
HOTSPOT_INVERSE_DISTANCE_EPSILON_FT = 1.0

# ==========================
# 세그먼트 지표 API — DynamoDB 서빙 저장소 설정
# ==========================
#
# 타입별로 완전히 분리된 테이블을 쓴다(팀원이 타입별로 독립 개발하기 때문 —
# 접두사 컨벤션이 아니라 물리적으로 다른 테이블). 자세한 설계 근거는
# docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md 6절 참고.

DYNAMODB_TABLE_TYPE1 = os.getenv("DYNAMODB_TABLE_TYPE1", "SegmentMetricsType1")
DYNAMODB_TABLE_TYPE2 = os.getenv("DYNAMODB_TABLE_TYPE2", "SegmentMetricsType2")

# APP_ENV=local이면 dynamodb-local 컨테이너를 가리킨다. aws(EC2)에서는 빈 값으로
# 둬서 boto3가 기본 리전 엔드포인트를 쓰게 한다(다른 AWS 자격증명 설정과 동일한
# 패턴 — 여기서 없다고 에러내지 않는다, 실제 클라이언트 생성 시점에서만 확인).
DYNAMODB_ENDPOINT_URL = (
    "http://dynamodb-local:8000" if APP_ENV == "local" else None
)

# Fallback 체인(설계 문서 7절)에서 쓰는 예약 키.
# GLOBAL_PARTITION_KEY: 실제 segment_id가 아닌 예약된 PK — 배포 시점에 수동으로
#   심어두는 전역 기본값 전용 파티션.
GLOBAL_PARTITION_KEY = "GLOBAL"
DEFAULT_SORT_KEY = "DEFAULT"
AVG_SORT_KEY = "AVG"
LENGTH_SORT_KEY = "LENGTH"

# 하루를 30분 단위로 나눈 버킷 수(00:00~23:30 -> 48개). 버킷 키는 "HHMM" 문자열.
BUCKET_MINUTES = 30

# type1(시간) 버킷 값을 계산할 때 참고하는 최근 관측치 범위(일). 조정 가능한
# 파라미터라 상수로 뺐다 — 실측 후 조정.
ROLLING_WINDOW_DAYS = 14

# ==========================
# EMR Serverless (Spark job 실행) 설정
# ==========================
#
# Airflow worker 프로세스 안에서 SparkSession을 직접 여는 대신, 변환 로직을
# 담은 스크립트(spark_jobs/*.py)를 EMR Serverless에 제출하고 완료를 기다린다
# (src/common/emr_serverless.py 참고).

EMR_APPLICATION_ID = os.getenv("EMR_APPLICATION_ID")
EMR_JOB_ROLE_ARN = os.getenv("EMR_JOB_ROLE_ARN")

if APP_ENV == "local":
    EMR_JOBS_DIR = PROJECT_ROOT / "data" / "emr-jobs"
else:
    EMR_JOBS_DIR = S3Path(f"s3://{S3_BUCKET_DATA}/emr-jobs")

# EMR Serverless job이 spark.archives로 실어가는 패키징된 파이썬 venv
# (pandas/geopandas/shapely/pyproj 등 서드파티 의존성). requirements.txt가
# 바뀌면 scripts/package_emr_dependencies.sh로 다시 만들어 올려야 한다.
EMR_PYTHON_ENV_S3_PATH = EMR_JOBS_DIR / "python-env" / "pyspark_deps.tar.gz"

# ==========================
# 속도(speed) - LION 매핑 설정
# ==========================
#
# ticketmaster/gold1.py의 venue-LION 매핑과 동일한 buffer+nearest 패턴을
# 쓴다 — 대상이 Point(venue)가 아니라 LineString(속도 링크)이라는 점만 다르다.

SPEED_CRS = "EPSG:4326"

# 속도 링크 주변 도로 매핑 반경(feet). 도로 링크는 보통 LION 세그먼트 여러
# 개로 쪼개져 있어(하나의 corridor가 여러 블록으로 나뉨), venue보다 좁게
# 잡아도 충분히 겹친다 — 정성적 초안(TODO, 팀 검토 필요).
SPEED_LION_BUFFER_FT = 50

# fallback nearest 매핑 품질 기준.
SPEED_LION_WARN_DISTANCE_FT = 200
SPEED_LION_MAX_DISTANCE_FT = 1000

# 이 미만인 속도 판독값은 계산에서 제외한다(0 또는 비정상적으로 낮은 값 —
# 정차/정지 상태로 잘못 기록된 값과 실제 정체를 구분하기 위한 정성적
# 초안, TODO 팀 검토 필요).
MIN_VALID_SPEED_MPH = 1.0
