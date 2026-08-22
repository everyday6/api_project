# EMR Serverless(Amazon Linux 기반 x86_64 런타임)와 바이너리 호환되는
# Python venv를 만들기 위한 빌드 전용 이미지. 이 이미지 자체를 실행하는
# 게 아니라, `docker build` 후 컨테이너에서 결과 tarball만 꺼내 쓴다
# (scripts/package_emr_dependencies.sh 참고).
#
# (이 줄은 build-emr-python-env 잡의 paths-filter가 이 파일 변경을 감지해야
# scripts/package_emr_dependencies.sh에 추가한 진단용 `ls -la`가 실제로
# 실행되므로 트리거 목적으로 추가 — 원인 확인되면 지워도 된다.)
#
# 로컬 Mac(arm64/Darwin)에서 그냥 pip install해서 만든 venv는 EMR
# Serverless(Amazon Linux, x86_64)에서 그대로 못 쓴다 — geopandas/shapely/
# pyproj 등은 C 확장을 포함해서 OS/아키텍처가 다르면 import 시점에
# 깨진다. 그래서 반드시 이 Dockerfile(linux/amd64, Amazon Linux 계열)
# 안에서 venv를 만들어야 한다.
FROM --platform=linux/amd64 amazonlinux:2023

RUN dnf install -y python3.11 python3.11-pip tar gzip && \
    dnf clean all

WORKDIR /build

COPY requirements.txt .

# EMR Serverless Spark job(spark_jobs/nav_length_job.py, nav_time_job.py)이
# 실제로 쓰는 서드파티 라이브러리만 골라 담는다:
#   - pandas, geopandas, shapely, pyproj : src/silver2/segment_speed_match.py,
#     src/nav_time/gold2.py 등의 지오메트리/좌표계 처리 + spark_jobs가 직접
#     쓰는 pd.read_parquet
#   - cloudpathlib[s3] : spark_jobs가 결과 JSON을 S3Path로 씀
#   - python-dotenv : src/common/config.py의 load_dotenv() (거의 모든
#     src.* import 시점에 실행됨)
#   - boto3 : src/common/dynamodb.py 및 cloudpathlib의 s3 백엔드
#   - pyarrow : spark_jobs/nav_time_job.py, spark_jobs/nav_length_job.py의
#     pd.read_parquet()가 실제로 쓰는 파케이 엔진. pandas가 있다고 자동으로
#     같이 오지 않는다 — 없으면 "Unable to find a usable engine" ImportError.
# pyspark 자체는 EMR Serverless가 이미 제공하므로 여기서 빼서 아카이브
# 크기를 줄인다(pytest, great_expectations, fastapi 등 드라이버 실행에
# 불필요한 나머지 requirements.txt 패키지도 마찬가지 이유로 제외).
RUN python3.11 -m venv /build/environment && \
    /build/environment/bin/pip install --no-cache-dir --upgrade pip venv-pack && \
    /build/environment/bin/pip install --no-cache-dir \
        pandas==2.2.2 \
        pyarrow==25.0.1 \
        geopandas \
        shapely \
        pyproj \
        "cloudpathlib[s3]" \
        python-dotenv==1.2.2 \
        boto3

# venv-pack의 check_prefix()는 prefix가 명시적으로 주어지지 않으면 오직
# VIRTUAL_ENV 환경변수만 보고 venv를 찾는다(pyvenv.cfg 존재 여부는 그
# 다음에야 확인한다) — `source .../activate`를 거치지 않는 Docker RUN
# 안에서는 이 변수가 비어 있어서 실제로 유효한 venv인데도
# "Current environment is not a virtual environment"로 실패한다.
# venv-pack 0.2.0 기준 확인된 동작이며, activate 없이도 VIRTUAL_ENV만
# 명시하면 정상 동작한다.
RUN VIRTUAL_ENV=/build/environment /build/environment/bin/venv-pack -o /build/pyspark_deps.tar.gz

CMD ["cp", "/build/pyspark_deps.tar.gz", "/output/pyspark_deps.tar.gz"]
