# 09. Lambda 서빙 이미지의 의존성 분리와 psycopg2 핀

## 문제

- nav-api 서빙은 Lambda 컨테이너 이미지로 배포되는데, 배치용 `Dockerfile`/`requirements.txt`는 Airflow·Spark·GDAL·pandas·geopandas·great_expectations까지 들어간 무거운 이미지다
- 이 목록을 Lambda에 그대로 쓰면 서빙이 전혀 임포트하지 않는 의존성까지 동봉되어 이미지가 커지고 콜드 스타트가 불리하다
- `psycopg2-binary`를 버전 고정 없이 최신으로 설치하면 Lambda 런타임에서 `undefined symbol: lo_truncate64`로 콜드 스타트마다 모듈 임포트가 실패한다 (직접 재현 확인)

## 원인

- Airflow 배치와 Lambda 서빙이 하나의 의존성 목록을 공유했다 — 두 경로의 요구가 다른데 분리 지점이 없었다
- `psycopg2-binary` 2.9.12+는 arm64 wheel을 `manylinux_2_27`/`manylinux_2_28`(glibc ≥ 2.27)로만 배포한다
- Lambda 베이스 이미지(`public.ecr.aws/lambda/python:3.11`, Amazon Linux 2, glibc 2.26)는 이 wheel 조건을 못 맞춰 prebuilt를 받지 못하고 소스 빌드로 폴백한다 — `pg_config`만 있으면 빌드는 되지만 링크되는 libpq가 9.2로 너무 오래돼 `lo_truncate64` 심볼이 없다
- 서빙은 RDS를 행 단위로 조회(`psycopg2`)해 딕셔너리로 다루므로, 애초에 pandas·pyarrow 같은 테이블 처리 라이브러리가 필요 없다

## 대안

| 방법 | 장점 | 단점 |
| --- | --- | --- |
| `requirements.txt` 하나로 통합, Lambda도 그대로 설치 | 목록이 하나라 동기화 부담 없음 | pyspark 등 배치 전용 의존성 동봉, 이미지 크기·빌드 시간·콜드 스타트 악화 |
| psycopg2 최신 + Lambda 이미지에서 소스 빌드 | 버전 고정 불필요 | libpq 9.2 링크로 런타임 `lo_truncate64` 크래시 |
| 베이스 이미지를 glibc ≥ 2.27짜리로 교체 | psycopg2 최신 wheel 사용 가능 | AWS 공식 Lambda 이미지에서 벗어나 런타임·빌드 파이프라인 재검증 필요 |
| Lambda 전용 목록 분리 + psycopg2 2.9.11 핀 | 서빙 최소 의존성만, prebuilt wheel 그대로 사용 | 두 목록을 수동 동기화, 버전 업 시 arm64 재현 빌드 필요 |

## 결정

- `requirements-lambda.txt`를 배치용 `requirements.txt`와 분리한다. Lambda 이미지엔 `fastapi`, `mangum`, `python-dotenv`, `cloudpathlib[s3]`, `s3fs`, `psycopg2-binary`만 담는다
- pandas·pyspark·geopandas·great_expectations 등 배치 전용 라이브러리는 Lambda 이미지에서 **의도적으로 제외**한다 — 서빙 임포트 체인(`nav_api.py` → `nav_lookup.py` → `common/{config,db,logger}.py`)이 요구하지 않는다
- `psycopg2-binary==2.9.11`로 고정한다. 2.9.11까지는 `manylinux2014`(glibc ≥ 2.17) wheel이 있어 이 베이스 이미지에서 prebuilt를 그대로 받는다
- 버전을 올리려면 먼저 로컬에서 `docker build --platform linux/arm64 -f docker/lambda/Dockerfile .`로 빌드해 실제 `import psycopg2`가 되는지 확인한다
- 비용으로 두 의존성 목록의 수동 동기화를 감수한다 — 서빙 임포트 체인에 새 패키지가 필요해지면 `requirements-lambda.txt`에도 반영해야 한다
