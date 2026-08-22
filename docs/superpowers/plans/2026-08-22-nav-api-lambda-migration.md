# nav-api Lambda 마이그레이션 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 인프라 스텝은 AWS 웹 콘솔에서 사람이 직접 수행해야 한다 — 이 세션의 IAM 계정(`edu/lsy341`)은 SCP와 권한 부재로 대부분의 AWS API가 CLI/콘솔 모두에서 막혀 있다(`docs/superpowers/plans/2026-08-22-nav-api-ecs-fargate-migration.md` 참고). GitHub Actions가 쓰는 별도 OIDC 역할(`AWS_ECR_PUSH_ROLE_ARN`)은 이 제약을 받지 않는다 — ECR push가 이미 그 역할로 잘 되고 있었다.

**Goal:** ECS Fargate + ALB 대신 **Lambda + API Gateway**로 `nav-api`를 옮긴다. `elasticloadbalancing:*`가 조직 SCP로 막혀 있어(이전 플랜에서 확인) ALB를 아예 못 만들기 때문에 방향을 바꿨다. Lambda/API Gateway는 로드밸런서 개념이 없어 이 SCP를 건드리지 않고, 멀티 AZ·장애 인스턴스 교체를 AWS가 알아서 처리해 "무조건 응답" 목표를 오히려 더 단순하게 달성한다.

**Architecture:** 기존 FastAPI 앱(`src/serving/nav_api.py`)은 거의 그대로 두고, Mangum으로 감싸 Lambda 핸들러로 노출한다. 지금 쓰는 무거운 `Dockerfile`(apache/airflow 베이스, Java/GDAL 포함)은 Lambda엔 안 맞아서, `fastapi`/`mangum`/`boto3`/DynamoDB 접근에 필요한 것만 담은 별도의 경량 Dockerfile을 새로 만든다. 같은 ECR 리포지토리(`nav-api-airflow`)에 다른 태그(`lambda-latest`)로 올린다 — 이 계정에서 새 ECR 리포지토리 생성도 막혀 있을 가능성이 높기 때문. DynamoDB 접근은 Lambda 실행 역할(IAM Role)로 인증한다 — ECS 태스크 role과 같은 원리로, boto3가 Lambda 컨테이너 자격증명을 자동으로 인식한다.

**Tech Stack:** AWS Lambda(컨테이너 이미지), API Gateway(HTTP API), Mangum, 기존 ECR 리포지토리(`nav-api-airflow`), 기존 FastAPI 코드

## Global Constraints

- 계정 ID `181252290322`, 리전 `ap-northeast-2` 고정.
- **인프라 생성 스텝(IAM/Lambda/API Gateway)은 AWS 웹 콘솔에서 수행한다.** 로컬 CLI로는 조회조차 안 된다.
- Lambda는 **VPC에 연결하지 않는다** — DynamoDB는 VPC 밖에서도 접근 가능한 서비스라 VPC 연결은 콜드 스타트만 늘리고 얻는 게 없다(이전 플랜에서 확인한 VPC/서브넷은 이 플랜에서 안 쓴다).
- ECR에 새 리포지토리를 만들지 않는다 — 기존 `nav-api-airflow` 리포지토리에 `lambda-latest` 태그로 올린다.
- 이미지 push는 GitHub Actions의 기존 OIDC 역할(`AWS_ECR_PUSH_ROLE_ARN`)로 한다 — 로컬/콘솔 계정으로는 ECR push 권한도 막혀 있을 가능성이 높다(ECR Describe가 이미 SCP explicit deny였다).
- DynamoDB의 4단계 fallback 체인(설계 문서 §7)은 이미 구현되어 있다 — 이 플랜은 건드리지 않는다.
- `src/serving/nav_api.py`/`nav_lookup.py`가 로컬 개발(EC2 docker-compose)에서 쓰는 파일 로깅은 그대로 유지한다 — Lambda의 읽기 전용 파일시스템에서만 조용히 건너뛰도록 `get_logger()` 쪽에서 방어한다(Task 1).
- **`nav` 브랜치엔 절대 직접 push하지 않는다 — 항상 PR로만 머지한다.** `nav`는 배포 파이프라인(`build-push-ecr.yml`)을 직접 트리거하는 브랜치라 리뷰 게이트가 필요하다.

## File Structure

- Modify: `src/common/logger.py` — 읽기 전용 파일시스템에서 파일 핸들러 생성 실패 시 조용히 건너뛰기
- Test: `tests/common/test_logger.py` — 신규 생성
- Create: `src/serving/lambda_handler.py` — Mangum으로 FastAPI 앱 감싸기
- Create: `requirements-lambda.txt` — Lambda 이미지 전용 최소 의존성
- Create: `docker/lambda/Dockerfile` — Lambda 컨테이너 이미지 빌드용
- Modify: `.github/workflows/build-push-ecr.yml` — Lambda 이미지 빌드/push job 추가, ECS 배포 job 제거(커밋 안 된 상태라 그냥 덮어씀)
- (레포에 파일 없음, AWS 콘솔에만 존재): IAM 실행 역할 1개, Lambda 함수, API Gateway HTTP API

---

### Task 1: `get_logger()`가 읽기 전용 파일시스템에서 안 죽게 방어 (TDD)

**Files:**
- Modify: `src/common/logger.py`
- Test: `tests/common/test_logger.py`

**Interfaces:**
- Consumes: 없음(기존 시그니처 그대로)
- Produces: `get_logger(name, log_to_file=True, ...)`가 파일 핸들러를 못 붙이는 상황(예외)에서도 예외를 던지지 않고 로거를 반환한다.

**왜 필요한가:** `nav_api.py`와 `nav_lookup.py`는 모듈 로드 시점에 `get_logger(..., log_to_file=True, ...)`를 호출한다. Lambda 컨테이너 이미지의 코드 디렉터리(`/var/task`)는 읽기 전용이라, 지금 코드 그대로면 `LOG_DIR.mkdir(...)`나 `RotatingFileHandler` 생성에서 `OSError`가 나서 **모듈 임포트 자체가 실패한다** — Lambda 콜드 스타트마다 100% 실패하게 된다. 로컬(EC2 docker-compose)에서는 계속 파일 로깅이 되어야 하므로, 호출부를 바꾸는 대신 `get_logger()`가 쓰기 실패를 스스로 감내하게 만든다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/common/test_logger.py` 새로 생성:

```python
import logging

import pytest

from src.common.logger import get_logger


def test_get_logger_returns_working_logger_without_file_handler():
    logger = get_logger("test.no_file_logging")

    assert isinstance(logger, logging.Logger)
    assert logger.level == logging.INFO


def test_get_logger_falls_back_silently_when_log_dir_is_read_only(monkeypatch, tmp_path):
    read_only_dir = tmp_path / "readonly-logs"
    read_only_dir.mkdir()
    read_only_dir.chmod(0o500)  # 쓰기 금지 — Lambda의 /var/task 흉내

    monkeypatch.setattr("src.common.logger.LOG_DIR", read_only_dir / "nested")

    logger = get_logger("test.read_only_log_dir", log_to_file=True, log_file_stem="ro")

    assert isinstance(logger, logging.Logger)
    assert not any(
        isinstance(h, logging.handlers.RotatingFileHandler) for h in logger.handlers
    )
```

- [ ] **Step 2: 실패 확인**

```bash
.venv/bin/pytest tests/common/test_logger.py -v
```

Expected: `test_get_logger_falls_back_silently_when_log_dir_is_read_only`가 `PermissionError`(또는 `OSError`)로 FAIL — 지금 코드는 예외를 그대로 던진다.

- [ ] **Step 3: 최소 구현**

`src/common/logger.py`의 `if not already_attached:` 블록을 아래로 교체:

```python
        if not already_attached:
            try:
                LOG_DIR.mkdir(exist_ok=True, parents=True)
                file_handler = RotatingFileHandler(
                    log_path,
                    maxBytes=MAX_BYTES,
                    backupCount=BACKUP_COUNT,
                    encoding="utf-8",
                )
                file_handler.setFormatter(
                    logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
                )
                logger.addHandler(file_handler)
            except OSError:
                # 읽기 전용 파일시스템(예: AWS Lambda의 /var/task)에서는 파일
                # 핸들러를 못 붙인다 — 표준 출력(→ CloudWatch Logs 등)으로만
                # 로깅을 계속한다.
                pass
```

- [ ] **Step 4: 통과 확인**

```bash
.venv/bin/pytest tests/common/test_logger.py tests/serving/ -v
```

Expected: 전체 PASS.

- [ ] **Step 5: 커밋**

```bash
git add src/common/logger.py tests/common/test_logger.py
git commit -m "fix: get_logger가 읽기 전용 파일시스템에서 예외 대신 폴백하도록 방어"
```

---

### Task 2: Mangum 어댑터로 Lambda 핸들러 추가

**Files:**
- Create: `src/serving/lambda_handler.py`
- Modify: `requirements.txt` — `mangum` 추가(로컬 개발/테스트 환경에도 설치해서 import 에러 없게)

**Interfaces:**
- Consumes: `src.serving.nav_api.app`
- Produces: `src.serving.lambda_handler.handler(event, context)` — API Gateway HTTP API 프록시 이벤트를 처리하는 Lambda 진입점

- [ ] **Step 1: `requirements.txt`에 `mangum` 추가**

`requirements.txt`의 `fastapi` / `uvicorn` 줄 근처에 추가:

```
fastapi
uvicorn
mangum
PyYAML
```

- [ ] **Step 2: 설치 확인**

```bash
.venv/bin/pip install -r requirements.txt
```

- [ ] **Step 3: 핸들러 작성**

`src/serving/lambda_handler.py` 새로 생성:

```python
"""API Gateway(HTTP API) → Lambda 진입점.

FastAPI 앱(src/serving/nav_api.py)은 그대로 두고 Mangum으로 ASGI 요청을
Lambda 이벤트/응답 포맷으로 변환한다. 라우팅·fallback 로직은 nav_api.py/
nav_lookup.py에 그대로 있다 — 이 파일은 어댑터일 뿐이다.
"""

from __future__ import annotations

from mangum import Mangum

from src.serving.nav_api import app

handler = Mangum(app)
```

- [ ] **Step 4: 로컬에서 동작 확인 (실제 Lambda 이벤트 형태로)**

```bash
.venv/bin/python -c "
from src.serving.lambda_handler import handler

event = {
    'version': '2.0',
    'routeKey': 'GET /health',
    'rawPath': '/health',
    'rawQueryString': '',
    'headers': {'host': 'example.com'},
    'requestContext': {
        'accountId': '181252290322',
        'apiId': 'testapi',
        'domainName': 'example.com',
        'http': {
            'method': 'GET',
            'path': '/health',
            'protocol': 'HTTP/1.1',
            'sourceIp': '127.0.0.1',
            'userAgent': 'curl',
        },
        'requestId': 'test-request-id',
        'routeKey': 'GET /health',
        'stage': '\$default',
        'time': '22/Aug/2026:00:00:00 +0000',
        'timeEpoch': 1755820800000,
    },
    'isBase64Encoded': False,
}
print(handler(event, None))
"
```

Expected: `{'statusCode': 200, ...}`에 `'body': '{\"status\":\"ok\"}'` 포함. (Mangum은 `requestContext.http.sourceIp` 같은 필드가 없으면 `KeyError`를 던진다 — 실제 API Gateway HTTP API 페이로드엔 항상 있는 필드라 배포 후엔 문제없다.)

- [ ] **Step 5: 커밋**

```bash
git add src/serving/lambda_handler.py requirements.txt
git commit -m "feat: nav-api를 Lambda에서 돌리기 위한 Mangum 핸들러 추가"
```

---

### Task 3: Lambda용 경량 Dockerfile + 의존성

**Files:**
- Create: `requirements-lambda.txt`
- Create: `docker/lambda/Dockerfile`

**Interfaces:**
- Produces: `docker/lambda/Dockerfile`를 리포 루트에서 `docker build -f docker/lambda/Dockerfile .`로 빌드하면 `src.serving.lambda_handler.handler`를 노출하는 이미지가 나온다.

**왜 따로 만드는가:** 기존 `Dockerfile`은 Airflow+Spark+GDAL까지 들어간 무거운 이미지라 Lambda 콜드 스타트에 불리하고, `pyspark`/`pandas`/`great_expectations` 같은 nav-api가 전혀 안 쓰는 의존성까지 끌고 들어간다. nav-api의 실제 임포트 체인(`nav_api.py` → `nav_lookup.py` → `common/config.py`, `common/dynamodb.py`, `common/logger.py`)이 필요로 하는 것만 담은 최소 이미지를 따로 둔다.

- [ ] **Step 1: 최소 의존성 목록 작성**

`requirements-lambda.txt` 새로 생성:

```
fastapi
mangum
boto3
python-dotenv
cloudpathlib[s3]
s3fs
```

(`uvicorn`은 로컬 개발용 ASGI 서버라 Lambda엔 불필요 — Mangum이 서버 없이 직접 이벤트를 처리한다. `config.py`가 임포트 시점에 `cloudpathlib.S3Path`를 무조건 생성하므로 `cloudpathlib[s3]`/`s3fs`는 빼면 임포트 자체가 깨진다.)

- [ ] **Step 2: Dockerfile 작성**

`docker/lambda/Dockerfile` 새로 생성:

```dockerfile
FROM public.ecr.aws/lambda/python:3.11

COPY requirements-lambda.txt .
RUN pip install --no-cache-dir -r requirements-lambda.txt

COPY src/common ${LAMBDA_TASK_ROOT}/src/common
COPY src/serving ${LAMBDA_TASK_ROOT}/src/serving

CMD ["src.serving.lambda_handler.handler"]
```

- [ ] **Step 3: 로컬에서 빌드 확인**

```bash
docker build -f docker/lambda/Dockerfile -t nav-api-lambda-local .
```

Expected: 빌드 성공(마지막 줄 `Successfully tagged nav-api-lambda-local`). Lambda 함수는 arm64(Graviton)로 만들 것이므로, Apple Silicon(M1/M2/M3) 로컬에서 빌드하면 별도 플래그 없이도 아키텍처가 그대로 맞는다. 실제 배포 이미지는 Task 4의 GitHub Actions(arm64 러너)가 만든다.

- [ ] **Step 4: 커밋**

```bash
git add requirements-lambda.txt docker/lambda/Dockerfile
git commit -m "feat: nav-api Lambda 컨테이너 이미지용 경량 Dockerfile 추가"
```

---

### Task 4: GitHub Actions에 Lambda 이미지 빌드/push job 추가

**Files:**
- Modify: `.github/workflows/build-push-ecr.yml`

**Interfaces:**
- Produces: `nav` 브랜치에 PR이 머지될 때마다 ECR `nav-api-airflow` 리포지토리에 `lambda-latest`, `lambda-<sha>` 태그로 이미지가 올라간다.

**참고:** 이전 ECS Fargate 플랜에서 이 파일에 `deploy-nav-api`(ECS 배포) job을 추가했었는데 커밋은 안 했다 — 그 변경은 버리고 대신 이 job으로 교체한다. 이 파일엔 이 플랜과 무관한 EMR Serverless 관련 커밋 안 된 변경(`build-emr-python-env` job 등)도 있다 — 그건 건드리지 않고 그대로 둔다.

- [ ] **Step 1: 혹시 남아있는 `deploy-nav-api`(ECS) job 제거**

`.github/workflows/build-push-ecr.yml`에 아래 블록이 있으면 통째로 삭제한다(없으면 이 스텝은 건너뛴다):

```yaml
  deploy-nav-api:
    # nav-api는 더 이상 EC2 docker-compose가 아니라 ECS Fargate에서 돈다
    ...
```

- [ ] **Step 2: Lambda 이미지 빌드/push job 추가**

기존 `build-and-push` job 아래(다른 job들 사이 아무 곳)에 추가:

```yaml
  build-and-push-lambda:
    runs-on: ubuntu-24.04-arm
    steps:
      - uses: actions/checkout@v4

      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ECR_PUSH_ROLE_ARN }}
          aws-region: ${{ env.AWS_REGION }}

      - name: Login to Amazon ECR
        id: ecr-login
        uses: aws-actions/amazon-ecr-login@v2

      - name: Build and push (Lambda 이미지)
        env:
          REGISTRY: ${{ steps.ecr-login.outputs.registry }}
        run: |
          IMAGE="$REGISTRY/$ECR_REPOSITORY"
          docker build --platform linux/arm64 -f docker/lambda/Dockerfile \
            -t "$IMAGE:lambda-${{ github.sha }}" -t "$IMAGE:lambda-latest" .
          docker push "$IMAGE:lambda-${{ github.sha }}"
          docker push "$IMAGE:lambda-latest"
```

Lambda 함수를 arm64(Graviton)로 만들 것이므로 `ubuntu-24.04-arm` 러너에서 그대로 네이티브 빌드한다 — 크로스 컴파일/QEMU가 필요 없다(처음엔 amd64로 크로스 빌드하려다 GitHub 호스팅 arm64 러너에 QEMU가 기본 설치돼 있지 않아 실패할 뻔했다 — arm64로 통일해서 이 문제 자체를 없앴다). arm64 Lambda가 x86_64보다 비용도 ~20% 저렴하다.

`build-emr-python-env`, `deploy`(EC2 Airflow SSH 배포) job은 손대지 않는다 — 이 job은 `needs:` 없이 `build-and-push`와 병렬로 독립 실행된다.

- [ ] **Step 3: YAML 문법 확인**

```bash
.venv/bin/python -c "import yaml; yaml.safe_load(open('.github/workflows/build-push-ecr.yml')); print('YAML OK')"
```

- [ ] **Step 4: 커밋**

```bash
git add .github/workflows/build-push-ecr.yml
git commit -m "ci: nav-api Lambda 이미지를 ECR에 빌드/push하는 job 추가"
```

- [ ] **Step 5: `nav`로 PR 생성 + 머지 (직접 push 금지 — 항상 PR로만)**

이 커밋들이 PR을 통해 `nav` 브랜치에 머지되어야 GitHub Actions가 실제로 이미지를 ECR에 만든다. **Task 6(Lambda 함수 생성)은 이 이미지가 ECR에 있어야 진행 가능하다** — 머지 후 GitHub Actions 탭에서 `build-and-push-lambda`가 성공했는지 확인한다.

---

### Task 5: IAM 역할 생성 — Lambda 실행 역할 ✅ 완료 (2026-08-22)

**결정된 값:**
```
LAMBDA_ROLE_ARN = arn:aws:iam::181252290322:role/navApiLambdaRole
```

**Files:** 없음 (콘솔)

**Interfaces:**
- Produces: `<LAMBDA_ROLE_ARN>` (이름 `navApiLambdaRole`)

- [x] **Step 1: 역할 생성**

IAM 콘솔 → 역할 → 역할 생성:
- 신뢰할 수 있는 엔터티: AWS 서비스
- 사용 사례: **"Lambda"** 선택
- 다음 → 권한 정책 검색창에 `AWSLambdaBasicExecutionRole` 체크(AWS 관리형 — CloudWatch Logs 쓰기 권한)
- 역할 이름: `navApiLambdaRole`
- 생성

- [x] **Step 2: DynamoDB 읽기 인라인 정책 추가**

생성된 `navApiLambdaRole` 상세 화면 → "권한 추가" → "인라인 정책 생성" → JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "dynamodb:BatchGetItem",
      "Resource": [
        "arn:aws:dynamodb:ap-northeast-2:181252290322:table/SegmentMetricsType1",
        "arn:aws:dynamodb:ap-northeast-2:181252290322:table/SegmentMetricsType2"
      ]
    }
  ]
}
```

정책 이름: `nav-api-dynamodb-read` → 생성

- [x] **Step 3: 확인**

역할 ARN을 복사해 적어둔다. 권한 탭에 `AWSLambdaBasicExecutionRole`(관리형)과 `nav-api-dynamodb-read`(인라인) 둘 다 보이는지 확인.

---

### Task 6: Lambda 함수 생성 (컨테이너 이미지) ✅ 완료 (2026-08-22)

**결정된 값:**
```
FUNCTION_ARN = arn:aws:lambda:ap-northeast-2:181252290322:function:nav-api
```
콘솔 테스트 탭에서 `/health` 이벤트로 200 확인함.

**Files:** 없음 (콘솔)

**Interfaces:**
- Consumes: Task 4에서 ECR에 push된 `nav-api-airflow:lambda-latest` 이미지, Task 5의 `<LAMBDA_ROLE_ARN>`
- Produces: Lambda 함수 `nav-api`

- [ ] **Step 1: 함수 생성**

Lambda 콘솔 → 함수 → "함수 생성":
- 옵션: **컨테이너 이미지**
- 함수 이름: `nav-api`
- 컨테이너 이미지 URI: "찾아보기"로 ECR → `nav-api-airflow` 리포지토리 → `lambda-latest` 태그 선택
- 아키텍처: **arm64** (이미지가 arm64로 빌드됨 — x86_64를 고르면 아키텍처 불일치로 함수 생성/실행이 실패한다)
- 권한: "기존 역할 사용" → Task 5의 `navApiLambdaRole`
- 함수 생성

- [ ] **Step 2: 환경 변수 설정**

함수 생성 후 "구성" 탭 → "환경 변수" → 편집 → 추가:

| 키 | 값 |
|---|---|
| `APP_ENV` | `aws` |
| `AWS_REGION` | `ap-northeast-2` |
| `DYNAMODB_TABLE_TYPE1` | `SegmentMetricsType1` |
| `DYNAMODB_TABLE_TYPE2` | `SegmentMetricsType2` |

저장

- [ ] **Step 3: 제한 시간/메모리 조정**

"구성" 탭 → "일반 구성" → 편집:
- 메모리: `512 MB` (기본 128MB는 boto3/fastapi 임포트만으로 빠듯하다)
- 제한 시간: `10초` (기본 3초는 콜드 스타트 때 부족할 수 있다)

저장

- [ ] **Step 4: 콘솔 테스트 탭으로 헬스체크 확인**

"테스트" 탭 → 새 이벤트 생성 → 아래 JSON 붙여넣기:

```json
{
  "version": "2.0",
  "routeKey": "GET /health",
  "rawPath": "/health",
  "rawQueryString": "",
  "headers": {},
  "requestContext": {
    "http": {"method": "GET", "path": "/health"},
    "stage": "$default"
  },
  "isBase64Encoded": false
}
```

테스트 실행 → 실행 결과에 `"statusCode": 200`, `"body": "{\"status\":\"ok\"}"`가 보이는지 확인. 에러가 나면 "실행 결과"에 나오는 CloudWatch 로그 링크에서 원인을 확인한다(대부분 환경 변수 누락이나 import 에러).

---

### Task 7: API Gateway HTTP API 생성 + Lambda 연동 ✅ 완료 (2026-08-22)

**결정된 값:**
```
API_URL = https://gv37o51ey6.execute-api.ap-northeast-2.amazonaws.com
```

**Files:** 없음 (콘솔)

**Interfaces:**
- Consumes: Task 6의 Lambda 함수 `nav-api`
- Produces: API Gateway 엔드포인트 `<API_URL>`

- [ ] **Step 1: HTTP API 생성**

API Gateway 콘솔 → "API 생성" → **HTTP API** "구축" 클릭:
- 통합 추가: Lambda 선택 → Task 6의 `nav-api` 함수 선택
- API 이름: `nav-api-http`
- 다음(라우트 구성): 메서드 `ANY`, 리소스 경로 `/{proxy+}`로 설정(기본으로 채워진 라우트를 이렇게 수정 — FastAPI 자체 라우팅에 모든 경로/메서드를 그대로 위임하기 위함)
- 다음(스테이지 정의): 기본 스테이지 `$default`, 자동 배포 활성화 — 그대로 두고 다음
- 생성

- [ ] **Step 2: 확인**

API 상세 화면에서 "호출 URL"(`https://xxxxxxxx.execute-api.ap-northeast-2.amazonaws.com` 형태)을 복사해 적어둔다.

---

### Task 8: 검증 ✅ 완료 (2026-08-22)

`curl`로 `/health`(200, `{"status":"ok"}`)와 `/segments/values`(존재하지 않는 세그먼트 → fallback 체인 타고 `{"values":[300]}` 반환) 둘 다 확인함 — "무조건 응답" 동작 검증됨.

**Files:** 없음 (로컬에서 `curl`)

**Interfaces:**
- Consumes: Task 7의 `<API_URL>`

- [ ] **Step 1: 헬스체크**

```bash
curl -i https://<API_URL>/health
```

Expected: `200 OK`, `{"status":"ok"}`

- [ ] **Step 2: 실제 엔드포인트**

```bash
curl -i -X POST https://<API_URL>/segments/values \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["nonexistent-1"], "type": 2, "time": "12:00"}'
```

Expected: `200 OK`, `{"values":[...]}` — fallback 체인을 타고 값이 채워져서 응답이 온다.

- [ ] **Step 3: 콜드 스타트 체감 확인 (선택)**

몇 분간 요청 없이 뒀다가(Lambda가 컨테이너를 내림) 다시 Step 1을 호출해서 첫 응답과 두 번째 응답의 체감 속도 차이를 확인한다. 너무 느리면(수 초 이상) Task 6의 메모리를 늘리는 걸 고려한다(메모리를 늘리면 CPU도 비례해서 늘어나 콜드 스타트가 빨라진다).

---

### Task 9: 배포 자동화 (CI/CD)

**Files:**
- Modify: `.github/workflows/build-push-ecr.yml`

**Interfaces:**
- Consumes: Task 4의 `build-and-push-lambda` job, Task 6의 함수 이름(`nav-api`)

- [ ] **Step 1: OIDC 역할에 Lambda 업데이트 권한 추가**

IAM 콘솔에서 `AWS_ECR_PUSH_ROLE_ARN`이 가리키는 역할 → "권한 추가" → "인라인 정책 생성" → JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "lambda:UpdateFunctionCode",
      "Resource": "arn:aws:lambda:ap-northeast-2:181252290322:function:nav-api"
    }
  ]
}
```

정책 이름: `nav-api-lambda-deploy`

- [ ] **Step 2: 배포 job 추가**

`.github/workflows/build-push-ecr.yml`의 `build-and-push-lambda` job 뒤에 추가:

```yaml
  deploy-lambda:
    needs: build-and-push-lambda
    runs-on: ubuntu-latest
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ECR_PUSH_ROLE_ARN }}
          aws-region: ap-northeast-2

      - name: Update Lambda function code
        run: |
          aws lambda update-function-code \
            --function-name nav-api \
            --image-uri 181252290322.dkr.ecr.ap-northeast-2.amazonaws.com/nav-api-airflow:lambda-latest
```

- [ ] **Step 3: 커밋 + push**

```bash
git add .github/workflows/build-push-ecr.yml
git commit -m "ci: nav-api Lambda 함수를 새 이미지로 자동 업데이트하는 job 추가"
```

PR이 `nav`에 머지된 뒤 Actions 탭에서 `build-and-push-lambda` → `deploy-lambda` 순서로 성공하는지 확인하고, Task 8의 `curl` 검증을 다시 돌려본다.
