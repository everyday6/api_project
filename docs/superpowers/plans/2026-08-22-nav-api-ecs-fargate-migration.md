# nav-api ECS Fargate 마이그레이션 Implementation Plan

> **⚠️ SUPERSEDED (2026-08-22):** Task 6에서 `ecs:CreateCluster`가 막혔고, 이전에 확인한 `elasticloadbalancing:*` SCP explicit deny(이 문서 하단 참고)가 Task 8/9(타겟그룹/ALB)에서도 그대로 적용될 게 거의 확실해서 이 계정에서는 ALB를 못 만든다. 팀 논의 끝에 **ALB가 필요 없는 Lambda + API Gateway로 방향을 바꿨다** — 새 플랜은 `docs/superpowers/plans/2026-08-22-nav-api-lambda-migration.md`. Task 1~5(VPC/서브넷, 보안그룹, IAM 역할, 헬스체크 코드, 로그 권한)에서 확인한 값과 결정들은 새 플랜에서 재사용하거나 참고한다. 이 문서는 기록용으로 남겨둔다 — 더 진행하지 않는다.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 인프라 스텝은 AWS 웹 콘솔에서 사람이 직접 수행해야 한다 — 이 세션의 CLI 자격증명(`edu/lsy341`)은 SCP와 권한 부재로 대부분의 AWS API가 막혀 있다(2026-08-22 확인: `ec2:Describe*`, `elasticloadbalancing:*`, `ecr:Describe*`, `iam:ListRoles`는 SCP explicit deny, `ecs:*`는 identity 정책 자체가 없음).

**Goal:** 서빙 API(`nav-api`)를 지금의 EC2 docker-compose 컨테이너에서, 설계 문서(`docs/superpowers/specs/2026-08-21-segment-metrics-api-design.md` §9)에 적힌 대로 **ALB + Multi-AZ ECS Fargate**로 옮긴다. 인스턴스 하나가 죽어도 API가 무조건 응답하게 만드는 것이 목적이다.

**Architecture:** 기존에 ECR로 이미지를 push하는 파이프라인(`build-push-ecr.yml`)은 그대로 재사용한다. 같은 이미지를 EC2 대신 ECS Fargate 태스크(서로 다른 AZ 2곳)로 띄우고, 앞에 ALB를 둬서 트래픽을 분산한다. DynamoDB 접근은 EC2 IAM Role 대신 ECS 태스크 role로 인증한다(boto3 기본 자격증명 체인이 ECS 컨테이너 자격증명 엔드포인트를 자동으로 인식하므로 코드 변경 불필요). 파이프라인(Airflow, EMR Serverless)은 건드리지 않는다 — EC2엔 Airflow만 남는다.

**Tech Stack:** ECS Fargate, Application Load Balancer, IAM, CloudWatch Logs, 기존 ECR 리포지토리(`nav-api-airflow`), FastAPI/uvicorn(기존 코드)

## Global Constraints

- 계정 ID `181252290322`, 리전 `ap-northeast-2` 고정 — DynamoDB 테이블(`SegmentMetricsType1/2`)도 이 리전에 있으므로 다른 리전을 쓰면 조회가 실패한다.
- **인프라 생성 스텝(보안그룹/IAM/ECS/ALB/CloudWatch)은 전부 AWS 웹 콘솔에서 수행한다.** 이 플랜엔 CLI 생성 명령을 쓰지 않는다 — 로컬 CLI 자격증명으로는 조회조차 안 된다(위 경고 참고). `curl`로 하는 검증만 로컬에서 실행 가능하다.
- 비용 절감을 위해 NAT Gateway 없이 퍼블릭 서브넷 + 태스크 퍼블릭 IP 자동 할당을 쓴다. VPC 엔드포인트/프라이빗 서브넷 전환은 이 플랜 범위 밖(향후 보안 강화 과제).
- HTTPS/ACM 인증서는 범위 밖 — 리스너는 HTTP:80만 만든다. 도메인이 생기면 별도 플랜으로.
- DynamoDB의 4단계 fallback 체인(설계 문서 §7)은 이미 구현되어 있다 — 이 플랜은 건드리지 않는다.
- 이 마이그레이션이 끝나면 EC2의 `nav-api` docker-compose 서비스는 제거한다. EC2엔 Airflow(및 나머지 오케스트레이션 서비스)만 남는다.

## File Structure

- Modify: `src/serving/nav_api.py` — ECS/ALB 헬스체크용 `GET /health` 추가
- Modify: `tests/serving/test_nav_api.py` — 헬스체크 테스트 추가
- Modify: `docker-compose.yml` — `nav-api` 서비스 제거, `crash-monitor`의 `WATCH_CONTAINERS`에서 `traffic-nav-api` 제거
- Modify: `.github/workflows/build-push-ecr.yml` — ECS 배포 job 추가
- (레포에 파일 없음, AWS 콘솔에만 존재): 보안그룹 2개, IAM 역할 2개, CloudWatch 로그 그룹, ECS 클러스터/태스크 정의/서비스, 타겟 그룹, ALB

---

### Task 1: VPC · 서브넷 확인 ✅ 완료 (2026-08-22)

**결정된 값 (이후 모든 태스크에서 재사용):**
```
VPC_ID    = vpc-08e96f1845b6d64d5
SUBNET_A  = subnet-0e3f9dec8b586df41 (프로젝트-subnet-public1-ap-northeast-2a)
SUBNET_C  = subnet-0406f5bf98a99ff1f (프로젝트-subnet-public2-ap-northeast-2b)
```
두 서브넷 모두 라우팅 테이블에 `0.0.0.0/0 → igw-...` 확인됨(퍼블릭). 서브넷 자체의 "퍼블릭 IP 자동 할당" 속성은 꺼져 있지만, Task 10에서 ECS 서비스 생성 시 태스크 단위로 직접 켜므로 무관하다.

**Files:** 없음 (콘솔 조회 전용)

**Interfaces:**
- Produces: `<VPC_ID>`, `<SUBNET_A_ID>`(AZ 하나), `<SUBNET_C_ID>`(다른 AZ) — 이후 모든 태스크에서 이 세 값을 그대로 재사용한다. 메모장에 적어둔다.

- [x] **Step 1: VPC 콘솔에서 사용할 VPC 확인**

AWS 콘솔 검색창에 "VPC" 입력 → VPC 대시보드. 지금 떠 있는 EC2(Airflow)와 같은 VPC를 그대로 쓴다 — 새 VPC를 만들 필요 없다. 좌측 메뉴 "VPC"에서 목록에 있는 VPC ID를 적어둔다.

- [x] **Step 2: 서로 다른 AZ의 퍼블릭 서브넷 2개 확인**

좌측 메뉴 "서브넷" → 필터에서 Step 1의 VPC ID로 필터링. 최소 2개, **서로 다른 가용 영역**(예: `ap-northeast-2a`, `ap-northeast-2c`)의 서브넷을 고른다. 각 서브넷 상세 화면에서:
- "라우팅 테이블" 탭 → `0.0.0.0/0 → igw-...` 라우트가 있는지 확인(있으면 퍼블릭 서브넷)
- "자동 할당 퍼블릭 IPv4 주소" 편집 화면에서 활성화 여부 확인 — 비활성화면 "예"로 편집해서 켜둔다(나중에 ECS 서비스가 태스크에 퍼블릭 IP를 주려면 필요)

두 서브넷 ID를 적어둔다.

- [x] **Step 3: 확인**

메모: `VPC_ID=vpc-xxxx`, `SUBNET_A_ID=subnet-xxxx (ap-northeast-2a)`, `SUBNET_C_ID=subnet-yyyy (ap-northeast-2c)`. 이후 태스크에서 "Task 1의 VPC/서브넷"이라고 하면 이 값을 쓴다.

---

### Task 2: 보안 그룹 2개 생성 ✅ 완료 (2026-08-22)

**결정된 값:**
```
ALB_SG_ID  = sg-0a8e31c653f5540b2 (nav-api-alb-sg)
TASK_SG_ID = sg-03be7507ebc4d519e (nav-api-task-sg)
```

**Files:** 없음

**Interfaces:**
- Consumes: Task 1의 `VPC_ID`
- Produces: `<ALB_SG_ID>` (이름 `nav-api-alb-sg`), `<TASK_SG_ID>` (이름 `nav-api-task-sg`)

- [x] **Step 1: ALB용 보안 그룹 생성**

EC2 콘솔 → 좌측 "보안 그룹" → "보안 그룹 생성":
- 보안 그룹 이름: `nav-api-alb-sg`
- 설명: `nav-api ALB — 인터넷에서 80 허용`
- VPC: Task 1의 VPC
- 인바운드 규칙 추가: 유형 `HTTP`, 포트 `80`, 소스 `Anywhere-IPv4 (0.0.0.0/0)`
- 아웃바운드 규칙: 기본값(전체 허용) 유지
- 생성 후 보안 그룹 ID를 적어둔다.

- [x] **Step 2: ECS 태스크용 보안 그룹 생성**

같은 화면에서 "보안 그룹 생성"을 한 번 더:
- 보안 그룹 이름: `nav-api-task-sg`
- 설명: `nav-api Fargate 태스크 — ALB에서만 8001 허용`
- VPC: Task 1의 VPC
- 인바운드 규칙 추가: 유형 `사용자 지정 TCP`, 포트 범위 `8001`, 소스 유형 `사용자 지정` → 소스 검색창에 `nav-api-alb-sg` 입력해서 Step 1에서 만든 보안 그룹 선택(인터넷에서 8001로 직접 못 들어오고 ALB를 거친 트래픽만 허용됨)
- 아웃바운드 규칙: 기본값 유지(ECR pull, DynamoDB 접근에 아웃바운드 인터넷 필요)
- 생성 후 보안 그룹 ID를 적어둔다.

- [x] **Step 3: 확인**

두 보안 그룹이 목록에 보이고, `nav-api-task-sg`의 인바운드 소스가 "그룹 ID"(nav-api-alb-sg)로 표시되는지 확인한다.

---

### Task 3: IAM 역할 2개 생성 ✅ 완료 (2026-08-22)

**결정된 값:**
```
TASK_EXEC_ROLE_ARN = arn:aws:iam::181252290322:role/navApiTaskExecutionRole
TASK_ROLE_ARN       = arn:aws:iam::181252290322:role/navApiTaskRole
```

**Files:** 없음

**Interfaces:**
- Produces: `<TASK_EXEC_ROLE_ARN>` (이름 `navApiTaskExecutionRole`), `<TASK_ROLE_ARN>` (이름 `navApiTaskRole`)

- [x] **Step 1: 태스크 실행 역할 생성 (이미지 pull + 로그 전송용)**

IAM 콘솔 → 역할 → 역할 생성:
- 신뢰할 수 있는 엔터티 유형: AWS 서비스
- 사용 사례: "Elastic Container Service" → "Elastic Container Service Task" 선택
- 다음 → 권한 정책 검색창에 `AmazonECSTaskExecutionRolePolicy` 입력해서 체크(AWS 관리형 정책 — ECR pull, CloudWatch Logs 쓰기 권한 포함)
- 역할 이름: `navApiTaskExecutionRole`
- 생성 후 역할 상세 화면에서 ARN을 복사해 적어둔다.

- [x] **Step 2: 태스크 역할 생성 (앱 코드가 DynamoDB 읽을 때 쓰는 역할)**

같은 방식으로 역할 생성하되:
- 신뢰할 수 있는 엔터티: 동일(AWS 서비스 → ECS → Elastic Container Service Task)
- 권한 정책은 아무것도 체크하지 않고 그대로 "역할 이름: `navApiTaskRole`"로 생성
- 생성된 역할 상세 화면 → "권한 추가" → "인라인 정책 생성" → JSON 탭에 아래를 그대로 붙여넣는다:

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

- 정책 이름: `nav-api-dynamodb-read` → 정책 생성
- 역할 ARN을 복사해 적어둔다.

- [x] **Step 3: 확인**

두 역할이 IAM 역할 목록에 보이고, `navApiTaskRole`의 권한 탭에 `nav-api-dynamodb-read` 인라인 정책이 붙어 있는지 확인한다.

---

### Task 4: `/health` 엔드포인트 추가 (ALB/ECS 헬스체크용)

**Files:**
- Modify: `src/serving/nav_api.py`
- Test: `tests/serving/test_nav_api.py`

**Interfaces:**
- Produces: `GET /health` → `200 {"status": "ok"}`

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/serving/test_nav_api.py` 맨 위(다른 테스트들 사이 아무 곳, 파일 끝에 추가):

```python
def test_health_returns_ok():
    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
```

- [ ] **Step 2: 실패 확인**

```bash
.venv/bin/pytest tests/serving/test_nav_api.py::test_health_returns_ok -v
```

Expected: FAIL — `404 Not Found` (라우트가 아직 없음)

- [ ] **Step 3: 최소 구현**

`src/serving/nav_api.py`의 `app = FastAPI(title="Segment Metrics API")` 바로 다음, `SegmentValuesRequest` 클래스 정의 앞에 추가:

```python
@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
```

- [ ] **Step 4: 통과 확인**

```bash
.venv/bin/pytest tests/serving/test_nav_api.py -v
```

Expected: 전체 PASS (기존 5개 + 새 1개 = 6개)

- [ ] **Step 5: 커밋 + `nav` 브랜치로 push**

```bash
git add src/serving/nav_api.py tests/serving/test_nav_api.py
git commit -m "feat: nav-api에 ECS/ALB 헬스체크용 /health 엔드포인트 추가"
```

이 커밋이 `nav` 브랜치에 올라가야 `build-push-ecr.yml`이 자동으로 새 이미지를 ECR에 push한다. push한 뒤 GitHub Actions 탭에서 `Build and Push to ECR` 워크플로가 성공했는지 확인한다 — **Task 8(태스크 정의 등록) 전에 반드시 이 이미지가 ECR에 있어야 한다.**

---

### Task 5: CloudWatch 로그 그룹 ✅ 완료 (2026-08-22, 방식 변경)

**변경 사유:** 이 콘솔 계정(`edu/lsy341`)에 `logs:CreateLogGroup` 콘솔 권한이 없어 로그 그룹을 직접 만들 수 없었다. 대신 `navApiTaskExecutionRole`(Task 3에서 생성)에 `logs:CreateLogGroup`/`CreateLogStream`/`PutLogEvents` 인라인 정책을 추가했다 — ECS가 태스크 최초 실행 시 이 권한으로 로그 그룹을 **자동 생성**한다(AWS 표준 동작). 보존 기간 미설정이므로 로그가 무기한 쌓인다 — 나중에 CloudWatch 콘솔 접근 권한이 생기면 `/ecs/nav-api` 로그 그룹에 보존 기간(30일 추천)을 걸어둘 것.

**적용한 인라인 정책** (`navApiTaskExecutionRole` → `nav-api-logs`):
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
      "Resource": "arn:aws:logs:ap-northeast-2:181252290322:log-group:/ecs/nav-api:*"
    }
  ]
}
```

**Files:** 없음

**Interfaces:**
- Produces: 로그 그룹 `/ecs/nav-api` (Task 10에서 서비스 최초 실행 시 자동 생성됨)

- [x] **Step 1: 실행 역할에 로그 생성 권한 추가 (콘솔에서 직접 로그 그룹 생성 대신)**

- [x] **Step 2: 확인** — 정책이 `navApiTaskExecutionRole` 권한 탭에 붙어 있는지 확인.

---

### Task 6: ECS 클러스터 생성

**Files:** 없음

**Interfaces:**
- Produces: 클러스터 `nav-api-cluster`

- [ ] **Step 1: 클러스터 생성**

ECS 콘솔 → 클러스터 → "클러스터 생성":
- 클러스터 이름: `nav-api-cluster`
- 인프라: "AWS Fargate (서버리스)" 체크(EC2 인스턴스 옵션은 체크 해제 상태 유지)
- 생성

- [ ] **Step 2: 확인**

클러스터 목록에 `nav-api-cluster`가 "Active" 상태로 보이는지 확인한다.

---

### Task 7: 태스크 정의 등록

**Files:** 없음

**Interfaces:**
- Consumes: Task 3의 `<TASK_EXEC_ROLE_ARN>`, `<TASK_ROLE_ARN>`, Task 5의 로그 그룹 `/ecs/nav-api`
- Produces: 태스크 정의 패밀리 `nav-api` 리비전 1

- [ ] **Step 1: 태스크 정의 생성 (JSON)**

ECS 콘솔 → 태스크 정의 → "새 태스크 정의 생성" → 우측 상단 "JSON으로 구성" 전환 → 아래 JSON을 붙여넣되, `executionRoleArn`과 `taskRoleArn` 두 줄만 Task 3에서 적어둔 실제 ARN으로 바꾼다:

```json
{
  "family": "nav-api",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "512",
  "memory": "1024",
  "executionRoleArn": "<Task 3의 navApiTaskExecutionRole ARN>",
  "taskRoleArn": "<Task 3의 navApiTaskRole ARN>",
  "containerDefinitions": [
    {
      "name": "nav-api",
      "image": "181252290322.dkr.ecr.ap-northeast-2.amazonaws.com/nav-api-airflow:latest",
      "workingDirectory": "/opt/airflow",
      "command": ["uvicorn", "src.serving.nav_api:app", "--host", "0.0.0.0", "--port", "8001"],
      "portMappings": [
        {"containerPort": 8001, "protocol": "tcp"}
      ],
      "environment": [
        {"name": "PYTHONPATH", "value": "/opt/airflow"},
        {"name": "APP_ENV", "value": "aws"},
        {"name": "AWS_REGION", "value": "ap-northeast-2"},
        {"name": "DYNAMODB_TABLE_TYPE1", "value": "SegmentMetricsType1"},
        {"name": "DYNAMODB_TABLE_TYPE2", "value": "SegmentMetricsType2"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/nav-api",
          "awslogs-region": "ap-northeast-2",
          "awslogs-stream-prefix": "nav-api"
        }
      }
    }
  ]
}
```

cpu/memory(512/1024 = 0.5 vCPU / 1GB)는 보수적인 시작값이다 — Task 12에서 실제 부하로 확인 후 필요하면 리비전을 새로 등록해서 조정한다.

- [ ] **Step 2: 생성**

"생성" 클릭. `AWS_REGION` env가 아니라 `executionRoleArn`/`taskRoleArn`이 비어있거나 형식이 틀리면 이 시점에 콘솔이 바로 에러를 띄운다 — 에러 나면 Task 3에서 복사한 ARN을 다시 확인한다.

- [ ] **Step 3: 확인**

태스크 정의 목록에 `nav-api` 패밀리, 리비전 1이 보이는지 확인한다.

---

### Task 8: 타겟 그룹 생성

**Files:** 없음

**Interfaces:**
- Consumes: Task 1의 `VPC_ID`
- Produces: 타겟 그룹 `nav-api-tg`

- [ ] **Step 1: 타겟 그룹 생성**

EC2 콘솔 → 좌측 "로드 밸런싱" → "대상 그룹" → "대상 그룹 생성":
- 대상 유형: **IP 주소** (Fargate `awsvpc` 네트워크 모드는 반드시 IP 유형이어야 한다 — 인스턴스 유형 아님)
- 대상 그룹 이름: `nav-api-tg`
- 프로토콜: HTTP, 포트: `8001`
- VPC: Task 1의 VPC
- 프로토콜 버전: HTTP1
- 상태 검사: 프로토콜 HTTP, 경로 `/health`
- "다음" → 대상 등록 화면은 **아무것도 등록하지 않고 건너뛴다**(ECS 서비스가 Task 11에서 자동으로 등록한다)
- 대상 그룹 생성

- [ ] **Step 2: 확인**

대상 그룹 목록에 `nav-api-tg`가 보이고, 상태 검사 경로가 `/health`로 설정돼 있는지 확인한다.

---

### Task 9: ALB 생성

**Files:** 없음

**Interfaces:**
- Consumes: Task 1의 `VPC_ID`/서브넷 2개, Task 2의 `nav-api-alb-sg`, Task 8의 `nav-api-tg`
- Produces: ALB `nav-api-alb`, DNS 이름 `<ALB_DNS>`

- [ ] **Step 1: ALB 생성**

EC2 콘솔 → "로드 밸런싱" → "로드 밸런서" → "로드 밸런서 생성" → Application Load Balancer 선택:
- 이름: `nav-api-alb`
- 체계: **인터넷 경계**
- IP 주소 유형: IPv4
- VPC: Task 1의 VPC
- 매핑: Task 1에서 확인한 서로 다른 AZ 서브넷 2개 모두 체크
- 보안 그룹: 기본 선택된 것을 제거하고 Task 2의 `nav-api-alb-sg`만 선택
- 리스너: HTTP : 80, 기본 작업 "전달 대상" → Task 8의 `nav-api-tg` 선택
- 로드 밸런서 생성

- [ ] **Step 2: 확인**

몇 분 기다린 뒤 로드 밸런서 상태가 "Active"인지 확인하고, 상세 화면의 **DNS 이름**(`nav-api-alb-xxxxxxxx.ap-northeast-2.elb.amazonaws.com` 형태)을 복사해 적어둔다.

---

### Task 10: ECS 서비스 생성 (Fargate + ALB 연결)

**Files:** 없음

**Interfaces:**
- Consumes: Task 6의 클러스터, Task 7의 태스크 정의, Task 1의 VPC/서브넷 2개, Task 2의 `nav-api-task-sg`, Task 9의 ALB/`nav-api-tg`
- Produces: 서비스 `nav-api-service`, 실행 중인 태스크 2개(서로 다른 AZ)

- [ ] **Step 1: 서비스 생성**

ECS 콘솔 → `nav-api-cluster` → "서비스" 탭 → "생성":
- 컴퓨팅 옵션: 시작 유형 → **FARGATE**
- 태스크 정의: `nav-api`, 리비전 최신(1)
- 서비스 이름: `nav-api-service`
- 원하는 태스크 수: `2`
- 네트워킹: VPC = Task 1의 VPC, 서브넷 = Task 1의 서브넷 2개 모두 선택, 보안 그룹은 기본값 제거 후 Task 2의 `nav-api-task-sg` 선택, 퍼블릭 IP 자동 할당: **켜기**(ON — NAT 게이트웨이가 없으므로 ECR pull/DynamoDB 접근용 아웃바운드 인터넷이 필요)
- 로드 밸런싱: Application Load Balancer → 기존 로드 밸런서 선택 → `nav-api-alb`
- 로드 밸런서에 사용할 컨테이너: `nav-api:8001` 선택 → 리스너: 기존 리스너 사용(HTTP:80) → 대상 그룹: 기존 대상 그룹 사용 → `nav-api-tg`
- 서비스 생성

- [ ] **Step 2: 확인**

서비스 상세 화면에서 "실행 중인 태스크 수"가 `2/2`가 될 때까지 기다린다(보통 1~3분). 태스크 2개의 가용 영역이 서로 다른지 확인한다.

---

### Task 11: 검증

**Files:** 없음 (로컬에서 `curl` 실행)

**Interfaces:**
- Consumes: Task 9의 `<ALB_DNS>`

- [ ] **Step 1: 타겟 그룹 헬스 체크 확인**

EC2 콘솔 → 대상 그룹 `nav-api-tg` → "대상" 탭. 등록된 IP 2개가 모두 **Healthy** 상태인지 확인한다. `Unhealthy`면 Task 5의 `/ecs/nav-api` 로그 그룹에서 원인을 확인한다(대부분 보안 그룹의 8001 인바운드 규칙 또는 `/health` 경로 오타).

- [ ] **Step 2: 헬스체크 엔드포인트 확인**

```bash
curl -i http://<ALB_DNS>/health
```

Expected: `HTTP/1.1 200 OK`, body `{"status":"ok"}`

- [ ] **Step 3: 실제 API 확인**

```bash
curl -i -X POST http://<ALB_DNS>/segments/values \
  -H "Content-Type: application/json" \
  -d '{"segment_ids": ["nonexistent-1"], "type": 2, "time": "12:00"}'
```

Expected: `200 OK`, `{"values":[...]}` — 존재하지 않는 세그먼트라도 fallback 체인(§7)을 타고 값이 채워져서 응답이 온다.

- [ ] **Step 4: 가용성 확인(선택, 강력 추천)**

ECS 콘솔에서 실행 중인 태스크 하나를 수동으로 "중지"한다. 몇 초 뒤 Step 2/3의 `curl`을 다시 실행해 계속 200이 오는지 확인한다(나머지 1개 태스크 + ALB가 트래픽을 흡수). 이후 몇 분 지나면 ECS가 자동으로 태스크를 다시 2개로 채우는지도 확인한다 — 이게 오늘 이 마이그레이션의 핵심 목표("무조건 응답")가 실제로 동작하는지 눈으로 보는 검증이다.

---

### Task 12: 배포 파이프라인 전환 + EC2에서 nav-api 제거

**Files:**
- Modify: `.github/workflows/build-push-ecr.yml`
- Modify: `docker-compose.yml`

**Interfaces:**
- Consumes: Task 6의 클러스터명(`nav-api-cluster`), Task 10의 서비스명(`nav-api-service`)

- [ ] **Step 1: GitHub Actions OIDC 역할에 ECS 배포 권한 추가**

IAM 콘솔에서 `AWS_ECR_PUSH_ROLE_ARN`이 가리키는 역할(`.github/workflows/build-push-ecr.yml` 상단 주석에 설명된, GitHub OIDC로 assume하는 그 역할)을 찾는다 → "권한 추가" → "인라인 정책 생성" → JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "ecs:UpdateService",
        "ecs:DescribeServices"
      ],
      "Resource": "arn:aws:ecs:ap-northeast-2:181252290322:service/nav-api-cluster/nav-api-service"
    }
  ]
}
```

정책 이름: `nav-api-ecs-deploy`

- [ ] **Step 2: `build-push-ecr.yml`에 ECS 배포 job 추가**

`.github/workflows/build-push-ecr.yml`의 기존 `deploy:` job 블록 **뒤**에 새 job을 추가한다:

```yaml
  deploy-nav-api:
    needs: build-and-push
    runs-on: ubuntu-latest
    steps:
      - name: Configure AWS credentials
        uses: aws-actions/configure-aws-credentials@v4
        with:
          role-to-assume: ${{ secrets.AWS_ECR_PUSH_ROLE_ARN }}
          aws-region: ap-northeast-2

      - name: Force new ECS deployment
        run: |
          aws ecs update-service \
            --cluster nav-api-cluster \
            --service nav-api-service \
            --force-new-deployment
```

이제 `nav` 브랜치에 push할 때마다: ECR에 새 이미지 push → EC2엔 Airflow만 SSH 배포 → **동시에** ECS 서비스가 새 이미지로 롤링 재배포된다.

- [ ] **Step 3: `docker-compose.yml`에서 `nav-api` 서비스 제거**

`docker-compose.yml`의 `nav-api:` 서비스 블록(약 373~406번 줄, `# =====` 구분선 포함) 전체를 삭제한다. `crash-monitor` 서비스의 `WATCH_CONTAINERS` 환경변수에서 `traffic-nav-api` 항목만 제거한다(다른 컨테이너명은 그대로 유지):

```yaml
      WATCH_CONTAINERS: navigation-postgres,navigation-redis,navigation-airflow-apiserver,navigation-airflow-scheduler,navigation-airflow-dag-processor,navigation-airflow-worker,navigation-api,navigation-spark-master,traffic-dynamodb-local
```

- [ ] **Step 4: 커밋**

```bash
git add .github/workflows/build-push-ecr.yml docker-compose.yml
git commit -m "chore: nav-api를 ECS Fargate로 이관, EC2에선 제거"
```

- [ ] **Step 5: 확인**

`nav` 브랜치에 push한 뒤 GitHub Actions 탭에서 `build-and-push` → `deploy`(EC2, Airflow만) → `deploy-nav-api`(ECS) 세 job이 모두 성공하는지 확인한다. Task 11의 `curl` 검증을 다시 한번 돌려서 배포 후에도 API가 정상 응답하는지 확인한다.

---

## 참고: 이번 범위에서 의도적으로 제외한 것

- **HTTPS(ACM 인증서 + 443 리스너)** — 도메인이 준비되면 별도 작업.
- **VPC 엔드포인트/프라이빗 서브넷** — 지금은 비용/복잡도 때문에 퍼블릭 서브넷 + 퍼블릭 IP로 시작. 트래픽이 늘면 재검토.
- **CloudWatch 알람(태스크 다운/5xx 급증 등)** — ECS/ALB 자체 헬스체크·자동 재시작으로 "무조건 응답"은 이미 확보되지만, 사람에게 알리는 알람은 없음. `crash-monitor`가 더는 nav-api를 못 보므로, 필요하면 별도 플랜으로 CloudWatch 알람 + 기존 Slack 웹훅 연동을 추가한다.
