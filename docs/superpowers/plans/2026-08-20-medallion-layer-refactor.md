# 브론즈-실버-골드 레이어 역할 통일 리팩토링 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 8개 도메인(construction, construction_stipulations, event, ticketmaster, lion,
road_closures, tlc, taxi_zone)의 Silver/Gold 레이어 로직을 Silver1(정제)/Silver2(조인)/
Gold1(필터)/Gold2(점수계산) 5단계 구조로 통일하고, `mapping/`→`silver2/`,
`scoring/`(api.py 제외)→`gold2/`, `scoring/api.py`→`serving/api.py`로 공용 폴더를 재배치한다.

**Architecture:** 설계 문서
[`docs/superpowers/specs/2026-08-20-medallion-layer-refactor-design.md`](../specs/2026-08-20-medallion-layer-refactor-design.md)
참고. 로직 자체는 바꾸지 않는 "순수 이동"이 원칙이고, 명시적으로 표시한 지점(참조 대상
변경, 컬럼 보존 추가 등)만 예외적으로 동작이 바뀐다.

**Tech Stack:** Python, pandas, PySpark, Airflow(Asset 기반 DAG), pytest.

## Global Constraints

- Bronze 레이어는 이번 리팩토링에서 건드리지 않는다 (taxi_zone의 validate 함수 이관 제외).
- 각 태스크는 "로직 이동"이 원칙이다 — 새 비즈니스 로직을 추가하지 않는다. 예외는 각 태스크에
  명시된 것만(예: construction_stipulations의 참조 경로 변경, event의 컬럼 보존).
- `ticketmaster/venue.py`의 capacity를 `event_boost.py`에 실제로 연결하는 것은 이번 범위 밖 —
  파일 위치만 바로잡고 아무도 호출하지 않는 상태 그대로 둔다.
- tlc의 결측치 정책(드롭 대신 로그만)은 그대로 유지한다 — 레이어 위치만 통일한다.
- 기존 `data/silver/*`, `data/gold/*` 파일은 옮기지 않는다. 다음 파이프라인 실행이 새 경로에
  새로 생성한다. 기존 파일은 그대로 두고 나중에 수동 정리한다.
- 이 프로젝트는 대부분의 도메인(construction, construction_stipulations, event, ticketmaster,
  lion, road_closures, taxi_zone)에 **기존 테스트가 없다**(tlc, common만 있음). 테스트가 없는
  도메인은 "smoke import" 커맨드로 최소 검증한다. 있는 도메인(tlc)은 기존 테스트를 그대로 통과
  시켜야 한다(로직 불변이므로 테스트도 안 바뀌는 것이 기본, 예외는 명시).
- 태스크는 **반드시 아래 순서대로** 진행한다 — 뒤 태스크가 앞 태스크가 만든 모듈 경로를
  참조한다(construction_stipulations/road_closures는 construction 완료 후, tlc는 lion 완료
  후 진행).
- 매 태스크 끝에 `git add`(관련 파일만, `git add -A` 금지) + commit.

---

### Task 1: 공용 경로 상수 추가 + `silver2/`, `gold2/`, `serving/` 폴더 이동

이 태스크는 로직을 전혀 바꾸지 않고 파일 위치와 import 경로만 바꾼다. 5개 mapping 모듈은
그대로 옮기고(내용 무변경), 3개 scoring 모듈(api.py 제외)도 그대로 옮긴다. `segment_spatial_weight.py`는
현재 이 브랜치에 없다(별도 PR `feature/segment-spatial-weight`에만 있음) — 이 태스크에서는
건드리지 않고 Task 8(tlc)에서 그 브랜치를 머지하며 처리한다.

**Files:**
- Modify: `src/common/config.py` (64번째 줄 `GOLD_DIR = DATA_DIR / "gold"` 바로 다음에 추가)
- Create: `src/silver2/__init__.py`, `src/gold2/__init__.py`, `src/serving/__init__.py` (빈 파일)
- Move: `src/mapping/event_lion.py` → `src/silver2/event_lion.py`
- Move: `src/mapping/road_closure_segment.py` → `src/silver2/road_closure_segment.py`
- Move: `src/mapping/road_control_segment.py` → `src/silver2/road_control_segment.py`
- Move: `src/mapping/ticketmaster_lion.py` → `src/silver2/ticketmaster_lion.py`
- Move: `src/mapping/zone_segment.py` → `src/silver2/zone_segment.py`
- Move: `src/scoring/closure_penalty.py` → `src/gold2/closure_penalty.py`
- Move: `src/scoring/event_boost.py` → `src/gold2/event_boost.py`
- Move: `src/scoring/traffic_score.py` → `src/gold2/traffic_score.py`
- Move: `src/scoring/api.py` → `src/serving/api.py`
- Modify: `src/tlc/gold.py:23`
- Modify: `src/gold2/event_boost.py` (구 `src/scoring/event_boost.py`, import 42행)
- Modify: `src/gold2/traffic_score.py` (구 `src/scoring/traffic_score.py`, import 42행)
- Modify: `src/serving/api.py` (구 `src/scoring/api.py`, import 24-34행, 주석 9행)
- Modify: `dags/event_pipeline.py:75`
- Modify: `dags/ticketmaster_pipeline.py:77`
- Modify: `dags/construction_pipeline.py:176,182,188,194`
- Modify: `dags/lion_pipeline.py:44`
- Modify: `dags/gold_closure_penalty.py:53,58`
- Modify: `docker-compose.yml:310`

**Interfaces:**
- Produces: `SILVER1_DIR`, `SILVER2_DIR`, `GOLD1_DIR`, `GOLD2_DIR` (모두 `pathlib.Path`, 이후
  모든 도메인 태스크가 이 상수를 쓴다). `src.silver2.*`, `src.gold2.*`, `src.serving.api` 모듈
  경로(함수 시그니처는 무변경).

- [ ] **Step 1: config.py에 새 경로 상수 추가**

`src/common/config.py`의 63-64행(`SILVER_DIR`, `GOLD_DIR` 정의) 바로 뒤에 추가:

```python
SILVER1_DIR = DATA_DIR / "silver1"
SILVER2_DIR = DATA_DIR / "silver2"
GOLD1_DIR = DATA_DIR / "gold1"
GOLD2_DIR = DATA_DIR / "gold2"
```

`SILVER_DIR`/`GOLD_DIR` 자체는 아직 지우지 않는다 — 아직 대부분의 도메인 코드가 참조 중이라
Task 10(최종 정리)에서 참조가 하나도 안 남았을 때 지운다.

- [ ] **Step 2: 폴더/파일 이동**

```bash
mkdir -p src/silver2 src/gold2 src/serving
touch src/silver2/__init__.py src/gold2/__init__.py src/serving/__init__.py

git mv src/mapping/event_lion.py src/silver2/event_lion.py
git mv src/mapping/road_closure_segment.py src/silver2/road_closure_segment.py
git mv src/mapping/road_control_segment.py src/silver2/road_control_segment.py
git mv src/mapping/ticketmaster_lion.py src/silver2/ticketmaster_lion.py
git mv src/mapping/zone_segment.py src/silver2/zone_segment.py

git mv src/scoring/closure_penalty.py src/gold2/closure_penalty.py
git mv src/scoring/event_boost.py src/gold2/event_boost.py
git mv src/scoring/traffic_score.py src/gold2/traffic_score.py
git mv src/scoring/api.py src/serving/api.py
```

`src/mapping/`, `src/scoring/`이 비면 디렉터리 자체는 git이 자동으로 안 남긴다(빈 폴더는
git이 추적 안 함). `src/mapping/__init__.py`/`src/scoring/__init__.py`가 있었다면 같이
`git rm`한다(먼저 `ls src/mapping src/scoring`로 확인).

- [ ] **Step 3: 이동된 파일 내부의 상호 import 수정**

`src/gold2/event_boost.py` 42행(구 `src/scoring/event_boost.py`):
```python
# 변경 전
from src.scoring.closure_penalty import spread_with_decay
# 변경 후
from src.gold2.closure_penalty import spread_with_decay
```

`src/gold2/traffic_score.py` 42행(구 `src/scoring/traffic_score.py`):
```python
# 변경 전
from src.scoring import closure_penalty, event_boost
# 변경 후
from src.gold2 import closure_penalty, event_boost
```
같은 파일의 38-41행(`from src.lion.silver import DIM_SEGMENT_PATH`,
`from src.lion.traffic_score import DIM_SEGMENT_TRAFFIC_SCORE_PATH`,
`from src.tlc.gold import DIM_SEGMENT_TLC_VOLUME_PATH`)은 lion/tlc가 아직 이 태스크에서
안 쪼개졌으므로 **그대로 둔다** — Task 7, Task 8에서 각각 수정한다.

`src/serving/api.py` 24-34행(구 `src/scoring/api.py`):
```python
# 변경 전
from src.scoring.traffic_score import (
    get_active_closures,
    get_closure_data_date_range,
    get_map_data,
    get_nearby_closures,
    get_nearby_segment_scores,
    get_newly_issued_closures,
    get_segment_geometries,
    get_traffic_score,
    get_traffic_score_hourly,
)
# 변경 후
from src.gold2.traffic_score import (
    get_active_closures,
    get_closure_data_date_range,
    get_map_data,
    get_nearby_closures,
    get_nearby_segment_scores,
    get_newly_issued_closures,
    get_segment_geometries,
    get_traffic_score,
    get_traffic_score_hourly,
)
```
같은 파일 9행 주석(`uvicorn src.scoring.api:app --reload --port 8000`)도
`uvicorn src.serving.api:app --reload --port 8000`으로 고친다.

`src/tlc/gold.py:23`:
```python
# 변경 전
from src.mapping.zone_segment import MAP_ZONE_SEGMENT_PATH
# 변경 후
from src.silver2.zone_segment import MAP_ZONE_SEGMENT_PATH
```

- [ ] **Step 4: DAG import 수정**

`dags/event_pipeline.py:75`:
```python
# 변경 전
from src.mapping.event_lion import build_event_lion_mapping
# 변경 후
from src.silver2.event_lion import build_event_lion_mapping
```

`dags/ticketmaster_pipeline.py:77`:
```python
# 변경 전
from src.mapping.ticketmaster_lion import build_ticketmaster_lion_mapping
# 변경 후
from src.silver2.ticketmaster_lion import build_ticketmaster_lion_mapping
```

`dags/construction_pipeline.py:176,182`(road_control_segment):
```python
# 변경 전
from src.mapping.road_control_segment import build
from src.mapping.road_control_segment import validate_output
# 변경 후
from src.silver2.road_control_segment import build
from src.silver2.road_control_segment import validate_output
```

`dags/construction_pipeline.py:188,194`(road_closure_segment):
```python
# 변경 전
from src.mapping.road_closure_segment import build
from src.mapping.road_closure_segment import validate_output
# 변경 후
from src.silver2.road_closure_segment import build
from src.silver2.road_closure_segment import validate_output
```

`dags/lion_pipeline.py:44`:
```python
# 변경 전
from src.mapping.zone_segment import build_map_zone_segment, validate_map_zone_segment
# 변경 후
from src.silver2.zone_segment import build_map_zone_segment, validate_map_zone_segment
```

`dags/gold_closure_penalty.py:53,58`:
```python
# 변경 전
from src.scoring.closure_penalty import build
from src.scoring.closure_penalty import validate_output
# 변경 후
from src.gold2.closure_penalty import build
from src.gold2.closure_penalty import validate_output
```

- [ ] **Step 5: docker-compose.yml 수정**

`docker-compose.yml:310`:
```yaml
# 변경 전
command: uvicorn src.scoring.api:app --host 0.0.0.0 --port 8000
# 변경 후
command: uvicorn src.serving.api:app --host 0.0.0.0 --port 8000
```

- [ ] **Step 6: 잔여 구경로 참조 확인**

```bash
grep -rn "from src.mapping\|from src\.scoring\|import src\.mapping\|import src\.scoring" \
  --include="*.py" --include="*.yml" . | grep -v "\.git/"
```
Expected: 결과 0건 (전부 위 단계에서 수정 완료).

- [ ] **Step 7: smoke import + 기존 테스트 실행**

```bash
cd /Users/admin/Desktop/프로젝트/my-project-new
python -c "
import src.silver2.event_lion, src.silver2.road_closure_segment
import src.silver2.road_control_segment, src.silver2.ticketmaster_lion, src.silver2.zone_segment
import src.gold2.closure_penalty, src.gold2.event_boost, src.gold2.traffic_score
import src.serving.api
print('OK')
"
```
Expected: `OK` 출력, ImportError 없음.

```bash
pytest tests/ -q
```
Expected: 리팩토링 전과 동일한 pass/fail 수(이 태스크는 tlc/common 로직을 안 건드리므로
전부 그대로 통과해야 함).

- [ ] **Step 8: 커밋**

```bash
git add src/common/config.py src/silver2 src/gold2 src/serving \
  src/tlc/gold.py dags/event_pipeline.py dags/ticketmaster_pipeline.py \
  dags/construction_pipeline.py dags/lion_pipeline.py dags/gold_closure_penalty.py \
  docker-compose.yml
git commit -m "refactor: mapping/→silver2/, scoring/→gold2/+serving/api.py 이름 통일

로직 변경 없이 폴더 위치와 import 경로만 이동. SILVER1_DIR/SILVER2_DIR/GOLD1_DIR/GOLD2_DIR
경로 상수 추가(이후 도메인별 분리 태스크에서 사용)."
```

---

### Task 2: construction 도메인 — silver1.py / gold1.py로 이름 통일

**Files:**
- Move: `src/construction/silver.py` → `src/construction/silver1.py` (내용 무변경)
- Move: `src/construction/gold.py` → `src/construction/gold1.py` (내용 무변경)
- Modify: `dags/construction_pipeline.py:98,104,113,119`

**Interfaces:**
- Consumes: 없음(construction은 다른 도메인을 참조하지 않음)
- Produces: `src.construction.silver1.build`, `.validate_output` / `src.construction.gold1.build`,
  `.validate_output` (Task 3의 `silver2/construction_work_hours_join.py`가 이 경로의 산출물
  `data/silver1/construction/`을 참조하게 됨)

- [ ] **Step 1: 파일 이동**

```bash
git mv src/construction/silver.py src/construction/silver1.py
git mv src/construction/gold.py src/construction/gold1.py
```

- [ ] **Step 2: DAG import 수정**

`dags/construction_pipeline.py:98,104`:
```python
# 변경 전
from src.construction.silver import build
from src.construction.silver import validate_output
# 변경 후
from src.construction.silver1 import build
from src.construction.silver1 import validate_output
```

`dags/construction_pipeline.py:113,119`:
```python
# 변경 전
from src.construction.gold import build
from src.construction.gold import validate_output
# 변경 후
from src.construction.gold1 import build
from src.construction.gold1 import validate_output
```

- [ ] **Step 3: smoke import + 테스트**

```bash
python -c "
from src.construction.silver1 import build, validate_output
from src.construction.gold1 import build, validate_output
print('OK')
"
grep -rn "from src.construction.silver\b\|from src.construction.gold\b" --include="*.py" . | grep -v "\.git/"
```
Expected: `OK` 출력, grep 결과 0건(이번에 고친 `dags/construction_pipeline.py`의 새 경로
`silver1`/`gold1`만 매칭에서 제외하고 확인).

```bash
pytest tests/ -q
```
Expected: 이전과 동일(construction 자체 테스트는 없음).

- [ ] **Step 4: 커밋**

```bash
git add src/construction dags/construction_pipeline.py
git commit -m "refactor: construction silver/gold를 silver1/gold1로 이름 통일 (로직 무변경)"
```

---

### Task 3: construction_stipulations 도메인 — silver1.py 분리 + silver2/construction_work_hours_join.py 신설

`_merge_work_hours()`를 감싸는 build/validate/main 전체가 하나의 조인 파이프라인이라
`silver2/construction_work_hours_join.py`로 통째로 옮긴다. 참조 대상을 construction
**Gold**(`GOLD_DIR/"construction"`)에서 construction **Silver1**(Task 2가 만든
`data/silver1/construction/`)로 바꾸는 것이 이 태스크의 핵심 로직 변경 지점이다.

**Files:**
- Create: `src/construction_stipulations/silver1.py`
- Create: `src/silver2/construction_work_hours_join.py`
- Delete: `src/construction_stipulations/silver.py`
- Modify: `src/gold2/closure_penalty.py:78`
- Modify: `dags/construction_pipeline.py:124,130,136,142,148,154`

**Interfaces:**
- Consumes: `data/silver1/construction/` (Task 2 산출물 경로)
- Produces: `src.construction_stipulations.silver1.{extract_work_hours, extract_work_embargoes,
  build_work_hours_rules, validate_work_hours_rules_output, load_built_work_hours_rules,
  build_embargoes, validate_embargoes_output, load_built_embargoes}` /
  `src.silver2.construction_work_hours_join.{build, validate, validate_output, main}`
  (Task 4의 road_closures가 이 산출물을 참조할 수도 있음 — Task 4에서 실제로 필요한지 확인)

- [ ] **Step 1: silver1.py 생성 — 텍스트추출+rule/LLM병합+dedup+quarantine 이동**

`src/construction_stipulations/silver.py`(824줄)에서 아래 라인 구간을 **그대로**
`src/construction_stipulations/silver1.py`로 옮긴다(함수 본문 변경 없음):

- 88-92 `WORK_HOUR_RE`
- 99-108 `_parse_work_hours`
- 111-126 `_rule_parse_work_hours_with_lineage`
- 129-132 `WORK_HOURS_COLUMNS`
- 135 `_WORK_HOURS_DEDUP_SUBSET`
- 141 `_WORK_HOURS_PARSE_KEYS`
- 144-163 `_load_raw_work_hours_rows`
- 166-189 `extract_work_hours`
- 218-219 `EMBARGO_DATE`, `EMBARGO_TIME`
- 224-228 `EMBARGO_RE_SINGLE`
- 230-233 `EMBARGO_RE_RECURRING`
- 235 `EMBARGO_REASON_BOILERPLATE_RE`
- 242-244 `_clean_embargo_reason`
- 247-288 `_parse_work_embargo`
- 291-303 `_rule_parse_embargo_with_lineage`
- 306-310 `EMBARGO_COLUMNS`
- 314-317 `_EMBARGO_PARSE_KEYS`
- 320-323 `_EMBARGO_DEDUP_SUBSET`
- 326 `_work_embargoes_cache`
- 329-351 `_load_raw_embargo_rows`
- 354-390 `extract_work_embargoes`
- 393 `EMBARGO_OUT_SOURCE = "construction_work_embargoes"`
- 403 `EMBARGO_NEW_FAILURE_ALERT_THRESHOLD = 0`
- 406-535 `build_embargoes`
- 538-563 `validate_embargoes_output`
- 566-573 `main_embargoes`
- 576-587 `load_built_embargoes`
- 595 `WORK_HOURS_OUT_SOURCE = "construction_work_hours_rules"`
- 600 `WORK_HOURS_NEW_FAILURE_ALERT_THRESHOLD = 0`
- 603-704 `build_work_hours_rules`
- 707-729 `validate_work_hours_rules_output`
- 732-739 `main_work_hours_rules`
- 742-751 `load_built_work_hours_rules`

파일 최상단 import는 원본 49-74행에서 이 함수들이 실제 쓰는 것만 남긴다:
```python
from __future__ import annotations

import glob
import os
import re
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

from src.common.config import BRONZE_DIR, SILVER1_DIR
from src.common.gemini import GEMINI_MODEL, parse_embargo_text_with_llm, parse_work_hours_text_with_llm
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.construction_stipulations.bronze import SOURCE as STIPULATIONS_SOURCE
from src.construction_stipulations.llm_pipeline import (
    DAY_MAP,
    _parse_embargo_date,
    _parse_embargo_time,
    _to_hour24,
    compute_and_log_quality_report,
    load_llm_cache,
    quarantined_texts,
    run_llm_fallback_batch,
    write_quarantine,
)
```
`GOLD_DIR`는 뺀다(silver1은 더 이상 construction Gold를 안 읽음). `SILVER_DIR` 대신
`SILVER1_DIR`을 쓴다(원본에서 `SILVER_DIR`을 참조하던 저장 경로들 — `EMBARGO_OUT_SOURCE`,
`WORK_HOURS_OUT_SOURCE`가 실제 저장 시 어느 상수를 쓰는지 `build_embargoes`/
`build_work_hours_rules` 본문에서 확인하고 `SILVER_DIR`을 쓰던 자리를 전부 `SILVER1_DIR`로
치환).

- [ ] **Step 2: silver2/construction_work_hours_join.py 생성 — LEFT JOIN 파이프라인 이동**

원본 78, 83, 590-592, 754-762, 765-783, 786-802, 805-811, 814-820행을 옮기되 **83행과
590-592행은 내용을 바꾼다**:

```python
from __future__ import annotations

import os
from datetime import date

import pandas as pd

from src.common.config import SILVER1_DIR, SILVER2_DIR
from src.common.logger import get_logger
from src.common.utils import save_parquet
from src.construction_stipulations.silver1 import load_built_work_hours_rules

logger = get_logger(__name__)

OUT_SOURCE = "construction_work_hours"
CONSTRUCTION_SILVER1_DIR = SILVER1_DIR / "construction"


def load_construction_silver1() -> pd.DataFrame:
    """construction Silver1(전 지역 permit) 최신 파티션을 읽는다.

    구 버전은 construction **Gold**(`GOLD_DIR/"construction"`, 이미 Manhattan 등으로
    필터링된 상태)를 읽었다. Silver2는 항상 상위 도메인의 Silver1을 읽어야 한다는 원칙에
    따라 Silver1로 참조를 바꾼다 — 필터링 전 전체 지역 permit과 조인해야 이후 Gold1
    필터가 이 조인 결과에도 온전히 적용될 수 있다.
    """
    partitions = sorted(CONSTRUCTION_SILVER1_DIR.glob("dt=*"))
    if not partitions:
        raise FileNotFoundError(f"construction silver1 파티션이 없다: {CONSTRUCTION_SILVER1_DIR}")
    return pd.read_parquet(partitions[-1])
```
(`load_construction_silver1`의 파티션 탐색 로직은 원본 `load_construction_gold`(590-592행)의
로직을 그대로 따르되 경로 상수와 함수명만 바꾼 것 — **구현 시 반드시 원본 590-592행을 먼저
읽고 정확한 파티션 glob 패턴을 확인할 것**, 위 코드는 패턴 형태 예시다.)

`_merge_work_hours`(754-762행)는 내부에서 `load_construction_gold()` 호출하던 자리를
`load_construction_silver1()` 호출로 바꾸고, `load_built_work_hours_rules()` 호출은 이제
`src.construction_stipulations.silver1`에서 import한 것이므로 그대로 둔다. `validate`,
`build`, `validate_output`, `main`(765-820행)은 내용 무변경으로 옮긴다.

- [ ] **Step 3: 구 silver.py 삭제**

```bash
git rm src/construction_stipulations/silver.py
```

- [ ] **Step 4: closure_penalty.py 참조 수정**

`src/gold2/closure_penalty.py:78`:
```python
# 변경 전
from src.construction_stipulations.silver import load_built_embargoes
# 변경 후
from src.construction_stipulations.silver1 import load_built_embargoes
```

- [ ] **Step 5: DAG import 수정**

`dags/construction_pipeline.py:124,130`(work_hours_rules):
```python
# 변경 전
from src.construction_stipulations.silver import build_work_hours_rules
from src.construction_stipulations.silver import validate_work_hours_rules_output
# 변경 후
from src.construction_stipulations.silver1 import build_work_hours_rules
from src.construction_stipulations.silver1 import validate_work_hours_rules_output
```

`dags/construction_pipeline.py:136,142`(조인, work_hours):
```python
# 변경 전
from src.construction_stipulations.silver import build
from src.construction_stipulations.silver import validate_output
# 변경 후
from src.silver2.construction_work_hours_join import build
from src.silver2.construction_work_hours_join import validate_output
```

`dags/construction_pipeline.py:148,154`(embargoes):
```python
# 변경 전
from src.construction_stipulations.silver import build_embargoes
from src.construction_stipulations.silver import validate_embargoes_output
# 변경 후
from src.construction_stipulations.silver1 import build_embargoes
from src.construction_stipulations.silver1 import validate_embargoes_output
```

DAG 의존관계(construction_pipeline.py 202-222행 부근)는 그대로 유지한다 — task_id는
안 바뀌고 import 경로만 바뀌므로 `>>` 체인 수정 불필요.

- [ ] **Step 6: smoke import + grep 확인 + 테스트**

```bash
python -c "
from src.construction_stipulations.silver1 import (
    extract_work_hours, extract_work_embargoes, build_work_hours_rules,
    validate_work_hours_rules_output, load_built_work_hours_rules,
    build_embargoes, validate_embargoes_output, load_built_embargoes,
)
from src.silver2.construction_work_hours_join import build, validate, validate_output, main
print('OK')
"
grep -rn "construction_stipulations\.silver\b" --include="*.py" . | grep -v "\.git/\|silver1"
```
Expected: `OK` 출력, grep 0건.

```bash
pytest tests/ -q
```
Expected: 이전과 동일(이 도메인 자체 테스트는 없음).

- [ ] **Step 7: 커밋**

```bash
git add src/construction_stipulations src/silver2/construction_work_hours_join.py \
  src/gold2/closure_penalty.py dags/construction_pipeline.py
git commit -m "refactor: construction_stipulations를 silver1(정제)/silver2(construction 조인)로 분리

Silver2 조인 참조 대상을 construction Gold에서 construction Silver1로 변경
(Silver2는 항상 상위 도메인의 Silver1을 읽는다는 원칙 적용)."
```

---

### Task 4: road_closures 도메인 — silver1.py 분리 + silver2/road_closure_construction_conflation.py 신설

**중요**: 구현 전에 `src/road_closures/silver.py`의 `_combine()`(85-116행)이 실제로
`load_construction_work_hours()`(construction_stipulations 조인 결과, `SILVER_DIR/
"construction_work_hours"`)의 어떤 컬럼을 쓰는지 먼저 확인한다. 겹침 판단에 street/시작일/
종료일만 쓴다면 construction **Silver1**(work_hours 미반영 permit)만으로 충분하다. 만약
work_hours(요일별 시간대) 스케줄 자체가 겹침 판단 로직에 실제로 쓰인다면, Task 3이 만든
`silver2/construction_work_hours_join.py`의 산출물(`data/silver2/construction_work_hours/`)을
그대로 계속 읽어야 한다 — 이 경우도 Silver2가 다른 Silver2 산출물을 읽는 것이라 레이어
원칙 위반이 아니다(둘 다 Silver2). 아래 단계는 **전자(Silver1만으로 충분)**를 기본으로
작성했으니, 실제로 읽어보고 다르면 `load_construction_work_hours()` 자리를
`load_construction_silver1()`(Task 3에서 만든 것)이 아니라 Task 3의
`construction_work_hours_join.build()` 산출물을 읽도록 바꾼다.

**Files:**
- Create: `src/road_closures/silver1.py`
- Create: `src/silver2/road_closure_construction_conflation.py`
- Delete: `src/road_closures/silver.py`
- Modify: `dags/construction_pipeline.py:160,166`

**Interfaces:**
- Consumes: `data/silver1/construction/` (Task 2) 또는 `data/silver2/construction_work_hours/`
  (Task 3) — 위 확인 결과에 따라 결정
- Produces: `src.road_closures.silver1.load_road_closures` /
  `src.silver2.road_closure_construction_conflation.{build, validate, validate_output, main}`

- [ ] **Step 1: 원본 확인**

```bash
sed -n '1,120p' src/road_closures/silver.py
```
`_combine()`(85-116행)이 참조하는 컬럼명을 정확히 적어두고, 아래 Step에서 그에 맞게
`load_construction_*` 호출을 결정한다.

- [ ] **Step 2: silver1.py 생성**

`src/road_closures/silver.py`의 아래 라인을 그대로 옮긴다:
- import 블록(28-38행)에서 `from src.road_closures.bronze import latest_bronze_file`까지 포함
- 43행 `RC_READ_COLS`
- 56-82행 `load_road_closures`

파일 상단 import에서 `CONSTRUCTION_WORK_HOURS_DIR`(43행)는 옮기지 않는다(silver2로 이동).

- [ ] **Step 3: silver2/road_closure_construction_conflation.py 생성**

`src/road_closures/silver.py`의 아래 라인을 옮긴다(41행 `CONSTRUCTION_WORK_HOURS_DIR`은
Step 1 확인 결과에 따라 참조 경로를 `SILVER1_DIR/"construction"` 또는
`SILVER2_DIR/"construction_work_hours"`로 바꿈):
- 51-53행 `load_construction_work_hours` (경로 상수만 변경, 나머지 로직 무변경)
- 85-116행 `_combine`
- 119-137행 `validate`
- 140-158행 `build`
- 161-168행 `validate_output`
- 171-177행 `main`

import는 `from src.road_closures.silver1 import load_road_closures`를 추가하고,
`from src.common.config import SILVER1_DIR, SILVER2_DIR`(또는 확인 결과에 맞게),
`from src.common.logger import get_logger`, `from src.common.utils import clean_street,
save_parquet`를 유지한다.

- [ ] **Step 4: 구 silver.py 삭제**

```bash
git rm src/road_closures/silver.py
```

- [ ] **Step 5: DAG import 수정**

`dags/construction_pipeline.py:160,166`:
```python
# 변경 전
from src.road_closures.silver import build
from src.road_closures.silver import validate_output
# 변경 후
from src.silver2.road_closure_construction_conflation import build
from src.silver2.road_closure_construction_conflation import validate_output
```

- [ ] **Step 6: smoke import + 테스트**

```bash
python -c "
from src.road_closures.silver1 import load_road_closures
from src.silver2.road_closure_construction_conflation import build, validate, validate_output, main
print('OK')
"
pytest tests/ -q
```
Expected: `OK`, 테스트는 이전과 동일(자체 테스트 없음).

- [ ] **Step 7: 커밋**

```bash
git add src/road_closures src/silver2/road_closure_construction_conflation.py \
  dags/construction_pipeline.py
git commit -m "refactor: road_closures를 silver1(정제)/silver2(construction conflation)로 분리"
```

---

### Task 5: event 도메인 — silver1.py 분리 + gold1.py 신설

**핵심 변경**: `event_borough` 컬럼이 현재 Silver 최종 select(256-264행)에서 빠져 있는데
Gold1의 Manhattan 필터가 이 컬럼을 써야 하므로, **Silver1 최종 컬럼 목록에
`event_borough`를 추가**한다(로직 변경 아님, 필터가 쓸 컬럼을 보존하는 것).

**Files:**
- Create: `src/event/silver1.py`
- Create: `src/event/gold1.py`
- Delete: `src/event/silver.py`
- Modify: `dags/event_pipeline.py:53,59` + gold1 태스크 신규 추가

**Interfaces:**
- Produces: `src.event.silver1.{build, validate_output}` (컬럼: 기존 + `event_borough` 추가) /
  `src.event.gold1.{build, validate_output}` (신규 작성, Manhattan+활성기간+SIDEWALK_ONLY 필터)

- [ ] **Step 1: silver1.py 생성**

`src/event/silver.py`(402줄)에서 아래를 옮긴다:
- import(19-31행), 단 `BOROUGH_EVENT`는 gold1이 쓰므로 silver1 import에서 빼도 되지만
  당장은 안 써도 무해하니 그대로 둬도 됨(다음 리뷰에서 lint 경고 있으면 제거)
- 43-46행 `RE_BETWEEN`, 48-55행 `READ_COLS`
- 58-81행 `load_bronze`
- 84-140행 `normalize_event_street`
- 144-184행 `parse_location`
- **`transform`(187-264행)을 재구성**: 원본의 195-210행(날짜/시간 변환+유효성 드롭),
  240-253행(parse_location 호출), 256-264행(최종 컬럼 선택)만 남기고, 190-192행(맨해튼
  필터), 212-221행(run_date 활성필터), 223-238행(SIDEWALK_ONLY 제외)은 **뺀다**. 최종
  컬럼 선택(256-264행)에 `event_borough`를 추가한다:
  ```python
  # 최종 select 예시 (원본 256-264행 기반, event_borough 추가)
  df = df[[
      "event_id", "event_name", "event_borough", "start_ts", "end_ts",
      "street_name", "cross_street_1", "cross_street_2", "closure_type",
      # ... 원본 256-264행의 나머지 컬럼 그대로
  ]]
  ```
  (구현 시 원본 256-264행을 그대로 옮기고 `event_borough` 한 줄만 추가할 것 — 나머지
  컬럼명은 원본을 그대로 베낀다.)
- 267-278행 `UNMATCHED_LOCATION_PATH`, `UNMATCHED_LOCATION_COLUMNS`, `_load_unmatched_locations`,
  `_save_unmatched_locations`
- 281-351행 `validate` (event_id/street 검증 — Silver1 성격 그대로 유지)
- 신규 작성: `build`, `validate_output`, `main` — 원본 354-398행의 오케스트레이션 구조를
  그대로 따라 `transform`(재구성판) + `validate`를 호출하도록 작성(원본과 동일한 저장/로그
  패턴, 저장 경로만 `SILVER1_DIR/"event"`로 변경)

- [ ] **Step 2: gold1.py 신규 작성**

```python
from __future__ import annotations

import sys
import os
from pathlib import Path
from datetime import date

import pandas as pd

sys.path.append(str(Path(__file__).resolve().parent.parent))

from common.config import SILVER1_DIR, GOLD1_DIR, BOROUGH_EVENT
from common.utils import save_parquet
from common.logger import get_logger

logger = get_logger(__name__)

SOURCE = "event"

# 원본 src/event/silver.py 38-41행 그대로
SIDEWALK_ONLY = {
    # 원본 38-41행의 값을 그대로 옮긴다
}


def load_silver1() -> pd.DataFrame:
    partitions = sorted((SILVER1_DIR / SOURCE).glob("dt=*"))
    if not partitions:
        raise FileNotFoundError(f"event silver1 파티션이 없다: {SILVER1_DIR / SOURCE}")
    return pd.read_parquet(partitions[-1])


def filter_for_traffic_score(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    # 원본 silver.py 190-192행: 맨해튼 필터
    df = df[df["event_borough"] == BOROUGH_EVENT]
    # 원본 silver.py 212-221행: run_date 기준 이미 끝난 행사 제외 (원본 로직 그대로 이식)
    df = df[df["end_ts"].dt.date >= run_date]
    # 원본 silver.py 223-238행: SIDEWALK_ONLY 제외 (원본 로직 그대로 이식)
    df = df[~df["closure_type"].isin(SIDEWALK_ONLY)]
    return df


def validate(df: pd.DataFrame) -> None:
    if df.empty:
        raise ValueError("event gold1 결과가 비어있다")


def build(run_date: date | None = None) -> Path:
    run_date = run_date or date.today()
    df = load_silver1()
    df = filter_for_traffic_score(df, run_date)
    validate(df)
    out_dir = GOLD1_DIR / SOURCE
    return save_parquet(df, out_dir / f"dt={run_date}")


def validate_output(path: Path) -> None:
    df = pd.read_parquet(path)
    validate(df)


def main() -> None:
    path = build()
    validate_output(path)
    logger.info(f"event gold1 완료: {path}")


if __name__ == "__main__":
    main()
```
`filter_for_traffic_score`/`validate`의 정확한 로직(컬럼명, 비교 연산자)은 **반드시 원본
`src/event/silver.py` 190-192, 212-221, 223-238행을 그대로 옮겨 적을 것** — 위 코드는
구조 예시이고 원본 조건식을 한 글자도 안 바꾸고 옮기는 것이 이 태스크의 핵심 규칙이다.

- [ ] **Step 3: 구 silver.py 삭제**

```bash
git rm src/event/silver.py
```

- [ ] **Step 4: DAG 수정 — silver1 경로 변경 + gold1 태스크 신규 추가**

`dags/event_pipeline.py:53,59`:
```python
# 변경 전
from src.event.silver import build
from src.event.silver import validate_output
# 변경 후
from src.event.silver1 import build
from src.event.silver1 import validate_output
```

`build_event`/`validate_event` task(62-71행 부근) 다음, `map_event_lion` task(73-77행) 이전에
새 task 2개를 추가한다(기존 task 정의 스타일을 그대로 따를 것 — 정확한 데코레이터/인자는
`dags/event_pipeline.py`의 기존 task 정의를 참고해 동일 패턴으로 작성):
```python
@task
def build_event_gold1():
    from src.event.gold1 import build
    return str(build())


@task
def validate_event_gold1(path: str):
    from src.event.gold1 import validate_output
    validate_output(Path(path))
```
의존관계를 `validate_event >> build_event_gold1 >> validate_event_gold1 >> map_event_lion`으로
잇는다(단, `map_event_lion`이 event_lion 매칭에 Gold1 필터링 전 전지역 데이터를 써야 한다는
설계 원칙에 따라 실제로는 `map_event_lion`이 **silver1** 출력을 계속 쓰고 gold1은 별도
분기로 두는 것이 맞다 — 이 경우 `validate_event >> [build_event_gold1, map_event_lion]`으로
병렬 분기시킨다. 구현 시 `src/silver2/event_lion.py`가 지금 무엇을 입력으로 받는지
확인하고 이 의존관계를 결정할 것).

- [ ] **Step 5: smoke import + 테스트**

```bash
python -c "
from src.event.silver1 import build, validate_output
from src.event.gold1 import build, validate_output, filter_for_traffic_score
print('OK')
"
pytest tests/ -q
```

- [ ] **Step 6: 커밋**

```bash
git add src/event dags/event_pipeline.py
git commit -m "refactor: event를 silver1(정제)/gold1(Manhattan+활성기간+차량무관 필터)로 분리

event_borough 컬럼을 silver1 최종 출력에 보존(gold1 필터가 사용)."
```

---

### Task 6: ticketmaster 도메인 — silver1.py 분리 + gold1.py 신설 + silver2.py(venue.py 이관)

ticketmaster는 event와 달리 필터에 필요한 컬럼(`lat`, `lon`, `event_date`)이 이미 최종
select(203-215행)에 남아있어 event보다 재구성이 단순하다.

**Files:**
- Create: `src/ticketmaster/silver1.py`
- Create: `src/ticketmaster/gold1.py`
- Create: `src/ticketmaster/silver2.py` (venue.py 이관, 호출자 없음 그대로 유지)
- Delete: `src/ticketmaster/silver.py`, `src/ticketmaster/venue.py`
- Modify: `dags/ticketmaster_pipeline.py:66,72` + gold1 태스크 신규 추가

**Interfaces:**
- Produces: `src.ticketmaster.silver1.{build, validate_output}` /
  `src.ticketmaster.gold1.{build, validate_output}` /
  `src.ticketmaster.silver2.{attach_capacity, VENUE_CAPACITY, get_capacity, normalize_venue}`
  (호출자 없음 — 이동만)

- [ ] **Step 1: silver1.py 생성**

`src/ticketmaster/silver.py`(289줄)에서 옮긴다:
- import(15-27행), `MH_LAT`/`MH_LON`(34-35행)은 **제외**(gold1로)
- 38-52행 `load_bronze`
- 55-82행 `parse_venue`
- `transform`(85-215행) 재구성: 88-91(중복컬럼제거), 94-97(id dedup), 100-118(venue파싱),
  128-136(event_date파싱), 148-200(start_ts/end_ts), 203-215(최종컬럼선택)만 남기고
  121-124(맨해튼 bbox 필터), 139-145(run_date 활성필터)는 **뺀다**. 최종 select(203-215행)에
  `lat`,`lon`,`event_date`가 이미 포함돼 있으므로 컬럼 추가 불필요(그대로 옮김).
- 218-238행 `validate`
- 신규 작성: `build`, `validate_output`, `main`(원본 241-285행 구조를 따라 저장 경로만
  `SILVER1_DIR/"ticketmaster"`로 변경)

- [ ] **Step 2: gold1.py 신규 작성**

Task 5의 event/gold1.py와 동일한 구조로 작성하되, 필터 로직만 다음으로 교체:
```python
MH_LAT = (40.68, 40.88)  # 원본 silver.py 34행 그대로
MH_LON = (-74.03, -73.90)  # 원본 silver.py 35행 그대로


def filter_for_traffic_score(df: pd.DataFrame, run_date: date) -> pd.DataFrame:
    # 원본 silver.py 121-124행: 맨해튼 bbox 필터
    df = df[
        df["lat"].between(*MH_LAT) & df["lon"].between(*MH_LON)
    ]
    # 원본 silver.py 139-145행: run_date 기준 지난 행사 제외 (원본 조건식 그대로 이식)
    df = df[df["event_date"] >= run_date]
    return df
```
정확한 비교 연산자/컬럼명은 원본 121-124, 139-145행을 그대로 옮겨 적을 것.

- [ ] **Step 3: silver2.py 생성 (venue.py 이관)**

`src/ticketmaster/venue.py`(302줄) 전체를 `src/ticketmaster/silver2.py`로 옮기되, 20-22행의
import를 고친다:
```python
# 변경 전 (venue.py 22행)
from common.logger import get_logger
# 변경 후 (silver2.py) — sys.path.append 없이 쓰던 잠재 버그 수정
from src.common.logger import get_logger
```
나머지(28-302행: `DEFAULT_CAPACITY`, `SUFFIX_PATTERNS`, `MANUAL_ALIASES`, `normalize_venue`,
`EXCLUDE_VENUES`, `VENUE_CAPACITY`, `get_capacity`, `attach_capacity`)는 무변경. 이 파일은
현재 아무 곳에서도 호출되지 않는다 — 이동 후에도 여전히 호출자가 없는 게 정상이다
(연결은 이번 범위 밖).

- [ ] **Step 4: 구 파일 삭제**

```bash
git rm src/ticketmaster/silver.py src/ticketmaster/venue.py
```

- [ ] **Step 5: DAG 수정**

`dags/ticketmaster_pipeline.py:66,72`:
```python
# 변경 전
from src.ticketmaster.silver import build
from src.ticketmaster.silver import validate_output
# 변경 후
from src.ticketmaster.silver1 import build
from src.ticketmaster.silver1 import validate_output
```

`build_ticketmaster`/`validate_ticketmaster` task(64-73행) 다음, `map_ticketmaster_lion`
task(75-79행) 이전에 gold1 task 2개 추가(Task 5의 event와 동일한 패턴 — `@task` 데코레이터
스타일은 `dags/ticketmaster_pipeline.py`의 기존 정의를 참고).

- [ ] **Step 6: smoke import + 테스트**

```bash
python -c "
from src.ticketmaster.silver1 import build, validate_output
from src.ticketmaster.gold1 import build, validate_output, filter_for_traffic_score
from src.ticketmaster.silver2 import attach_capacity, VENUE_CAPACITY
print('OK')
"
pytest tests/ -q
```

- [ ] **Step 7: 커밋**

```bash
git add src/ticketmaster dags/ticketmaster_pipeline.py
git commit -m "refactor: ticketmaster를 silver1/gold1로 분리, venue.py를 silver2.py로 이관

venue.py의 sys.path 없이 쓰던 import 버그(from common.logger, ModuleNotFoundError 유발
가능)를 이관하며 수정. event_boost 연결은 이번 범위 밖(계속 미사용 상태)."
```

---

### Task 7: lion 도메인 — silver1.py / silver2.py(segment_adjacency) / gold2.py(계산+traffic_score) 분리

**가장 복잡한 태스크.** `dim_segment.parquet`을 참조하는 8개 소비처가 Silver1 컬럼
(`segment_id, street_name, borough_code, geometry, length_ft, node_from, node_to`)과
Gold2 컬럼(`road_class, is_two_way, lanes_total, lane_miles, base_capacity_per_lane,
capacity_per_hour, is_routable`)을 섞어서 쓰고 있어, **Gold2가 Silver1 산출물을 읽어
컬럼을 추가하고 같은 파일 이름(`dim_segment.parquet`)으로 완성본을 다시 저장**하는
구조로 만든다(파일을 둘로 쪼개면 8곳을 전부 "이 컬럼은 어느 파일에서" 식으로 다시
분기해야 해서 훨씬 위험함 — 완성본 하나 방식이 기존 소비처 8곳의 코드를 안 건드려도
되게 해준다). `DIM_SEGMENT_PATH` 상수는 이제 `GOLD2_DIR/"dim_segment.parquet"`를
가리키도록 옮긴다.

**Files:**
- Create: `src/lion/silver1.py`
- Rename: `src/lion/segment_adjacency.py` → `src/lion/silver2.py` (내용 무변경, import만 수정)
- Create: `src/lion/gold2.py` (road_class 등 계산 + traffic_score.py 전체 합침)
- Delete: `src/lion/silver.py`, `src/lion/traffic_score.py`
- Modify: `src/silver2/road_closure_segment.py:35`, `src/silver2/road_control_segment.py:57-58`,
  `src/silver2/zone_segment.py:35`, `src/gold2/closure_penalty.py:79-80`,
  `src/gold2/traffic_score.py:38-41` (구 41,43,44행 — Task1에서 안 건드린 부분)
- Modify: `dags/lion_pipeline.py:41-44` + task 의존관계 재배선

**Interfaces:**
- Produces: `src.lion.silver1.build_dim_segment_base` (silver1 전용 컬럼만) /
  `src.lion.gold2.{build_dim_segment, validate_dim_segment, DIM_SEGMENT_PATH,
  build_dim_segment_traffic_score, validate_dim_segment_traffic_score,
  DIM_SEGMENT_TRAFFIC_SCORE_PATH}` / `src.lion.silver2.{build_graph_segment_adjacency,
  validate_graph_segment_adjacency, GRAPH_SEGMENT_ADJACENCY_PATH}`

- [ ] **Step 1: silver1.py 생성 — silver1 전용 컬럼만 만드는 base 빌더**

`src/lion/silver.py`(281줄)의 `build_dim_segment`(152-233행)를 재구성한다. 152-193행
(ogr2ogr 실행, csv읽기, 컬럼명통일, 숫자캐스팅, 도로명정제, SegmentID dedup)까지는 그대로
쓰되, 195-202행(road_class/is_routable/is_two_way/base_capacity_per_lane/capacity_per_hour/
lane_miles 계산)은 **빼고**, 204-207행(street_name 재정제)은 살리고, 최종 select(209-223행)를
silver1 컬럼만 남기도록 좁힌다:
```python
# 최종 select — silver1 컬럼만 (원본 209-223행 기반, gold2 컬럼 제외)
df = df[[
    "segment_id", "street_name", "borough_code", "geometry",
    "length_ft", "node_from", "node_to",
]]
```
(정확한 컬럼명은 원본 209-223행을 확인해 grep된 컬럼명 그대로 쓸 것.) 저장 함수명을
`build_dim_segment_base`로 짓고 저장 경로를 `SILVER1_DIR/"dim_segment.parquet"`로 한다.
함께 옮기는 것: import(33-43행, `LION_COLUMNS`(51-55), `_latest_bronze_version`(82-87),
`_find_gdb`(90-94), `_gdb_to_flat_csv`(97-130)). `validate_dim_segment`(247-276행)는
**silver1로 옮기지 않는다** — 실제로는 `road_class`/`is_routable`(gold2 컬럼)을 검증하므로
Task의 매핑표와 달리 gold2로 보낸다(아래 Step 3). silver1에는 최소 검증(row count, segment_id
유니크, geometry notna)만 하는 새 `validate_dim_segment_base` 함수를 신규 작성한다.

- [ ] **Step 2: segment_adjacency.py → silver2.py 이름 변경**

```bash
git mv src/lion/segment_adjacency.py src/lion/silver2.py
```
`src/lion/silver2.py`(구 130줄)의 31행 import 수정:
```python
# 변경 전
from src.lion.silver import DIM_SEGMENT_PATH, LION_BRONZE_ROOT, _find_gdb, _latest_bronze_version
# 변경 후
from src.lion.silver1 import LION_BRONZE_ROOT, _find_gdb, _latest_bronze_version
from src.lion.gold2 import DIM_SEGMENT_PATH
```
(`is_routable`을 쓰는 77-79행 로직 때문에 `DIM_SEGMENT_PATH`는 gold2에서 가져와야 함 —
아래 Step 3에서 gold2가 완성본을 만든 뒤에만 이 silver2 모듈이 정상 동작한다는 뜻이므로
DAG 의존관계도 그렇게 재배선한다, Step 6 참고.)

- [ ] **Step 3: gold2.py 생성 — road_class 계산 + traffic_score.py 합침**

`src/lion/silver.py`에서 다음을 가져온다: `HIGHWAY_RW_TYPES`(58), `NON_ROUTABLE_RW_TYPES`
(59-60), `ARTERIAL_TRUCK_ROUTE_TYPES`(63), `ARTERIAL_MIN_LANES`(64), `BASE_CAPACITY_PER_LANE`
(68-73), `DIRECTION_FACTOR`(76-79), `_classify_road_class`(133-149), `VALID_ROAD_CLASSES`(236),
`VALID_BOROUGH_CODES`(237), `MIN_EXPECTED_ROWS`/`MAX_EXPECTED_ROWS`(243-244),
`validate_dim_segment`(247-276, 함수명은 `validate_dim_segment`로 유지).

새 `build_dim_segment(run_date=None)` 함수를 작성한다 — `src.lion.silver1`이 만든
`SILVER1_DIR/"dim_segment.parquet"`을 읽어서 `road_class`/`is_routable`/`is_two_way`/
`base_capacity_per_lane`/`capacity_per_hour`/`lane_miles`(원본 195-202행 계산식 그대로)를
계산해 컬럼을 추가한 뒤, **완성본을 `GOLD2_DIR/"dim_segment.parquet"`에 저장**한다:
```python
DIM_SEGMENT_PATH = GOLD2_DIR / "dim_segment.parquet"


def build_dim_segment() -> Path:
    df = pd.read_parquet(SILVER1_DIR / "dim_segment.parquet")
    df["road_class"] = df.apply(_classify_road_class, axis=1)  # 원본 195행 로직 그대로
    df["is_routable"] = ~df["RW_TYPE"].isin(NON_ROUTABLE_RW_TYPES)  # 원본 196행 그대로
    df["is_two_way"] = ...  # 원본 197행 그대로
    df["base_capacity_per_lane"] = ...  # 원본 199행 그대로
    df["capacity_per_hour"] = ...  # 원본 200-201행 그대로
    df["lane_miles"] = ...  # 원본 202행 그대로
    save_parquet(df, DIM_SEGMENT_PATH)
    return DIM_SEGMENT_PATH
```
(각 계산식은 원본 195-202행을 그대로 옮겨 적을 것 — 위는 자리 표시일 뿐 실제 구현 시
원본 코드를 1:1로 복사한다.) 이어서 `src/lion/traffic_score.py`(114줄) 전체(`DIM_SEGMENT_
TRAFFIC_SCORE_PATH`(48), `BETWEENNESS_K`(52), `BETWEENNESS_SEED`(53),
`build_dim_segment_traffic_score`(56-90), `validate_dim_segment_traffic_score`(93-109))를
같은 `gold2.py` 파일에 이어 붙인다 — import 43-44행(`from src.lion.segment_adjacency import
GRAPH_SEGMENT_ADJACENCY_PATH`, `from src.lion.silver import DIM_SEGMENT_PATH`)을
`from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH`로, `DIM_SEGMENT_PATH`는 같은
파일 안에 이미 정의돼 있으니 import 제거.

- [ ] **Step 4: 구 파일 삭제**

```bash
git rm src/lion/silver.py src/lion/traffic_score.py
```

- [ ] **Step 5: 8개 소비처 import 수정**

`src/silver2/road_closure_segment.py:35`:
```python
# 변경 전
from src.lion.silver import DIM_SEGMENT_PATH
# 변경 후
from src.lion.gold2 import DIM_SEGMENT_PATH
```
`src/silver2/road_control_segment.py:57-58`:
```python
# 변경 전
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.silver import DIM_SEGMENT_PATH
# 변경 후
from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.gold2 import DIM_SEGMENT_PATH
```
`src/silver2/zone_segment.py:35`:
```python
# 변경 전
from src.lion.silver import DIM_SEGMENT_PATH
# 변경 후
from src.lion.gold2 import DIM_SEGMENT_PATH
```
`src/gold2/closure_penalty.py:79-80`:
```python
# 변경 전
from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.silver import DIM_SEGMENT_PATH
# 변경 후
from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH
from src.lion.gold2 import DIM_SEGMENT_PATH
```
`src/gold2/traffic_score.py`(Task 1에서 안 건드린 부분, 원본 기준 38-41행 부근):
```python
# 변경 전
from src.lion.silver import DIM_SEGMENT_PATH
from src.lion.traffic_score import DIM_SEGMENT_TRAFFIC_SCORE_PATH
# 변경 후
from src.lion.gold2 import DIM_SEGMENT_PATH, DIM_SEGMENT_TRAFFIC_SCORE_PATH
```

- [ ] **Step 6: DAG 수정 — import + 의존관계 재배선**

`dags/lion_pipeline.py:41-44`:
```python
# 변경 전
from src.lion.silver import build_dim_segment, validate_dim_segment
from src.lion.segment_adjacency import build_graph_segment_adjacency, validate_graph_segment_adjacency
from src.lion.traffic_score import build_dim_segment_traffic_score, validate_dim_segment_traffic_score
from src.mapping.zone_segment import build_map_zone_segment, validate_map_zone_segment
# 변경 후
from src.lion.silver1 import build_dim_segment_base
from src.lion.gold2 import (
    build_dim_segment, validate_dim_segment,
    build_dim_segment_traffic_score, validate_dim_segment_traffic_score,
)
from src.lion.silver2 import build_graph_segment_adjacency, validate_graph_segment_adjacency
from src.silver2.zone_segment import build_map_zone_segment, validate_map_zone_segment
```
`build_dim_segment` task(71-77행)를 **두 task로 쪼갠다**: `build_dim_segment_base`(silver1,
신규) → `build_dim_segment`(gold2, silver1 산출물을 완성) → `validate_dim_segment`(gold2).
기존 의존관계(원본 74행 부근 `ingest_lion >> build_dim_segment >> validate_dim_segment`,
88-108행의 `validate_dim_segment >> [build_map_zone_segment, build_graph_segment_adjacency]`
병렬 분기, 그 뒤 `traffic_score` 체인)는 그대로 유지하되 맨 앞에 `build_dim_segment_base`만
끼워 넣는다:
```
ingest_lion >> build_dim_segment_base >> build_dim_segment >> validate_dim_segment
validate_dim_segment >> build_map_zone_segment >> validate_map_zone_segment
validate_dim_segment >> build_graph_segment_adjacency >> validate_graph_segment_adjacency
validate_graph_segment_adjacency >> build_dim_segment_traffic_score >> validate_dim_segment_traffic_score
```
`build_dim_segment_base` task는 Asset outlet 없이(중간 산출물), `build_dim_segment`가
기존에 `outlets=[Asset("dim_segment")]`를 갖던 것(원본 74행)을 그대로 이어받는다.

- [ ] **Step 7: smoke import + 테스트**

```bash
python -c "
from src.lion.silver1 import build_dim_segment_base
from src.lion.silver2 import build_graph_segment_adjacency, validate_graph_segment_adjacency
from src.lion.gold2 import (
    build_dim_segment, validate_dim_segment,
    build_dim_segment_traffic_score, validate_dim_segment_traffic_score,
    DIM_SEGMENT_PATH, DIM_SEGMENT_TRAFFIC_SCORE_PATH,
)
print('OK')
"
pytest tests/ -q
```

- [ ] **Step 8: 커밋**

```bash
git add src/lion src/silver2/road_closure_segment.py src/silver2/road_control_segment.py \
  src/silver2/zone_segment.py src/gold2/closure_penalty.py src/gold2/traffic_score.py \
  dags/lion_pipeline.py
git commit -m "refactor: lion을 silver1(정제)/silver2(인접그래프)/gold2(capacity+centrality)로 분리

dim_segment.parquet은 gold2가 silver1 산출물에 컬럼을 추가해 완성하는 구조로 변경
(DIM_SEGMENT_PATH가 이제 GOLD2_DIR를 가리킴). validate_dim_segment는 road_class/
is_routable을 검증하므로 매핑표 초안과 달리 gold2로 재분류."
```

---

### Task 8: tlc 도메인 — silver1.py 통합 + gold1.py/gold2.py 분리

(갱신: `feature/segment-spatial-weight`는 PR #58로 이미 `develop`에 머지됐고, 이 리팩토링
브랜치도 2026-08-20에 `origin/develop`을 merge하며 `src/mapping/segment_spatial_weight.py`를
이미 확보했다. 별도 머지 단계 불필요 — 아래는 이미 존재하는 파일 기준으로 진행한다.)

**Files:**
- Create: `src/tlc/silver1.py` (구 silver.py + transform.py 통합)
- Create: `src/tlc/gold1.py`
- Create: `src/tlc/gold2.py` (구 gold.py 대부분 + segment_spatial_weight.py 이관)
- Delete: `src/tlc/silver.py`, `src/tlc/transform.py`, `src/tlc/gold.py`,
  `src/mapping/segment_spatial_weight.py`
- Modify: `dags/tlc_pipeline.py`, `dags/tlc_daily.py`, `dags/tlc_gold_volume.py`

**Interfaces:**
- Produces: `src.tlc.silver1.{transform, build_silver, chunk_bronze_files}` (task 함수는
  기존 이름 유지) / `src.tlc.gold1.filter_weekday_and_known_zone` (신규) /
  `src.tlc.gold2.{build_dim_segment_tlc_volume, validate_dim_segment_tlc_volume,
  build_map_segment_spatial_weight, validate_map_segment_spatial_weight,
  DIM_SEGMENT_TLC_VOLUME_PATH}`

- [ ] **Step 1: silver1.py 생성 — silver.py + transform.py 통합**

`src/tlc/transform.py`(383줄) 전체(`SILVER_SCHEMA`(58-65), `SILVER_COLUMNS`(70-73),
`COLUMN_MAPPING`(86-133), `rename_columns`(140-171), `add_missing_columns`(178-206),
`cast_columns`(213-228), `select_columns`(235-244), `check_null`(251-306, **로그만
남기고 드롭 안 하는 정책 그대로 유지**), `transform`(313-356))과 `src/tlc/silver.py`(167줄)
전체(`TAXI_TYPE_PRIORITY`(30), `chunk_bronze_files`(34-59), `build_silver`(65-167))를
`src/tlc/silver1.py` 하나로 합친다. `union_all`(363-383행)은 호출부가 repo 전체에 없는
것으로 확인됐으니 **옮기지 않고 삭제**(죽은 코드).

- [ ] **Step 2: gold1.py 신규 작성 — 평일 필터 + zone_id notna**

`src/tlc/gold.py`(머지 후 버전)의 `collect_zone_hour_counts` 안에 있던 63행(평일 필터)과
79-84행(zone_id notna 드롭)을 분리한 새 함수로 작성:
```python
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, dayofweek

def filter_weekday_and_known_zone(df: DataFrame) -> DataFrame:
    # 원본 gold.py 63행 그대로: 평일(월~금) 필터
    df = df.filter(dayofweek(col("dropoff_datetime")).between(2, 6))
    # 원본 gold.py 79-84행 그대로: zone_id notna 드롭
    df = df.filter(col("dropoff_location_id").isNotNull())
    return df
```
정확한 컬럼명/조건식은 머지 후 `src/tlc/gold.py`의 실제 63행, 79-84행을 그대로 옮겨 적을 것.

- [ ] **Step 3: gold2.py 생성**

`src/tlc/gold.py`에서 `filter_weekday_and_known_zone`으로 옮긴 부분만 빼고 나머지
(`collect_zone_hour_counts`의 파일읽기/groupBy/toPandas 부분, `_expand_zone_to_segment_hour`,
`_normalize_tlc_volume`, `build_dim_segment_tlc_volume`, `validate_dim_segment_tlc_volume`,
`_neighbor_hop_distances`, `get_tlc_traffic_score_for_construction`, `DIM_SEGMENT_TLC_VOLUME_PATH`,
`HOURS`, `DEFAULT_HOPS`)를 전부 `gold2.py`로 옮긴다. `collect_zone_hour_counts`는 내부에서
`filter_weekday_and_known_zone(df)`를 호출하도록 수정(직접 인라인하던 필터 로직을 함수 호출로
교체). import 22-23행(`from src.lion.segment_adjacency import GRAPH_SEGMENT_ADJACENCY_PATH`,
`from src.silver2.zone_segment import MAP_ZONE_SEGMENT_PATH` — 후자는 Task 1에서 이미 고침)을
Task 7 결과에 맞게 `from src.lion.silver2 import GRAPH_SEGMENT_ADJACENCY_PATH`로 갱신한다.

이어서 (머지로 들어온) `src/mapping/segment_spatial_weight.py`의 `ingest_hotspot_grid`,
`_points_from_grid`, `_match_points_to_zone`, `_match_points_to_segment`,
`_aggregate_hotspot_counts`, `_compute_spatial_weight`, `build_map_segment_spatial_weight`,
`validate_map_segment_spatial_weight`를 같은 `gold2.py`에 이어 붙인다. 이 파일의 import 중
`from src.lion.silver import DIM_SEGMENT_PATH`를 `from src.lion.gold2 import DIM_SEGMENT_PATH`로,
`from src.mapping.zone_segment import ...`를 `from src.silver2.zone_segment import ...`로 갱신.

- [ ] **Step 4: 구 파일 삭제**

```bash
git rm src/tlc/silver.py src/tlc/transform.py src/tlc/gold.py src/mapping/segment_spatial_weight.py
```

- [ ] **Step 5: DAG 수정**

`dags/tlc_pipeline.py`, `dags/tlc_daily.py`: `from src.tlc.silver import build_silver,
chunk_bronze_files` → `from src.tlc.silver1 import build_silver, chunk_bronze_files`
(각 파일에서 이 import가 있는 라인 전부).

`dags/tlc_gold_volume.py:33-37`:
```python
# 변경 전
from src.tlc.gold import build_dim_segment_tlc_volume, collect_zone_hour_counts, validate_dim_segment_tlc_volume
# 변경 후
from src.tlc.gold2 import build_dim_segment_tlc_volume, collect_zone_hour_counts, validate_dim_segment_tlc_volume
```
같은 파일 19-24행 부근 docstring이 `data/silver/map_segment_spatial_weight.parquet`를
언급하는데, 이 산출물이 이제 `data/gold2/`로 가므로 docstring도 갱신한다.

- [ ] **Step 6: 테스트 이관 — tests/tlc/test_gold.py 분리**

`tests/tlc/test_gold.py`(574줄)를 아래 표에 따라 나눈다(기존 general-purpose 조사 결과
그대로):

| 테스트 | 원본 라인 | 이동 대상 |
|---|---|---|
| test_expand_zone_to_segment_hour_fills_missing_with_zero | 35-58 | `tests/tlc/test_gold2.py` |
| test_expand_zone_to_segment_hour_every_segment_has_24_hours | 61-67 | `test_gold2.py` |
| test_normalize_tlc_volume_percentile_rank | 70-84 | `test_gold2.py` |
| test_normalize_tlc_volume_keeps_original_columns | 87-96 | `test_gold2.py` |
| test_collect_zone_hour_counts_filters_weekday_and_counts | 99-135 | `test_gold1.py`(필터 부분 검증)+`test_gold2.py`(집계 부분 검증)로 쪼개서 재작성 |
| test_collect_zone_hour_counts_drops_null_zone_id | 138-193 | `tests/tlc/test_gold1.py` |
| test_collect_zone_hour_counts_reads_multiple_taxi_types | 196-220 | `test_gold2.py` |
| test_neighbor_hop_distances_* (3개) | 231-263 | `test_gold2.py` |
| test_build_and_validate_dim_segment_tlc_volume | 266-306 | `test_gold2.py` |
| test_build_dim_segment_tlc_volume_logs_unmatched_zone_trips | 309-352 | `test_gold2.py` |
| test_validate_dim_segment_tlc_volume_rejects_duplicate_rows | 355-372 | `test_gold2.py` |
| test_validate_dim_segment_tlc_volume_rejects_zero_matching_segments | 375-408 | `test_gold2.py` |
| test_get_tlc_traffic_score_for_construction_* (4개) | 433-477 | `test_gold2.py` |
| test_build_then_query_full_pipeline_seam | 480-574 | `test_gold2.py`(gold1 필터를 gold2 진입 전 단계로 호출하도록 재작성) |

각 테스트의 `from src.tlc.gold import ...`(7행, 223-228행)를 이동 대상에 맞게
`from src.tlc.gold1 import filter_weekday_and_known_zone` /
`from src.tlc.gold2 import (...)`로 나눠 쓴다. `tests/mapping/test_segment_spatial_weight.py`도
`tests/tlc/test_gold2.py`에 흡수(import 경로를 `src.tlc.gold2`로 변경)한다.

- [ ] **Step 7: 테스트 실행**

```bash
pytest tests/tlc/ -v
```
Expected: 머지 전과 동일한 개수의 테스트가 전부 PASS(파일만 나뉘고 로직은 무변경이므로
테스트 결과가 바뀌면 안 됨 — `test_collect_zone_hour_counts_filters_weekday_and_counts`처럼
재작성한 테스트만 새로 통과 여부 확인).

- [ ] **Step 8: 커밋**

```bash
git add src/tlc src/common/config.py tests/tlc dags/tlc_pipeline.py dags/tlc_daily.py \
  dags/tlc_gold_volume.py
git rm tests/mapping/test_segment_spatial_weight.py 2>/dev/null || true
git commit -m "refactor: tlc를 silver1/gold1(평일+notna필터)/gold2(집계+spatial_weight+정규화)로 분리

feature/segment-spatial-weight를 머지하고 segment_spatial_weight.py를 mapping/(Silver2)이
아니라 tlc/gold2.py로 재분류(거리역가중+라플라스 스무딩으로 새 수치를 만드는 연산이라
Gold2 정의에 부합, tlc 전용 소유)."
```

---

### Task 9: taxi_zone 도메인 — silver1.py 신설 + bronze.py 축소

**Files:**
- Modify: `src/taxi_zone/bronze.py` (validate 함수 축소)
- Create: `src/taxi_zone/silver1.py`
- Modify: `src/silver2/zone_segment.py` (taxi_zone Bronze 대신 Silver1을 읽도록)
- Modify: `dags/taxi_zone_pipeline.py:24-29,51-57,65-71` + silver1 task 추가

**Interfaces:**
- Produces: `src.taxi_zone.silver1.{validate_taxi_zone_lookup, validate_taxi_zone_shapefile}`
  (기존 함수를 그대로 옮김, bronze.py에는 존재여부 확인만 남는 새 `validate_output` 신규 작성)

- [ ] **Step 1: silver1.py 생성**

`src/taxi_zone/bronze.py`(156줄)의 `validate_taxi_zone_lookup`(91-112행),
`validate_taxi_zone_shapefile`(115-135행)을 그대로 `src/taxi_zone/silver1.py`로 옮긴다.
`get_manhattan_zone_ids`(138-149행)는 repo 전체에서 정의부와 `__main__` 블록 자체 호출
외 호출처가 0건으로 확인된 죽은 코드다 — **옮기지 않고 삭제**한다(다른 곳에서 안 쓰는
것으로 확인됐으므로 완전히 지운다).

- [ ] **Step 2: bronze.py 축소**

`src/taxi_zone/bronze.py`에는 `ingest_taxi_zone_lookup`(35-59), `ingest_taxi_zone_shapefile`
(62-88)만 남기고, `validate_taxi_zone_lookup`/`validate_taxi_zone_shapefile`/
`get_manhattan_zone_ids`를 삭제한다. 대신 파일 존재 여부만 확인하는 최소 검증 함수를
신규 작성:
```python
def validate_bronze_output(lookup_path: Path, shapefile_dir: Path) -> None:
    if not lookup_path.exists():
        raise FileNotFoundError(f"taxi_zone lookup bronze 파일이 없다: {lookup_path}")
    if not shapefile_dir.exists() or not any(shapefile_dir.iterdir()):
        raise FileNotFoundError(f"taxi_zone shapefile bronze가 없다: {shapefile_dir}")
```

- [ ] **Step 3: zone_segment.py 참조 변경**

`src/silver2/zone_segment.py:39`(구 `TAXI_ZONE_SHAPEFILE = BRONZE_DIR/"taxi_zone"/...`):
```python
# 변경 전
TAXI_ZONE_SHAPEFILE = BRONZE_DIR / "taxi_zone" / ...
# 변경 후
TAXI_ZONE_SHAPEFILE = SILVER1_DIR / "taxi_zone" / ...
```
단, taxi_zone silver1은 shapefile 원본 자체를 복사/재생성하지 않고 **검증만** 하므로,
`validate_taxi_zone_shapefile`이 검증에 성공한 Bronze shapefile 경로를 silver1 산출물
경로에 그대로 링크/복사해두는 작은 단계가 `silver1.py`의 `build()`에 필요하다(신규 작성):
```python
def build() -> Path:
    from src.common.config import BRONZE_DIR, SILVER1_DIR
    lookup_path = BRONZE_DIR / "taxi_zone" / "taxi_zone_lookup.csv"  # 원본 bronze.py 경로 규칙 확인 후 정확히 맞출 것
    shapefile_dir = BRONZE_DIR / "taxi_zone" / "shapefile"
    validate_taxi_zone_lookup(lookup_path)
    validate_taxi_zone_shapefile(shapefile_dir)
    out_dir = SILVER1_DIR / "taxi_zone"
    out_dir.mkdir(parents=True, exist_ok=True)
    shutil.copytree(shapefile_dir, out_dir / "shapefile", dirs_exist_ok=True)
    shutil.copy(lookup_path, out_dir / "taxi_zone_lookup.csv")
    return out_dir
```
(정확한 원본 파일명 규칙은 `src/taxi_zone/bronze.py`의 `ingest_taxi_zone_lookup`/
`ingest_taxi_zone_shapefile` 실제 저장 경로를 확인하고 맞출 것.)

- [ ] **Step 4: DAG 수정**

`dags/taxi_zone_pipeline.py:24-29`:
```python
# 변경 전
from src.taxi_zone.bronze import (
    ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile,
    validate_taxi_zone_lookup, validate_taxi_zone_shapefile,
)
# 변경 후
from src.taxi_zone.bronze import ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile
from src.taxi_zone.silver1 import build as build_taxi_zone_silver1
```
`validate_taxi_zone_lookup`/`validate_taxi_zone_shapefile` task(51-57행, 65-71행)를
`build_taxi_zone_silver1` 호출 하나로 교체하고, 두 체인(`ingest_lookup>>validate_lookup`,
`ingest_shapefile>>validate_shapefile`)을 `[ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile]
>> build_taxi_zone_silver1`로 합친다(silver1이 lookup+shapefile 둘 다 검증하므로).
기존 `outlets=[Asset("taxi_zone")]`(원본 62행, shapefile ingest에 달려있던 것)는
`build_taxi_zone_silver1` task로 옮긴다(silver1이 완성돼야 `map_zone_segment`가 쓸 수
있으므로 Asset은 여기 붙는 게 맞다).

- [ ] **Step 5: smoke import + 테스트**

```bash
python -c "
from src.taxi_zone.bronze import ingest_taxi_zone_lookup, ingest_taxi_zone_shapefile, validate_bronze_output
from src.taxi_zone.silver1 import build, validate_taxi_zone_lookup, validate_taxi_zone_shapefile
print('OK')
"
grep -rn "get_manhattan_zone_ids" --include="*.py" . | grep -v "\.git/"
```
Expected: `OK`, grep 결과 0건(삭제 확인).

```bash
pytest tests/ -q
```

- [ ] **Step 6: 커밋**

```bash
git add src/taxi_zone src/silver2/zone_segment.py dags/taxi_zone_pipeline.py
git commit -m "refactor: taxi_zone에 silver1(검증 이관) 신설, bronze는 존재확인만

get_manhattan_zone_ids()는 repo 전체에서 미사용 확인되어 삭제."
```

---

### Task 10: 최종 정리 — 미사용 경로 상수 제거 + 전체 회귀 검증

**Files:**
- Modify: `src/common/config.py` (`SILVER_DIR`, `GOLD_DIR` 참조가 없으면 제거)

**Interfaces:**
- Consumes: Task 1-9의 모든 산출물
- Produces: 없음(정리 작업)

- [ ] **Step 1: 잔여 구경로 상수 참조 확인**

```bash
grep -rn "\bSILVER_DIR\b\|\bGOLD_DIR\b" --include="*.py" src/ dags/ | grep -v "src/common/config.py"
```
결과가 있으면 그 파일이 아직 이번 리팩토링에서 안 옮겨진 것이니, 어느 도메인 소속인지
확인하고 Task 2-9 중 빠진 게 있는지 되짚는다. 결과가 0건이어야 다음 단계로 진행 가능.

- [ ] **Step 2: config.py에서 SILVER_DIR/GOLD_DIR 제거**

Step 1에서 0건 확인되면 `src/common/config.py`의 `SILVER_DIR = DATA_DIR / "silver"`,
`GOLD_DIR = DATA_DIR / "gold"` 두 줄을 삭제한다.

- [ ] **Step 3: 잔여 mapping/scoring 참조 최종 확인**

```bash
grep -rn "src\.mapping\|src\.scoring" --include="*.py" --include="*.yml" . | grep -v "\.git/"
ls src/mapping src/scoring 2>&1  # "No such file or directory"가 나와야 정상(디렉터리 삭제 확인)
```

- [ ] **Step 4: 전체 테스트 스위트 + 전체 도메인 smoke import**

```bash
pytest tests/ -v
python -c "
import src.construction.silver1, src.construction.gold1
import src.construction_stipulations.silver1
import src.silver2.construction_work_hours_join
import src.road_closures.silver1
import src.silver2.road_closure_construction_conflation
import src.event.silver1, src.event.gold1
import src.ticketmaster.silver1, src.ticketmaster.gold1, src.ticketmaster.silver2
import src.lion.silver1, src.lion.silver2, src.lion.gold2
import src.tlc.silver1, src.tlc.gold1, src.tlc.gold2
import src.taxi_zone.bronze, src.taxi_zone.silver1
import src.silver2.event_lion, src.silver2.road_closure_segment, src.silver2.road_control_segment
import src.silver2.ticketmaster_lion, src.silver2.zone_segment
import src.gold2.closure_penalty, src.gold2.event_boost, src.gold2.traffic_score
import src.serving.api
print('전체 OK')
"
```
Expected: 테스트 전부 PASS, `전체 OK` 출력.

- [ ] **Step 5: 커밋**

```bash
git add src/common/config.py
git commit -m "refactor: 미사용 SILVER_DIR/GOLD_DIR 경로 상수 제거 (전 도메인 마이그레이션 완료)"
```

---

## Self-Review 메모

- **Spec 커버리지**: 설계 문서의 5단계 레이어 정의, 폴더 구조, 8개 도메인 매핑표, 크로스도메인
  참조 규칙 전부 Task 1-10에 반영됨. "제외" 항목(venue.py 연결, DAG task 이름 세부, tlc 결측치
  정책)은 각 태스크에서 명시적으로 "이번 범위 밖"이라고 표시함.
- **매핑표 대비 수정한 지점** (조사 단계에서 발견, 실행 전 확정): (1) `lion/validate_dim_segment`는
  실제로 gold2 컬럼을 검증하므로 gold1이 아니라 gold2 소속으로 정정, (2)
  `construction_stipulations`의 `build`/`validate`/`main` 전체가 `_merge_work_hours`와 함께
  묶여서 silver2로 이동, (3) `road_closures`가 정말 construction Silver1만 읽어도 되는지는
  Task 4 Step 1에서 구현 시점에 재확인하도록 명시, (4) `event`/`ticketmaster`의 gold1이 쓸
  컬럼(`event_borough` 등) 보존을 명시적 단계로 추가, (5) `segment_spatial_weight.py`는 이번
  조사에서 다시 확인해도 tlc 전용 gold2 소속이 맞음(공용 아님).
- **타입/이름 일관성**: `build`/`validate_output`/`main` 함수명 컨벤션은 전 도메인에서 동일하게
  유지(Task 2-9 전부 이 이름을 그대로 씀). `DIM_SEGMENT_PATH`, `GRAPH_SEGMENT_ADJACENCY_PATH`,
  `MAP_ZONE_SEGMENT_PATH` 등 경로 상수는 Task 7에서 한 번 정의되면 이후 모든 참조가 같은
  이름·같은 모듈(`src.lion.gold2`, `src.lion.silver2`)을 가리키도록 Step별로 명시함.
- **잔여 미결정**: taxi_zone silver1의 `build()`(Task 9 Step 3)가 원본 bronze.py의 정확한
  파일명 규칙을 아직 안 읽어보고 작성한 예시 코드다 — 구현 시 반드시 원본을 먼저 읽고
  경로를 맞출 것(Task 9 Step 3 안에 이미 이 주의사항을 남겨둠).

---

## 실행 순서 요약

Task 1(공용 폴더) → Task 2(construction) → Task 3(construction_stipulations) →
Task 4(road_closures) → Task 5(event) → Task 6(ticketmaster) → Task 7(lion) →
Task 8(tlc, segment-spatial-weight 머지 포함) → Task 9(taxi_zone) → Task 10(최종 정리)
