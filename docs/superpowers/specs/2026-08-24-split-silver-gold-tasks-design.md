# segment_time_pipeline Silver/Gold Task 분리 Design

**Goal:** `segment_time_pipeline`의 `submit_nav_time_job` task 하나에 뭉쳐있는 Silver1~Gold2(정제→LION 조인→필터→버킷 평균+RDS upsert)를 Silver 단계와 Gold 단계, 두 개의 Airflow task로 나눠서, EMR job이 실패했을 때 Airflow 화면에서 바로 "Silver에서 죽었는지 Gold에서 죽었는지" 알 수 있게 한다.

**Architecture:** 현재 `spark_jobs/nav_time_job.py` 하나(Bronze parquet 읽어서 Silver1→Silver2→Gold1→Gold2까지 한 Spark 세션 안에서 처리)를 두 개의 EMR job 엔트리포인트로 쪼갠다 — `nav_time_silver_job.py`(Bronze→Silver1→Silver2, 결과를 S3에 parquet로 씀)와 `nav_time_gold_job.py`(그 Silver2 parquet를 읽어서 Gold1→Gold2→RDS upsert+S3 스냅샷). DAG도 `submit_nav_time_job` 하나였던 걸 `submit_silver_job`→`submit_gold_job` 두 task로 나눈다.

**Tech Stack:** 기존 `src/common/emr_serverless.py`의 `run_spark_job`/`read_json_result`, 기존 `EMR_JOBS_DIR`(EMR 임시 산출물 저장 위치) 재사용. 새 인프라 없음.

## Global Constraints

- Bronze 쪽(`collect_bronze`, `validate_bronze`)은 이번 작업 범위 밖 — 손대지 않는다.
- Silver/Gold 각 단계에서 실제로 하는 일 자체는 바꾸지 않는다(순수 리팩터 — 함수 로직 이동만, 계산 결과가 달라지면 안 됨).
- 중간 산출물(Silver2 결과)은 도메인 데이터 폴더(`SILVER2_DIR` 등)가 아니라 `EMR_JOBS_DIR/outputs/` 밑에 run_id로 구분해서 저장한다 — 이미 EMR job의 JSON 결과물도 같은 위치에 저장하는 기존 관례를 따름(전체 이력을 남기는 도메인 데이터가 아니라, 이번 실행 하나를 위한 일회성 중간 산출물이라서).
- 실패 알림은 새로 만들지 않는다 — 두 task 모두 일반 `@task`(short_circuit 아님)라서 EMR job이 진짜로 실패하면 예외가 그대로 올라가 Airflow task 자체가 실패 처리되고, DAG의 기존 `on_failure_callback=notify_slack_failure`가 자동으로 어느 task인지 포함해서 알림을 보낸다.
- Gold job도 `dim_segment_path`를 자기 인자로 따로 받는다 — Silver 단계(LION 매핑)뿐 아니라 Gold2(`compute_time_seconds`)도 `length_ft` 조회에 필요하기 때문(기존 `nav_time_job.py`도 이미 같은 `dim_segment_df`를 두 단계 모두에 재사용하고 있었음).

## 새 파일

- `spark_jobs/nav_time_silver_job.py` — 인자: `--speed-bronze-path`, `--dim-segment-path`, `--silver2-output`, `--output-s3`. `clean_speed_silver1` → `build_segment_speed_silver2` 실행 후 결과를 `--silver2-output` 경로에 parquet로 저장, `--output-s3`엔 `{"silver2_path": ..., "row_count": N}` JSON을 남긴다.
- `spark_jobs/nav_time_gold_job.py` — 인자: `--silver2-path`, `--dim-segment-path`, `--serving-table`, `--output-s3`. `filter_valid_speed` → `compute_time_seconds` → `to_serving_items` → `write_to_rds` 실행, `--output-s3`엔 기존과 동일하게 `{"count": N}`을 남긴다.

## 삭제되는 파일

- `spark_jobs/nav_time_job.py` — 위 두 파일로 대체되어 더 이상 쓰이지 않음.

## DAG 변경 (`dags/segment_time_pipeline.py`)

`submit_nav_time_job` 하나를 아래 두 task로 교체한다:

```python
@task
def submit_silver_job(speed_bronze_path: str) -> dict:
    run_id = uuid.uuid4().hex
    silver2_path = EMR_JOBS_DIR / "outputs" / f"nav_time_silver2_{run_id}.parquet"
    output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_silver_{run_id}.json"

    run_spark_job(
        job_name=f"nav-time-silver-{run_id}",
        entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_silver_job.py",
        entry_point_args=[
            "--speed-bronze-path", speed_bronze_path,
            "--dim-segment-path", str(DIM_SEGMENT_PATH),
            "--silver2-output", str(silver2_path),
            "--output-s3", str(output_s3),
        ],
    )

    result = read_json_result(str(output_s3))
    return {"silver2_path": str(silver2_path), **result}


@task
def submit_gold_job(silver_result: dict) -> dict:
    run_id = uuid.uuid4().hex
    output_s3 = EMR_JOBS_DIR / "outputs" / f"nav_time_gold_{run_id}.json"

    run_spark_job(
        job_name=f"nav-time-gold-{run_id}",
        entry_point_script=PROJECT_ROOT / "spark_jobs" / "nav_time_gold_job.py",
        entry_point_args=[
            "--silver2-path", silver_result["silver2_path"],
            "--dim-segment-path", str(DIM_SEGMENT_PATH),
            "--serving-table", SERVING_TABLE_TYPE1,
            "--output-s3", str(output_s3),
        ],
    )

    return read_json_result(str(output_s3))
```

배선(기존 `submit_result = submit_nav_time_job(bronze_path)` 자리를 대체):

```python
silver_result = submit_silver_job(bronze_path)
silver_result.set_upstream(dim_segment_ready)
silver_result.set_upstream(bronze_valid)

gold_result = submit_gold_job(silver_result)
```

`check_dim_segment_exists`/`validate_bronze` 게이트는 그대로 `silver_result`(옛 `submit_result`가 있던 자리) 앞에 걸린다 — Gold job은 Silver job의 성공 여부에 자연히 종속되므로(입력값을 Silver job 결과에서 받음) 별도 게이트가 필요 없다.

## 알려진 리스크 / 후속 과제 (이 설계 범위 밖)

- Silver job은 성공하고 Gold job이 실패하면, `EMR_JOBS_DIR/outputs/`에 안 쓰이는 Silver2 parquet가 하나 남는다. 다음 사이클은 새 Bronze 파일부터 처음부터 다시 시작하므로 데이터 유실/오염은 없고, S3 비용도 무시할 수준(브레인스토밍 중 확인) — 별도 정리(TTL/수명주기 정책)는 이번 범위 밖.
