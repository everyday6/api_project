# three-idiots
현대자동차 소프티어 8기 DE 5조 팀프로젝트 레포

## S3 staging Lifecycle

파이프라인 실패로 남은 임시 결과만 7일 뒤 삭제한다. Bronze와 운영
Silver/Gold 데이터는 이 규칙의 대상이 아니다.

- `silver1/lion/_staging/`
- `silver2/_staging/map_zone_segment/`
- `gold2/tlc/type3_zone_daily/_staging/`

설정 파일은 `config/s3-staging-lifecycle.json`이다. AWS에 적용하기 전에 기존
버킷 Lifecycle 규칙을 먼저 확인하고, 기존 규칙이 있다면 이 세 규칙을 병합해야
한다. `put-bucket-lifecycle-configuration`은 기존 설정 전체를 교체한다.
