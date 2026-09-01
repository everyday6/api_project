"""LION Silver1 dim_segment 검증에 쓰는 임계값 상수.

이 모듈은 원래 speed/tlc처럼 GX(Great Expectations) 객체 목록을 정의했지만,
lion에는 그 목록을 실제로 GX 엔진에 돌리는 실행부가 없었다(정의만 있고
호출되지 않는 죽은 코드였다). 그래서 GX 객체는 제거하고, 두 곳
- src/lion/silver1.py의 mark_suspect_rows() : 행 단위 `is_suspect` 표시
- src/lion/silver1.py의 validate_dim_segment_base() : suspect 비율 publish 게이트
- 이 함께 참조하는 임계값 상수만 남긴다.

왜 lion만 GX log-only 레이어를 두지 않는가:
- LION은 분기 1회 갱신되는 저빈도 릴리즈라 사람이 결과를 들여다볼 가능성이
  매우 높다. speed(30분마다)처럼 매 사이클을 사람이 못 보는 고빈도
  파이프라인과 달리 "부드러운 경고"(log-only Slack)의 가치가 낮다.
- 값 수준 이상치는 mark_suspect_rows()가 `is_suspect`로 표시하고, 그 비율이
  임계치를 넘으면 validate_dim_segment_base()의 assert가 Airflow task 자체를
  실패시켜 기존 on_failure_callback 알림 경로를 그대로 탄다 - 알림 체계에
  구멍이 생기지 않는다.
- 구조적 critical 검증(segment_id 유일성, borough_code 유효성, 행 수 범위)은
  이미 validate_dim_segment_base()가 raw assert로 수행한다(운영 검증된 로직).
"""

# POSTED_SPEED(제한속도)는 미표기 segment가 실측 기준 약 32%라 흔한
# 결측이다(src/lion/silver1.py 참고) - null 자체는 정상이므로 검사
# 대상에서 제외하고, 값이 있을 때만 현실적인 범위인지 본다. NYC 도로의
# 실제 제한속도 분포(대부분 25mph, 고속도로 50~65mph)보다 넉넉하게 잡아
# 정상 범위의 도로를 오탐하지 않게 한다.
SPEED_LIMIT_MIN_MPH = 1
SPEED_LIMIT_MAX_MPH = 80
