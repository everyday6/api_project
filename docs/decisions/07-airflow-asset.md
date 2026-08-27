# 07. Publish 완료 Asset 기반 DAG 의존성 관리

## 문제

- 서로 다른 DAG와 주기로 갱신되는 원천 데이터
- 데이터 준비 완료 시점에 맞춘 후속 파이프라인 실행 필요
- Upstream의 변경을 사용하는 여러 Downstream DAG

## 원인

- DAG 실행 완료와 데이터 준비 완료를 동일하게 판단
- 데이터가 준비되기 전에 다음 DAG가 실행될 가능성
- Upstream DAG가 Downstream DAG를 직접 호출하는 구조
- DAG 간 강한 의존성

## 대안

| 방법 | 장점 | 단점 |
| --- | --- | --- |
| Downstream DAG 직접 실행 | 단순한 구현과 실행 흐름 | DAG 간 결합 및 데이터 준비 상태 확인 어려움 |
| Sensor로 완료 상태 확인 | 기존 DAG 구조 유지 | 특정 DAG·Task 의존 및 지속적인 폴링 |
| Publish 완료 Asset 발행 | 데이터 준비 상태 중심의 실행 | Asset 발행 시점과 소유권 관리 필요 |

## 결과

- Publish 성공 후에만 Asset 발행
- 필요한 Asset 변경 시 Downstream DAG 실행
- 최신 운영 데이터 기반의 후속 처리
- DAG 간 직접 의존 제거
