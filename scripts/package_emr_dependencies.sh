#!/usr/bin/env bash
# EMR Serverless Spark job이 쓰는 서드파티 파이썬 의존성을 패키징해서 S3에 올린다.
# docker/emr-python-env/Dockerfile 내용이 바뀔 때마다 다시 실행해야 한다.
#
# 사용법: ./scripts/package_emr_dependencies.sh
#
# .github/workflows/build-push-ecr.yml의 build-emr-python-env 잡이 amd64
# GitHub Actions 러너(ubuntu-latest)에서 이 스크립트를 자동 실행한다
# (docker/emr-python-env/Dockerfile이 변경된 push에서만). 예전엔 deploy
# 단계에서 (arm64) EC2가 직접 실행했는데, Dockerfile이 amd64로 고정돼
# 있어서 arm64 EC2에서는 QEMU 에뮬레이션 위에서 dnf가 죽었다(exit 255).
# 로컬에서 수동으로 돌릴 때도 동일하게 동작한다 — 단, amd64/Linux
# 호스트여야 에뮬레이션 없이 빌드된다.

set -euo pipefail

cd "$(dirname "$0")/.."

if [ -z "${S3_BUCKET_DATA:-}" ]; then
  echo "S3_BUCKET_DATA 환경변수가 필요합니다 (.env를 source 하거나 직접 export)" >&2
  exit 1
fi

echo "EMR Python 환경 이미지 빌드 중..."
docker build -t emr-python-env -f docker/emr-python-env/Dockerfile .

echo "빌드된 tarball 추출 중..."
mkdir -p /tmp/emr-python-env-output
docker run --rm -v /tmp/emr-python-env-output:/output emr-python-env

DEST="s3://${S3_BUCKET_DATA}/emr-jobs/python-env/pyspark_deps.tar.gz"
echo "S3 업로드: ${DEST}"
aws s3 cp /tmp/emr-python-env-output/pyspark_deps.tar.gz "${DEST}"

echo "완료: ${DEST}"
