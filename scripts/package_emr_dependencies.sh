#!/usr/bin/env bash
# EMR Serverless Spark job이 쓰는 서드파티 파이썬 의존성을 패키징해서 S3에 올린다.
# 의존성(requirements.txt)이 바뀔 때마다 다시 실행해야 한다.
#
# 사용법: ./scripts/package_emr_dependencies.sh
#
# .github/workflows/build-push-ecr.yml의 deploy 작업이 매 nav 배포마다
# EC2에서 이 스크립트를 자동 실행한다(Dockerfile 내용이 안 바뀌면 대부분
# 레이어 캐시로 스킵되어 대체로 빠름) - 수동으로 직접 돌릴 때도 동일하게
# 동작한다.

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
