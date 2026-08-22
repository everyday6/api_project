#!/usr/bin/env bash
# EMR Serverless Spark job이 쓰는 서드파티 파이썬 의존성을 패키징해서 S3에 올린다.
# 의존성(requirements.txt)이 바뀔 때마다 다시 실행해야 한다.
#
# 사용법: ./scripts/package_emr_dependencies.sh
#
# 주의: 이 스크립트는 실제 AWS 계정의 S3 버킷에 파일을 업로드한다 — 팀이
# 의도적으로 실행할 때만 돌려야 한다(CI에서 자동 실행하려면 별도 검토 필요).

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
