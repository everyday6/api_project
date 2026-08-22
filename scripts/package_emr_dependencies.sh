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
# Dockerfile에 USER 지정이 없어 컨테이너가 root로 돈다 — --user 없이
# 돌리면 /output에 나온 tarball이 root 소유가 되어, root가 아닌 유저로
# 도는 CI 러너(또는 로컬 비-root 유저)가 뒤이어 aws s3 cp에서 "File/
# Directory is not readable"로 못 읽는다. 호스트 UID:GID로 맞춰서 실행.
docker run --rm --user "$(id -u):$(id -g)" -v /tmp/emr-python-env-output:/output emr-python-env

# TODO(진단용, 원인 확인되면 제거): --user를 넘겼는데도 aws s3 cp가
# "File/Directory is not readable"를 내서, 실제 소유자/권한을 찍어본다.
ls -la /tmp/emr-python-env-output/

DEST="s3://${S3_BUCKET_DATA}/emr-jobs/python-env/pyspark_deps.tar.gz"
echo "S3 업로드: ${DEST}"
aws s3 cp /tmp/emr-python-env-output/pyspark_deps.tar.gz "${DEST}"

echo "완료: ${DEST}"
