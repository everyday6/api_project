#!/bin/bash
# EC2 호스트 자체의 CPU/메모리/디스크를 CloudWatch로 보내는 CloudWatch Agent를
# EC2에 직접 설치한다(컨테이너 아님).
#
# 컨테이너로 띄워서 호스트를 보게 하는 것도 가능은 한데, HOST_PROC/HOST_SYS
# 같은 경로 오버라이드와 여러 host mount가 필요해 이 EC2(ECS 아닌 순수 EC2)
# 용도로는 불필요하게 불안정하다 - AWS가 순수 EC2엔 이 방식(호스트에 직접
# 설치, systemd 서비스로 상주)을 공식 권장한다.
#
# 이 EC2는 Graviton(arm64)이라 arm64 패키지를 받는다(.github/workflows/
# build-push-ecr.yml 상단 주석 참고 - 다른 이유로 이미 확인된 사실).
#
# 사전 조건: EC2 인스턴스 역할에 cloudwatch:PutMetricData 권한이 있어야 한다
# (지금 붙어있는 CloudWatchReadOnlyAccess는 읽기 전용이라 별도로 추가
# 필요 - AWS 콘솔에서 사람이 직접 해야 함, 이 스크립트는 그 권한이 있다고
# 가정하고 에이전트만 설치한다).
set -euo pipefail

CONFIG_PATH="/opt/aws/amazon-cloudwatch-agent/etc/amazon-cloudwatch-agent.json"

echo "[install_cloudwatch_agent] .deb 패키지 다운로드 중..."
curl -fsSL -o /tmp/amazon-cloudwatch-agent.deb \
  https://amazoncloudwatch-agent.s3.amazonaws.com/ubuntu/arm64/latest/amazon-cloudwatch-agent.deb

echo "[install_cloudwatch_agent] 설치 중..."
sudo dpkg -i -E /tmp/amazon-cloudwatch-agent.deb

echo "[install_cloudwatch_agent] 설정 파일 작성 중..."
sudo mkdir -p "$(dirname "$CONFIG_PATH")"
sudo tee "$CONFIG_PATH" > /dev/null <<'EOF'
{
  "agent": {
    "metrics_collection_interval": 60
  },
  "metrics": {
    "namespace": "CWAgent",
    "append_dimensions": {
      "InstanceId": "${aws:InstanceId}"
    },
    "metrics_collected": {
      "cpu": {
        "measurement": ["cpu_usage_idle", "cpu_usage_user", "cpu_usage_system"],
        "metrics_collection_interval": 60,
        "totalcpu": true
      },
      "mem": {
        "measurement": ["mem_used_percent"],
        "metrics_collection_interval": 60
      },
      "disk": {
        "measurement": ["disk_used_percent"],
        "metrics_collection_interval": 60,
        "resources": ["/"]
      }
    }
  }
}
EOF

echo "[install_cloudwatch_agent] 에이전트 시작 중..."
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl \
  -a fetch-config -m ec2 -c "file:${CONFIG_PATH}" -s

echo "[install_cloudwatch_agent] 완료. 상태 확인:"
sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a status
