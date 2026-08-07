#!/usr/bin/env bash
# 반복 배포 전용: ECR 로그인 -> build(arm64) -> push -> Lambda 코드 업데이트.
# 1회성 작업(ECR 리포 생성, Lambda 함수 생성, SQS 트리거 연결)은 콘솔에서 한다.
set -euo pipefail

# 로컬: 배포 전용 최소권한 유저 프로필 (default 프로필은 다른 계정이다).
# CI(GitHub Actions): 프로필 없이 env 자격증명(AWS_ACCESS_KEY_ID 등)을 쓴다.
if [ -z "${CI:-}" ]; then
  export AWS_PROFILE="${AWS_PROFILE:-kbap-lambda-deployer}"
fi
REGION=ap-northeast-2
ACCOUNT=118178010621
REPO=kbap/langchain
FUNCTION=kbap-generate-content
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
IMAGE="$REGISTRY/$REPO:latest"

aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

# provenance 매니페스트가 붙으면 Lambda가 이미지를 거부한다(단일 매니페스트만 허용)
docker build --platform linux/arm64 --provenance=false -t "$IMAGE" .
docker push "$IMAGE"

# "함수 없음"과 권한 에러를 구분한다 — AccessDenied 를 삼키면 배포가 조용히 누락된다.
if err=$(aws lambda get-function --function-name "$FUNCTION" --region "$REGION" 2>&1 >/dev/null); then
  aws lambda update-function-code --function-name "$FUNCTION" \
    --image-uri "$IMAGE" --region "$REGION" --no-cli-pager >/dev/null
  echo "배포 완료: $FUNCTION <- $IMAGE"
elif grep -q ResourceNotFound <<<"$err"; then
  echo "push 완료: $IMAGE"
  echo "Lambda 함수($FUNCTION)가 아직 없다 — 콘솔에서 이 이미지로 생성한 뒤 다시 실행하면 코드 업데이트까지 수행된다."
else
  echo "$err" >&2
  exit 1
fi
