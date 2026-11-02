#!/usr/bin/env bash
# 반복 배포 전용: ECR 로그인 -> build(arm64) -> push -> dev·prod Lambda 코드 업데이트.
#   ./deploy.sh             dev·prod 둘 다 (CI 가 main 푸시 때 실행)
#   ./deploy.sh --dev-only  dev 함수(kbap-generate-content)만 — 기능 브랜치를 prod 에 내보내지 않고
#                           dev 에서 확인할 때(KB-913). :latest 도 옮기지 않는다.
# 1회성 작업(ECR 리포 생성, Lambda 함수 생성, SQS 트리거 연결)은 kbap-infra 프로필 CLI 로 한다 —
# 함수 생성 시 아키텍처 arm64 를 명시할 것(기본값 x86_64 면 Runtime.InvalidEntrypoint).
set -euo pipefail

DEV_ONLY=false
case "${1:-}" in
  --dev-only) DEV_ONLY=true ;;
  "") ;;
  *) echo "사용법: $0 [--dev-only]" >&2; exit 2 ;;
esac
DEV_FUNCTION=kbap-generate-content
# 수동 실행(workflow_dispatch)은 어느 브랜치에서든 돌 수 있으므로 dev 전용으로만 허용한다.
if [ "${GITHUB_EVENT_NAME:-}" = workflow_dispatch ] && ! $DEV_ONLY; then
  echo "workflow_dispatch 는 --dev-only 로만 실행한다 — prod 는 main 푸시 경로" >&2
  exit 1
fi

# 로컬: 배포 전용 최소권한 유저 프로필 (default 프로필은 다른 계정이다).
# CI(GitHub Actions): 프로필 없이 env 자격증명(AWS_ACCESS_KEY_ID 등)을 쓴다.
if [ -z "${CI:-}" ]; then
  export AWS_PROFILE="${AWS_PROFILE:-kbap-lambda-deployer}"
fi
REGION=ap-northeast-2
ACCOUNT=118178010621
REPO=kbap/langchain
# dev·prod 는 같은 이미지를 쓰고 Lambda 환경변수(KBAP_API_BASE_URL·KBAP_API_TOKEN)만 다르다.
# prod 큐 트리거는 2026-09-14 연결됨 — main 푸시가 곧 prod 배포다. prod 만 가르려면 FUNCTIONS 를 나눌 것.
FUNCTIONS=(kbap-generate-content kbap-generate-content-prod)
if $DEV_ONLY; then
  FUNCTIONS=("$DEV_FUNCTION") # 함수명 하드코딩 — prod 함수는 이 모드에서 절대 갱신하지 않는다
fi
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
# 태그 전략: 커밋 SHA(불변)로 함수를 갱신해 "어느 커밋이 떠 있나"를 함수에서 읽고,
# 롤백은 이전 SHA 로 update-function-code 한 번. latest 는 로컬 RIE·수동 생성용 이동 포인터.
TAG="$(git rev-parse --short=12 HEAD)$(git diff --quiet HEAD || echo -dirty)"
IMAGE="$REGISTRY/$REPO:$TAG"
LATEST="$REGISTRY/$REPO:latest"

echo "대상 함수: ${FUNCTIONS[*]} (프로필 ${AWS_PROFILE:-env 자격증명}, 태그 $TAG)"
# 어느 IAM 주체의 키로 도는지 — 권한 오류(예: ECR manifest HEAD 403) 원인을 바로 짚기 위해. ARN 은 비밀이 아니다.
echo "AWS 주체: $(aws sts get-caller-identity --query Arn --output text)"

aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

# provenance 매니페스트가 붙으면 Lambda가 이미지를 거부한다(단일 매니페스트만 허용)
docker build --platform linux/arm64 --provenance=false --build-arg KBAP_IMAGE_TAG="$TAG" -t "$IMAGE" -t "$LATEST" .
docker push "$IMAGE"
# dev-only 는 기능 브랜치 이미지일 수 있어 이동 포인터(latest)를 옮기지 않는다.
$DEV_ONLY || docker push "$LATEST"

update_function() {
  local fn=$1
  if $DEV_ONLY && [ "$fn" != "$DEV_FUNCTION" ]; then
    echo "dev-only 인데 $fn 갱신 시도 — 중단" >&2
    exit 1
  fi
  # "함수 없음"과 권한 에러를 구분한다 — AccessDenied 를 삼키면 배포가 조용히 누락된다.
  if err=$(aws lambda get-function --function-name "$fn" --region "$REGION" 2>&1 >/dev/null); then
    aws lambda update-function-code --function-name "$fn" \
      --image-uri "$IMAGE" --region "$REGION" --no-cli-pager >/dev/null
    echo "배포 완료: $fn <- $IMAGE"
  elif grep -q ResourceNotFound <<<"$err"; then
    echo "Lambda 함수($fn)가 아직 없다 — 콘솔에서 $IMAGE 로 생성(arm64)한 뒤 다시 실행하면 코드 업데이트까지 수행된다."
  else
    echo "$err" >&2
    exit 1
  fi
}

for fn in "${FUNCTIONS[@]}"; do
  update_function "$fn"
done
