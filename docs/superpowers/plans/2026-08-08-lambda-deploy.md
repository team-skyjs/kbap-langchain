# Lambda 컨테이너 배포 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 기존 SQS 핸들러(`kbap.main.handler`)를 arm64 Lambda 컨테이너 이미지로 패키징하고 반복 배포 스크립트를 만든다.

**Architecture:** 멀티스테이지 Dockerfile — build 스테이지에서 `uv export`로 uv.lock을 requirements로 변환, 런타임 스테이지(`public.ecr.aws/lambda/python:3.12`)에서 pip 설치 후 `src/kbap` + `config.yaml`만 복사. deploy.sh는 ECR 로그인 → build → push → `update-function-code`만 담당(리소스 생성은 콘솔 1회성 작업).

**Tech Stack:** Docker(buildx, arm64), AWS CLI, uv, Lambda RIE(로컬 스모크 테스트)

## Global Constraints

- 리전 `ap-northeast-2`, 계정 `118178010621`, ECR 리포 `kbap/langchain`, Lambda 함수명 `kbap-generate-content`
- 아키텍처 arm64 (`--platform linux/arm64`, buildx provenance 비활성화 — Lambda는 단일 매니페스트만 허용)
- `.env`는 절대 이미지에 넣지 않는다 (COPY는 명시 경로만: `pyproject.toml`, `uv.lock`, `config.yaml`, `src/kbap`)
- 의존성은 `uv export --frozen --no-dev`로 잠긴 버전 그대로 설치

---

### Task 1: Dockerfile + 로컬 RIE 스모크 테스트

**Files:**
- Create: `Dockerfile`

**Interfaces:**
- Consumes: `kbap.main.handler` (기존 재노출 진입점), `config.yaml`
- Produces: 로컬 태그 `kbap-lambda:local` 이미지 — Task 2의 deploy.sh가 같은 Dockerfile을 빌드

- [ ] **Step 1: Dockerfile 작성**

```dockerfile
# build 스테이지: uv.lock -> requirements.txt (최종 이미지에 uv 를 남기지 않기 위한 분리)
FROM public.ecr.aws/lambda/python:3.12 AS build
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
RUN uv export --frozen --no-dev --no-emit-project -o /requirements.txt

FROM public.ecr.aws/lambda/python:3.12
COPY --from=build /requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
# .env 는 복사하지 않는다 — API 키는 Lambda 환경변수로 주입된다
COPY config.yaml ${LAMBDA_TASK_ROOT}/
COPY src/kbap ${LAMBDA_TASK_ROOT}/kbap
CMD ["kbap.main.handler"]
```

- [ ] **Step 2: 이미지 빌드**

Run: `docker build --platform linux/arm64 --provenance=false -t kbap-lambda:local .`
Expected: 성공 (수 분 소요 — langchain 의존성 설치)

- [ ] **Step 3: RIE 스모크 테스트 — 컨테이너 기동 후 계약 위반 이벤트 주입**

계약 위반 body("not-json")는 LLM 콜 없이 batchItemFailures 경로를 타므로, API 키 없이 핸들러 기동·임포트·이벤트 파싱 전체를 검증한다.

Run:
```bash
docker run --rm -d -p 9000:8080 --name kbap-rie kbap-lambda:local
sleep 2
curl -s -XPOST "http://localhost:9000/2015-03-31/functions/function/invocations" \
  -d '{"Records":[{"messageId":"smoke-1","body":"not-json"}]}'
docker rm -f kbap-rie
```
Expected: `{"batchItemFailures": [{"itemIdentifier": "smoke-1"}]}` (langfuse 키 없음 경고 로그는 무해)

- [ ] **Step 4: Commit**

```bash
git add Dockerfile
git commit -m "feat: Lambda arm64 컨테이너 이미지 — uv.lock 고정 의존성, RIE 스모크 검증"
```

### Task 2: deploy.sh

**Files:**
- Create: `deploy.sh`

**Interfaces:**
- Consumes: Task 1의 `Dockerfile`, 사용자가 콘솔에서 만든 ECR 리포 `kbap/langchain`
- Produces: ECR `latest` 이미지 push + (함수 존재 시) 코드 업데이트

- [ ] **Step 1: deploy.sh 작성**

```bash
#!/usr/bin/env bash
# 반복 배포 전용: ECR 로그인 -> build(arm64) -> push -> Lambda 코드 업데이트.
# 1회성 작업(ECR 리포 생성, Lambda 함수 생성, SQS 트리거 연결)은 콘솔에서 한다.
set -euo pipefail

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

if aws lambda get-function --function-name "$FUNCTION" --region "$REGION" >/dev/null 2>&1; then
  aws lambda update-function-code --function-name "$FUNCTION" \
    --image-uri "$IMAGE" --region "$REGION" --no-cli-pager >/dev/null
  echo "배포 완료: $FUNCTION <- $IMAGE"
else
  echo "push 완료: $IMAGE"
  echo "Lambda 함수($FUNCTION)가 아직 없다 — 콘솔에서 이 이미지로 생성한 뒤 다시 실행하면 코드 업데이트까지 수행된다."
fi
```

- [ ] **Step 2: 실행 권한 부여 및 실행 (첫 push)**

Run: `chmod +x deploy.sh && ./deploy.sh`
Expected: push 성공 + "Lambda 함수(kbap-generate-content)가 아직 없다" 안내 (함수는 사용자가 콘솔에서 생성 예정)

- [ ] **Step 3: Commit**

```bash
git add deploy.sh
git commit -m "feat: ECR push + Lambda 코드 업데이트 배포 스크립트"
```
