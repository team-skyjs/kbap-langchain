# Lambda 컨테이너 배포 설계 (2026-08-08)

## 배경

SQS → Lambda(콘텐츠 생성·검수 그래프) → Spring API → DB 아키텍처로 확정.
SQS 핸들러는 `kbap.content.handler`에 이미 구현돼 있다(계약: `{"foodId": int, "scannedName": str}`,
부분 실패는 `batchItemFailures`로 보고). 남은 것은 배포 패키징뿐이다.

Spring 결과 반영 API 호출은 이 설계의 범위가 아니다 — 엔드포인트 스펙 확정 후 별도 작업.

## 산출물

### 1. `Dockerfile`

- 베이스: `public.ecr.aws/lambda/python:3.12` (awslambdaric 내장)
- 아키텍처: **arm64** — Apple Silicon 네이티브 빌드 + Lambda Graviton 비용 절감.
  의존성 전부 순수 파이썬 또는 aarch64 휠 제공이라 문제없음
- 의존성 설치: `uv export --no-dev`로 잠긴 버전을 requirements 형태로 뽑아 `pip install`
  (uv.lock 재현성 유지, 이미지에 uv 불필요)
- 복사물: `src/kbap/` + `config.yaml` → `LAMBDA_TASK_ROOT`
- `.env`는 이미지에 넣지 않는다 — API 키는 Lambda 환경변수로 주입,
  `load_dotenv()`는 파일이 없으면 조용히 넘어가므로 코드 수정 불필요
- CMD: `kbap.main.handler`

### 2. `deploy.sh`

- 상단 변수: `REGION=ap-northeast-2`, `ACCOUNT=118178010621`, ECR 리포·함수 이름
- 동작: ECR 로그인 → `docker build --platform linux/arm64` → push →
  `aws lambda update-function-code`
- ECR 리포 생성·Lambda 함수 최초 생성은 1회성 콘솔 작업이라 스크립트 범위 밖
  (스크립트 주석에 명시). 스크립트는 반복 배포만 담당

## AWS 리소스 (콘솔, 사용자 작업)

- SQS: `kbap-generate-content-queue` (visibility 30분, 보존 2일, DLQ maxReceiveCount 3)
  + `kbap-generate-content-dlq` (보존 4일) — 완료
- ECR 리포 생성 → 첫 push 후 Lambda 함수 생성(컨테이너 이미지, arm64, timeout 5분)
- 실행 역할에 `AWSLambdaSQSQueueExecutionRole` 추가
- SQS 트리거: batch size 10, **부분 배치 응답 보고(ReportBatchItemFailures) 활성화 필수**
  (핸들러가 `batchItemFailures`를 반환하므로, 미활성화 시 실패 메시지가 배치 통째로 재시도됨)

## 테스트

- 로컬: `docker run` + Lambda RIE로 샘플 SQS 이벤트를 넣어 핸들러 기동 확인
- 실배포: SQS 콘솔에서 메시지 직접 전송 → CloudWatch Logs 확인

## 스킵

- Terraform/CDK — 리소스가 3개뿐, 콘솔로 충분
- GitHub Actions — 배포 안정화 후
- Spring 콜백 — 엔드포인트 스펙 대기
