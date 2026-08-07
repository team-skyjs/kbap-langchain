# kbap 적재 콜백 설계 (2026-08-08)

계약: agenthub `wiki/langchain-food-ingest-contract.md` (2026-08-08 확정) — 그 문서가 단일
진실 원천이고, 이 스펙은 람다 쪽 구현 결정만 담는다.

## 변경

### 1. `JudgeVerdict.failure_kind` — 세 생성 지점에서 코드가 찍는다

`failure_kind: str | None = None` 필드 추가. LLM 출력에 맡기지 않는다:

- 정제 노드 조기 종료 → `NOT_FOOD`
- 기피성분 fail-closed 가드 → `INGREDIENT_GUARD`
- 그 외 judge 탈락 → judge 노드 코드가 `JUDGE_REJECTED` 로 **덮어쓴다**
  (LLM 이 뭘 뱉어도 무시 — 계약 밖 enum 값이 나갈 수 없다)

### 2. `build_ingest_payload(state)` — 최종 상태 → 계약 요청 본문

- `displayName` = `cleaned_name` (스캔 원본 아님 — 콘텐츠가 정제된 이름 기준으로 생성됐고,
  스캔 노이즈가 DB 행 이름이 되면 안 된다. 조기 종료 경로도 cleaned_name 은 항상 있다)
- passed=true: `description`, `spiciness`(= ingredients.spiciness),
  `nameTranslations`/`descriptionTranslations`(9키 전수는 그래프 pydantic 이 이미 보장),
  `ingredients`(= ingredients.substances, 키 변환 `inclusionPercent` → `inclusion_percent`)
- passed=false: `failureKind`, `reason` 만

### 3. `KbapClient.post_food_content(payload)` — 기존 클라이언트 재사용

`POST /api/v1/admin/foods/contents`, 200 외는 예외. base_url 은 config.yaml
`kbap_api.base_url`, 인증은 `KBAP_API_TOKEN` (Lambda 환경변수 등록 완료).
Lambda 는 invoke 마다 이벤트 루프가 새로 생기므로 클라이언트는 invoke 단위로
생성·종료한다 (모듈 캐시 불가).

### 4. 실패 처리

- POST 실패는 종류 불문(네트워크·5xx·409 소프트삭제 충돌) 해당 메시지를
  `batchItemFailures` 로 보고 → SQS 재시도 3회 → DLQ. 계약의 "409 는 사람이 판단" 의도.
- POST 만 실패한 재시도는 그래프를 재실행해 LLM 비용이 다시 들지만 서버가 멱등이라
  정합성 문제 없음. 결과 캐시는 만들지 않는다 — 드문 경로.
- 그래프 런타임 예외는 지금처럼 POST 없이 실패 보고 — FAILED 에 인프라 장애를 섞지
  않는다는 계약 전제.

## 스킵

- `rejected_fields` 전송 — 계약이 채택하지 않음
- 결과 캐시 — 위 4번
