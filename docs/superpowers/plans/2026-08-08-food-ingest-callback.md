# kbap 적재 콜백 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 그래프 최종 상태를 kbap 적재 API(`POST /api/v1/admin/foods/contents`)로 전송한다.

**Architecture:** `JudgeVerdict.failure_kind` 를 세 생성 지점에서 코드가 찍고, `build_ingest_payload` 가 상태를 계약 본문으로 변환, `KbapClient.post_food_content` 로 전송. POST 실패는 `batchItemFailures` 로 보고해 SQS 재시도 → DLQ 경로를 탄다.

**Tech Stack:** 기존 httpx `KbapClient`, pytest

## Global Constraints

- 계약 문서: agenthub `wiki/langchain-food-ingest-contract.md` (2026-08-08 확정)
- `failureKind` enum: `NOT_FOOD` | `JUDGE_REJECTED` | `INGREDIENT_GUARD` — LLM 출력 금지, 코드가 확정
- `ingredients` 키 변환: 내부 `inclusionPercent` → 계약 `inclusion_percent`
- `displayName` = `cleaned_name`

---

### Task 1: failure_kind 스탬핑 + 페이로드 조립 (content.py)

**Files:**
- Modify: `src/kbap/content.py` (JudgeVerdict, clean_name 노드, judge 노드, build_ingest_payload 추가)
- Test: `tests/test_content.py`

**Interfaces:**
- Produces: `build_ingest_payload(state: dict) -> dict`, `JudgeVerdict.failure_kind: str | None`

- [ ] 실패 테스트: 기존 3경로 테스트에 failure_kind 단언 추가 + build_ingest_payload 성공/실패 형태 테스트
- [ ] 구현: JudgeVerdict 필드, 세 지점 스탬프(judge 노드는 model_copy 덮어쓰기), build_ingest_payload
- [ ] `uv run pytest` 통과 → 커밋

### Task 2: 전송 배선 (review.py 클라이언트 + process_event + handler)

**Files:**
- Modify: `src/kbap/review.py` (post_food_content), `src/kbap/content.py` (process_event 에 kbap 파라미터, handler 에 클라이언트 생성·종료)
- Test: `tests/test_content.py` (FakeKbap)

**Interfaces:**
- Consumes: Task 1 의 `build_ingest_payload`
- Produces: `KbapClient.post_food_content(payload: dict) -> None`, `process_event(event, graph, kbap, concurrency, callbacks=[])`

- [ ] 실패 테스트: 성공 시 POST 페이로드 검증 / verdict 실패 시 failureKind 포함 POST / POST 예외 → 실패 보고 / 그래프 예외 → POST 없음 (기존 SQS 테스트에 FakeKbap 주입)
- [ ] 구현: post_food_content, process_event 배선, handler 에서 config.yaml 로 클라이언트 생성 + finally aclose, TODO 주석 제거
- [ ] `uv run pytest` 통과 → RIE 스모크(계약 위반 이벤트) 재확인 → 커밋

### Task 3: PR

- [ ] push + `gh pr create` (writing-pull-requests 스킬 규칙)
