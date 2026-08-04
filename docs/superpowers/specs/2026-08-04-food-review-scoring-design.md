# KB-286: food 최종 검수(scoring) LangGraph 설계

2026-08-04 승인, 2026-08-05 갱신. 구현 범위는 **kbap-langchain(파이썬)만**.
kbap(Kotlin) 쪽 enum·컬럼·어드민 API 는 이미 구현돼 있어(커밋 `33681655`) 그 계약에 맞춘다.

## 목적

배치가 채운 `food` 레코드(`PENDING_REVIEW`)를 사람 승인 **직전에** LLM으로 최종 검수한다.
통과분은 `REVIEWED`로 넘겨 사람이 확인만 하게 하고, 미달분은 문제 컬럼을 비워
기존 콘텐츠 배치가 재생성하도록 `INCOMPLETE`로 되돌린다. 재생성 기회는 2회까지 —
그 이후에도 미달이면 컬럼을 유지한 채 `REVIEW_REJECTED`로 표시하고 사유(개조식, 1000자 이내)를
붙여 사람이 직접 판단하게 한다.

## 상태 전이 (kbap 변경 후 기준)

```
INCOMPLETE → PENDING_IMAGE → PENDING_REVIEW ─┬─ AI 통과 → REVIEWED ── 사람 승인 → READY
                  ↑                          │
                  └── AI 탈락(attempts < 2): │
                      문제 컬럼 비움 + INCOMPLETE 롤백, attempts+1
                                             │
                                             └─ AI 탈락(attempts ≥ 2):
                                                컬럼 유지 + REVIEW_REJECTED (+ review_note)
```

- enum `REVIEWED`, `REVIEW_REJECTED` 와 컬럼 `content_review_attempts`,
  `content_review_rejection_reason` 은 kbap 에 이미 반영돼 있다.
- 어드민 승인 큐: `REVIEWED`(통과분) / `REVIEW_REJECTED`(문제분) 두 개

## kbap API 계약 (구현됨)

kbap `develop` 에 이미 있다 — 커밋 `33681655` (PR #131).

```
GET  /api/v1/admin/foods/content-reviews?limit=N        (1..200, 기본 50)
  → { success, payload: { items: [{ foodId, koreanName, description,
        nameTranslations, descriptionTranslations, avoidanceSubstances,
        spiciness, imageUrl, contentReviewAttempts }] } }
  조건: content_status = PENDING_REVIEW

POST /api/v1/admin/foods/content-reviews/{foodId}
  { passed: true }                                   → REVIEWED
  { passed: false, rejectedFields: [...], reason }   → kbap 이 분기
```

- **재시도 상한은 kbap 이 갖는다.** `Food.rejectContentReview()` 가
  `contentReviewAttempts >= MAX_CONTENT_REVIEW_ATTEMPTS(2)` 를 보고, 소진 전이면 필드를 비우고
  `INCOMPLETE` 로, 소진 후면 `REVIEW_REJECTED` + 사유 저장으로 간다. 파이썬은 통과 여부와
  문제 필드만 보낸다.
- 거부 사유도 kbap 이 앞 10줄 → 앞 1000자로 자른다(`MAX_REJECTION_REASON_LINES/_LENGTH`).
- 상태 가드(`PENDING_REVIEW` 일 때만 반영)와 낙관적 락은 `Food` 엔티티 책임.

**rejectedFields (`FoodContentReviewField`) → kbap 이 비우는 컬럼:**

| 채점 필드군 | 보내는 값 | 비워지는 것 |
|---|---|---|
| description | `DESCRIPTION` | `description`(placeholder) — 배치가 설명·설명번역을 함께 재생성 |
| translations | `NAME_TRANSLATIONS`, `DESCRIPTION_TRANSLATIONS` | 두 번역 맵 |
| avoidance | `AVOIDANCE_SUBSTANCES`, `SPICINESS` | `avoidance_substances`(null), `spiciness`(-1) |

`IMAGE` 도 enum 에 있으나 이미지 검수는 범위 밖이라 보내지 않는다.

## 그래프 (음식 1건 = 1 실행)

```
                    ┌─ score_description ─┐
fetch(입력) ────────┼─ score_translations ─┼── aggregate ── report(kbap POST)
                    └─ score_avoidance ────┘
```

**State:**

```python
class FieldScore(TypedDict):
    score: int          # 0~100
    reason: str

class ReviewState(TypedDict):
    food: dict                                        # kbap 조회 응답 1건
    description_score: FieldScore | None
    translation_scores: dict[str, FieldScore] | None  # 언어코드 → 점수
    avoidance_score: FieldScore | None
    verdict: dict | None                              # aggregate 산출
```

**노드:**

1. `score_description` — 한국어 설명이 그 음식을 정확히 설명하는지, 환각·오기 없는지. LLM 1회, structured output.
2. `score_translations` — 9개 언어 × (이름+설명)을 한 호출에, 언어별 점수 배열. 한 언어라도 임계값 미만이면 번역 필드군 전체 탈락 (컬럼을 언어별로 부분 비울 수 없음 — 배치가 통짜 재생성).
3. `score_avoidance` — `avoidance_substances`의 레시피상 타당성 + `spiciness` 적정성. 안전 직결 — 프롬프트에 "확신 없으면 낮은 점수" 지시.
4. `aggregate` — LLM 없는 순수 함수. 필드군 점수 vs 임계값 →
   - 전부 통과 → `passed=true`
   - 미달 → `passed=false` + rejectedFields + 개조식 사유 (재시도 소진 여부는 kbap 이 판단)
5. `report` — kbap POST.

**실행기 (그래프 밖 평범한 파이썬):**
`GET content-reviews` → 음식별 `graph.ainvoke`를 `asyncio.gather`(동시 5건, `return_exceptions=True`) → 결과 카운트 출력.
CLI: `python -m kbap_review --limit 50 [--dry-run]`. 홈서버에서 수동/cron 실행 — 서버 프레임워크 없음.

## 에러 처리

- **LLM 실패/타임아웃**: 노드 `retry_policy` 2회. 최종 실패 시 그 음식은 **판정 보류** — POST 없이 `PENDING_REVIEW`에 남겨 다음 실행에서 자연 재시도. 탈락은 오직 점수 미달만.
- **structured output 파싱 실패**: 재시도 대상에 포함시킨 뒤 최종 실패 시 보류. LangGraph 기본 `retry_on` 이
  `ValueError` 계열을 제외해 `OutputParserException`·`ValidationError` 가 1회만 시도되므로 `retry_on` 을 덮어쓴다.
- **kbap POST 실패**: 로그 후 보류.
- 음식 간 실패 격리: `gather(return_exceptions=True)`.

## 설정

```yaml
kbap_api:
  base_url: https://dev.kbap.site  # 호스트까지만. /api/v1/admin/... 은 클라이언트가 붙인다
llm:
  model: openai:gpt-5-mini       # 기본. "provider:model" 또는 접두어 없는 gemini*
  avoidance_model: openai:gpt-5-mini # 안전 직결 노드만 별도 오버라이드
  timeout_seconds: 120           # LLM 콜 타임아웃 — 무인 배치라 필수. 없으면 멈춘 호출 하나가 전체 실행을 잡는다
thresholds:                      # 필드군별 임계값 — 운영하며 튜닝
  description: 70
  translations: 70
  avoidance: 70
concurrency: 5                   # 동시 실행 "그래프" 수(최소 1). 그래프당 LLM 3콜이라 실제 상한은 15
```

- 모델 선택 근거: 채점(판정) 작업이라 생성보다 요구 지능이 낮고, 다국어 판정은 Gemini 강점.
  kbap 배치도 flash-lite로 번역 스코어링 운영 중. 가격은 gpt-5-mini($0.25/$2.00)와
  Gemini 2.5 Flash($0.30/$2.50)가 동급이라 비용보다 키 운영 편의로 선택.
- 관측: **Langfuse** — `CallbackHandler`를 invoke `config.callbacks`로 전달,
  `LANGFUSE_PUBLIC_KEY`/`SECRET_KEY`/`HOST` 환경변수로 연결. 코드 몇 줄.

## 프로젝트 구조

```
kbap-langchain/
├─ pyproject.toml            # uv; langgraph, langchain-google-genai, httpx, pydantic, langfuse
├─ config.yaml
├─ src/kbap_review/
│  ├─ __main__.py            # CLI 진입점 + 실행기
│  ├─ graph.py               # 그래프 조립 + 노드
│  ├─ scoring.py             # 프롬프트 + structured output 스키마
│  ├─ aggregate.py           # 판정 로직 (순수 함수)
│  └─ kbap_client.py         # httpx 클라이언트
└─ tests/
```

## 테스트

- `aggregate` 유닛 테스트 집중 — 전부 통과 / 필드군별 미달 → rejectedFields 매핑 / 사유 조립. LLM 없는 순수 함수.
- 그래프 통합 테스트: LLM fake 주입, 통과 경로 + 탈락 경로 + 파싱 실패 재시도.
- 실 LLM 스모크: 실제 음식 2~3건 `--dry-run` 수동 확인 (자동화 안 함). avoidance 판정 품질을 특히 확인.

## 범위 밖 (명시적 제외)

- 이미지(vision) 검수 — image_ref는 채점하지 않는다.
- 다중 모델 합의(min-agreement) — 나중에 avoidance 노드만 감싸면 되는 구조. 지금은 단일 모델.
- LangGraph 서버/체크포인트 영속화 — CLI 배치라 불필요. 보류 건은 상태 기반 자연 재시도로 충분.
- 빈 값·언어 누락 등 기계적 검사 — 배치의 `needsXxx()`/`transitionByContentState()`가 이미 보장.
