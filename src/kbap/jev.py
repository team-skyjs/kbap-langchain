"""검수 3곳(이름·설명·기피성분)의 Jev(TypeSafe System One) 구현 — KB-913.

생성은 기존 LLM 이 그대로 하고, 검수만 Jev 로 바꿀 수 있게 content.ContentFns 의 검수 함수
3개를 같은 시그니처로 대체한다. 그래프·적재 payload 는 Jev 응답 객체를 모른다 — 결과는
기존 도메인 모델(NameReview·DescReview·IngredientsReview)로 변환되고, 판정 근거(확률 원본·
사유 코드·모델/정책 버전·사용량·지연·대체 여부)는 도메인 모델의 _trace 에만 실린다.

내부 판정은 PASS / REVISE / ABSTAIN 이다.
- PASS·REVISE(사유 코드 1개 이상) → Jev 판정을 그대로 쓴다. REVISE 의 재생성 피드백은
  사유 코드별 고정 문구다 — 관측하지 않은 구체 성분·사실을 지어내지 않는다.
- ABSTAIN, 또는 REVISE 인데 사유 코드가 없음(무엇을 고칠지 불명확) → 기존 LLM 검수로 대체.
- Jev 장애(5xx·529·연결 실패) → 제한 재시도 뒤 기존 LLM 검수로 대체.
- timeout·429 → 제한 재시도 뒤 JevTransientError — 콘텐츠 실패가 아니라 SQS 실패 경로로 간다.
- 인증·요청 형식 오류(400·401·403·413·415·422) → 재시도 없이 JevRequestError.
두 예외는 RuntimeError 계열이라 LangGraph 기본 재시도(review._retryable)에 걸리지 않는다 —
Jev 재시도 × 그래프 재시도 × SQS 재시도가 곱해지지 않게 Jev 재시도는 여기 한 곳만 둔다.

임계값은 아직 보정 전이다(POLICY_VERSION). 조정용 데이터로 정하고 최종용 데이터로 확인한다.
API: https://docs.typesafe.ai/api/system-one.md
"""

from collections.abc import Awaitable, Callable
from dataclasses import asdict, dataclass
import asyncio
import hashlib
import json
import re
import time

import httpx

from kbap.content import DescReview, IngredientsReview, NameReview
from kbap.review import AVOIDANCE_CODES

JEV_URL = "https://api.typesafe.ai/v1/systemone"
POLICY_VERSION = "kb913-v1-uncalibrated"  # v1: 기피성분 질문을 성분별로 바꾸며 임계값도 새로 둠

PASS, REVISE, ABSTAIN = "PASS", "REVISE", "ABSTAIN"


class JevRequestError(RuntimeError):
    """인증·요청 형식 오류 — 다시 보내도 같은 결과라 재시도하지 않는다."""


class JevTransientError(RuntimeError):
    """timeout·429 가 재시도 예산을 넘김 — 콘텐츠 판정이 아니므로 메시지 실패(SQS 재시도)로 보낸다."""


class JevOutageError(RuntimeError):
    """5xx·529·연결 실패가 재시도 예산을 넘김 — 기존 LLM 검수로 대체한다."""


@dataclass(frozen=True)
class JevPolicy:
    """확률 → 내부 판정 임계값. Jev 확률의 의미는 LLM 점수와 달라 따로 보정한다(공유 금지)."""

    # Noul 시작값은 TypeSafe 권장 0.7(예) / 0.3(아니오), 사이는 미결 — 보류(ABSTAIN)로 둔다.
    # confidence 는 분포 집중도일 뿐 정답 확률이 아니라 게이트로 쓰지 않는다.
    # 이름: 음식 여부 — food_min 이상 음식, not_food_max 이하 비음식, 사이는 보류.
    food_min: float = 0.7
    not_food_max: float = 0.3
    # 결함 질문 공통 — 모두 defect_pass_max 미만이면 통과, 하나라도 defect_min 이상이면 수정 필요.
    defect_pass_max: float = 0.3
    defect_min: float = 0.7
    # 기피성분(성분별 질문, ing-v1): 포함확률 이 % 이상으로 적힌 성분인데 "들어가는가" 가 unrelated_max 이하면
    # 무관 성분. 안 적힌 숨은 성분이 missing_min 이상이면 누락, missing_abstain_min 이상이면 보류(→ LLM) —
    # 알레르기 데이터는 오통과가 누락보다 위험해 누락 쪽 보류 구간을 넓게 둔다.
    listed_min_percent: int = 50
    unrelated_max: float = 0.2
    missing_min: float = 0.8
    missing_abstain_min: float = 0.5
    # 설명 Score(0~2 등급): 최상 등급 확률이 이 값 이상이어야 3점.
    desc_pass_min: float = 0.8
    # 하위 두 등급 확률 합이 이 값 이상이면 미달(1·2점), 둘 다 아니면 보류.
    desc_fail_min: float = 0.6


@dataclass(frozen=True)
class JevSettings:
    api_key: str
    model: str = "jev-1.14.0"  # 별칭(jev-latest)은 버전이 바뀌면 같은 입력의 답이 달라져 A/B 재현이 깨진다
    timeout_seconds: float = 10.0
    max_attempts: int = 2  # timeout·429·5xx 공통, 최초 포함
    backoff_seconds: float = 0.5
    max_backoff_seconds: float = 4.0


# 후보 코드 → 한국어 이름 ("EGG(계란)" → {"EGG": "계란"})
CATALOG = dict(re.findall(r"([A-Z_]+)\(([^)]+)\)", AVOIDANCE_CODES))


def question_hash(questions: dict) -> str:
    return hashlib.sha256(json.dumps(questions, sort_keys=True, ensure_ascii=False).encode()).hexdigest()[:12]


# ===== 질문 — 문구·기준은 전부 여기서 감사·수정한다 =====
# 원칙(TypeSafe 스킬·인계 §4): 질문 하나에 판단 하나("또는"으로 묶지 않음), 결정은 코드(decide_*)가 한다.
# 노드별로 그 노드 입력만 state 에 넣는다 — 아직 생성되지 않은 설명·성분을 이름 검수와 합치지 않는다.
# 같은 요청의 질문은 서로의 답을 못 본다. 질문 ID 는 전송되지 않고 문구·criteria 만 모델에 간다.
# state 안의 지시문·OCR 노이즈는 데이터로 다뤄지는지 평가 사례로 확인한다(질문에 서문을 붙이지 않는다).

NAME_QUESTIONS = {
    "is_food": {
        "type": "noul",
        "instructions": "`cleaned` 는 손님이 주문하는 한 가지 음식이나 음료의 메뉴 이름인가?",
        "criteria": {
            "true": "김치찌개, 할매 순대국, 양꼬치처럼 한 가지 음식·음료의 메뉴명. 희귀하거나 지역·상호 수식어가 붙어도 메뉴명이다",
            "false": "사리 추가 같은 옵션, '찌개류' 같은 분류 제목, '포장 가능' 같은 안내 문구, 가게 이름, 읽을 수 없는 문자열",
        },
    },
    "noise_left": {
        "type": "noul",
        "instructions": "`cleaned` 에 가격·메뉴 번호·기호 같은 메뉴판 노이즈가 남아 있는가?",
        "criteria": {"true": "'김치찌개 8,000원', '3. 비빔밥', '★냉면'", "false": "'김치찌개'처럼 메뉴명 글자만 있다"},
    },
    "typo_left": {
        "type": "noul",
        "instructions": "`cleaned` 에 한글 오탈자가 남아 있는가?",
        "criteria": {"true": "'김치찌게', '된장찌계'처럼 실제 메뉴명과 글자가 어긋난다", "false": "표준 표기의 메뉴명이다"},
    },
    "changed_dish": {
        "type": "noul",
        "instructions": "`cleaned` 는 `original` 과 다른 음식을 가리키는가?",
        "criteria": {
            "true": "'돼지고기 김치찜'이 '돼지고기 김치찌개'가 된 것처럼 음식이 바뀌었다",
            "false": "노이즈·오타만 고쳐 `original` 과 같은 음식이다",
        },
    },
    "dropped_modifier": {
        "type": "noul",
        "instructions": "`original` 에 있던 메뉴 수식어(왕·매운·할매 등)가 `cleaned` 에서 빠졌는가?",
        "criteria": {"true": "'왕김치찌개'가 '김치찌개'가 됐다", "false": "`original` 의 수식어가 그대로 있거나 원래 수식어가 없다"},
    },
}

DESC_QUESTIONS = {
    "quality": {
        "type": "score",
        "instructions": "`description` 은 `name` 을 설명하는 화면용 한국어 한 줄로 얼마나 알맞은가?",
        "criteria": [
            "설명에 이 음식에 없는 재료나 다른 음식의 조리법이 들어 있다",
            "설명의 사실은 맞지만 표현이 어색하거나 부차적인 정보가 틀렸다",
            "설명이 이 음식을 사실대로 정확히, 자연스러운 한국어로 설명한다",
        ],
    },
    "other_dish": {
        "type": "noul",
        "instructions": "`description` 은 `name` 이 아닌 다른 음식을 설명하는가?",
        "criteria": {"true": "'김치찌개'에 '된장을 풀어 끓인 찌개'라는 설명", "false": "`name` 그 음식을 설명한다"},
    },
    "wrong_fact": {
        "type": "noul",
        "instructions": "`description` 에 `name` 의 일반적인 레시피에 없는 재료가 들어 있는가?",
    },
    "unnatural": {
        "type": "noul",
        "instructions": "`description` 의 한국어 문장이 어색한가?",
        "criteria": {"true": "번역투·비문·어순이 어색해 다시 읽어야 한다", "false": "한국어 화자가 바로 읽히는 자연스러운 문장이다"},
    },
}

# 기피성분 ing-v1 — v0 의 존재형 질문("목록 중 하나라도 ~인가")은 정답과 무관하게 '예'였다(live run 41122710568:
# 정상·오류 레코드의 unrelated 중앙값 둘 다 0.91). 성분 하나 = 질문 하나로 쪼개고 판단은 decide_ingredients 가 한다.
# 적힌 성분(listed_*)과 숨은 성분 후보(hidden_*)는 같은 문구로 묻는다 — 같은 확률을 같은 뜻으로 읽기 위해서다.
INGREDIENT_QUESTION_VERSION = "ing-v1-per-item"
# 누락은 84개 전체가 아니라 양념·육수·가공 재료에 숨는 고위험 코드만 묻는다(평가 데이터셋 안전 세트 기준).
# 묶음 코드(SEAFOOD 등)만 적힌 경우에도 하위 코드(SHRIMP)를 따로 묻는다 — 이 때문에 누락 오탐이 나면 tune 에서 본다.
HIDDEN_CODES = (
    "SALTED_SHRIMP", "SHRIMP", "FISH_SAUCE", "ANCHOVY", "FISH", "WHEAT", "SOY",
    "EGG", "MILK", "PEANUT", "BUCKWHEAT", "MUSTARD", "SESAME",
)
CONTAINS_TEMPLATE = (
    "`name` 을 일반적인 레시피대로 만들면 {ingredient} 성분이 들어가는가? "
    "양념·육수·소스·가공 재료 속에 든 경우도 포함한다."
)
CONTAINS_CRITERIA = {"true": "대부분의 가게 레시피에 그 성분이 들어간다", "false": "들어가지 않거나 일부 가게만 넣는다"}
SPICINESS_QUESTION = {
    "type": "noul",
    "instructions": "`spiciness`(0 맵지 않음 ~ 10 매우 매움)는 `name` 의 일반적인 매운 정도와 3단계 이상 차이 나는가?",
}
# 감사·manifest 용 고정 부분 — 실제 요청의 질문은 ingredient_questions() 가 성분 목록으로 만든다.
INGREDIENT_QUESTIONS = {
    "version": INGREDIENT_QUESTION_VERSION,
    "contains": {"type": "noul", "instructions": CONTAINS_TEMPLATE, "criteria": CONTAINS_CRITERIA},
    "hidden_codes": list(HIDDEN_CODES),
    "spiciness_wrong": SPICINESS_QUESTION,
}


def _contains(code: str) -> dict:
    return {
        "type": "noul",
        "instructions": CONTAINS_TEMPLATE.format(ingredient=f"{CATALOG[code]}({code})"),
        "criteria": CONTAINS_CRITERIA,
    }


def ingredient_questions(substances: list[dict]) -> dict:
    listed = list(dict.fromkeys(s["code"] for s in substances))
    return (
        {f"listed_{c}": _contains(c) for c in listed}
        | {f"hidden_{c}": _contains(c) for c in HIDDEN_CODES if c not in listed}
        | {"spiciness_wrong": SPICINESS_QUESTION}
    )


# 사유 코드 → 재생성 피드백 고정 문구. 관측한 질문만 근거로 하며 구체 성분명을 지어내지 않는다.
FEEDBACK = {
    "NOISE_LEFT": "가격·메뉴 번호·기호 같은 메뉴판 노이즈가 이름에 남아 있다 — 노이즈만 지워라",
    "TYPO_LEFT": "이름에 오탈자가 남아 있다 — 가장 가까운 실제 메뉴명으로 고쳐라",
    "CHANGED_DISH": "정제한 이름이 원본과 다른 음식이 됐다 — 원본의 음식을 유지하고 노이즈·오타만 고쳐라",
    "DROPPED_MODIFIER": "원본에 있던 메뉴 수식어가 사라졌다 — 수식어를 보존하라",
    "OTHER_DISH": "설명이 이 음식이 아닌 다른 음식을 설명한다 — 이 음식 자체를 설명하라",
    "WRONG_FACT": "설명에 이 음식의 일반적 레시피에 없는 재료가 있다 — 대표 레시피 기준으로 사실만 써라",
    "UNNATURAL_EXPRESSION": "설명의 한국어 표현이 어색하다 — 같은 사실을 자연스러운 한 문장으로 다시 써라",
    "MISSING_INGREDIENT": (
        "이 음식에 일반적으로 들어가는 주요 기피성분이 빠졌을 가능성이 높다 — "
        "대표 레시피의 양념·육수·가공 재료까지 다시 점검해 빠진 후보 성분을 추가하라"
    ),
    "UNRELATED_INGREDIENT": "이 음식에 들어가지 않는 성분이 포함됐을 가능성이 높다 — 근거 없는 성분을 빼라",
    "SPICINESS_WRONG": "매운맛 등급이 이 음식의 일반적인 매운맛과 다르다 — 대표 레시피 기준으로 다시 매겨라",
}


def feedback_for(codes: list[str]) -> str:
    return " / ".join(FEEDBACK[c] for c in codes)


# ===== 확률 → 내부 판정 (순수 함수 — 보정·테스트 대상) =====


def _defects(answers: dict, codes: dict[str, str], policy: JevPolicy, pass_max: dict[str, float]):
    """결함 noul 들을 PASS / REVISE(사유 코드) / ABSTAIN 으로 묶는다."""
    fired = [code for q, code in codes.items() if answers[q]["noul"] >= policy.defect_min]
    if fired:
        return REVISE, fired
    if all(answers[q]["noul"] < pass_max.get(q, policy.defect_pass_max) for q in codes):
        return PASS, []
    return ABSTAIN, []


def decide_name(answers: dict, policy: JevPolicy) -> tuple[str, bool | None, list[str]]:
    """(판정, 음식 여부, 사유 코드). 음식 여부가 불확실하면 비음식으로 강제하지 않고 보류한다."""
    p_food = answers["is_food"]["noul"]
    if p_food <= policy.not_food_max:
        return REVISE, False, ["NOT_FOOD"]
    if p_food < policy.food_min:
        return ABSTAIN, None, []
    outcome, codes = _defects(
        answers,
        {
            "noise_left": "NOISE_LEFT",
            "typo_left": "TYPO_LEFT",
            "changed_dish": "CHANGED_DISH",
            "dropped_modifier": "DROPPED_MODIFIER",
        },
        policy,
        {},
    )
    return outcome, True, codes


def desc_score(probabilities: dict[str, float], policy: JevPolicy) -> int | None:
    """Jev Score 등급 확률(0~2) → 기존 1~3 정수. 확률 가중 평균(score)을 int 로 자르지 않는다.

    최상 등급 확률이 desc_pass_min 이상일 때만 3점, 하위 두 등급 합이 desc_fail_min 이상이면
    더 확률이 높은 쪽(0→1점, 1→2점), 둘 다 아니면 None(보류)."""
    p0, p1, p2 = (float(probabilities[k]) for k in ("0", "1", "2"))
    if p2 >= policy.desc_pass_min:
        return 3
    if p0 + p1 >= policy.desc_fail_min:
        return 1 if p0 > p1 else 2
    return None


def decide_description(answers: dict, policy: JevPolicy) -> tuple[str, int | None, list[str]]:
    score = desc_score(answers["quality"]["probabilities"], policy)
    if score is None:
        return ABSTAIN, None, []
    codes = [
        code
        for q, code in (("other_dish", "OTHER_DISH"), ("wrong_fact", "WRONG_FACT"), ("unnatural", "UNNATURAL_EXPRESSION"))
        if answers[q]["noul"] >= policy.defect_min
    ]
    if score == 3:
        # 등급은 최상인데 결함 질문이 강하게 걸리면 서로 모순 — 보류해 LLM 에 넘긴다.
        return (ABSTAIN, None, []) if codes else (PASS, 3, [])
    return REVISE, score, codes


def decide_ingredients(answers: dict, substances: list[dict], policy: JevPolicy) -> tuple[str, list[str]]:
    hidden = [a["noul"] for q, a in answers.items() if q.startswith("hidden_")]
    unrelated = any(
        s["inclusionPercent"] >= policy.listed_min_percent and answers[f"listed_{s['code']}"]["noul"] <= policy.unrelated_max
        for s in substances
    )
    spicy = answers["spiciness_wrong"]["noul"]
    codes = (
        ["MISSING_INGREDIENT"] * any(p >= policy.missing_min for p in hidden)
        + ["UNRELATED_INGREDIENT"] * unrelated
        + ["SPICINESS_WRONG"] * (spicy >= policy.defect_min)
    )
    if codes:
        return REVISE, codes
    if any(p >= policy.missing_abstain_min for p in hidden) or spicy >= policy.defect_pass_max:
        return ABSTAIN, []
    return PASS, []


# ===== HTTP =====

_NO_RETRY = {400, 401, 403, 404, 413, 415, 422}


def _retry_after(resp: httpx.Response) -> float | None:
    try:
        return float(resp.headers["retry-after"])
    except (KeyError, ValueError):
        return None


async def ask(
    settings: JevSettings,
    state: dict,
    questions: dict,
    transport: httpx.AsyncBaseTransport | None = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    on_attempt: Callable[[], None] = lambda: None,
) -> dict:
    """System One 요청 1건. 재시도 예산을 다 쓰면 원인별 예외를 던진다.

    클라이언트를 요청마다 만든다 — Lambda 는 invoke 마다 asyncio.run 으로 새 루프를 쓰므로
    전역 AsyncClient 는 닫힌 루프에 묶인다.
    # ponytail: 요청마다 TLS 핸드셰이크, 지연이 문제되면 invoke 단위 클라이언트로 올린다
    """
    body = {"model": settings.model, "state": state, "questions": questions}
    headers = {"Authorization": f"Bearer {settings.api_key}"}
    last: Exception | None = None
    transient = False
    wait = 0.0
    for attempt in range(settings.max_attempts):
        if attempt:
            await sleep(wait)
        wait = min(settings.backoff_seconds * 2**attempt, settings.max_backoff_seconds)
        on_attempt()
        try:
            async with httpx.AsyncClient(timeout=settings.timeout_seconds, transport=transport) as client:
                resp = await client.post(JEV_URL, json=body, headers=headers)
        except httpx.TimeoutException as e:
            last, transient = e, True
            continue
        except httpx.TransportError as e:
            last, transient = e, False
            continue
        if resp.is_success:
            try:
                data = resp.json()
                missing = set(questions) - set(data["answers"])
            except (json.JSONDecodeError, KeyError, TypeError) as e:
                raise JevRequestError(f"Jev 응답 형식 오류: {resp.text[:300]}") from e
            if missing:
                raise JevRequestError(f"Jev 응답에 답 누락: {sorted(missing)}")
            return data
        if resp.status_code in _NO_RETRY:
            raise JevRequestError(f"Jev {resp.status_code}: {resp.text[:300]}")
        transient = resp.status_code == 429
        last = httpx.HTTPStatusError(f"Jev {resp.status_code}", request=resp.request, response=resp)
        server_wait = _retry_after(resp)
        if server_wait is not None:
            if server_wait > settings.max_backoff_seconds:
                # 서버가 허용한 시점보다 일찍 재시도해 마지막 시도를 버리지 않는다 — 예산 밖이면 바로 넘긴다.
                break
            wait = server_wait
    if transient:
        raise JevTransientError(f"Jev timeout/429 재시도 소진: {last}") from last
    raise JevOutageError(f"Jev 장애 재시도 소진: {last}") from last


# ===== 검수 함수 3곳 — ContentFns 와 같은 시그니처 =====


class JevReviews:
    """Jev 검수 + 기존 LLM 대체. llm 은 같은 시그니처의 기존 검수 함수 3개를 가진 ContentFns."""

    def __init__(self, settings: JevSettings, llm, policy: JevPolicy = JevPolicy(), transport=None, sleep=asyncio.sleep):
        self.settings, self.llm, self.policy = settings, llm, policy
        self._transport, self._sleep = transport, sleep
        self.requests = 0  # 재시도 포함 실제 HTTP 요청 수 — 평가 집계용

    def _count(self):
        self.requests += 1

    async def _ask(self, node: str, state: dict, questions: dict):
        """(응답 or None, trace). 장애면 응답 None — 호출자가 LLM 으로 대체한다."""
        started = time.monotonic()
        trace = {
            "node": node,
            "engine": "jev",
            "policy_version": POLICY_VERSION,
            "policy": asdict(self.policy),
            "question_hash": question_hash(questions),
            "requested_model": self.settings.model,
        }
        before = self.requests
        try:
            data = await ask(self.settings, state, questions, self._transport, self._sleep, self._count)
        except JevOutageError as e:
            # 장애는 판단 보류가 아니다 — outcome 을 비워 보류율에 섞지 않는다(대체 호출률에는 잡힌다).
            trace.update(outcome=None, fallback="outage", error=str(e)[:300])
            return None, trace
        finally:
            trace["latency_ms"] = round((time.monotonic() - started) * 1000)
            trace["requests"] = self.requests - before
        trace.update(model=data["model"], usage=data.get("usage"), answers=data["answers"])
        return data["answers"], trace

    async def _fallback(self, trace: dict, cause: str, call):
        started = time.monotonic()
        result = await call()
        trace.update(fallback=cause, fallback_latency_ms=round((time.monotonic() - started) * 1000))
        result._trace = trace
        return result

    async def review_name(self, original: str, cleaned: str) -> NameReview:
        answers, trace = await self._ask("review_name", {"original": original, "cleaned": cleaned}, NAME_QUESTIONS)
        call = lambda: self.llm.review_name(original, cleaned)  # noqa: E731
        if answers is None:
            return await self._fallback(trace, "outage", call)
        outcome, is_food, codes = decide_name(answers, self.policy)
        trace.update(outcome=outcome, reason_codes=codes)
        if outcome == ABSTAIN:
            return await self._fallback(trace, "abstain", call)
        if is_food is False:
            result = NameReview(reason="음식 메뉴명이 아님(Jev)", is_food=False, passed=False)
        else:
            result = NameReview(reason=feedback_for(codes) or "통과", is_food=True, passed=outcome == PASS)
        result._trace = trace
        return result

    async def review_description(self, name: str, description: str) -> DescReview:
        answers, trace = await self._ask(
            "review_description", {"name": name, "description": description}, DESC_QUESTIONS
        )
        call = lambda: self.llm.review_description(name, description)  # noqa: E731
        if answers is None:
            return await self._fallback(trace, "outage", call)
        outcome, score, codes = decide_description(answers, self.policy)
        trace.update(outcome=outcome, reason_codes=codes, mapped_score=score)
        if outcome == ABSTAIN:
            return await self._fallback(trace, "abstain", call)
        if outcome == REVISE and not codes:
            # 미달인데 무엇을 고칠지 모름 — 지어내지 않고 LLM 에서 구체 사유를 받는다.
            return await self._fallback(trace, "no_reason", call)
        result = DescReview(reason=feedback_for(codes) or "통과", score=score)
        result._trace = trace
        return result

    async def review_ingredients(self, name: str, ingredients: dict) -> IngredientsReview:
        # 성분 목록은 state 가 아니라 질문으로 간다(성분 하나 = 질문 하나). 후보 밖 코드는 valid_substances 가 이미 걸렀다.
        substances = ingredients["substances"]
        state = {"name": name, "spiciness": ingredients["spiciness"]}
        answers, trace = await self._ask("review_ingredients", state, ingredient_questions(substances))
        trace["question_version"] = INGREDIENT_QUESTION_VERSION
        call = lambda: self.llm.review_ingredients(name, ingredients)  # noqa: E731
        if answers is None:
            return await self._fallback(trace, "outage", call)
        outcome, codes = decide_ingredients(answers, substances, self.policy)
        trace.update(outcome=outcome, reason_codes=codes)
        if outcome == ABSTAIN:
            return await self._fallback(trace, "abstain", call)
        result = IngredientsReview(reason=feedback_for(codes) or "통과", passed=outcome == PASS)
        result._trace = trace
        return result


def with_jev_reviews(llm_fns, reviews: JevReviews, nodes=("name", "description", "ingredients")):
    """기존 LLM ContentFns 에서 고른 검수 노드만 Jev 로 바꾼다. 생성 함수와 나머지 검수는 그대로다."""
    return llm_fns._replace(**{f"review_{n}": getattr(reviews, f"review_{n}") for n in nodes})
