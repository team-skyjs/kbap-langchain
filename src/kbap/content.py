"""이 프로젝트의 본체 — 음식 콘텐츠 생성·검수 전체 그래프와 SQS Lambda 핸들러.

스캔된 이름 하나를 받아 이름 정제 → 이름 검수(비음식 즉시 거절, 품질 미달은 최대 3회
재정제) → 4개 분기 생성(이름 번역 / 설명→설명 번역 / 기피성분·매운맛 / 검색용 긴 설명)
→ 분기별 검수(실패 시 1회 재생성) → 종합 판정까지 끝낸다.
kbap(Spring) 콘텐츠 배치가 하던 생성·검수를 이 그래프가 전부 대체하는 것이 목표이며,
이관이 끝나면 스프링에는 수집·저장·SQS 발행·관리자 승인 UI만 남는다.

생성 프롬프트는 kbap(Spring) infra/llm/food 운영 프롬프트를 옮긴 것이다.
원본이 바뀌면 여기도 맞춘다. JSON 형식 지시는 structured output이 대신한다.

SQS 메시지 계약: body = {"scannedName": <str>, "foodId": <int>, "outboxId": <int>}
foodId·outboxId 는 필수 — 적재 API 본문에 그대로 왕복시킨다(foodId 로 대상 특정,
outboxId 로 조건부 UPDATE 게이트). 없으면 계약 위반으로 DLQ 행이다.
처리 결과는 kbap 적재 API로 POST 한다 — 계약은 agenthub wiki/langchain-food-ingest-contract.md.
부분 실패 보고(ReportBatchItemFailures)가 활성화되어 있어야 한다."""

from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict
import asyncio
import json
import logging
import os
import re
import yaml

from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy
from pydantic import BaseModel, Field, field_validator, model_validator

from kbap.review import DuplicateIngestError

from kbap.prompts import (
    INGREDIENTS_GEN_TEMPLATE,
    INGREDIENTS_REVIEW_TEMPLATE,
    DESC_REVIEW_TEMPLATE,
    DESC_TEMPLATE,
    DESC_TR_TEMPLATE,
    LONG_DESC_TEMPLATE,
    NAME_REVIEW_TEMPLATE,
    JUDGE_TEMPLATE,
    NAME_TR_TEMPLATE,
    render_prompt,
)
from kbap.review import (
    AVOIDANCE_CODES,
    TARGET_LANGS,
    _retryable,
    make_callbacks,
)


# is_food 를 passed 와 분리한 이유: 비음식은 재정제해도 음식이 안 되므로
# 재정제 루프 없이 즉시 종료해 콜을 아낀다.
class NameReview(BaseModel):
    reason: str
    is_food: bool
    passed: bool


# 설명 검수는 1~3점, 3점만 통과 — 0~100은 기준이 모호해 임계값 근처 판정이 흔들렸다.
DESC_PASS_SCORE = 3


class DescReview(BaseModel):
    reason: str
    score: int = Field(ge=1, le=3)


class IngredientsReview(BaseModel):
    reason: str
    passed: bool


class JudgeVerdict(BaseModel):
    reason: str
    passed: bool
    rejected_fields: list[str] = []
    # kbap 적재 계약의 failureKind — LLM 출력이 아니라 세 생성 지점의 코드가 찍는다.
    # (정제 조기 종료→NOT_FOOD, 기피성분 가드→INGREDIENT_GUARD, 그 외 judge 탈락→JUDGE_REJECTED)
    failure_kind: str | None = None


class ContentFns(NamedTuple):
    clean_name: Callable[[str, str], Awaitable[dict]]  # (원본, 검수 탈락 피드백)
    review_name: Callable[[str, str], Awaitable[NameReview]]  # (스캔 원본, 정제된 이름)
    generate_name_translations: Callable[[str], Awaitable[dict]]
    generate_description: Callable[[str, str], Awaitable[str]]
    generate_description_translations: Callable[[str, str], Awaitable[dict]]
    generate_ingredients: Callable[[str, str], Awaitable[dict]]
    generate_long_description: Callable[[str], Awaitable[str]]
    review_description: Callable[[str, str], Awaitable[DescReview]]  # 한국어 설명만 검수
    review_ingredients: Callable[[str, dict], Awaitable[IngredientsReview]]
    judge: Callable[[dict], Awaitable[JudgeVerdict]]


class ContentState(TypedDict, total=False):
    food_name: str  # 스캔 원본
    cleaned_name: str
    clean_reason: str
    name_review: NameReview
    name_attempts: int
    name_feedback: str
    name_translations: dict[str, str]
    description: str
    description_translations: dict[str, str]
    description_score: DescReview
    description_attempts: int
    description_feedback: str
    ingredients: dict
    ingredient_review: IngredientsReview
    ingredient_attempts: int
    ingredient_feedback: str
    long_description: str  # 벡터 검색 메타데이터 (화면 미노출)
    verdict: JudgeVerdict


def build_content_graph(fns: ContentFns, max_attempts: int = 2, name_max_attempts: int = 3):
    """max_attempts=2이면 최초 호출 1회와 재시도 1회 후 실패 결과·사유를 담아 판정한다.
    name_max_attempts 는 이름 정제의 총 실행 횟수 상한(초회 포함)이다."""

    # ① 이름 정제 — 스캔 원본에서 노이즈·오타를 제거한 이름을 모든 후속 노드에 전달한다.
    async def clean_name(state: ContentState):
        fix = await fns.clean_name(state["food_name"], state.get("name_feedback", ""))
        return {
            "cleaned_name": fix["name"],
            "clean_reason": fix.get("reason", ""),
            "name_attempts": state.get("name_attempts", 0) + 1,
        }

    # ①' 이름 검수 — 단일 음식 메뉴명인지(is_food)·그대로 써도 되는지(passed) 판정한다.
    # 여기를 통과해야만 생성 분기가 시작된다.
    async def review_name(state: ContentState):
        r = await fns.review_name(state["food_name"], state["cleaned_name"])
        return {"name_review": r, "name_feedback": r.reason}

    # 비음식·재정제 소진 — NOT_FOOD verdict 를 만들고 그래프를 끝내 생성·검수 콜을 아낀다.
    async def reject_name(state: ContentState):
        reason = state.get("name_feedback", "") or "음식 메뉴명이 아님"
        return {
            "verdict": JudgeVerdict(
                passed=False, reason=f"콘텐츠 생성 부적합: {reason}", failure_kind="NOT_FOOD"
            )
        }

    # 비음식은 재정제해도 음식이 안 되므로 즉시 거절, 불통과만 재정제 루프를 탄다.
    def route_after_review_name(state: ContentState):
        r = state["name_review"]
        if not r.is_food:
            return "reject_name"
        if not r.passed:
            if state["name_attempts"] < name_max_attempts:
                return "clean_name"
            return "reject_name"
        return [
            "generate_name_translations",
            "generate_description",
            "generate_ingredients",
            "generate_long_description",
        ]

    # ② 이름 번역 생성(분기 1) — 9개 언어, 검수 없이 1회. 9키 전수·빈 값 금지는 pydantic이 보장한다.
    async def generate_name_translations(state: ContentState):
        tr = await fns.generate_name_translations(state["cleaned_name"])
        return {"name_translations": tr}

    # ③ 한국어 설명 생성(분기 2 시작) — 한 문장, 255자 이하
    async def generate_description(state: ContentState):
        desc = await fns.generate_description(state["cleaned_name"], state.get("description_feedback", ""))
        return {"description": desc, "description_attempts": state.get("description_attempts", 0) + 1}

    # 설명 검수 — 한국어 설명만 본다. 번역보다 먼저 실행해 탈락한 설명의 번역을 만들지 않는다.
    async def review_description(state: ContentState):
        score = await fns.review_description(state["cleaned_name"], state["description"])
        return {"description_score": score, "description_feedback": score.reason}

    # 설명 번역 생성(분기 2 후속) — 확정된 설명으로 1회만
    async def generate_description_translations(state: ContentState):
        tr = await fns.generate_description_translations(state["cleaned_name"], state["description"])
        return {"description_translations": tr}

    # ④ 기피성분·매운맛 생성(분기 3) — 81종 후보에서 선택하고 후보 밖 코드는 저장 전에 거른다
    async def generate_ingredients(state: ContentState):
        ing = await fns.generate_ingredients(state["cleaned_name"], state.get("ingredient_feedback", ""))
        return {"ingredients": ing, "ingredient_attempts": state.get("ingredient_attempts", 0) + 1}

    # 기피성분·매운맛 검수(분기 3 후속) — 불통과면 재생성
    async def review_ingredients(state: ContentState):
        r = await fns.review_ingredients(state["cleaned_name"], state["ingredients"])
        return {"ingredient_review": r, "ingredient_feedback": r.reason}

    # ④' 검색용 긴 설명(분기 4) — 벡터 DB 메타데이터. 안전 직결이 아니라 검수·judge 판정에
    # 참여하지 않는다. 검색 품질 문제가 실측되면 그때 검수를 붙인다.
    async def generate_long_description(state: ContentState):
        text = await fns.generate_long_description(state["cleaned_name"])
        return {"long_description": text}

    # ⑤ 종합 판정 — 분기가 모두 끝난 뒤(defer) 검수 결과·사유로 PASS/FAIL을 최종 결정한다.
    # 기피성분만은 결정론적 fail-closed: 프롬프트의 "불통과는 탈락" 지시는 강제력이
    # 없고, 알레르기 데이터는 오통과가 누락보다 위험하다(valid_substances 필터와 같은 철학).
    async def judge(state: ContentState):
        verdict = await fns.judge(dict(state))
        if not verdict.passed:
            # LLM 출력의 failure_kind 는 신뢰하지 않고 덮어쓴다 — 계약 밖 enum 값 차단.
            verdict = verdict.model_copy(update={"failure_kind": "JUDGE_REJECTED"})
        ingredient = state["ingredient_review"]
        if verdict.passed and not ingredient.passed:
            verdict = JudgeVerdict(
                passed=False,
                reason=f"기피성분 검수 불통과: {ingredient.reason}",
                rejected_fields=["avoidance"],
                failure_kind="INGREDIENT_GUARD",
            )
        return {"verdict": verdict}

    # 설명은 3점 만점(DESC_PASS_SCORE)만 통과, 기피성분은 통과/불통과 — 미달이면 1회 재생성.
    # 재시도 소진 시에도 번역은 만들어 judge 로 보낸다 — judge 가 통과시키면 payload 에 필요하다.
    def route_after_review_description(state: ContentState) -> str:
        failed = state["description_score"].score < DESC_PASS_SCORE
        if failed and state["description_attempts"] < max_attempts:
            return "generate_description"
        return "generate_description_translations"

    def route_after_review_ingredients(state: ContentState) -> str:
        if not state["ingredient_review"].passed and state["ingredient_attempts"] < max_attempts:
            return "generate_ingredients"
        return "judge"

    # structured output 파싱 실패 시 재시도 — kbap_review.graph와 같은 이유이며 모두 LLM 노드다.
    retry = RetryPolicy(max_attempts=2, retry_on=_retryable)
    g = StateGraph(ContentState)
    g.add_node("clean_name", clean_name, retry_policy=retry)
    g.add_node("review_name", review_name, retry_policy=retry)
    g.add_node("reject_name", reject_name)  # LLM 없음 — 재시도 불필요
    g.add_node("generate_name_translations", generate_name_translations, retry_policy=retry)
    g.add_node("generate_description", generate_description, retry_policy=retry)
    g.add_node("generate_description_translations", generate_description_translations, retry_policy=retry)
    g.add_node("review_description", review_description, retry_policy=retry)
    g.add_node("generate_ingredients", generate_ingredients, retry_policy=retry)
    g.add_node("review_ingredients", review_ingredients, retry_policy=retry)
    g.add_node("generate_long_description", generate_long_description, retry_policy=retry)
    # defer=True — 세 분기의 재시도 횟수가 달라도 모두 끝난 뒤 정확히 한 번 실행된다.
    g.add_node("judge", judge, defer=True, retry_policy=retry)

    g.add_edge(START, "clean_name")
    g.add_edge("clean_name", "review_name")
    g.add_conditional_edges(
        "review_name",
        route_after_review_name,
        [
            "clean_name",
            "reject_name",
            "generate_name_translations",
            "generate_description",
            "generate_ingredients",
            "generate_long_description",
        ],
    )
    g.add_edge("reject_name", END)
    g.add_edge("generate_name_translations", "judge")  # 검수 없이 join만
    g.add_edge("generate_description", "review_description")  # 검수 먼저 — 탈락한 설명의 번역을 만들지 않는다
    g.add_edge("generate_description_translations", "judge")
    g.add_edge("generate_ingredients", "review_ingredients")
    g.add_edge("generate_long_description", "judge")  # 검수 없이 join만 — judge는 defer라 4분기를 기다린다
    g.add_conditional_edges(
        "review_description",
        route_after_review_description,
        ["generate_description", "generate_description_translations"],
    )
    g.add_conditional_edges(
        "review_ingredients",
        route_after_review_ingredients,
        ["generate_ingredients", "judge"],
    )
    g.add_edge("judge", END)
    return g.compile()


LANGS = ", ".join(TARGET_LANGS)


# 생성 결과 형식 검증 — 스프링이 LLM 응답 경계에서 require 로 잡던 규칙의 이관.
# pydantic ValidationError 는 _retryable 에 걸려 그래프 RetryPolicy 가 재생성을 유도한다.
# scoring.FieldScore와 같은 이유로 reason을 결과보다 앞에 두어 근거를 먼저 세우게 한다.
# 마침표 금지 정책이 잘라내는 종결 부호 — 반각·전각·일본어/중국어 마침표.
_TRAILING_PERIODS = ".。．"


class TranslationItem(BaseModel):
    lang: str
    text: str = Field(min_length=1)

    @field_validator("text")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        # 설명과 같은 마침표 금지 정책 — 언어별 종결 부호까지 잘라낸다.
        v = v.strip().rstrip(_TRAILING_PERIODS)
        if not v:
            raise ValueError("번역 값은 빈 문자열일 수 없다")
        return v


class Translations(BaseModel):
    reason: str
    items: list[TranslationItem]

    @model_validator(mode="after")
    def _all_target_langs(self):
        # 9개 언어 전수·초과 금지 (스프링 TargetLanguageTexts 와 동일한 경계)
        got = {i.lang for i in self.items}
        if got != set(TARGET_LANGS) or len(self.items) != len(TARGET_LANGS):
            raise ValueError(f"9개 언어 전수가 필요하다: got={sorted(got)}")
        return self


class IngredientItem(BaseModel):
    code: str
    inclusionPercent: int = Field(ge=0, le=100)


class IngredientsGen(BaseModel):
    reason: str
    items: list[IngredientItem]
    spiciness: int = Field(ge=0, le=10)


class LongDescGen(BaseModel):
    long_description: str

    @field_validator("long_description")
    @classmethod
    def _not_blank_or_over_limit(cls, v: str) -> str:
        # DescGen과 달리 여러 문장이므로 마침표를 제거하지 않는다.
        v = v.strip()
        if not v or v == "설명 준비 중":
            raise ValueError("상세 설명은 비거나 플레이스홀더일 수 없다")
        if len(v) > 500:
            raise ValueError("상세 설명은 500자 이하여야 한다")
        return v


class DescGen(BaseModel):
    description: str

    @field_validator("description")
    @classmethod
    def _not_blank_or_placeholder(cls, v: str) -> str:
        # 마침표 금지는 프롬프트로 지시하되, 모델이 어겨도 재생성 대신 잘라낸다(콘텐츠 정책).
        # 길이 검사는 마침표 제거 후에 한다 — Field(max_length)는 validator보다 먼저 실행돼
        # "255자 + 마침표" 입력을 잘라내지 못하고 탈락시킨다.
        v = v.strip().rstrip(_TRAILING_PERIODS)
        if not v or v == "설명 준비 중":
            raise ValueError("설명은 비거나 플레이스홀더일 수 없다")
        if len(v) > 255:
            raise ValueError("설명은 255자 이하여야 한다")
        return v


# 과거 모델이 후보 밖 코드를 반환한 사례가 있어 저장 전에 걸러내며, 프롬프트에만 의존하지 않는다.
# 중복 코드는 첫 번째 값만 남긴다.
_CODE = re.compile(r"([A-Z_]+)\(")
VALID_CODES = frozenset(_CODE.findall(AVOIDANCE_CODES))


def valid_substances(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out = []
    for item in items:
        code = item["code"]
        # 0% 항목 제거 — 스프링 RiskLevel 은 1~100만 허용, 저장 전 동일하게 거른다.
        if code not in VALID_CODES or code in seen or item["inclusionPercent"] < 1:
            continue
        seen.add(code)
        out.append(item)
    return out


def _feedback_block(feedback: str) -> str:
    if not feedback:
        return ""
    return f"""

## 이전 시도 탈락 사유 — 반드시 반영해 다시 만드세요
{feedback}"""


def name_tr_prompt(name: str, feedback: str) -> str:
    return render_prompt(
        "food-name-translation",
        NAME_TR_TEMPLATE,
        name=name,
        langs=LANGS,
        feedback_block=_feedback_block(feedback),
    )


def desc_prompt(name: str, feedback: str) -> str:
    return render_prompt(
        "food-description", DESC_TEMPLATE, name=name, feedback_block=_feedback_block(feedback)
    )


def long_desc_prompt(name: str) -> str:
    return render_prompt("food-long-description", LONG_DESC_TEMPLATE, name=name)


def name_review_prompt(original: str, name: str) -> str:
    return render_prompt("food-name-review", NAME_REVIEW_TEMPLATE, original=original, name=name)


def desc_tr_prompt(name: str, description: str) -> str:
    return render_prompt(
        "food-description-translation",
        DESC_TR_TEMPLATE,
        name=name,
        description=description,
        langs=LANGS,
    )


def ingredients_gen_prompt(name: str, feedback: str) -> str:
    return render_prompt(
        "food-ingredients",
        INGREDIENTS_GEN_TEMPLATE,
        name=name,
        candidate_codes=AVOIDANCE_CODES,
        feedback_block=_feedback_block(feedback),
    )


def desc_review_prompt(name: str, description: str) -> str:
    return render_prompt(
        "food-description-review", DESC_REVIEW_TEMPLATE, name=name, description=description
    )


def ingredients_review_prompt(name: str, ingredients: dict) -> str:
    return render_prompt(
        "food-ingredients-review",
        INGREDIENTS_REVIEW_TEMPLATE,
        name=name,
        ingredients=json.dumps(ingredients["substances"], ensure_ascii=False),
        spiciness=ingredients["spiciness"],
        candidate_codes=AVOIDANCE_CODES,
    )


def judge_prompt(state: dict) -> str:
    # 조건·반복이 있는 부분은 코드에서 미리 계산해 변수로 넣는다 — Langfuse 템플릿엔 로직이 없다.
    desc = state["description_score"]
    ingredient = state["ingredient_review"]
    lines = "\n".join(
        [
            f"- 설명: {desc.score}/3점 — {desc.reason}",
            f"- 기피성분·매운맛: {'통과' if ingredient.passed else '불통과'} — {ingredient.reason}",
        ]
    )
    return render_prompt(
        "food-judge", JUDGE_TEMPLATE, name=state["cleaned_name"], score_lines=lines
    )


def make_fns(
    model: str,
    timeout: int,
    judge_model: str | None = None,
    gen_model: str | None = None,
    namefix_model: str | None = None,
    callbacks: list = [],
):
    """실제 LLM 기반 노드 모음. 테스트가 LLM 패키지 없이 실행되도록 함수 안에서 가져온다.

    역할별 모델 분리: judge 는 짧은 점수 요약을 읽고 통과/탈락만 정하므로 저렴한 모델로
    내릴 수 있고(judge_model), 이름 정제는 _plausible·길이 방어선이 있어 실패해도 원본
    유지로 끝나므로 역시 내릴 수 있다(namefix_model). 품질이 곧 결과물인 번역·설명 생성만
    올릴 수 있다(gen_model). 기피성분 생성·검수는 안전 데이터라 기본 모델(model)을 유지한다.

    callbacks 는 노드를 그래프 밖에서 직접 호출할 때(노트북) 트레이싱용이다.
    그래프로 실행하면 ainvoke 의 config 콜백이 전파되므로 비워 둔다 — 둘 다 주면 중복 기록된다.
    """
    from kbap.namefix import clean_one, make_normalizer
    from kbap.review import init_model

    def _bind(llm):
        return llm.with_config(callbacks=callbacks) if callbacks else llm

    base = init_model(model, timeout)
    gen_base = init_model(gen_model, timeout) if gen_model and gen_model != model else base
    judge_llm = _bind(init_model(judge_model or model, timeout).with_structured_output(JudgeVerdict))
    tr_llm = _bind(gen_base.with_structured_output(Translations))
    desc_llm = _bind(gen_base.with_structured_output(DescGen))
    long_desc_llm = _bind(gen_base.with_structured_output(LongDescGen))
    ingredients_llm = _bind(base.with_structured_output(IngredientsGen))
    # 이름 검수 — 비음식 거름이 콘텐츠 파이프라인의 입구 게이트라 기본 모델을 쓴다.
    name_review_llm = _bind(base.with_structured_output(NameReview))
    desc_review_llm = _bind(base.with_structured_output(DescReview))
    ingredients_review_llm = _bind(base.with_structured_output(IngredientsReview))
    normalize = make_normalizer(namefix_model or model, timeout, callbacks=callbacks)

    async def clean_name(name: str, feedback: str) -> dict:
        # ponytail: 앵커 없이 시작 — 수집 데이터가 쌓이면 확정된 음식명을 앵커로 주입
        return await clean_one(name, [], normalize, feedback)

    async def review_name(original: str, cleaned: str) -> NameReview:
        return await name_review_llm.ainvoke(name_review_prompt(original, cleaned))

    async def generate_name_translations(name: str) -> dict:
        result: Translations = await tr_llm.ainvoke(name_tr_prompt(name, ""))
        return {i.lang: i.text for i in result.items}

    async def generate_description(name: str, feedback: str) -> str:
        result: DescGen = await desc_llm.ainvoke(desc_prompt(name, feedback))
        return result.description

    async def generate_long_description(name: str) -> str:
        result: LongDescGen = await long_desc_llm.ainvoke(long_desc_prompt(name))
        return result.long_description

    async def generate_description_translations(name: str, description: str) -> dict:
        result: Translations = await tr_llm.ainvoke(desc_tr_prompt(name, description))
        return {i.lang: i.text for i in result.items}

    async def generate_ingredients(name: str, feedback: str) -> dict:
        # ponytail: 단일 모델 — kbap은 여러 모델의 fan-out 결과를 minAgreement(2)로 종합한다.
        # 안전 데이터의 정확도가 부족하면 해당 합의 구조나 web_search 도구를 이식한다.
        result: IngredientsGen = await ingredients_llm.ainvoke(ingredients_gen_prompt(name, feedback))
        return {
            "substances": valid_substances([i.model_dump() for i in result.items]),
            "spiciness": result.spiciness,
        }

    async def review_description(name: str, description: str) -> DescReview:
        return await desc_review_llm.ainvoke(desc_review_prompt(name, description))

    async def review_ingredients(name: str, ingredients: dict) -> IngredientsReview:
        return await ingredients_review_llm.ainvoke(ingredients_review_prompt(name, ingredients))

    async def judge(state: dict) -> JudgeVerdict:
        return await judge_llm.ainvoke(judge_prompt(state))

    return ContentFns(
        clean_name=clean_name,
        review_name=review_name,
        generate_name_translations=generate_name_translations,
        generate_description=generate_description,
        generate_description_translations=generate_description_translations,
        generate_ingredients=generate_ingredients,
        generate_long_description=generate_long_description,
        review_description=review_description,
        review_ingredients=review_ingredients,
        judge=judge,
    )


log = logging.getLogger("kbap.content")


def load_graph(config_path: str | None = None):
    """config.yaml 하나로 콘텐츠 그래프를 한 번에 조립하는 원샷 팩토리.

    노트북·스크립트·Lambda 가 전부 이 함수를 쓴다:
        from kbap.content import load_graph
        graph = load_graph()          # 루트에서
        graph = load_graph("../config.yaml")  # notebooks/ 에서
    """
    from dotenv import load_dotenv

    load_dotenv()
    with open(config_path or os.environ.get("CONFIG_PATH", "config.yaml")) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]
    fns = make_fns(
        llm["model"],
        llm.get("timeout_seconds", 120),
        judge_model=llm.get("judge_model"),
        gen_model=llm.get("gen_model"),
        namefix_model=llm.get("namefix_model"),
    )
    return build_content_graph(fns)


_graph = None


def _cached_graph():
    """Lambda 콜드 스타트 시 그래프를 한 번만 만든다."""
    global _graph
    if _graph is None:
        _graph = load_graph()
    return _graph


def build_ingest_payload(state: dict, food_id: int, outbox_id: int) -> dict:
    """그래프 최종 상태 → kbap 적재 계약 요청 본문 (agenthub wiki/langchain-food-ingest-contract.md).

    foodId·outboxId 는 큐 메시지에서 받은 값을 그대로 왕복시킨다 — 서버는 이름이
    아니라 foodId 로만 대상을 찾고(2026-08-11 개정), outboxId 로 조건부 UPDATE 를
    게이트한다.
    displayName 은 스캔 원본이 아닌 정제된 이름이다 — 콘텐츠가 그 이름 기준으로
    생성됐고, 스캔 노이즈("김치찌게 8,000원")가 DB 행 이름이 되면 안 된다.
    """
    verdict = state["verdict"]
    if not verdict.passed:
        return {
            "outboxId": outbox_id,
            "foodId": food_id,
            "displayName": state["cleaned_name"],
            "passed": False,
            "failureKind": verdict.failure_kind,
            "reason": verdict.reason,
        }
    ingredients = state["ingredients"]
    return {
        "outboxId": outbox_id,
        "foodId": food_id,
        "displayName": state["cleaned_name"],
        "passed": True,
        "description": state["description"],
        "spiciness": ingredients["spiciness"],
        "nameTranslations": state["name_translations"],
        "descriptionTranslations": state["description_translations"],
        "ingredients": [
            {"code": i["code"], "inclusion_percent": i["inclusionPercent"]}
            for i in ingredients["substances"]
        ],
        "longDescription": state["long_description"],
    }


async def process_event(
    event: dict, graph, kbap, concurrency: int, callbacks: list = []
) -> list[str]:
    """레코드를 동시에 처리해 kbap에 적재하고 실패한 messageId 목록을 반환한다."""
    sem = asyncio.Semaphore(concurrency)

    async def one(record: dict) -> str | None:
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            # foodId·outboxId 는 적재 API 필수 왕복 값 — 없거나 타입·값이 잘못된
            # 메시지는 적재 불가라 그래프(LLM 비용)를 태우기 전에 여기서 거른다.
            food_id, outbox_id = body["foodId"], body["outboxId"]
            name = body["scannedName"]
            if not all(
                type(i) is int and i > 0 for i in (food_id, outbox_id)
            ) or not (isinstance(name, str) and name.strip()):
                raise ValueError(f"foodId={food_id!r} outboxId={outbox_id!r} scannedName={name!r}")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
            log.warning("계약 위반 메시지 %s: %s", message_id, e)
            return message_id
        async with sem:
            try:
                result = await graph.ainvoke(
                    {"food_name": name}, config={"callbacks": callbacks}
                )
            except Exception:
                # 런타임 에러는 POST 없이 재시도로 — FAILED에 인프라 장애를 섞지 않는다(계약 전제).
                log.exception("그래프 실패 foodId=%s (%s)", food_id, name)
                return message_id
        verdict = result["verdict"]
        try:
            await kbap.post_food_content(build_ingest_payload(result, food_id, outbox_id))
        except DuplicateIngestError:
            # 같은 outboxId 의 결과가 이미 반영됨(FOOD-004) — 먼저 온 처리가 이겼다.
            # 재시도해도 결과가 같으니 성공 ACK 로 끝내 SQS 재시도·DLQ 를 막는다.
            log.warning("중복 적재 스킵 outboxId=%s foodId=%s (%s)", outbox_id, food_id, name)
            return None
        except Exception:
            # 그 밖의 4xx·5xx·응답 해석 실패·네트워크 오류 — 재시도(→소진 시 DLQ)로.
            log.exception("적재 실패 outboxId=%s foodId=%s (%s)", outbox_id, food_id, name)
            return message_id
        log.info("foodId=%s (%s) passed=%s %s", food_id, name, verdict.passed, verdict.reason)
        return None

    results = await asyncio.gather(*(one(r) for r in event.get("Records", [])))
    return [message_id for message_id in results if message_id]


async def _consume(event, graph, concurrency: int, callbacks: list) -> list[str]:
    # invoke마다 이벤트 루프가 새로 생기므로 httpx 클라이언트는 루프 단위로 만들고 닫는다.
    from kbap.review import KbapClient, kbap_base_url

    with open(os.environ.get("CONFIG_PATH", "config.yaml")) as f:
        raw = yaml.safe_load(f)
    kbap = KbapClient(kbap_base_url(raw), os.environ["KBAP_API_TOKEN"])
    try:
        return await process_event(event, graph, kbap, concurrency, callbacks)
    finally:
        await kbap.aclose()


def handler(event, context):
    graph = _cached_graph()
    concurrency = int(os.environ.get("GRAPH_CONCURRENCY", "20"))
    failed = asyncio.run(_consume(event, graph, concurrency, make_callbacks()))

    # Lambda는 응답 반환 후 프로세스를 멈추므로 atexit이 호출되지 않아 여기서 직접 flush한다.
    from langfuse import get_client

    get_client().flush()
    return {"batchItemFailures": [{"itemIdentifier": message_id} for message_id in failed]}
