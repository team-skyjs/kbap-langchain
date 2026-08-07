"""이 프로젝트의 본체 — 음식 콘텐츠 생성·검수 전체 그래프와 SQS Lambda 핸들러.

스캔된 이름 하나를 받아 이름 정제 → 3개 분기 생성(이름 번역 / 설명→설명 번역 /
기피성분·매운맛) → 분기별 검수(실패 시 1회 재생성) → 종합 판정까지 끝낸다.
kbap(Spring) 콘텐츠 배치가 하던 생성·검수를 이 그래프가 전부 대체하는 것이 목표이며,
이관이 끝나면 스프링에는 수집·저장·SQS 발행·관리자 승인 UI만 남는다.

생성 프롬프트는 kbap(Spring) infra/llm/food 운영 프롬프트를 옮긴 것이다.
원본이 바뀌면 여기도 맞춘다. JSON 형식 지시는 structured output이 대신한다.

SQS 메시지 계약(초안): body = {"foodId": <int>, "scannedName": <str>}
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

from kbap.prompts import (
    INGREDIENTS_GEN_TEMPLATE,
    DESC_REVIEW_TEMPLATE,
    DESC_TEMPLATE,
    DESC_TR_TEMPLATE,
    JUDGE_TEMPLATE,
    NAME_TR_REVIEW_TEMPLATE,
    NAME_TR_TEMPLATE,
    render_prompt,
)
from kbap.review import (
    AVOIDANCE_CODES,
    TARGET_LANGS,
    FieldScore,
    Thresholds,
    TranslationScores,
    _retryable,
    avoidance_prompt,
    lang_scores,
    make_callbacks,
)


class JudgeVerdict(BaseModel):
    reason: str
    passed: bool
    rejected_fields: list[str] = []


class ContentFns(NamedTuple):
    clean_name: Callable[[str], Awaitable[dict]]
    generate_name_translations: Callable[[str, str], Awaitable[dict]]
    generate_description: Callable[[str, str], Awaitable[str]]
    generate_description_translations: Callable[[str, str], Awaitable[dict]]
    generate_ingredients: Callable[[str, str], Awaitable[dict]]
    review_name_translations: Callable[[str, dict], Awaitable[FieldScore]]
    review_description: Callable[[str, str, dict], Awaitable[FieldScore]]
    review_ingredients: Callable[[str, dict], Awaitable[FieldScore]]
    judge: Callable[[dict], Awaitable[JudgeVerdict]]


class ContentState(TypedDict, total=False):
    food_name: str  # 스캔 원본
    cleaned_name: str
    clean_reason: str
    name_translations: dict[str, str]
    name_translation_score: FieldScore
    name_translation_attempts: int
    name_translation_feedback: str
    description: str
    description_translations: dict[str, str]
    description_score: FieldScore
    description_attempts: int
    description_feedback: str
    ingredients: dict
    ingredient_score: FieldScore
    ingredient_attempts: int
    ingredient_feedback: str
    verdict: JudgeVerdict


def build_content_graph(fns: ContentFns, thresholds: Thresholds, max_attempts: int = 2):
    """max_attempts=2이면 최초 호출 1회와 재시도 1회 후 실패 점수·사유를 담아 판정한다."""

    # ① 이름 정제 — 스캔 원본에서 노이즈·오타를 제거한 이름을 모든 후속 노드에 전달한다
    async def clean_name(state: ContentState):
        fix = await fns.clean_name(state["food_name"])
        return {"cleaned_name": fix["name"], "clean_reason": fix.get("reason", "")}

    # ② 이름 번역 생성(분기 1) — 9개 언어. 재시도 시 탈락 사유(name_translation_feedback)를 프롬프트에 넣는다
    async def generate_name_translations(state: ContentState):
        tr = await fns.generate_name_translations(state["cleaned_name"], state.get("name_translation_feedback", ""))
        return {"name_translations": tr, "name_translation_attempts": state.get("name_translation_attempts", 0) + 1}

    # 이름 번역 검수 — 언어별 점수 중 최저점을 적용하고 탈락 사유를 재생성 피드백으로 저장한다
    async def review_name_translations(state: ContentState):
        score = await fns.review_name_translations(state["cleaned_name"], state["name_translations"])
        return {"name_translation_score": score, "name_translation_feedback": score.reason}

    # ③ 한국어 설명 생성(분기 2 시작) — 한 문장, 255자 이하
    async def generate_description(state: ContentState):
        desc = await fns.generate_description(state["cleaned_name"], state.get("description_feedback", ""))
        return {"description": desc, "description_attempts": state.get("description_attempts", 0) + 1}

    # 설명 번역 생성(분기 2 후속) — 설명을 재생성할 때마다 번역도 다시 생성한다
    async def generate_description_translations(state: ContentState):
        tr = await fns.generate_description_translations(state["cleaned_name"], state["description"])
        return {"description_translations": tr}

    # 설명·설명 번역 검수 — 내용의 사실성과 번역 품질을 하나의 점수로 판정한다
    async def review_description(state: ContentState):
        score = await fns.review_description(
            state["cleaned_name"], state["description"], state["description_translations"]
        )
        return {"description_score": score, "description_feedback": score.reason}

    # ④ 기피성분·매운맛 생성(분기 3) — 81종 후보에서 선택하고 후보 밖 코드는 저장 전에 거른다
    async def generate_ingredients(state: ContentState):
        ing = await fns.generate_ingredients(state["cleaned_name"], state.get("ingredient_feedback", ""))
        return {"ingredients": ing, "ingredient_attempts": state.get("ingredient_attempts", 0) + 1}

    # 기피성분·매운맛 검수 — 안전과 직결된 주요 성분 누락을 최우선으로 감점한다
    async def review_ingredients(state: ContentState):
        score = await fns.review_ingredients(state["cleaned_name"], state["ingredients"])
        return {"ingredient_score": score, "ingredient_feedback": score.reason}

    # ⑤ 종합 판정 — 세 분기가 모두 끝난 뒤(defer) 점수·사유로 PASS/FAIL을 최종 결정한다
    async def judge(state: ContentState):
        return {"verdict": await fns.judge(dict(state))}

    def _route(score_key: str, attempts_key: str, threshold: int, retry_target: str):
        def route(state: ContentState) -> str:
            failed = state[score_key].score < threshold
            if failed and state[attempts_key] < max_attempts:
                return retry_target
            return "judge"

        return route

    # structured output 파싱 실패 시 재시도 — kbap_review.graph와 같은 이유이며 모두 LLM 노드다.
    retry = RetryPolicy(max_attempts=2, retry_on=_retryable)
    g = StateGraph(ContentState)
    g.add_node("clean_name", clean_name, retry_policy=retry)
    g.add_node("generate_name_translations", generate_name_translations, retry_policy=retry)
    g.add_node("review_name_translations", review_name_translations, retry_policy=retry)
    g.add_node("generate_description", generate_description, retry_policy=retry)
    g.add_node("generate_description_translations", generate_description_translations, retry_policy=retry)
    g.add_node("review_description", review_description, retry_policy=retry)
    g.add_node("generate_ingredients", generate_ingredients, retry_policy=retry)
    g.add_node("review_ingredients", review_ingredients, retry_policy=retry)
    # defer=True — 세 분기의 재시도 횟수가 달라도 모두 끝난 뒤 정확히 한 번 실행된다.
    g.add_node("judge", judge, defer=True, retry_policy=retry)

    g.add_edge(START, "clean_name")
    g.add_edge("clean_name", "generate_name_translations")
    g.add_edge("clean_name", "generate_description")
    g.add_edge("clean_name", "generate_ingredients")
    g.add_edge("generate_name_translations", "review_name_translations")
    g.add_edge("generate_description", "generate_description_translations")  # 설명을 재생성하면 번역도 함께 다시 만든다
    g.add_edge("generate_description_translations", "review_description")
    g.add_edge("generate_ingredients", "review_ingredients")
    g.add_conditional_edges(
        "review_name_translations",
        _route("name_translation_score", "name_translation_attempts", thresholds.translations, "generate_name_translations"),
        ["generate_name_translations", "judge"],
    )
    g.add_conditional_edges(
        "review_description",
        _route("description_score", "description_attempts", thresholds.description, "generate_description"),
        ["generate_description", "judge"],
    )
    g.add_conditional_edges(
        "review_ingredients",
        _route("ingredient_score", "ingredient_attempts", thresholds.avoidance, "generate_ingredients"),
        ["generate_ingredients", "judge"],
    )
    g.add_edge("judge", END)
    return g.compile()


LANGS = ", ".join(TARGET_LANGS)


# 생성 결과 형식 검증 — 스프링이 LLM 응답 경계에서 require 로 잡던 규칙의 이관.
# pydantic ValidationError 는 _retryable 에 걸려 그래프 RetryPolicy 가 재생성을 유도한다.
# scoring.FieldScore와 같은 이유로 reason을 결과보다 앞에 두어 근거를 먼저 세우게 한다.
class TranslationItem(BaseModel):
    lang: str
    text: str = Field(min_length=1)

    @field_validator("text")
    @classmethod
    def _not_blank(cls, v: str) -> str:
        # 설명과 같은 마침표 금지 정책 — 언어별 종결 부호(. 。)까지 잘라낸다.
        v = v.strip().rstrip(".。")
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


class DescGen(BaseModel):
    description: str = Field(max_length=255)

    @field_validator("description")
    @classmethod
    def _not_blank_or_placeholder(cls, v: str) -> str:
        # 마침표 금지는 프롬프트로 지시하되, 모델이 어겨도 재생성 대신 잘라낸다(콘텐츠 정책).
        v = v.strip().rstrip(".")
        if not v or v == "설명 준비 중":
            raise ValueError("설명은 비거나 플레이스홀더일 수 없다")
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


def name_tr_review_prompt(name: str, translations: dict, pass_score: int) -> str:
    return render_prompt(
        "food-name-translation-review",
        NAME_TR_REVIEW_TEMPLATE,
        name=name,
        translations=json.dumps(translations, ensure_ascii=False),
        lang_count=len(TARGET_LANGS),
        langs=LANGS,
        pass_score=pass_score,
    )


def desc_review_prompt(name: str, description: str, translations: dict) -> str:
    return render_prompt(
        "food-description-review",
        DESC_REVIEW_TEMPLATE,
        name=name,
        description=description,
        translations=json.dumps(translations, ensure_ascii=False),
    )


def judge_prompt(state: dict, thresholds) -> str:
    scores = {
        "이름 번역": (state["name_translation_score"], thresholds.translations),
        "설명·설명 번역": (state["description_score"], thresholds.description),
        "기피성분·매운맛": (state["ingredient_score"], thresholds.avoidance),
    }
    # 조건·반복이 있는 부분은 코드에서 미리 계산해 변수로 넣는다 — Langfuse 템플릿엔 로직이 없다.
    lines = "\n".join(
        f"- {field}: {s.score}점 (임계값 {th}) — {s.reason}" for field, (s, th) in scores.items()
    )
    return render_prompt(
        "food-judge", JUDGE_TEMPLATE, name=state["cleaned_name"], score_lines=lines
    )


def make_fns(
    model: str,
    timeout: int,
    thresholds,
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
    ingredients_llm = _bind(base.with_structured_output(IngredientsGen))
    score_llm = _bind(base.with_structured_output(FieldScore))
    tr_score_llm = _bind(base.with_structured_output(TranslationScores))
    normalize = make_normalizer(namefix_model or model, timeout, callbacks=callbacks)

    async def clean_name(name: str) -> dict:
        # ponytail: 앵커 없이 시작 — 수집 데이터가 쌓이면 확정된 음식명을 앵커로 주입
        return await clean_one(name, [], normalize)

    async def generate_name_translations(name: str, feedback: str) -> dict:
        result: Translations = await tr_llm.ainvoke(name_tr_prompt(name, feedback))
        return {i.lang: i.text for i in result.items}

    async def generate_description(name: str, feedback: str) -> str:
        result: DescGen = await desc_llm.ainvoke(desc_prompt(name, feedback))
        return result.description

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

    async def review_name_translations(name: str, translations: dict) -> FieldScore:
        result: TranslationScores = await tr_score_llm.ainvoke(
            name_tr_review_prompt(name, translations, thresholds.translations)
        )
        scores = lang_scores(result)  # 누락 언어를 0점으로 채우는 fail-closed 로직 재사용
        worst = min(scores.values(), key=lambda s: s.score)
        # 기준 미만 언어만 reason이 오므로(출력 토큰 절감) 피드백도 그 언어들로만 구성한다.
        # 모델이 지시를 어기고 reason 없이 기준 미만 점수를 준 언어도 점수는 남긴다(fail-closed).
        failing = [
            f"{lang} {s.score}점: {s.reason}".rstrip(": ")
            for lang, s in scores.items()
            if s.reason or s.score < thresholds.translations
        ]
        return FieldScore(score=worst.score, reason="; ".join(failing) or "이상 없음")

    async def review_description(name: str, description: str, translations: dict) -> FieldScore:
        return await score_llm.ainvoke(desc_review_prompt(name, description, translations))

    async def review_ingredients(name: str, ingredients: dict) -> FieldScore:
        food = {
            "koreanName": name,
            "avoidanceSubstances": ingredients["substances"],
            "spiciness": ingredients["spiciness"],
        }
        return await score_llm.ainvoke(avoidance_prompt(food))

    async def judge(state: dict) -> JudgeVerdict:
        return await judge_llm.ainvoke(judge_prompt(state, thresholds))

    return ContentFns(
        clean_name=clean_name,
        generate_name_translations=generate_name_translations,
        generate_description=generate_description,
        generate_description_translations=generate_description_translations,
        generate_ingredients=generate_ingredients,
        review_name_translations=review_name_translations,
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
    thresholds = Thresholds(**raw["thresholds"])
    fns = make_fns(
        llm["model"],
        llm.get("timeout_seconds", 120),
        thresholds,
        judge_model=llm.get("judge_model"),
        gen_model=llm.get("gen_model"),
        namefix_model=llm.get("namefix_model"),
    )
    return build_content_graph(fns, thresholds)


_graph = None


def _cached_graph():
    """Lambda 콜드 스타트 시 그래프를 한 번만 만든다."""
    global _graph
    if _graph is None:
        _graph = load_graph()
    return _graph


async def process_event(event: dict, graph, concurrency: int, callbacks: list = []) -> list[str]:
    """레코드를 동시에 처리하고 실패한 messageId 목록을 반환한다."""
    sem = asyncio.Semaphore(concurrency)

    async def one(record: dict) -> str | None:
        message_id = record["messageId"]
        try:
            body = json.loads(record["body"])
            food_id, name = body["foodId"], body["scannedName"]
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            log.warning("계약 위반 메시지 %s: %s", message_id, e)
            return message_id
        async with sem:
            try:
                result = await graph.ainvoke(
                    {"food_name": name}, config={"callbacks": callbacks}
                )
            except Exception:
                log.exception("그래프 실패 foodId=%s (%s)", food_id, name)
                return message_id
        verdict = result["verdict"]
        # TODO(kbap 계약 확정 시): PASS/FAIL과 사유를 결과 반영 API로 POST — 멱등성을 보장해야 한다.
        log.info("foodId=%s (%s) passed=%s %s", food_id, name, verdict.passed, verdict.reason)
        return None

    results = await asyncio.gather(*(one(r) for r in event.get("Records", [])))
    return [message_id for message_id in results if message_id]


def handler(event, context):
    graph = _cached_graph()
    concurrency = int(os.environ.get("GRAPH_CONCURRENCY", "20"))
    failed = asyncio.run(process_event(event, graph, concurrency, make_callbacks()))

    # Lambda는 응답 반환 후 프로세스를 멈추므로 atexit이 호출되지 않아 여기서 직접 flush한다.
    from langfuse import get_client

    get_client().flush()
    return {"batchItemFailures": [{"itemIdentifier": message_id} for message_id in failed]}
