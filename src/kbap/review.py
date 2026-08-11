"""음식 콘텐츠 최종 검수 배치 — 설정·kbap 클라이언트·채점 프롬프트·판정·그래프·실행기.

이관 과도기용이다: 아직 스프링 배치가 생성한 콘텐츠(PENDING_REVIEW)를 검수해 반영한다.
생성까지 kbap.content 그래프로 전량 이관되면 이 배치는 은퇴하고, 채점 프롬프트·판정
로직은 content 그래프의 검수 노드가 이미 재사용하고 있다."""

# Lambda 런타임은 3.12(어노테이션 즉시 평가) — Verdict 등 전방 참조가 임포트에서 죽는다.
from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict
import asyncio
import httpx
import json
import logging
import os
import yaml

from dotenv import load_dotenv
from langchain_core.exceptions import OutputParserException
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy, default_retry_on
from pydantic import BaseModel
from pydantic import BaseModel, Field
from pydantic import ValidationError


class Thresholds(BaseModel):
    # 모델 점수가 0~100이므로 임계값도 같은 범위여야 한다. 음수면 모두 통과해
    # 기피성분 미달 건까지 REVIEWED로 넘어가고, 100을 초과하면 모두 탈락한다.
    description: int = Field(ge=0, le=100)
    avoidance: int = Field(ge=0, le=100)


class AppConfig(BaseModel):
    kbap_base_url: str
    kbap_token: str
    model: str
    avoidance_model: str
    thresholds: Thresholds
    # 0이면 Semaphore(0)이 모든 코루틴을 영구히 막아 무인 배치가 조용히 멈춘다.
    concurrency: int = Field(ge=1)
    timeout_seconds: int = Field(default=120, ge=1)


def load_config(path: str = "config.yaml") -> AppConfig:
    # 상위 디렉터리까지 탐색해 .env를 읽으므로 notebooks/에서 실행한 노트북도 루트 .env를 찾는다.
    # override=False이므로 기존 환경변수가 우선하며 cron/CI가 설정한 값을 .env가 덮지 않는다.
    load_dotenv()
    with open(path) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]
    return AppConfig(
        kbap_base_url=raw["kbap_api"]["base_url"],
        kbap_token=os.environ["KBAP_API_TOKEN"],
        model=llm["model"],
        avoidance_model=llm.get("avoidance_model", llm["model"]),
        thresholds=Thresholds(**raw["thresholds"]),
        concurrency=raw["concurrency"],
        timeout_seconds=llm.get("timeout_seconds", 120),
    )


# admin API 는 2026-08-11 개정으로 URI 버전(/api/v1) 대신 X-API-Version 헤더 버저닝을
# 쓴다 — 구 경로는 제거됐다. base_url에는 호스트만 지정한다.
CONTENT_REVIEWS = "/api/admin/foods/content-reviews"
FOOD_CONTENTS = "/api/admin/foods/contents"


class DuplicateIngestError(Exception):
    """이미 COMPLETE 인 outboxId(FOOD-004) — 재시도해도 결과가 같은 종료 신호."""


class KbapClient:
    def __init__(self, base_url: str, token: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}", "X-API-Version": "1.0"},
            timeout=30.0,
        )

    async def fetch_review_candidates(self, limit: int) -> list[dict]:
        """PENDING_REVIEW 상태의 음식 목록을 조회한다. 항목 키는 foodId / contentReviewAttempts."""
        resp = await self._client.get(CONTENT_REVIEWS, params={"limit": limit})
        resp.raise_for_status()
        # BaseResponse<AdminFoodContentReviewTargetsResponse> — {success, payload:{items:[...]}}
        return resp.json()["payload"]["items"]

    async def post_review_result(self, food_id: int, verdict: Verdict) -> dict:
        """검수 결과를 반영한다. 재시도 소진 여부 판단과 컬럼 비우기는 kbap이 담당한다.

        반영 후 {foodId, contentStatus, contentReviewAttempts,
        contentReviewRejectionReason} 상태를 반환한다. 탈락 건이 재생성 단계로 갔는지
        REVIEW_REJECTED로 갔는지는 이 값으로만 알 수 있다.
        """
        body: dict = {"passed": verdict.passed}
        if not verdict.passed:
            body["rejectedFields"] = verdict.rejected_fields
            body["reason"] = verdict.reason
        resp = await self._client.post(f"{CONTENT_REVIEWS}/{food_id}", json=body)
        resp.raise_for_status()
        return resp.json()["payload"]

    async def post_food_content(self, payload: dict) -> None:
        """완성/실패 판정을 음식 단건으로 적재한다 (agenthub wiki/langchain-food-ingest-contract.md).

        동일 outboxId 가 이미 COMPLETE 이면 서버가 409 + code=FOOD-004 를 주는데,
        이것만 DuplicateIngestError 로 구분한다 — 재시도해도 결과가 같아 호출자가
        정상 종료(ACK)해야 하는 유일한 실패다. 분기는 HTTP 상태나 message 문자열이
        아니라 응답 본문의 code 로만 한다. 그 밖의 응답은 전부 예외 — 호출자가
        실패로 보고해 재시도(→소진 시 DLQ) 경로를 탄다.
        """
        resp = await self._client.post(FOOD_CONTENTS, json=payload)
        if resp.is_success:
            return
        try:
            code = resp.json().get("code")
        except (json.JSONDecodeError, AttributeError):
            code = None
        if code == "FOOD-004":
            raise DuplicateIngestError(f"이미 처리된 적재 요청: {payload.get('outboxId')}")
        resp.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()


# kbap LanguageCode에서 ko를 제외한 9개 언어이며 순서까지 일치해야 한다.
TARGET_LANGS = ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]

# 기피성분 후보 코드는 kbap AvoidanceSubstanceCode enum(= avoidance_substance 시드)과 같아야 한다.
# 생성기(SpringAiFoodAvoidanceAssessmentClient)는 이 목록에서만 코드를 선택하므로 검수기도
# 같은 목록을 알아야 "후보에 없어 넣지 못한 성분"을 누락으로 오인해 감점하지 않는다.
# 카탈로그 API가 없어 하드코딩한다. enum이 바뀌면 여기도 갱신한다.
AVOIDANCE_CODES = """EGG(계란) MILK(우유) DAIRY(유제품) GOAT_MILK(산양유) BUTTER(버터) GHEE(기버터)
CHEESE(치즈) GELATIN(젤라틴) RENNET(레닛) HONEY(꿀) CARMINE(카민) PEANUT(땅콩) WALNUT(호두)
PINE_NUT(잣) ALMOND(아몬드) CASHEW(캐슈넛) PISTACHIO(피스타치오) HAZELNUT(헤이즐넛)
MACADAMIA(마카다미아) PECAN(피칸) BRAZIL_NUT(브라질너트) CHESTNUT(밤) SESAME(참깨)
SUNFLOWER_SEED(해바라기씨) MUSTARD(겨자) WHEAT(밀) BUCKWHEAT(메밀) BARLEY(보리) RYE(호밀)
OAT(귀리) CORN(옥수수) SOY(대두) LUPIN(루핀) PEA(완두콩) CHICKPEA(병아리콩) LENTIL(렌틸콩)
SHRIMP(새우) SALTED_SHRIMP(새우젓) CRAB(게) CRAYFISH(가재) LOBSTER(랍스터) SQUID(오징어)
OCTOPUS(문어) OYSTER(굴) OYSTER_SAUCE(굴소스) ABALONE(전복) MUSSEL(홍합) CLAM(조개)
SHORT_NECK_CLAM(바지락) SCALLOP(가리비) SEAFOOD(해산물) FISH(생선) MACKEREL(고등어) SALMON(연어)
TUNA(참치) COD(대구) ANCHOVY(멸치) FISH_SAUCE(액젓) BROTH(육수) DASHI(다시) BEEF(소고기)
PORK(돼지고기) LARD(라드) TALLOW(우지) CHICKEN(닭고기) POULTRY(가금류) PEACH(복숭아)
TOMATO(토마토) CELERY(셀러리) POTATO(감자) CARROT(당근) ONION(양파) GARLIC(마늘) SCALLION(파)
CHIVE(부추) WILD_CHIVE(달래) ASAFOETIDA(흥거) ALCOHOL(알코올) MIRIN(미림) COOKING_WINE(맛술)
SULFITES(아황산류)"""

# 템플릿 본문은 kbap.prompts 에 모여 있다. 여기 함수들은 변수 조립만 담당한다.
#
# 형식 검증(글자 수, 9개 언어 존재 여부, spiciness 범위)은 상위 kbap 배치가
# PENDING_REVIEW로 올리기 전에 끝낸다(Food.needsNameTranslations / assessAvoidance).
# 여기서 다시 검증하면 모델의 주의력만 분산되고 배치가 보장한 값을 감점할 수 있어
# 각 템플릿에 "내용만 판단" 지시를 넣는다. 단, 언어 태그와 실제 표기 언어의 일치는
# 내용 검증이므로 유지한다.
from kbap.prompts import (
    REVIEW_AVOIDANCE_TEMPLATE,
    REVIEW_DESCRIPTION_TEMPLATE,
    render_prompt,
)


# structured output은 필드 순서대로 생성되므로 reason을 score보다 앞에 둔다. score가
# 먼저면 모델이 근거를 세우기 전에 숫자부터 생성한다. 기피성분 스모크 테스트에서 주요 성분(PORK)
# 누락을 6회 중 2회만 찾았지만 순서를 바꾼 뒤 개선됐다.
class FieldScore(BaseModel):
    reason: str
    score: int = Field(ge=0, le=100)


def description_prompt(food: dict) -> str:
    return render_prompt(
        "food-review-description",
        REVIEW_DESCRIPTION_TEMPLATE,
        name=food["koreanName"],
        description=food["description"],
    )


def avoidance_prompt(food: dict) -> str:
    # 명칭 규약: 프롬프트·변수는 ingredients — 음식 입장에선 재료, 사용자 입장에선 기피 재료.
    # 파이썬 식별자·kbap 필드(avoidanceSubstances)는 Spring enum 계약이라 그대로 둔다.
    return render_prompt(
        "food-review-ingredients",
        REVIEW_AVOIDANCE_TEMPLATE,
        name=food["koreanName"],
        ingredients=json.dumps(food["avoidanceSubstances"], ensure_ascii=False),
        spiciness=food["spiciness"],
        candidate_codes=AVOIDANCE_CODES,
    )


def init_model(name: str, timeout: int):
    """모델명 문자열로 chat model을 생성한다. 테스트가 LLM 패키지 없이 실행되도록 함수 안에서 가져온다.

    "gemini-*"는 provider를 자동으로 추론하지 못하는 버전이 있어 명시한다.
    gpt-* 등 다른 제공자로 바꿀 때는 "openai:gpt-5-mini"처럼 "provider:model" 형식으로 설정한다.
    timeout을 설정하지 않아 호출 하나가 멈추면 무인 배치에서 해당 세마포어 슬롯을
    계속 점유해 gather 전체가 멈추므로 반드시 설정한다.
    """
    from langchain.chat_models import init_chat_model

    if ":" in name:
        provider, model = name.split(":", 1)
        return init_chat_model(model, model_provider=provider, timeout=timeout)
    if name.startswith("gemini"):
        return init_chat_model(name, model_provider="google_genai", timeout=timeout)
    return init_chat_model(name, timeout=timeout)


def make_scorers(config):
    """실제 LLM 기반 채점기."""
    base = init_model(config.model, config.timeout_seconds)
    avoid = init_model(config.avoidance_model, config.timeout_seconds)
    desc_llm = base.with_structured_output(FieldScore)
    avoid_llm = avoid.with_structured_output(FieldScore)

    async def description(food: dict) -> FieldScore:
        return await desc_llm.ainvoke(description_prompt(food))

    async def avoidance(food: dict) -> FieldScore:
        return await avoid_llm.ainvoke(avoidance_prompt(food))

    return Scorers(description=description, avoidance=avoidance)


# 사유 길이 상한. kbap도 Food.MAX_REJECTION_REASON_LINES(10) / _LENGTH(1000)로 자르지만,
# 서버는 "앞 10줄 → 앞 1000자" 순으로 처리하므로 여기서 미리 다듬어 문장이 줄 중간에서
# 잘리지 않게 한다.
MAX_NOTE_CHARS = 1000
MAX_REASON_CHARS = 200

# 채점 필드군을 kbap FoodContentReviewField에 매핑한다.
# 배치가 함께 재생성하는 단위에 맞춘다.
#   DESCRIPTION만 비우면 배치가 설명과 설명 번역을 함께 다시 만든다(FoodContentItemProcessor).
#   AVOIDANCE_SUBSTANCES와 spiciness는 한 번에 산출되므로 둘 다 비운다.
REJECTED_FIELDS = {
    "description": ["DESCRIPTION"],
    "avoidance": ["AVOIDANCE_SUBSTANCES", "SPICINESS"],
}


class Verdict(BaseModel):
    passed: bool
    rejected_fields: list[str]
    scores: dict
    reason: str | None = None


def decide(
    description_score: FieldScore,
    avoidance_score: FieldScore,
    thresholds: Thresholds,
) -> Verdict:
    """필드군별 점수를 kbap 검수 결과로 변환한다.

    재시도 허용 횟수는 kbap이 정한다(Food.rejectContentReview가
    contentReviewAttempts를 보고 컬럼을 비울지 REVIEW_REJECTED로 보낼지 결정한다).
    여기서는 통과 여부와 문제 필드만 판단한다.
    """
    failed: list[str] = []
    if description_score.score < thresholds.description:
        failed.append("description")

    if avoidance_score.score < thresholds.avoidance:
        failed.append("avoidance")

    scores = {
        "description": description_score.score,
        "avoidance": avoidance_score.score,
    }

    if not failed:
        return Verdict(passed=True, rejected_fields=[], scores=scores)

    rejected_fields = [f for group in failed for f in REJECTED_FIELDS[group]]
    return Verdict(
        passed=False,
        rejected_fields=rejected_fields,
        scores=scores,
        reason=_reason(failed, description_score, avoidance_score),
    )


def _reason(
    failed: list[str],
    description_score: FieldScore,
    avoidance_score: FieldScore,
) -> str:
    """재시도를 모두 소진했을 때 사람이 읽을 수 있는 개조식 사유를 만든다."""
    lines: list[str] = []
    if "description" in failed:
        lines.append(
            f"- 설명({description_score.score}점): {description_score.reason[:MAX_REASON_CHARS]}"
        )
    if "avoidance" in failed:
        lines.append(
            f"- 기피성분·매운맛({avoidance_score.score}점): "
            f"{avoidance_score.reason[:MAX_REASON_CHARS]}"
        )
    return "\n".join(lines)[:MAX_NOTE_CHARS]


def _retryable(exc: Exception) -> bool:
    """LangGraph 기본 정책에 structured output 파싱 실패를 추가한다."""
    if isinstance(exc, (OutputParserException, ValidationError)):
        return True
    return default_retry_on(exc)


class Scorers(NamedTuple):
    description: Callable[[dict], Awaitable[FieldScore]]
    avoidance: Callable[[dict], Awaitable[FieldScore]]


class ReviewState(TypedDict, total=False):
    food: dict
    description_score: FieldScore
    avoidance_score: FieldScore
    verdict: Verdict
    applied: dict  # kbap 반영 후 상태. dry_run이면 없다.


def build_graph(scorers: Scorers, client, thresholds: Thresholds, dry_run: bool = False):
    # 설명 검수 — 음식을 사실대로 설명하는지 0~100점으로 채점한다(팬아웃 분기 1)
    async def score_description(state: ReviewState):
        return {"description_score": await scorers.description(state["food"])}

    # 기피성분·매운맛 검수 — 주요 성분 누락을 최우선으로 채점한다(팬아웃 분기 3)
    async def score_avoidance(state: ReviewState):
        return {"avoidance_score": await scorers.avoidance(state["food"])}

    # 종합 판정 — 두 점수를 임계값과 비교해 통과/탈락과 문제 필드를 결정한다(순수 함수, LLM 없음)
    def aggregate(state: ReviewState):
        return {
            "verdict": decide(
                description_score=state["description_score"],
                avoidance_score=state["avoidance_score"],
                thresholds=thresholds,
            )
        }

    # 결과 반영 — 판정을 kbap API로 POST해 DB 상태를 변경한다(dry_run이면 건너뜀)
    async def report(state: ReviewState):
        if dry_run:
            return {}
        applied = await client.post_review_result(state["food"]["foodId"], state["verdict"])
        return {"applied": applied}

    # LLM 노드만 재시도한다. aggregate는 순수 함수이고 report 실패는 실행기의 보류 처리로 충분하다.
    # 기본 retry_on은 ValueError 계열을 제외하며 structured output 파싱 실패
    # (OutputParserException·pydantic ValidationError)는 모두 ValueError 하위라 재시도되지 않는다.
    # 모델이 다시 생성하면 성공하는 경우가 많아 명시적으로 포함한다.
    retry = RetryPolicy(max_attempts=2, retry_on=_retryable)
    g = StateGraph(ReviewState)
    g.add_node("score_description", score_description, retry_policy=retry)
    g.add_node("score_avoidance", score_avoidance, retry_policy=retry)
    g.add_node("aggregate", aggregate)
    g.add_node("report", report)

    g.add_edge(START, "score_description")
    g.add_edge(START, "score_avoidance")
    # 리스트 엣지는 join으로, 두 채점이 모두 끝난 뒤 aggregate를 실행한다
    g.add_edge(["score_description", "score_avoidance"], "aggregate")
    g.add_edge("aggregate", "report")
    g.add_edge("report", END)
    return g.compile()


log = logging.getLogger("kbap.review")


async def run_batch(graph, foods: list[dict], concurrency: int, callbacks: list) -> dict[str, int]:
    # concurrency는 동시에 실행할 그래프 수를 제한한다. 그래프 하나가 3개 필드군으로
    # 팬아웃하므로 동시 LLM 호출 상한은 concurrency * 3이며 Gemini RPM 할당량도 이 값으로 확인한다.
    sem = asyncio.Semaphore(concurrency)

    async def one(food: dict):
        async with sem:
            return await graph.ainvoke({"food": food}, config={"callbacks": callbacks})

    results = await asyncio.gather(*(one(f) for f in foods), return_exceptions=True)

    # 탈락 건을 재생성으로 돌릴지 REVIEW_REJECTED로 보낼지는 kbap이 정한다.
    # 여기서는 통과/탈락/보류만 센다.
    counts = {"PASS": 0, "FAIL": 0, "HELD": 0}
    for food, result in zip(foods, results):
        if isinstance(result, BaseException):
            # LLM/POST 실패 시 판정을 보류한다. PENDING_REVIEW에 남아 다음 실행에서 다시 시도한다.
            counts["HELD"] += 1
            # foodId가 없는 계약 위반도 이 경로로 들어오므로 get으로 읽는다.
            log.warning(
                "보류 foodId=%s (%s): %s", food.get("foodId"), food.get("koreanName"), result
            )
        else:
            verdict = result["verdict"]
            key = "PASS" if verdict.passed else "FAIL"
            counts[key] += 1
            applied = result.get("applied") or {}
            log.info(
                "%s foodId=%s (%s) %s -> %s",
                key,
                food.get("foodId"),
                food.get("koreanName"),
                verdict.rejected_fields or "",
                applied.get("contentStatus", "dry-run"),
            )
    return counts


def make_callbacks() -> list:
    # LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY / LANGFUSE_HOST 환경변수로 연결한다
    if not os.environ.get("LANGFUSE_PUBLIC_KEY"):
        return []
    from langfuse.langchain import CallbackHandler

    return [CallbackHandler()]
