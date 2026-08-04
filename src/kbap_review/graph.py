from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict

from langchain_core.exceptions import OutputParserException
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy, default_retry_on
from pydantic import ValidationError

from kbap_review.aggregate import Verdict, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore


def _retryable(exc: Exception) -> bool:
    """LangGraph 기본 정책 + structured output 파싱 실패."""
    if isinstance(exc, (OutputParserException, ValidationError)):
        return True
    return default_retry_on(exc)


class Scorers(NamedTuple):
    description: Callable[[dict], Awaitable[FieldScore]]
    translations: Callable[[dict], Awaitable[dict[str, FieldScore]]]
    avoidance: Callable[[dict], Awaitable[FieldScore]]


class ReviewState(TypedDict, total=False):
    food: dict
    description_score: FieldScore
    translation_scores: dict[str, FieldScore]
    avoidance_score: FieldScore
    verdict: Verdict
    applied: dict  # kbap 반영 후 상태. dry_run 이면 없다.


def build_graph(scorers: Scorers, client, thresholds: Thresholds, dry_run: bool = False):
    async def score_description(state: ReviewState):
        return {"description_score": await scorers.description(state["food"])}

    async def score_translations(state: ReviewState):
        return {"translation_scores": await scorers.translations(state["food"])}

    async def score_avoidance(state: ReviewState):
        return {"avoidance_score": await scorers.avoidance(state["food"])}

    def aggregate(state: ReviewState):
        return {
            "verdict": decide(
                description_score=state["description_score"],
                translation_scores=state["translation_scores"],
                avoidance_score=state["avoidance_score"],
                thresholds=thresholds,
            )
        }

    async def report(state: ReviewState):
        if dry_run:
            return {}
        applied = await client.post_review_result(state["food"]["foodId"], state["verdict"])
        return {"applied": applied}

    # LLM 노드만 재시도 — aggregate는 순수 함수, report 실패는 실행기의 보류 처리로 충분.
    # 기본 retry_on 은 ValueError 계열을 제외하는데, structured output 파싱 실패
    # (OutputParserException·pydantic ValidationError)가 전부 ValueError 하위라 한 번도
    # 재시도되지 않는다. 모델이 다시 뽑으면 통과하는 경우가 많아 명시적으로 포함시킨다.
    retry = RetryPolicy(max_attempts=2, retry_on=_retryable)
    g = StateGraph(ReviewState)
    g.add_node("score_description", score_description, retry_policy=retry)
    g.add_node("score_translations", score_translations, retry_policy=retry)
    g.add_node("score_avoidance", score_avoidance, retry_policy=retry)
    g.add_node("aggregate", aggregate)
    g.add_node("report", report)

    g.add_edge(START, "score_description")
    g.add_edge(START, "score_translations")
    g.add_edge(START, "score_avoidance")
    # 리스트 엣지 = join: 세 채점이 모두 끝난 뒤 aggregate 실행
    g.add_edge(["score_description", "score_translations", "score_avoidance"], "aggregate")
    g.add_edge("aggregate", "report")
    g.add_edge("report", END)
    return g.compile()
