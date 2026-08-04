from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy

from kbap_review.aggregate import Verdict, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore


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
                review_attempts=state["food"]["reviewAttempts"],
                description_score=state["description_score"],
                translation_scores=state["translation_scores"],
                avoidance_score=state["avoidance_score"],
                thresholds=thresholds,
            )
        }

    async def report(state: ReviewState):
        if not dry_run:
            await client.post_review_result(state["food"]["id"], state["verdict"])
        return {}

    # LLM 노드만 재시도 — aggregate는 순수 함수, report 실패는 실행기의 보류 처리로 충분.
    retry = RetryPolicy(max_attempts=2)
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
