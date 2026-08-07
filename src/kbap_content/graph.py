"""이름 정제 → 생성 3갈래 → 검수 → 종합판정 전체 콘텐츠 그래프.

노드 함수는 ContentFns 로 주입한다(kbap_review.graph.Scorers 와 같은 패턴) —
그래프 배선·재시도 루프를 LLM 없이 테스트하기 위해서다.
"""

from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict

from pydantic import BaseModel

from langgraph.graph import END, START, StateGraph

from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore


class JudgeVerdict(BaseModel):
    reason: str
    passed: bool
    rejected_fields: list[str] = []


class ContentFns(NamedTuple):
    clean_name: Callable[[str], Awaitable[dict]]
    gen_name_tr: Callable[[str, str], Awaitable[dict]]
    gen_desc: Callable[[str, str], Awaitable[str]]
    gen_desc_tr: Callable[[str, str], Awaitable[dict]]
    gen_avoid: Callable[[str, str], Awaitable[dict]]
    rev_name_tr: Callable[[str, dict], Awaitable[FieldScore]]
    rev_desc: Callable[[str, str, dict], Awaitable[FieldScore]]
    rev_avoid: Callable[[str, dict], Awaitable[FieldScore]]
    judge: Callable[[dict], Awaitable[JudgeVerdict]]


class ContentState(TypedDict, total=False):
    food_name: str  # 스캔 원본
    cleaned_name: str
    clean_reason: str
    name_translations: dict[str, str]
    nt_score: FieldScore
    nt_attempts: int
    nt_feedback: str
    description: str
    description_translations: dict[str, str]
    desc_score: FieldScore
    desc_attempts: int
    desc_feedback: str
    avoidance: dict
    avoid_score: FieldScore
    avoid_attempts: int
    avoid_feedback: str
    verdict: JudgeVerdict


def build_content_graph(fns: ContentFns, thresholds: Thresholds, max_attempts: int = 2):
    """max_attempts=2 → 최초 1회 + 재시도 1회. 그 뒤에는 실패 점수·사유를 안고 판정으로 간다."""

    async def clean_name(state: ContentState):
        fix = await fns.clean_name(state["food_name"])
        return {"cleaned_name": fix["name"], "clean_reason": fix.get("reason", "")}

    async def gen_name_tr(state: ContentState):
        tr = await fns.gen_name_tr(state["cleaned_name"], state.get("nt_feedback", ""))
        return {"name_translations": tr, "nt_attempts": state.get("nt_attempts", 0) + 1}

    async def rev_name_tr(state: ContentState):
        score = await fns.rev_name_tr(state["cleaned_name"], state["name_translations"])
        return {"nt_score": score, "nt_feedback": score.reason}

    async def gen_desc(state: ContentState):
        desc = await fns.gen_desc(state["cleaned_name"], state.get("desc_feedback", ""))
        return {"description": desc, "desc_attempts": state.get("desc_attempts", 0) + 1}

    async def gen_desc_tr(state: ContentState):
        tr = await fns.gen_desc_tr(state["cleaned_name"], state["description"])
        return {"description_translations": tr}

    async def rev_desc(state: ContentState):
        score = await fns.rev_desc(
            state["cleaned_name"], state["description"], state["description_translations"]
        )
        return {"desc_score": score, "desc_feedback": score.reason}

    async def gen_avoid(state: ContentState):
        avoid = await fns.gen_avoid(state["cleaned_name"], state.get("avoid_feedback", ""))
        return {"avoidance": avoid, "avoid_attempts": state.get("avoid_attempts", 0) + 1}

    async def rev_avoid(state: ContentState):
        score = await fns.rev_avoid(state["cleaned_name"], state["avoidance"])
        return {"avoid_score": score, "avoid_feedback": score.reason}

    async def judge(state: ContentState):
        return {"verdict": await fns.judge(dict(state))}

    def _route(score_key: str, attempts_key: str, threshold: int, retry_target: str):
        def route(state: ContentState) -> str:
            failed = state[score_key].score < threshold
            if failed and state[attempts_key] < max_attempts:
                return retry_target
            return "judge"

        return route

    g = StateGraph(ContentState)
    g.add_node("clean_name", clean_name)
    g.add_node("gen_name_tr", gen_name_tr)
    g.add_node("rev_name_tr", rev_name_tr)
    g.add_node("gen_desc", gen_desc)
    g.add_node("gen_desc_tr", gen_desc_tr)
    g.add_node("rev_desc", rev_desc)
    g.add_node("gen_avoid", gen_avoid)
    g.add_node("rev_avoid", rev_avoid)
    # defer=True — 세 갈래가 서로 다른 횟수로 재시도해도, 전부 끝난 뒤 정확히 한 번 실행된다.
    g.add_node("judge", judge, defer=True)

    g.add_edge(START, "clean_name")
    g.add_edge("clean_name", "gen_name_tr")
    g.add_edge("clean_name", "gen_desc")
    g.add_edge("clean_name", "gen_avoid")
    g.add_edge("gen_name_tr", "rev_name_tr")
    g.add_edge("gen_desc", "gen_desc_tr")  # 설명 재생성 시 번역도 같이 다시 만든다
    g.add_edge("gen_desc_tr", "rev_desc")
    g.add_edge("gen_avoid", "rev_avoid")
    g.add_conditional_edges(
        "rev_name_tr",
        _route("nt_score", "nt_attempts", thresholds.translations, "gen_name_tr"),
        ["gen_name_tr", "judge"],
    )
    g.add_conditional_edges(
        "rev_desc",
        _route("desc_score", "desc_attempts", thresholds.description, "gen_desc"),
        ["gen_desc", "judge"],
    )
    g.add_conditional_edges(
        "rev_avoid",
        _route("avoid_score", "avoid_attempts", thresholds.avoidance, "gen_avoid"),
        ["gen_avoid", "judge"],
    )
    g.add_edge("judge", END)
    return g.compile()
