import pytest

from kbap_review.config import Thresholds
from kbap_review.graph import Scorers, build_graph
from kbap_review.scoring import TARGET_LANGS, FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)

FOOD = {"foodId": 7, "koreanName": "김치찌개", "contentReviewAttempts": 0}


class FakeClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append((food_id, verdict))


def make_scorers(desc=85, trans=90, avoid=80) -> Scorers:
    async def d(food):
        return FieldScore(score=desc, reason="설명 사유")

    async def t(food):
        return {lang: FieldScore(score=trans, reason="번역 사유") for lang in TARGET_LANGS}

    async def a(food):
        return FieldScore(score=avoid, reason="성분 사유")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_pass_path_posts_with_food_id():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].passed is True
    assert client.posts == [(7, state["verdict"])]


async def test_fail_path_posts_rejected_fields():
    client = FakeClient()
    graph = build_graph(make_scorers(desc=30), client, TH)

    await graph.ainvoke({"food": FOOD})

    ((food_id, verdict),) = client.posts
    assert food_id == 7
    assert verdict.passed is False
    assert verdict.rejected_fields == ["DESCRIPTION"]
    assert "설명 사유" in verdict.reason


async def test_dry_run_does_not_post():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH, dry_run=True)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].passed is True
    assert client.posts == []


async def test_scorer_exception_propagates():
    # LLM 실패는 그래프 실행 실패로 전파 — 실행기가 보류 처리 (POST 없음)
    async def boom(food):
        raise RuntimeError("LLM down")

    scorers = make_scorers()._replace(description=boom)
    client = FakeClient()
    graph = build_graph(scorers, client, TH)

    with pytest.raises(RuntimeError):
        await graph.ainvoke({"food": FOOD})
    assert client.posts == []


async def test_parse_failure_is_retried():
    # structured output 파싱 실패는 ValueError 하위라 LangGraph 기본 정책이 제외한다.
    # retry_on 을 덮어썼으므로 두 번 시도돼야 한다.
    from langchain_core.exceptions import OutputParserException

    calls = 0

    async def flaky(food):
        nonlocal calls
        calls += 1
        raise OutputParserException("malformed structured output")

    scorers = make_scorers()._replace(description=flaky)
    client = FakeClient()
    graph = build_graph(scorers, client, TH)

    with pytest.raises(OutputParserException):
        await graph.ainvoke({"food": FOOD})
    assert calls == 2
    assert client.posts == []
