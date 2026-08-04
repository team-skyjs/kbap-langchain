from kbap_review.config import Thresholds
from kbap_review.graph import Scorers, build_graph
from kbap_review.scoring import TARGET_LANGS, FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)

FOOD = {"id": 7, "koreanName": "김치찌개", "reviewAttempts": 0}


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


async def test_pass_path_posts_pass():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].verdict == "PASS"
    assert client.posts == [(7, state["verdict"])]


async def test_retry_path_posts_failed_fields():
    client = FakeClient()
    graph = build_graph(make_scorers(desc=30), client, TH)

    state = await graph.ainvoke({"food": FOOD})

    (food_id, verdict), = client.posts
    assert verdict.verdict == "RETRY"
    assert verdict.failed_fields == ["description"]


async def test_reject_path_when_attempts_exhausted():
    client = FakeClient()
    graph = build_graph(make_scorers(avoid=10), client, TH)

    state = await graph.ainvoke({"food": {**FOOD, "reviewAttempts": 2}})

    (_, verdict), = client.posts
    assert verdict.verdict == "REJECT"
    assert "성분 사유" in verdict.review_note


async def test_dry_run_does_not_post():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH, dry_run=True)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].verdict == "PASS"
    assert client.posts == []


async def test_scorer_exception_propagates():
    # LLM 실패는 그래프 실행 실패로 전파 — 실행기가 보류 처리 (POST 없음)
    async def boom(food):
        raise RuntimeError("LLM down")

    scorers = make_scorers()._replace(description=boom)
    client = FakeClient()
    graph = build_graph(scorers, client, TH)

    import pytest

    with pytest.raises(RuntimeError):
        await graph.ainvoke({"food": FOOD})
    assert client.posts == []
