from kbap_review.__main__ import run_batch
from kbap_review.config import Thresholds
from kbap_review.graph import Scorers, build_graph
from kbap_review.scoring import FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


class FakeClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append(food_id)


def scorers_failing_for(bad_id: int) -> Scorers:
    async def d(food):
        if food["id"] == bad_id:
            raise RuntimeError("LLM down")
        return FieldScore(score=90, reason="ok")

    async def t(food):
        return {"en": FieldScore(score=90, reason="ok")}

    async def a(food):
        return FieldScore(score=90, reason="ok")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_run_batch_counts_and_isolates_failures():
    client = FakeClient()
    graph = build_graph(scorers_failing_for(bad_id=2), client, TH)
    foods = [
        {"id": 1, "koreanName": "김치찌개", "reviewAttempts": 0},
        {"id": 2, "koreanName": "불고기", "reviewAttempts": 0},
        {"id": 3, "koreanName": "비빔밥", "reviewAttempts": 0},
    ]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    # id=2는 LLM 실패 → 보류(HELD), POST 없음. 나머지는 PASS + POST.
    assert counts == {"PASS": 2, "RETRY": 0, "REJECT": 0, "HELD": 1}
    assert sorted(client.posts) == [1, 3]
