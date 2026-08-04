from kbap_review.__main__ import run_batch
from kbap_review.config import Thresholds
from kbap_review.graph import Scorers, build_graph
from kbap_review.scoring import TARGET_LANGS, FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


class FakeClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append(food_id)
        return {"foodId": food_id, "contentStatus": "REVIEWED" if verdict.passed else "INCOMPLETE"}


def scorers_failing_for(bad_id: int | None) -> Scorers:
    async def d(food):
        if bad_id is not None and food.get("foodId") == bad_id:
            raise RuntimeError("LLM down")
        if food.get("fail_desc"):
            return FieldScore(score=30, reason="설명 문제")
        return FieldScore(score=90, reason="ok")

    async def t(food):
        return {lang: FieldScore(score=90, reason="ok") for lang in TARGET_LANGS}

    async def a(food):
        return FieldScore(score=90, reason="ok")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_run_batch_counts_and_isolates_failures():
    client = FakeClient()
    graph = build_graph(scorers_failing_for(bad_id=2), client, TH)
    foods = [
        {"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0},
        {"foodId": 2, "koreanName": "불고기", "contentReviewAttempts": 0},
        {"foodId": 3, "koreanName": "비빔밥", "contentReviewAttempts": 0},
    ]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    # foodId=2 는 LLM 실패 → 보류(HELD), POST 없음. 나머지는 PASS + POST.
    assert counts == {"PASS": 2, "FAIL": 0, "HELD": 1}
    assert sorted(client.posts) == [1, 3]


async def test_run_batch_counts_all_outcomes():
    client = FakeClient()
    graph = build_graph(scorers_failing_for(bad_id=2), client, TH)
    foods = [
        {"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0},
        {"foodId": 2, "koreanName": "불고기", "contentReviewAttempts": 0},
        {"foodId": 3, "koreanName": "비빔밥", "contentReviewAttempts": 0, "fail_desc": True},
    ]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    # 탈락 건이 재생성으로 갈지 REVIEW_REJECTED 로 갈지는 kbap 이 정하므로 여기선 FAIL 하나로 센다.
    assert counts == {"PASS": 1, "FAIL": 1, "HELD": 1}
    assert sorted(client.posts) == [1, 3]


async def test_run_batch_missing_food_id_is_held():
    # kbap 응답에 foodId 가 없으면(계약 위반) 조용히 넘어가지 않고 보류로 떨어진다.
    client = FakeClient()
    graph = build_graph(scorers_failing_for(bad_id=None), client, TH)
    foods = [{"koreanName": "떡볶이", "contentReviewAttempts": 0}]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    assert counts == {"PASS": 0, "FAIL": 0, "HELD": 1}
    assert client.posts == []
