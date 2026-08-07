import json

from kbap_content.graph import JudgeVerdict
from kbap_content.handler import process_event


class FakeGraph:
    def __init__(self, fail_names=()):
        self.calls = []
        self.fail_names = set(fail_names)

    async def ainvoke(self, state, config=None):
        self.calls.append(state["food_name"])
        if state["food_name"] in self.fail_names:
            raise RuntimeError("LLM down")
        return {**state, "verdict": JudgeVerdict(reason="ok", passed=True)}


def record(message_id: str, food_id: int, name: str) -> dict:
    return {"messageId": message_id, "body": json.dumps({"foodId": food_id, "scannedName": name})}


async def test_all_success_reports_no_failures():
    graph = FakeGraph()
    event = {"Records": [record("m1", 1, "김치찌개"), record("m2", 2, "불고기")]}

    failures = await process_event(event, graph, concurrency=20)

    assert failures == []
    assert sorted(graph.calls) == ["김치찌개", "불고기"]


async def test_partial_failure_reports_only_failed_message():
    # 10건 묶음 소비에서 1건만 실패하면 그 메시지만 재수신돼야 한다 —
    # 전체 재수신은 성공한 9건의 LLM 비용을 다시 태운다.
    graph = FakeGraph(fail_names={"불고기"})
    event = {"Records": [record("m1", 1, "김치찌개"), record("m2", 2, "불고기")]}

    failures = await process_event(event, graph, concurrency=20)

    assert failures == ["m2"]


async def test_malformed_body_is_reported_as_failure():
    # 계약 위반 메시지는 조용히 버리지 않고 실패로 보고해 DLQ 로 흘려보낸다.
    graph = FakeGraph()
    event = {"Records": [{"messageId": "bad", "body": "not-json"}]}

    failures = await process_event(event, graph, concurrency=20)

    assert failures == ["bad"]
    assert graph.calls == []
