"""SQS → Lambda 진입점. batchSize 10 묶음을 받아 그래프를 동시 실행한다.

메시지 계약(초안 — Spring 발행 측과 확정 필요):
  body = {"foodId": <int>, "scannedName": <str>}

부분 실패 보고: 실패한 메시지 id 만 batchItemFailures 로 돌려줘 그 건만 재수신되게 한다.
이벤트 소스 매핑에 ReportBatchItemFailures 활성화가 전제다 — 없으면 1건 실패가
성공한 9건까지 재수신시켜 LLM 비용을 두 번 태운다.
"""

import asyncio
import json
import logging
import os

import yaml

log = logging.getLogger("kbap_content")

_graph = None


def _load_graph():
    """콜드스타트 1회만 그래프를 만든다."""
    global _graph
    if _graph is None:
        from dotenv import load_dotenv

        from kbap_content.graph import build_content_graph
        from kbap_content.nodes import make_fns
        from kbap_review.config import Thresholds

        load_dotenv()
        with open(os.environ.get("CONFIG_PATH", "config.yaml")) as f:
            raw = yaml.safe_load(f)
        llm = raw["llm"]
        thresholds = Thresholds(**raw["thresholds"])
        fns = make_fns(
            llm["model"],
            llm.get("timeout_seconds", 120),
            thresholds,
            judge_model=llm.get("judge_model"),
        )
        _graph = build_content_graph(fns, thresholds)
    return _graph


async def process_event(event: dict, graph, concurrency: int) -> list[str]:
    """레코드들을 동시 처리하고 실패한 messageId 목록을 돌려준다."""
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
                result = await graph.ainvoke({"food_name": name})
            except Exception:
                log.exception("그래프 실패 foodId=%s (%s)", food_id, name)
                return message_id
        verdict = result["verdict"]
        # TODO(kbap 계약 확정 시): PASS/FAIL + 사유를 결과 반영 API 로 POST — 멱등이어야 한다.
        log.info("foodId=%s (%s) passed=%s %s", food_id, name, verdict.passed, verdict.reason)
        return None

    results = await asyncio.gather(*(one(r) for r in event.get("Records", [])))
    return [message_id for message_id in results if message_id]


def handler(event, context):
    graph = _load_graph()
    concurrency = int(os.environ.get("GRAPH_CONCURRENCY", "20"))
    failed = asyncio.run(process_event(event, graph, concurrency))

    # Lambda 는 리턴 후 프로세스를 얼린다 — atexit 이 안 불리므로 여기서 직접 flush.
    from langfuse import get_client

    get_client().flush()
    return {"batchItemFailures": [{"itemIdentifier": message_id} for message_id in failed]}
