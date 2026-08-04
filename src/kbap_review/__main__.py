import argparse
import asyncio
import logging
import os

from kbap_review.config import load_config
from kbap_review.graph import build_graph
from kbap_review.kbap_client import KbapClient
from kbap_review.scoring import make_scorers

log = logging.getLogger("kbap_review")


async def run_batch(graph, foods: list[dict], concurrency: int, callbacks: list) -> dict[str, int]:
    sem = asyncio.Semaphore(concurrency)

    async def one(food: dict):
        async with sem:
            return await graph.ainvoke({"food": food}, config={"callbacks": callbacks})

    results = await asyncio.gather(*(one(f) for f in foods), return_exceptions=True)

    counts = {"PASS": 0, "RETRY": 0, "REJECT": 0, "HELD": 0}
    for food, result in zip(foods, results):
        if isinstance(result, BaseException):
            # LLM/POST 실패 — 판정 보류. PENDING_REVIEW에 남아 다음 실행에서 자연 재시도.
            counts["HELD"] += 1
            log.warning("보류 id=%s (%s): %s", food["id"], food.get("koreanName"), result)
        else:
            verdict = result["verdict"]
            counts[verdict.verdict] += 1
            log.info("%s id=%s (%s)", verdict.verdict, food["id"], food.get("koreanName"))
    return counts


def make_callbacks() -> list:
    # LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY / LANGFUSE_HOST 환경변수로 연결
    if not os.environ.get("LANGFUSE_PUBLIC_KEY"):
        return []
    from langfuse.langchain import CallbackHandler

    return [CallbackHandler()]


async def main() -> None:
    parser = argparse.ArgumentParser(description="KB-286 food 최종 검수 배치")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true", help="kbap에 결과를 반영하지 않고 판정만 출력")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = load_config(args.config)
    client = KbapClient(config.kbap_base_url, config.kbap_token)
    try:
        foods = await client.fetch_review_candidates(limit=args.limit)
        if not foods:
            log.info("검수 대상 없음")
            return
        log.info("검수 대상 %d건 (dry_run=%s)", len(foods), args.dry_run)
        graph = build_graph(make_scorers(config), client, config.thresholds, dry_run=args.dry_run)
        counts = await run_batch(graph, foods, config.concurrency, make_callbacks())
        log.info("완료: %s", counts)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
