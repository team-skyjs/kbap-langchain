"""모든 배치의 단일 진입점.

  uv run python main.py review  --limit 50 [--dry-run] [--config config.yaml]
  uv run python main.py namefix --input names.json [--anchors anchors.json] [--output out.json]

Lambda(SQS batchSize 10 소비)는 main.handler 를 진입점으로 잡는다.
"""

import argparse
import asyncio
import json
import logging
import os

import yaml

from kbap.content import handler  # noqa: F401 — Lambda 진입점 re-export

log = logging.getLogger("kbap")


async def _review(args) -> None:
    from kbap.review import load_config
    from kbap.review import build_graph
    from kbap.review import KbapClient
    from kbap.review import make_callbacks, run_batch
    from kbap.review import make_scorers

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


def _load_names(path: str) -> list[str]:
    with open(path) as f:
        names = json.load(f)
    if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
        raise SystemExit(f"{path}: 문자열 배열 JSON 이어야 합니다")
    return names


async def _namefix(args) -> None:
    from dotenv import load_dotenv

    from kbap.namefix import clean_batch, make_normalizer
    from kbap.review import make_callbacks

    # LLM API 키 로드 — kbap_review.config.load_config 와 같은 환경 경계(override=False).
    load_dotenv()
    with open(args.config) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]
    concurrency = raw["concurrency"]
    if concurrency < 1:
        # Semaphore(0) 은 모든 코루틴을 영구히 막아 배치가 조용히 멈춘다.
        raise SystemExit("concurrency 는 1 이상이어야 합니다")

    names = _load_names(args.input)
    anchors = _load_names(args.anchors) if args.anchors else []

    normalize = make_normalizer(
        llm.get("namefix_model", llm["model"]),
        llm.get("timeout_seconds", 120),
        make_callbacks(),
    )
    results = await clean_batch(names, anchors, normalize, concurrency)

    counts: dict[str, int] = {}
    for r in results:
        counts[r["method"]] = counts.get(r["method"], 0) + 1
        if r["method"] != "unchanged":
            log.info("%s %r -> %r %s", r["method"], r["original"], r["name"], r["reason"])

    # 임시 파일에 쓴 뒤 원자적 교체 — 중단돼도 깨진 JSON 이나 유실된 이전 결과를 남기지 않는다.
    tmp = args.output + ".tmp"
    with open(tmp, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    os.replace(tmp, args.output)
    log.info("완료: %s -> %s", counts, args.output)


def main() -> None:
    parser = argparse.ArgumentParser(description="kbap 관리자 배치 모음")
    sub = parser.add_subparsers(dest="command", required=True)

    review = sub.add_parser("review", help="KB-286 food 최종 검수 배치")
    review.add_argument("--limit", type=int, default=50)
    review.add_argument("--dry-run", action="store_true", help="kbap에 결과를 반영하지 않고 판정만 출력")
    review.add_argument("--config", default="config.yaml")

    namefix = sub.add_parser("namefix", help="스캔 음식 이름 정제 배치 (로컬 JSON 입출력)")
    namefix.add_argument("--input", required=True, help="정제할 이름 배열 JSON 파일")
    namefix.add_argument("--anchors", help="fuzzy 스냅 앵커(확정 음식명) 배열 JSON 파일")
    namefix.add_argument("--output", default="namefix_results.json")
    namefix.add_argument("--config", default="config.yaml")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run({"review": _review, "namefix": _namefix}[args.command](args))


if __name__ == "__main__":
    main()
