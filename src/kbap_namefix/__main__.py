"""스캔 수집된 음식 이름 정제 배치 — 로컬 JSON 입출력.

kbap 조회/반영 API 계약이 확정되면 입출력만 KbapClient 로 바꾼다.
입력: 이름 문자열 배열 JSON. 앵커: 확정 음식명 배열 JSON(선택).
"""

import argparse
import asyncio
import json
import logging

import yaml

from kbap_namefix.pipeline import clean_batch, make_normalizer
from kbap_review.__main__ import make_callbacks

log = logging.getLogger("kbap_namefix")


async def main() -> None:
    parser = argparse.ArgumentParser(description="스캔 음식 이름 정제 배치")
    parser.add_argument("--input", required=True, help="정제할 이름 배열 JSON 파일")
    parser.add_argument("--anchors", help="fuzzy 스냅 앵커(확정 음식명) 배열 JSON 파일")
    parser.add_argument("--output", default="namefix_results.json")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    with open(args.config) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]

    with open(args.input) as f:
        names = json.load(f)
    anchors: list[str] = []
    if args.anchors:
        with open(args.anchors) as f:
            anchors = json.load(f)

    normalize = make_normalizer(
        llm.get("namefix_model", llm["model"]),
        llm.get("timeout_seconds", 120),
        make_callbacks(),
    )
    results = await clean_batch(names, anchors, normalize, raw["concurrency"])

    counts: dict[str, int] = {}
    for r in results:
        counts[r["method"]] = counts.get(r["method"], 0) + 1
        if r["method"] != "unchanged":
            log.info("%s %r -> %r %s", r["method"], r["original"], r["name"], r["reason"])

    with open(args.output, "w") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    log.info("완료: %s -> %s", counts, args.output)


if __name__ == "__main__":
    asyncio.run(main())
