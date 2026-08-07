"""스캔 수집된 음식 이름 정제 배치 — 로컬 JSON 입출력.

kbap 조회/반영 API 계약이 확정되면 입출력만 KbapClient 로 바꾼다.
입력: 이름 문자열 배열 JSON. 앵커: 확정 음식명 배열 JSON(선택).
"""

import argparse
import asyncio
import json
import logging
import os

import yaml
from dotenv import load_dotenv

from kbap_namefix.pipeline import clean_batch, make_normalizer
from kbap_review.__main__ import make_callbacks

log = logging.getLogger("kbap_namefix")


def _load_names(path: str, parser: argparse.ArgumentParser) -> list[str]:
    with open(path) as f:
        names = json.load(f)
    if not isinstance(names, list) or not all(isinstance(n, str) for n in names):
        parser.error(f"{path}: 문자열 배열 JSON 이어야 합니다")
    return names


async def main() -> None:
    parser = argparse.ArgumentParser(description="스캔 음식 이름 정제 배치")
    parser.add_argument("--input", required=True, help="정제할 이름 배열 JSON 파일")
    parser.add_argument("--anchors", help="fuzzy 스냅 앵커(확정 음식명) 배열 JSON 파일")
    parser.add_argument("--output", default="namefix_results.json")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    # LLM API 키 로드 — kbap_review.config.load_config 와 같은 환경 경계(override=False).
    load_dotenv()
    with open(args.config) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]
    concurrency = raw["concurrency"]
    if concurrency < 1:
        # Semaphore(0) 은 모든 코루틴을 영구히 막아 배치가 조용히 멈춘다.
        parser.error("concurrency 는 1 이상이어야 합니다")

    names = _load_names(args.input, parser)
    anchors = _load_names(args.anchors, parser) if args.anchors else []

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


if __name__ == "__main__":
    asyncio.run(main())
