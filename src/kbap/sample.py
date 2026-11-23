"""그래프만 돌리는 표본 실행기 (KB-1028) — 적재 API(POST)는 부르지 않는다.

    python -m kbap.sample names.txt > out.jsonl      # 한 줄에 이름 하나, 빈 줄·# 줄은 건너뜀

이름마다 JSONL 한 줄(통과 여부·실패 종류·정제 이름·기피성분·소요 시간)을 stdout 으로 내고,
요약(모델·통과율·실패 종류별 건수)은 stderr 로 낸다. 프롬프트 조회·트레이싱은 운영과 같다
(LANGFUSE_* 가 있으면 운영 프롬프트, 없으면 코드 템플릿). 모델 바꿔 보기: KBAP_MODEL·KBAP_INGREDIENTS_MODEL·
KBAP_REVIEW_MODEL·KBAP_NAMEFIX_MODEL·KBAP_GEN_MODEL (content.llm_models). 동시 실행 수: GRAPH_CONCURRENCY(기본 5).
"""

import asyncio
import json
import os
import sys
import time
from collections import Counter

import yaml

from kbap.content import llm_models, load_graph
from kbap.review import make_callbacks


def row(name: str, result: dict, seconds: float) -> dict:
    verdict = result["verdict"]
    ing = result.get("ingredients") or {}
    review = result.get("ingredient_review")
    desc = result.get("description_score")
    return {
        "name": name,
        "passed": verdict.passed,
        "failure_kind": verdict.failure_kind,
        "reason": verdict.reason,
        "cleaned_name": result.get("cleaned_name"),
        "description": result.get("description"),
        "description_score": getattr(desc, "score", None),
        "ingredients": ing.get("substances"),
        "spiciness": ing.get("spiciness"),
        "ingredient_review": getattr(review, "reason", None),
        "ingredient_attempts": result.get("ingredient_attempts"),
        "seconds": round(seconds, 1),
    }


async def run(names: list[str], graph, concurrency: int, callbacks: list):
    sem = asyncio.Semaphore(concurrency)

    async def one(name: str) -> dict:
        async with sem:
            t = time.time()
            try:
                result = await graph.ainvoke({"food_name": name}, config={"callbacks": callbacks})
                return row(name, result, time.time() - t)
            except Exception as e:  # 한 건 실패가 표본 전체를 멈추지 않게 — 운영이면 재시도 대상
                return {"name": name, "error": repr(e)[:500], "seconds": round(time.time() - t, 1)}

    rows = []
    for fut in asyncio.as_completed([one(n) for n in names]):
        r = await fut
        print(json.dumps(r, ensure_ascii=False), flush=True)
        rows.append(r)
    return rows


def main(path: str):
    names = [s for line in open(path, encoding="utf-8") if (s := line.strip()) and not s.startswith("#")]
    with open(os.environ.get("CONFIG_PATH", "config.yaml")) as f:
        models = llm_models(yaml.safe_load(f)["llm"])
    graph = load_graph()
    callbacks = make_callbacks()
    rows = asyncio.run(run(names, graph, int(os.environ.get("GRAPH_CONCURRENCY", "5")), callbacks))
    if callbacks:
        from langfuse import get_client

        get_client().flush()
    kinds = Counter("ERROR" if "error" in r else (r["failure_kind"] or "PASS") for r in rows)
    print(json.dumps({"models": models, "method": os.environ.get("KBAP_STRUCTURED_METHOD", "auto"),
                      "n": len(rows), "kinds": dict(kinds)}, ensure_ascii=False), file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit(__doc__)
    main(sys.argv[1])
