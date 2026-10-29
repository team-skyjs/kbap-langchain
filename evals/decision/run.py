"""검수 엔진 오프라인 A/B 평가 — KB-913. A = 기존 LLM 검수, B = Jev 검수 + LLM 대체. 최종 판정은 둘 다 KB-897 코드 규칙.

  uv run python evals/decision/run.py fixed    --dataset D.json --out runs/<id> [--live] [--split tune|final]
  uv run python evals/decision/run.py pipeline --dataset D.json --out runs/<id> [--live] [--concurrency 2]
  uv run python evals/decision/run.py summarize --out runs/<id>

fixed    — 저장된 생성물(데이터셋의 review 입력)에 A/B 검수만 적용한다. 같은 입력을 두 엔진이 본다.
pipeline — 같은 음식 이름으로 A/B 전체 그래프를 돌린다. 생성 결과가 달라질 수 있어 fixed 와 역할이 다르다.
           SQS 소비·kbap 적재 API 호출은 하지 않고 결과를 파일로만 남긴다.

기본은 모의 응답이다 — 유료 호출·네트워크 없음. 모의 결과는 도구가 돈다는 증거일 뿐 정확도 증거가 아니다.
--live 일 때만 실제 LLM(OPENAI_API_KEY 등)과 Jev(TYPESAFE_API_KEY)를 부른다. 키는 어떤 파일에도 쓰지 않는다.
"""

import argparse
import asyncio
import csv
import hashlib
import json
import os
import random
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Literal

import httpx
import yaml
from pydantic import BaseModel, model_validator

from kbap import jev
from kbap.content import (
    ContentFns,
    DescReview,
    IngredientsReview,
    NameReview,
    build_content_graph,
    make_fns,
)

ROOT = Path(__file__).resolve().parents[2]
NODES = ("review_name", "review_description", "review_ingredients")


# ===== 데이터셋 매니페스트 =====


class NameInput(BaseModel):
    original: str
    cleaned: str


class DescInput(BaseModel):
    name: str
    description: str


class IngredientsInput(BaseModel):
    name: str
    substances: list[dict]  # [{"code": "PORK", "inclusionPercent": 95}]
    spiciness: int


class Labels(BaseModel):
    """사람이 검토한 정답. 모르면 null — 해당 지표 분모에서 빠진다. LLM·Jev 답을 정답으로 쓰지 않는다."""

    is_food: bool | None = None
    name_ok: bool | None = None
    description_ok: bool | None = None  # 3점(통과)이어야 하는 설명인가
    ingredients_ok: bool | None = None
    critical_missing: bool | None = None  # 중요 기피성분 누락 사례(안전 세트)


class Item(BaseModel):
    id: str
    group: str  # 같은 음식의 정상·오류 변형은 같은 그룹 — 그룹 단위로 split 이 정해진다
    split: Literal["tune", "final"]
    source: Literal["real", "synthetic"]  # 실제 사례와 인공 오류를 섞은 평균을 운영 정확도로 읽지 않는다
    scanned_name: str | None = None  # pipeline 입력
    name_review: NameInput | None = None
    description: DescInput | None = None
    ingredients: IngredientsInput | None = None
    labels: Labels = Labels()
    note: str = ""


class Dataset(BaseModel):
    dataset_version: str
    items: list[Item]

    @model_validator(mode="after")
    def _no_leak(self):
        ids = [i.id for i in self.items]
        if len(ids) != len(set(ids)):
            raise ValueError("item id 중복")
        splits: dict[str, set] = defaultdict(set)
        for i in self.items:
            splits[i.group].add(i.split)
        leaked = sorted(g for g, s in splits.items() if len(s) > 1)
        if leaked:
            raise ValueError(f"같은 그룹이 tune·final 에 나뉘었다(누출): {leaked}")
        return self


def load_dataset(path: str, split: str | None) -> Dataset:
    ds = Dataset.model_validate_json(Path(path).read_text())
    if split:
        ds = ds.model_copy(update={"items": [i for i in ds.items if i.split == split]})
    return ds


# ===== 모의 엔진 — 결정적(시드=입력 해시). 정확도 증거 아님 =====


def _rng(*parts) -> random.Random:
    return random.Random(hashlib.sha256("|".join(map(str, parts)).encode()).hexdigest())


def mock_llm_fns() -> ContentFns:
    async def clean_name(name, feedback):
        return {"name": name.split(" ")[0], "reason": "모의"}

    async def review_name(original, cleaned):
        r = _rng("llm-name", original, cleaned).random()
        return NameReview(reason="모의 이름 검수", is_food=r > 0.1, passed=r > 0.25)

    async def gen_tr(name, *_):
        return {lang: f"{name}-{lang}" for lang in ("zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es")}

    async def gen_desc(name, feedback):
        return f"{name} 모의 설명 {len(feedback)}"

    async def gen_ing(name, feedback):
        return {"substances": [{"code": "SOY", "inclusionPercent": 80}], "spiciness": 2}

    async def gen_long(name):
        return f"{name} 모의 상세 설명."

    async def review_description(name, description):
        return DescReview(reason="모의 설명 검수", score=_rng("llm-desc", name, description).choice([1, 2, 3, 3]))

    async def review_ingredients(name, ingredients):
        return IngredientsReview(reason="모의 성분 검수", passed=_rng("llm-ing", name, json.dumps(ingredients)).random() > 0.2)

    return ContentFns(
        clean_name=clean_name,
        review_name=review_name,
        generate_name_translations=gen_tr,
        generate_description=gen_desc,
        generate_description_translations=gen_tr,
        generate_ingredients=gen_ing,
        generate_long_description=gen_long,
        review_description=review_description,
        review_ingredients=review_ingredients,
    )


def mock_jev_transport() -> httpx.MockTransport:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        answers = {}
        for qid, q in body["questions"].items():
            rng = _rng("jev", qid, json.dumps(body["state"], sort_keys=True, ensure_ascii=False))
            if q["type"] == "score":
                w = [rng.random() ** 2 for _ in q["criteria"]]
                probs = {str(i): x / sum(w) for i, x in enumerate(w)}
                answers[qid] = {
                    "type": "score",
                    "score": sum(i * p for i, p in enumerate(probs.values())),
                    "probabilities": probs,
                    "confidence": None,
                }
            else:
                answers[qid] = {"type": "noul", "noul": rng.random()}
        usage = {"input_tokens": len(request.content) // 3, "output_tokens": 0}
        return httpx.Response(200, json={"model": "mock-jev", "answers": answers, "usage": usage})

    return httpx.MockTransport(handler)


# ===== 엔진 조립 =====


def load_config() -> dict:
    with open(os.environ.get("CONFIG_PATH", ROOT / "config.yaml")) as f:
        return yaml.safe_load(f)


def build_engines(live: bool, raw: dict) -> tuple[dict, dict]:
    """({"A"|"B": 엔진 팩토리}, 실행 설정 기록). B 의 LLM 대체는 A 의 검수 함수와 같은 것을 쓴다(실제 출시 구성).

    팩토리는 (fns, JevReviews|None) 를 돌려준다. 평가 행마다 새로 만들어 Jev 요청 수(재시도 포함)를
    그 행에 귀속시킨다 — 그래프가 실패해도 요청 수는 남는다."""
    jev_cfg = dict(raw.get("jev") or {})
    policy = jev.JevPolicy(**jev_cfg.pop("policy", {}))
    if live:
        from dotenv import load_dotenv

        load_dotenv()
        llm = raw["llm"]
        a = make_fns(llm["model"], llm.get("timeout_seconds", 120), gen_model=llm.get("gen_model"), namefix_model=llm.get("namefix_model"))
        settings = jev.JevSettings(api_key=os.environ["TYPESAFE_API_KEY"], **jev_cfg)
        transport = None
    else:
        a = mock_llm_fns()
        settings = jev.JevSettings(api_key="mock", **jev_cfg)
        transport = mock_jev_transport()
    def new_b():
        reviews = jev.JevReviews(settings, a, policy, transport=transport)
        return jev.with_jev_reviews(a, reviews), reviews

    settings_record = {k: v for k, v in vars(settings).items() if k != "api_key"}
    return {"A": lambda: (a, None), "B": new_b}, {"jev_settings": settings_record, "jev_policy": vars(policy), "llm": raw["llm"]}


def run_manifest(args, ds: Dataset, engine_record: dict) -> dict:
    from kbap import prompts

    def git(*a):
        return subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True).stdout.strip()

    langfuse = bool(os.environ.get("LANGFUSE_PUBLIC_KEY")) and args.live
    m = {
        "mode": "live" if args.live else "mock",
        "accuracy_evidence": bool(args.live),
        "command": args.command,
        "started_at": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
        "git_commit": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain", "--", "src", "evals", "config.yaml")),
        "dataset_version": ds.dataset_version,
        "dataset_sha256": hashlib.sha256(Path(args.dataset).read_bytes()).hexdigest(),
        "split": args.split,
        "items": len(ds.items),
        **engine_record,
        "jev_policy_version": jev.POLICY_VERSION,
        "jev_questions": {
            "review_name": {"hash": jev.question_hash(jev.NAME_QUESTIONS), "questions": jev.NAME_QUESTIONS},
            "review_description": {"hash": jev.question_hash(jev.DESC_QUESTIONS), "questions": jev.DESC_QUESTIONS},
            "review_ingredients": {"hash": jev.question_hash(jev.INGREDIENT_QUESTIONS), "questions": jev.INGREDIENT_QUESTIONS},
        },
        "jev_feedback": jev.FEEDBACK,
        "prompt_source": "langfuse-production" if langfuse else "local-fallback",
        "prompt_template_sha256": {
            name: hashlib.sha256(t.encode()).hexdigest()[:12] for name, t in prompts.PROMPTS.items()
        },
    }
    if langfuse:
        try:
            from langfuse import get_client

            m["langfuse_prompt_versions"] = {
                name: get_client().get_prompt(name, label="production").version for name in prompts.PROMPTS
            }
        except Exception as e:  # 기록 실패가 평가를 막지 않게 — 대신 manifest 에 남긴다
            m["langfuse_prompt_versions"] = f"조회 실패: {e}"
    if args.live:
        # 모델 ID·요금·한도는 실행 시점 값을 남긴다(문서 값은 바뀐다).
        try:
            r = httpx.get(
                "https://api.typesafe.ai/v1/models",
                headers={"Authorization": f"Bearer {os.environ['TYPESAFE_API_KEY']}"},
                timeout=10,
            )
            m["jev_models"] = r.json() if r.is_success else f"HTTP {r.status_code}"
        except Exception as e:
            m["jev_models"] = f"조회 실패: {e}"
    return m


# ===== 실행 =====


def _decision(node: str, r) -> dict:
    if node == "review_name":
        return {"is_food": r.is_food, "passed": r.passed}
    if node == "review_description":
        return {"score": r.score, "passed": r.score == 3}
    return {"passed": r.passed}


async def _review(fns: ContentFns, node: str, item: Item):
    if node == "review_name":
        return await fns.review_name(item.name_review.original, item.name_review.cleaned)
    if node == "review_description":
        return await fns.review_description(item.description.name, item.description.description)
    i = item.ingredients
    return await fns.review_ingredients(i.name, {"substances": i.substances, "spiciness": i.spiciness})


_INPUT = {"review_name": "name_review", "review_description": "description", "review_ingredients": "ingredients"}


async def run_fixed(ds: Dataset, engines: dict, concurrency: int) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)

    async def one(item: Item, node: str, engine: str) -> dict:
        row = {"item": item.id, "group": item.group, "split": item.split, "source": item.source, "node": node, "engine": engine}
        fns, reviews = engines[engine]()
        async with sem:
            started = time.monotonic()
            try:
                r = await _review(fns, node, item)
                row.update(decision=_decision(node, r), reason=r.reason, trace=r._trace)
            except Exception as e:  # 실패는 통계에서 빼지 않는다 — 실패율 분자다
                row.update(error=f"{type(e).__name__}: {e}"[:300])
            row["latency_ms"] = round((time.monotonic() - started) * 1000)
        row["jev_requests"] = reviews.requests if reviews else 0
        return row

    jobs = []
    for idx, item in enumerate(ds.items):
        order = ("A", "B") if idx % 2 == 0 else ("B", "A")  # 순서 교차
        for node in NODES:
            if getattr(item, _INPUT[node]) is not None:
                jobs += [one(item, node, e) for e in order]
    return await asyncio.gather(*jobs)


def counting(fns: ContentFns, counts: dict) -> ContentFns:
    def wrap(name, fn):
        async def counted(*a):
            counts[name] += 1
            return await fn(*a)

        return counted

    return ContentFns(*(wrap(n, f) for n, f in zip(ContentFns._fields, fns)))


async def run_pipeline(ds: Dataset, engines: dict, concurrency: int) -> list[dict]:
    names = list(dict.fromkeys(i.scanned_name for i in ds.items if i.scanned_name))
    sem = asyncio.Semaphore(concurrency)

    async def one(idx: int, name: str, engine: str) -> dict:
        counts: dict = defaultdict(int)
        fns, reviews = engines[engine]()
        graph = build_content_graph(counting(fns, counts))
        row = {"scanned_name": name, "engine": engine, "order": idx % 2}
        async with sem:
            started = time.monotonic()
            try:
                s = await graph.ainvoke({"food_name": name})
                v = s["verdict"]
                row.update(
                    passed=v.passed,
                    failure_kind=v.failure_kind,
                    reason=v.reason,
                    cleaned_name=s.get("cleaned_name"),
                    description=s.get("description"),
                    ingredients=s.get("ingredients"),
                    attempts={k: s.get(k) for k in ("name_attempts", "description_attempts", "ingredient_attempts")},
                    traces=s.get("review_traces", []),
                )
            except Exception as e:
                row["error"] = f"{type(e).__name__}: {e}"[:300]
            row["elapsed_ms"] = round((time.monotonic() - started) * 1000)
        row["calls"] = dict(counts)
        row["jev_requests"] = reviews.requests if reviews else 0  # 재시도·실패 그래프 포함
        return row

    started = time.monotonic()
    jobs = []
    for idx, name in enumerate(names):
        order = ("A", "B") if idx % 2 == 0 else ("B", "A")
        jobs += [one(idx, name, e) for e in order]
    rows = await asyncio.gather(*jobs)
    wall = time.monotonic() - started
    for r in rows:
        r["batch_wall_s"] = round(wall, 2)
    return rows


# ===== 집계 =====


def _rate(num: int, den: int) -> str:
    return f"{num}/{den} ({num / den:.1%})" if den else "0/0 (–)"


def _p(values: list[float], q: float):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(round(q * (len(s) - 1))))]


def fixed_metrics(rows: list[dict], labels: dict[str, Labels]) -> dict[str, dict[str, str]]:
    """§6.4 지표 — 분모를 함께 적는다. 실패·보류를 제외하지 않는다."""
    out = {}
    for engine in ("A", "B"):
        er = [r for r in rows if r["engine"] == engine]
        c = defaultdict(lambda: [0, 0])  # 지표 → [분자, 분모]
        for r in er:
            lab, d = labels[r["item"]], r.get("decision")
            if r["node"] == "review_name" and lab.is_food is not None:
                key = "음식 오거절률" if lab.is_food else "비음식 오통과율"
                c[key][1] += 1
                if d and (d["is_food"] is False if lab.is_food else d["is_food"] is not False):
                    c[key][0] += 1
            if r["node"] == "review_description" and lab.description_ok is not None:
                key = "정상 설명 오거절률" if lab.description_ok else "설명 오류 오통과율"
                c[key][1] += 1
                if d and (not d["passed"] if lab.description_ok else d["passed"]):
                    c[key][0] += 1
            if r["node"] == "review_ingredients" and lab.critical_missing:
                c["중요 성분 누락 오통과"][1] += 1
                c["중요 성분 누락 오통과"][0] += bool(d and d["passed"])
            t = r.get("trace") or {}
            c["판단 보류율"][1] += 1
            c["판단 보류율"][0] += t.get("outcome") == jev.ABSTAIN
            c["대체 호출률"][1] += 1
            c["대체 호출률"][0] += bool(t.get("fallback"))
            c["실패율"][1] += 1
            c["실패율"][0] += "error" in r
        lat = [r["latency_ms"] for r in er]
        tokens = sum(((r.get("trace") or {}).get("usage") or {}).get("input_tokens", 0) for r in er)
        out[engine] = {k: _rate(*v) for k, v in c.items()} | {
            "검수 지연 p50/p95 ms": f"{_p(lat, .5)} / {_p(lat, .95)} (n={len(lat)})",
            "Jev 입력 토큰 합": str(tokens),
            "Jev 요청 수(재시도 포함)": str(sum(r.get("jev_requests", 0) for r in er)),
        }
    return out


def disagreements(rows: list[dict], items: dict[str, Item]) -> list[dict]:
    by = defaultdict(dict)
    for r in rows:
        by[(r["item"], r["node"])][r["engine"]] = r
    out = []
    for (item_id, node), pair in sorted(by.items()):
        a, b = pair.get("A"), pair.get("B")
        if not a or not b or a.get("decision") == b.get("decision") and "error" not in a and "error" not in b:
            continue
        t = b.get("trace") or {}
        out.append(
            {
                "item": item_id,
                "node": node,
                "source": a["source"],
                "input": getattr(items[item_id], _INPUT[node]).model_dump_json(),
                "labels": items[item_id].labels.model_dump_json(exclude_none=True),
                "A": json.dumps(a.get("decision") or a.get("error"), ensure_ascii=False),
                "B": json.dumps(b.get("decision") or b.get("error"), ensure_ascii=False),
                "B_outcome": t.get("outcome"),
                "B_fallback": t.get("fallback") or "",
                "B_probabilities": json.dumps(t.get("answers"), ensure_ascii=False),
                "A_reason": a.get("reason", ""),
                "B_reason": b.get("reason", ""),
            }
        )
    return out


def pipeline_metrics(rows: list[dict]) -> dict[str, dict[str, str]]:
    out = {}
    for engine in ("A", "B"):
        er = [r for r in rows if r["engine"] == engine]
        ok = [r for r in er if "error" not in r]
        el = [r["elapsed_ms"] for r in ok]
        # LLM 호출 = 생성 노드 + (A: 검수 노드 / B: 검수 중 LLM 대체분). Jev 요청은 따로 센다.
        calls = [
            sum(n for k, n in r["calls"].items() if k not in NODES)
            + (sum(r["calls"].get(k, 0) for k in NODES) if engine == "A" else sum(bool(t.get("fallback")) for t in r.get("traces", [])))
            for r in er
        ]
        regen = sum(((r.get("attempts") or {}).get("description_attempts") or 0) > 1 for r in ok)
        out[engine] = {
            "완료/실패": f"{len(ok)}/{len(er) - len(ok)}",
            "통과율": _rate(sum(bool(r.get("passed")) for r in ok), len(ok)),
            "1건 완료 p50/p95 ms": f"{_p(el, .5)} / {_p(el, .95)} (n={len(el)})",
            "건당 LLM 호출 평균": f"{statistics.mean(calls):.2f}" if calls else "–",
            "설명 재생성률": _rate(regen, len(ok)),
            "Jev 요청 수(재시도 포함)": str(sum(r.get("jev_requests", 0) for r in er)),
            "대체 호출 수": str(sum(len([t for t in r.get("traces", []) if t.get("fallback")]) for r in er)),
        }
    return out


# §6.4 지표표 순서
ORDER = ["음식 오거절률", "비음식 오통과율", "설명 오류 오통과율", "정상 설명 오거절률", "중요 성분 누락 오통과", "판단 보류율", "대체 호출률", "실패율"]


def _table(metrics: dict[str, dict[str, str]]) -> str:
    seen = list(dict.fromkeys(k for m in metrics.values() for k in m))
    keys = [k for k in ORDER if k in seen] + [k for k in seen if k not in ORDER]
    lines = ["| 지표 | A 기존 LLM | B Jev+대체 |", "|---|---|---|"]
    lines += [f"| {k} | {metrics['A'].get(k, '–')} | {metrics['B'].get(k, '–')} |" for k in keys]
    return "\n".join(lines)


def summarize(out: Path) -> str:
    manifest = json.loads((out / "manifest.json").read_text())
    ds = load_dataset(manifest["dataset_path"], manifest.get("split"))
    items = {i.id: i for i in ds.items}
    head = [
        f"# 검수 A/B — {manifest['command']} ({manifest['mode']})",
        "",
        f"- commit `{manifest['git_commit'][:8]}`{' (dirty)' if manifest['git_dirty'] else ''} · dataset `{manifest['dataset_version']}` · split {manifest.get('split') or '전체'} · Jev 정책 `{manifest['jev_policy_version']}`",
    ]
    if manifest["mode"] == "mock":
        head.append("- **모의 응답 실행 — 정확도·지연·비용의 증거가 아니다.** 도구 동작 확인용.")
    body = []
    if (out / "results.jsonl").exists():
        rows = [json.loads(line) for line in (out / "results.jsonl").read_text().splitlines()]
        labels = {k: v.labels for k, v in items.items()}
        for source in ("real", "synthetic"):
            sub = [r for r in rows if r["source"] == source]
            if sub:
                body += [f"\n## 고정 입력 검수 — {source}\n", _table(fixed_metrics(sub, labels))]
        dis = disagreements(rows, items)
        if dis:
            with open(out / "disagreements.csv", "w", newline="") as f:
                w = csv.DictWriter(f, fieldnames=list(dis[0]))
                w.writeheader()
                w.writerows(dis)
        body.append(f"\n불일치 {len(dis)}건 → `disagreements.csv`")
    if (out / "pipeline.jsonl").exists():
        rows = [json.loads(line) for line in (out / "pipeline.jsonl").read_text().splitlines()]
        body += ["\n## 전체 파이프라인\n", _table(pipeline_metrics(rows))]
    text = "\n".join(head + body) + "\n"
    (out / "summary.md").write_text(text)
    return text


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["fixed", "pipeline", "summarize"])
    p.add_argument("--dataset")
    p.add_argument("--out", required=True)
    p.add_argument("--split", choices=["tune", "final"])
    p.add_argument("--live", action="store_true", help="실제 LLM·Jev 유료 호출. 없으면 모의 응답")
    p.add_argument("--concurrency", type=int, default=2)
    args = p.parse_args()
    out = Path(args.out)

    if args.command == "summarize":
        print(summarize(out))
        return
    if not args.dataset:
        p.error("--dataset 필요")
    ds = load_dataset(args.dataset, args.split)
    raw = load_config()
    if args.live:
        fields = sorted({f for i in ds.items for f in ("scanned_name", "name_review", "description", "ingredients") if getattr(i, f)})
        print(f"[live] {len(ds.items)}건의 {fields} 필드를 LLM 제공자와 TypeSafe 로 전송한다.", file=sys.stderr)
    engines, record = build_engines(args.live, raw)
    out.mkdir(parents=True, exist_ok=True)
    manifest = run_manifest(args, ds, record) | {"dataset_path": str(Path(args.dataset).resolve())}
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

    runner = run_fixed if args.command == "fixed" else run_pipeline
    rows = asyncio.run(runner(ds, engines, args.concurrency))
    name = "results.jsonl" if args.command == "fixed" else "pipeline.jsonl"
    with open(out / name, "w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")
    print(summarize(out))


if __name__ == "__main__":
    main()
