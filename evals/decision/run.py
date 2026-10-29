"""검수 엔진 오프라인 A/B 평가 CLI — KB-913. 로직은 kbap.evaluate(Lambda 평가 모드와 공유).

  uv run python evals/decision/run.py fixed    --dataset D.json --out runs/<id> [--live] [--split tune|final]
  uv run python evals/decision/run.py pipeline --dataset D.json --out runs/<id> [--live] [--concurrency 2]
  uv run python evals/decision/run.py summarize --out runs/<id>
  uv run python evals/decision/run.py collect  --dataset D.json --out runs/<id> resp1.json [resp2.json ...]
  uv run python evals/decision/run.py schema

기본은 모의 응답이다 — 유료 호출·네트워크 없음. 모의 결과는 도구가 돈다는 증거일 뿐 정확도 증거가 아니다.
--live 일 때만 로컬에서 실제 LLM(OPENAI_API_KEY 등)과 Jev(TYPESAFE_API_KEY)를 부른다.
dev Lambda 에서 돌리려면 eval 이벤트로 invoke 하고 응답 파일을 collect 로 모은다(README).
"""

import argparse
import asyncio
import csv
import hashlib
import json
import subprocess
import sys
from pathlib import Path

from kbap.evaluate import *  # noqa: F403 — 테스트·노트북이 이 모듈로 같은 이름을 쓴다
from kbap.evaluate import (
    Dataset,
    Item,
    build_engines,
    build_manifest,
    load_config,
    load_dataset,
    run_fixed,
    run_pipeline,
    summary_text,
)

ROOT = Path(__file__).resolve().parents[2]


def _git_version() -> dict:
    def git(*a):
        return subprocess.run(["git", *a], cwd=ROOT, capture_output=True, text=True).stdout.strip()

    return {
        "git_commit": git("rev-parse", "HEAD"),
        "git_dirty": bool(git("status", "--porcelain", "--", "src", "evals", "config.yaml")),
    }


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines()] if path.exists() else []


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    with open(path, "w") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False, default=str) + "\n")


def summarize(out: Path) -> str:
    manifest = json.loads((out / "manifest.json").read_text())
    ds = load_dataset(manifest["dataset_path"], manifest.get("split"))
    items: dict[str, Item] = {i.id: i for i in ds.items}
    text, dis = summary_text(manifest, items, _read_jsonl(out / "results.jsonl"), _read_jsonl(out / "pipeline.jsonl"))
    if dis:
        with open(out / "disagreements.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(dis[0]))
            w.writeheader()
            w.writerows(dis)
    (out / "summary.md").write_text(text)
    return text


def collect(dataset: str, out: Path, responses: list[str]) -> str:
    """Lambda 평가 응답(조각 여러 개)을 로컬 실행과 같은 파일 구성으로 합친다."""
    parsed = [json.loads(Path(p).read_text()) for p in responses]
    if any(r.get("rows") is None for r in parsed):
        sys.exit("rows 가 응답에 없다(rows_in_logs) — CloudWatch 의 'EVAL_ROW ' 줄을 모아 rows 로 채운 뒤 다시 실행")
    first = parsed[0]["manifest"]
    if {r["manifest"]["command"] for r in parsed} != {first["command"]}:
        sys.exit("fixed·pipeline 응답이 섞였다 — 따로 collect 한다")
    out.mkdir(parents=True, exist_ok=True)
    manifest = first | {
        "dataset_path": str(Path(dataset).resolve()),
        "dataset_sha256": hashlib.sha256(Path(dataset).read_bytes()).hexdigest(),
        "chunks": [{"items": r["manifest"]["items"], "dataset_sha256": r["manifest"]["dataset_sha256"]} for r in parsed],
        "split": None,  # 조각마다 split 이 달라도 전체 데이터셋 기준으로 집계한다(행의 split 필드 유지)
    }
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    rows = [row for r in parsed for row in r["rows"]]
    _write_jsonl(out / ("results.jsonl" if first["command"] == "fixed" else "pipeline.jsonl"), rows)
    return summarize(out)


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("command", choices=["fixed", "pipeline", "summarize", "collect", "schema"])
    p.add_argument("responses", nargs="*", help="collect: Lambda 평가 응답 JSON 파일들")
    p.add_argument("--dataset")
    p.add_argument("--out")
    p.add_argument("--split", choices=["tune", "final"])
    p.add_argument("--engine", choices=["A", "B", "both"], default="both")
    p.add_argument("--live", action="store_true", help="실제 LLM·Jev 유료 호출. 없으면 모의 응답")
    p.add_argument("--concurrency", type=int, default=2)
    args = p.parse_args()
    if args.command == "schema":
        print(json.dumps(Dataset.model_json_schema(), ensure_ascii=False, indent=2))
        return
    if not args.out:
        p.error("--out 필요")
    out = Path(args.out)

    if args.command == "summarize":
        print(summarize(out))
        return
    if not args.dataset:
        p.error("--dataset 필요")
    if args.command == "collect":
        if not args.responses:
            p.error("collect: 응답 파일 필요")
        print(collect(args.dataset, out, args.responses))
        return
    ds = load_dataset(args.dataset, args.split)
    if args.live:
        from dotenv import load_dotenv

        load_dotenv()
        fields = sorted({f for i in ds.items for f in ("scanned_name", "name_review", "description", "ingredients") if getattr(i, f)})
        print(f"[live] {len(ds.items)}건의 {fields} 필드를 LLM 제공자와 TypeSafe 로 전송한다.", file=sys.stderr)
    which = {"A": ("A",), "B": ("B",), "both": ("A", "B")}[args.engine]
    engines, record = build_engines(args.live, load_config(), which)
    out.mkdir(parents=True, exist_ok=True)
    manifest = build_manifest(
        args.command, args.live, ds, hashlib.sha256(Path(args.dataset).read_bytes()).hexdigest(), args.split, record, _git_version()
    ) | {"dataset_path": str(Path(args.dataset).resolve())}
    (out / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))

    runner = run_fixed if args.command == "fixed" else run_pipeline
    rows = asyncio.run(runner(ds, engines, args.concurrency))
    _write_jsonl(out / ("results.jsonl" if args.command == "fixed" else "pipeline.jsonl"), rows)
    print(summarize(out))


if __name__ == "__main__":
    main()
