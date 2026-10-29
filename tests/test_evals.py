"""평가 도구(evals/decision/run.py) — 데이터셋 누출 검사·모의 실행·집계. 모의 결과의 수치는 검증하지 않는다."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest
from pydantic import ValidationError

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "evals/decision/sample_dataset.json"


@pytest.fixture(scope="module")
def ev():
    spec = importlib.util.spec_from_file_location("decision_run", ROOT / "evals/decision/run.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_same_group_across_splits_is_rejected(ev):
    items = [
        {"id": "a", "group": "kimchi", "split": "tune", "source": "synthetic"},
        {"id": "b", "group": "kimchi", "split": "final", "source": "synthetic"},
    ]
    with pytest.raises(ValidationError, match="누출"):
        ev.Dataset.model_validate({"dataset_version": "t", "items": items})


def test_split_filter_keeps_only_requested_split(ev):
    ds = ev.load_dataset(str(SAMPLE), "final")
    assert ds.items and {i.split for i in ds.items} == {"final"}


@pytest.mark.parametrize("command", ["fixed", "pipeline"])
def test_mock_run_writes_manifest_results_and_summary(ev, tmp_path, monkeypatch, command):
    monkeypatch.setenv("TYPESAFE_API_KEY", "secret-should-not-leak")
    monkeypatch.setattr(
        sys, "argv", ["run.py", command, "--dataset", str(SAMPLE), "--out", str(tmp_path)]
    )
    ev.main()

    manifest = json.loads((tmp_path / "manifest.json").read_text())
    assert manifest["mode"] == "mock" and manifest["accuracy_evidence"] is False
    assert set(manifest["jev_questions"]) == {"review_name", "review_description", "review_ingredients"}
    assert all(len(q["hash"]) == 12 for q in manifest["jev_questions"].values())
    out = "results.jsonl" if command == "fixed" else "pipeline.jsonl"
    rows = [json.loads(line) for line in (tmp_path / out).read_text().splitlines()]
    assert {r["engine"] for r in rows} == {"A", "B"}
    summary = (tmp_path / "summary.md").read_text()
    assert "정확도·지연·비용의 증거가 아니다" in summary
    # 키는 어떤 산출물에도 남지 않는다.
    for f in tmp_path.iterdir():
        assert "secret-should-not-leak" not in f.read_text()


def test_fixed_metrics_count_denominators_and_failures(ev):
    labels = {"x": ev.Labels(is_food=True), "y": ev.Labels(critical_missing=True)}
    rows = [
        {"item": "x", "engine": "A", "node": "review_name", "decision": {"is_food": False, "passed": False}, "latency_ms": 1},
        {"item": "y", "engine": "A", "node": "review_ingredients", "error": "JevTransientError", "latency_ms": 1},
        {"item": "x", "engine": "B", "node": "review_name", "decision": {"is_food": True, "passed": True},
         "trace": {"outcome": "ABSTAIN", "fallback": "abstain"}, "latency_ms": 1},
    ]
    m = ev.fixed_metrics(rows, labels)
    assert m["A"]["음식 오거절률"] == "1/1 (100.0%)"
    assert m["A"]["실패율"] == "1/2 (50.0%)"
    # 실패한 요청도 안전 세트 분모에 남는다 — 통과로도 빠지지도 않는다.
    assert m["A"]["중요 성분 누락 오통과"] == "0/1 (0.0%)"
    assert m["B"]["대체 호출률"] == "1/1 (100.0%)"


async def test_pipeline_counts_jev_requests_even_when_graph_fails(ev):
    # 재시도 요청과 실패한 그래프의 요청도 집계에 남아야 한다 — 장애·한도 상황 비교가 목적이다.
    import httpx

    from kbap import jev

    def handler(request):
        return httpx.Response(429, json={"detail": "slow down"})

    a = ev.mock_llm_fns()

    async def no_sleep(_):
        return None

    def new_b():
        reviews = jev.JevReviews(jev.JevSettings(api_key="k"), a, transport=httpx.MockTransport(handler), sleep=no_sleep)
        return jev.with_jev_reviews(a, reviews), reviews

    ds = ev.Dataset.model_validate(
        {"dataset_version": "t", "items": [{"id": "x", "group": "g", "split": "tune", "source": "synthetic", "scanned_name": "김치찌개"}]}
    )
    rows = await ev.run_pipeline(ds, {"A": lambda: (a, None), "B": new_b}, concurrency=1)
    b = next(r for r in rows if r["engine"] == "B")

    assert "error" in b and b["jev_requests"] == 2  # 이름 검수 1회 + 429 재시도 1회
    assert ev.pipeline_metrics(rows)["B"]["Jev 요청 수(재시도 포함)"] == "2"
