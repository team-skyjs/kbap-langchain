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


def test_checked_in_schema_matches_model(ev):
    # 라벨링 쪽이 쓰는 dataset.schema.json 이 Dataset 모델과 어긋나지 않게 — 다르면 `run.py schema` 로 다시 만든다.
    checked_in = json.loads((ROOT / "evals/decision/dataset.schema.json").read_text())
    assert checked_in == ev.Dataset.model_json_schema()


# ===== Lambda 평가 모드 (content.handler → kbap.evaluate.lambda_eval) =====


def eval_event(mode="fixed", engine="both", **extra):
    return {"eval": {"mode": mode, "engine": engine, "live": False, "dataset": json.loads(SAMPLE.read_text()), **extra}}


def test_eval_event_ignored_without_flag(monkeypatch):
    # prod 에는 KBAP_EVAL_ENABLED 가 없다 — 평가 이벤트는 그래프도 키도 건드리지 않고 무시된다.
    from kbap import content

    monkeypatch.delenv("KBAP_EVAL_ENABLED", raising=False)
    monkeypatch.setattr(content, "_cached_graph", lambda: pytest.fail("SQS 경로로 가면 안 된다"))
    assert content.handler(eval_event(), None) == {"eval": "disabled"}


@pytest.mark.parametrize("engine, expected", [("both", {"A", "B"}), ("A", {"A"}), ("B", {"B"})])
def test_eval_event_runs_requested_engines_without_ingest(monkeypatch, engine, expected):
    from kbap import content

    monkeypatch.setenv("KBAP_EVAL_ENABLED", "true")
    monkeypatch.setenv("KBAP_IMAGE_TAG", "abc123def456")
    monkeypatch.setenv("REVIEW_ENGINE", "llm")  # 평가 모드는 REVIEW_ENGINE 과 무관하게 A·B 를 직접 조립한다
    monkeypatch.setattr(content, "_cached_graph", lambda: pytest.fail("평가 모드는 운영 그래프·적재를 쓰지 않는다"))

    result = content.handler(eval_event(engine=engine), None)

    assert {r["engine"] for r in result["rows"]} == expected
    assert result["manifest"]["image_tag"] == "abc123def456"
    assert result["manifest"]["engines"] == sorted(expected)
    assert "검수 A/B" in result["summary"]
    json.dumps(result)  # 그대로 직렬화돼야 invoke 응답이 된다


def test_eval_event_with_records_is_treated_as_sqs(monkeypatch):
    # Records 가 있으면 실제 SQS 배치다 — eval 키가 섞여 있어도 평가로 새지 않는다.
    from kbap import content, evaluate

    monkeypatch.setenv("KBAP_EVAL_ENABLED", "true")
    monkeypatch.setattr(evaluate, "lambda_eval", lambda spec: pytest.fail("평가로 가면 안 된다"))
    monkeypatch.setattr(content, "_cached_graph", lambda: object())

    async def fake_consume(event, graph, concurrency, callbacks):
        return []

    monkeypatch.setattr(content, "_consume", fake_consume)
    monkeypatch.setattr(content, "make_callbacks", lambda: [])
    import langfuse

    monkeypatch.setattr(langfuse, "get_client", lambda: type("C", (), {"flush": lambda self: None})())
    assert content.handler({"Records": [], "eval": {}}, None) == {"batchItemFailures": []}


def test_oversized_eval_result_moves_rows_to_logs(monkeypatch, capsys):
    from kbap import content, evaluate

    monkeypatch.setenv("KBAP_EVAL_ENABLED", "true")
    monkeypatch.setattr(evaluate, "MAX_RESPONSE_BYTES", 100)
    result = content.handler(eval_event(), None)

    assert result["rows"] is None and result["rows_in_logs"] > 0
    logged = [line for line in capsys.readouterr().out.splitlines() if line.startswith("EVAL_ROW ")]
    assert len(logged) == result["rows_in_logs"]


def test_collect_turns_lambda_responses_into_run_files(ev, monkeypatch, tmp_path):
    from kbap import content

    monkeypatch.setenv("KBAP_EVAL_ENABLED", "true")
    resp = tmp_path / "resp.json"
    resp.write_text(json.dumps(content.handler(eval_event(), None), ensure_ascii=False))

    out = tmp_path / "run"
    text = ev.collect(str(SAMPLE), out, [str(resp), str(resp)])

    assert (out / "results.jsonl").exists() and (out / "manifest.json").exists()
    assert "고정 입력 검수" in text
    # 건수는 첫 조각이 아니라 조각 합계 — 조각 2개면 두 배.
    n = len(json.loads(SAMPLE.read_text())["items"])
    manifest = json.loads((out / "manifest.json").read_text())
    assert manifest["items"] == 2 * n and sum(manifest["label_status"].values()) == 2 * n


def test_draft_labels_are_flagged_in_manifest_and_summary(ev, monkeypatch, tmp_path):
    data = json.loads(SAMPLE.read_text())
    for item in data["items"]:
        item["label_status"] = "draft"
    ds_path = tmp_path / "d.json"
    ds_path.write_text(json.dumps(data, ensure_ascii=False))
    monkeypatch.setattr(sys, "argv", ["run.py", "fixed", "--dataset", str(ds_path), "--out", str(tmp_path / "run")])
    ev.main()

    manifest = json.loads((tmp_path / "run/manifest.json").read_text())
    assert manifest["label_status"]["draft"] == len(data["items"])
    assert "라벨 초안" in (tmp_path / "run/summary.md").read_text()


async def test_fixed_rows_record_llm_tokens_and_cost(ev):
    # live 비용 집계·중단선의 근거 — LLM 토큰이 행마다 따로 잡혀야 한다(동시 실행 태스크끼리 섞이지 않게).
    from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel
    from langchain_core.messages import AIMessage

    msg = AIMessage("x", usage_metadata={"input_tokens": 1000, "output_tokens": 100, "total_tokens": 1100},
                    response_metadata={"model_name": "m"})
    model = FakeMessagesListChatModel(responses=[msg] * 10)
    a = ev.mock_llm_fns()

    async def review_name(original, cleaned):
        await model.ainvoke("q")
        return await a.review_name(original, cleaned)

    fns = a._replace(review_name=review_name)
    ds = ev.Dataset.model_validate({"dataset_version": "t", "items": [
        {"id": f"x{i}", "group": f"g{i}", "split": "tune", "source": "real",
         "name_review": {"original": "김치찌개", "cleaned": "김치찌개"}} for i in range(3)]})
    rows = await ev.run_fixed(ds, {"A": lambda: (fns, None)}, concurrency=3)

    assert [r["llm_usage"] for r in rows] == [{"input_tokens": 1000, "output_tokens": 100}] * 3
    assert ev.cost(rows)["usd"] == pytest.approx(3 * (1000 * 0.10 + 100 * 0.50) / 1e6)
