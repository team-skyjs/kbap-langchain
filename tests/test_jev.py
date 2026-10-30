"""검수 엔진 경계와 Jev 어댑터(kbap.jev) — KB-913.

모든 Jev 응답은 httpx.MockTransport 모의 응답이다. 여기서 통과하는 것은 변환·대체·재시도
규칙이지 Jev 의 정확도가 아니다.
"""

from collections import defaultdict

import json

import httpx
import pytest

from kbap.content import apply_review_engine, build_content_graph, build_ingest_payload, parse_review_engine, process_event
from kbap.jev import (
    DESC_QUESTIONS,
    FEEDBACK,
    HIDDEN_CODES,
    INGREDIENT_QUESTION_VERSION,
    NAME_QUESTIONS,
    JevPolicy,
    JevRequestError,
    JevReviews,
    JevSettings,
    JevTransientError,
    decide_ingredients,
    decide_name,
    ingredient_questions,
    desc_score,
    with_jev_reviews,
)
from kbap.review import DuplicateIngestError, _retryable
from test_content import FakeKbap, build_fns, counting_fns, record

POLICY = JevPolicy()


# ===== 확률 → 판정 변환 =====


@pytest.mark.parametrize(
    "probs, expected",
    [
        ({"0": 0.05, "1": 0.1, "2": 0.85}, 3),
        ({"0": 0.5, "1": 0.3, "2": 0.2}, 1),
        ({"0": 0.2, "1": 0.6, "2": 0.2}, 2),
        # 가중 평균 1.6 — int() 는 1점(사실 오류)으로, +1 반올림은 3점(통과)으로 만든다. 둘 다 틀렸다: 보류.
        ({"0": 0.1, "1": 0.2, "2": 0.7}, None),
    ],
)
def test_desc_score_reads_level_probabilities_not_weighted_mean(probs, expected):
    assert desc_score(probs, POLICY) == expected


def noul(p):
    return {"type": "noul", "noul": p}


def name_answers(is_food, noise=0.0, typo=0.0):
    return {
        "is_food": noul(is_food),
        "noise_left": noul(noise),
        "typo_left": noul(typo),
        "changed_dish": noul(0.0),
        "dropped_modifier": noul(0.0),
    }


def test_ingredient_questions_one_per_item_and_hidden_excludes_listed():
    q = ingredient_questions([{"code": "PORK", "inclusionPercent": 95}, {"code": "SOY", "inclusionPercent": 60}])
    assert {"listed_PORK", "listed_SOY", "spiciness_wrong"} <= set(q)
    assert "hidden_SOY" not in q and {f"hidden_{c}" for c in HIDDEN_CODES if c != "SOY"} <= set(q)
    # 적힌 성분과 숨은 후보는 같은 문구 — 성분 이름만 다르다.
    assert q["listed_SOY"]["instructions"] == q["hidden_EGG"]["instructions"].replace("계란(EGG)", "대두(SOY)")


@pytest.mark.parametrize(
    "overrides, expected",
    [
        ({}, ("PASS", [])),
        ({"listed_SOY": noul(0.1)}, ("REVISE", ["UNRELATED_INGREDIENT"])),  # 포함확률 60% 인데 안 들어감
        ({"listed_GARLIC": noul(0.1)}, ("PASS", [])),  # 포함확률 30% 는 무관 판단에서 뺀다
        ({"hidden_FISH_SAUCE": noul(0.85)}, ("REVISE", ["MISSING_INGREDIENT"])),
        ({"hidden_FISH_SAUCE": noul(0.6)}, ("ABSTAIN", [])),
        ({"spiciness_wrong": noul(0.9), "hidden_EGG": noul(0.9)}, ("REVISE", ["MISSING_INGREDIENT", "SPICINESS_WRONG"])),
    ],
)
def test_decide_ingredients_per_item(overrides, expected):
    subs = [{"code": "PORK", "inclusionPercent": 95}, {"code": "SOY", "inclusionPercent": 60},
            {"code": "GARLIC", "inclusionPercent": 30}]
    answers = answers_for(ingredient_questions(subs), overrides)
    assert decide_ingredients(answers, subs, JevPolicy()) == expected


def test_uncertain_food_is_abstain_not_forced_to_not_food():
    assert decide_name(name_answers(0.5), POLICY) == ("ABSTAIN", None, [])
    assert decide_name(name_answers(0.1), POLICY) == ("REVISE", False, ["NOT_FOOD"])
    assert decide_name(name_answers(0.95, typo=0.9), POLICY) == ("REVISE", True, ["TYPO_LEFT"])
    assert decide_name(name_answers(0.95), POLICY) == ("PASS", True, [])


# ===== 모의 Jev 서버 =====


def answers_for(questions, overrides):
    """기본은 모두 '깨끗함' — 음식 확실, 결함 없음, 설명 최상 등급."""
    out = {}
    for qid, q in questions.items():
        if q["type"] == "score":
            out[qid] = {"type": "score", "score": 1.9, "confidence": 0.8, "probabilities": {"0": 0.02, "1": 0.08, "2": 0.9}}
        else:
            # 적힌 성분은 "들어간다"가 깨끗한 답이다(성분별 질문 ing-v1).
            out[qid] = noul(0.97 if qid == "is_food" or qid.startswith("listed_") else 0.02)
    out.update(overrides)
    return out


class FakeJev:
    """질문 세트로 노드를 구분해 노드별 응답 시퀀스를 돌려준다. 시퀀스 항목은 응답 또는 상태코드/예외."""

    def __init__(self, **per_node):
        self.per_node = {k: list(v) for k, v in per_node.items()}
        self.requests = defaultdict(int)

    def handler(self, request: httpx.Request):
        import json

        body = json.loads(request.content)
        # 기피성분 질문은 성분 목록마다 달라 질문 키로 구분한다.
        node = "ing" if "spiciness_wrong" in body["questions"] else {
            frozenset(NAME_QUESTIONS): "name",
            frozenset(DESC_QUESTIONS): "desc",
        }[frozenset(body["questions"])]
        self.requests[node] += 1
        assert request.headers["authorization"] == "Bearer test-key"
        seq = self.per_node.get(node)
        item = seq.pop(0) if seq else {}
        if isinstance(item, Exception):
            raise item
        if isinstance(item, tuple):
            status, headers = item
            return httpx.Response(status, headers=headers, json={"detail": "x"})
        return httpx.Response(
            200,
            json={
                "model": "jev-1.14.0",
                "answers": answers_for(body["questions"], item),
                "usage": {"input_tokens": 100, "output_tokens": 5},
            },
        )


async def no_sleep(_):
    return None


def jev_graph(fake: FakeJev, rec, nodes=("name", "description", "ingredients"), **seqs):
    llm = build_fns(rec, **seqs)
    # 대체 경로 호출을 세기 위해 LLM 검수 함수를 감싼다.
    async def counted_desc(name, desc, _f=llm.review_description):
        rec["llm_review_description"].append(name)
        return await _f(name, desc)

    async def counted_ing(name, ing, _f=llm.review_ingredients):
        rec["llm_review_ingredients"].append(name)
        return await _f(name, ing)

    async def counted_name(o, c, _f=llm.review_name):
        rec["llm_review_name"].append(c)
        return await _f(o, c)

    llm = llm._replace(review_description=counted_desc, review_ingredients=counted_ing, review_name=counted_name)
    reviews = JevReviews(
        JevSettings(api_key="test-key"), llm, transport=httpx.MockTransport(fake.handler), sleep=no_sleep
    )
    return build_content_graph(with_jev_reviews(llm, reviews, nodes))


async def run_jev(fake, rec=None, **seqs):
    rec = rec if rec is not None else defaultdict(list)
    state = await jev_graph(fake, rec, **seqs).ainvoke({"food_name": "김치찌게 8,000원"})
    return state, rec


# ===== 그래프 경로 =====


async def test_clean_jev_run_uses_six_llm_calls_and_three_jev_requests():
    fake = FakeJev()
    state, rec = await run_jev(fake)

    assert state["verdict"].passed is True
    assert dict(fake.requests) == {"name": 1, "desc": 1, "ing": 1}
    assert not rec["llm_review_name"] and not rec["llm_review_description"] and not rec["llm_review_ingredients"]
    traces = {t["node"]: t for t in state["review_traces"]}
    assert set(traces) == {"review_name", "review_description", "review_ingredients"}
    t = traces["review_description"]
    # 재현에 필요한 근거가 남는다 — 확률 원본·버전·질문 해시·사용량·지연.
    assert t["model"] == "jev-1.14.0" and t["policy_version"] and len(t["question_hash"]) == 12
    assert t["answers"]["quality"]["probabilities"]["2"] == 0.9
    assert t["usage"]["input_tokens"] == 100 and "latency_ms" in t
    assert "fallback" not in t
    assert traces["review_ingredients"]["question_version"] == INGREDIENT_QUESTION_VERSION


async def test_abstain_falls_back_to_llm_review_and_is_recorded():
    # 설명 등급이 애매하면(최상 0.7) Jev 판정을 쓰지 않고 기존 LLM 검수로 넘긴다.
    fake = FakeJev(desc=[{"quality": {"type": "score", "score": 1.6, "probabilities": {"0": 0.1, "1": 0.2, "2": 0.7}}}])
    state, rec = await run_jev(fake)

    assert rec["llm_review_description"] == ["김치찌개"]
    trace = next(t for t in state["review_traces"] if t["node"] == "review_description")
    assert trace["outcome"] == "ABSTAIN" and trace["fallback"] == "abstain"
    assert state["verdict"].passed is True  # LLM 스텁이 3점


async def test_reason_code_becomes_fixed_feedback_and_retry_cap_holds():
    # 사실 오류 등급 + wrong_fact 결함 → 고정 문구가 재생성 프롬프트로 간다. 두 번 다 미달이면 상한 2회에서 멈춘다.
    bad = {
        "quality": {"type": "score", "score": 0.3, "probabilities": {"0": 0.75, "1": 0.2, "2": 0.05}},
        "wrong_fact": noul(0.9),
    }
    fake = FakeJev(desc=[bad, bad])
    state, rec = await run_jev(fake)

    assert [fb for (_, fb) in rec["generate_description"]] == ["", FEEDBACK["WRONG_FACT"]]
    assert fake.requests["desc"] == 2
    assert state["description_score"].score == 1
    assert state["verdict"].failure_kind == "JUDGE_REJECTED"
    assert rec["llm_review_description"] == []  # 사유가 명확하면 LLM 대체 없음


async def test_revise_without_reason_code_asks_llm_for_reason():
    # 등급은 미달인데 어느 결함 질문도 걸리지 않음 — 무엇을 고칠지 지어내지 않고 LLM 에 사유를 받는다.
    fake = FakeJev(desc=[{"quality": {"type": "score", "score": 0.9, "probabilities": {"0": 0.2, "1": 0.7, "2": 0.1}}}])
    state, rec = await run_jev(fake)

    assert rec["llm_review_description"] == ["김치찌개"]
    trace = next(t for t in state["review_traces"] if t["node"] == "review_description")
    assert trace["fallback"] == "no_reason"


async def test_uncertain_missing_ingredient_falls_back_instead_of_passing():
    # 숨은 성분(새우젓) 확률 0.6 — 누락 확정(0.8)은 아니지만 안전 직결이라 통과시키지 않고 보류 → LLM.
    fake = FakeJev(ing=[{"hidden_SALTED_SHRIMP": noul(0.6)}])
    state, rec = await run_jev(fake)

    assert rec["llm_review_ingredients"] == ["김치찌개"]
    assert state["ingredients"]["substances"] == [{"code": "PORK", "inclusionPercent": 95}]


async def test_jev_outage_falls_back_to_llm_after_limited_retry():
    fake = FakeJev(name=[(503, {}), (529, {})])
    state, rec = await run_jev(fake)

    assert fake.requests["name"] == 2  # max_attempts=2
    assert rec["llm_review_name"] == ["김치찌개"]
    trace = next(t for t in state["review_traces"] if t["node"] == "review_name")
    assert trace["fallback"] == "outage"
    assert state["verdict"].passed is True


async def test_429_retries_with_retry_after_then_succeeds():
    slept = []

    async def sleep(s):
        slept.append(s)

    fake = FakeJev(ing=[(429, {"retry-after": "1.5"})])
    llm = build_fns(defaultdict(list))
    reviews = JevReviews(JevSettings(api_key="test-key"), llm, transport=httpx.MockTransport(fake.handler), sleep=sleep)
    r = await reviews.review_ingredients("김치찌개", {"substances": [], "spiciness": 3})

    assert r.passed is True and fake.requests["ing"] == 2 and slept == [1.5]


@pytest.mark.parametrize("failure", [(429, {}), httpx.ReadTimeout("slow")])
async def test_timeout_or_429_exhausted_fails_message_not_content(failure):
    # timeout·429 는 콘텐츠 판정이 아니다 — passed=false·빈 성분으로 적재하지 않고 메시지를 실패시킨다.
    fake = FakeJev(ing=[failure, failure])
    graph = jev_graph(fake, defaultdict(list))
    kbap = FakeKbap()

    failures = await process_event({"Records": [record("m1", 1, "김치찌개")]}, graph, kbap, concurrency=1)

    assert failures == ["m1"]
    assert kbap.posts == []
    assert fake.requests["ing"] == 2  # 그래프 RetryPolicy 가 다시 돌리지 않는다(곱셈 재시도 없음)


@pytest.mark.parametrize("status", [400, 401, 422])
async def test_auth_or_format_error_is_not_retried(status):
    fake = FakeJev(name=[(status, {})])
    llm = build_fns(defaultdict(list))
    reviews = JevReviews(JevSettings(api_key="test-key"), llm, transport=httpx.MockTransport(fake.handler), sleep=no_sleep)

    with pytest.raises(JevRequestError):
        await reviews.review_name("김치찌게", "김치찌개")
    assert fake.requests["name"] == 1


def test_jev_errors_are_not_retried_by_graph_policy():
    assert _retryable(JevTransientError("x")) is False
    assert _retryable(JevRequestError("x")) is False


async def test_duplicate_ingest_is_acked_but_other_errors_fail_with_jev_engine():
    graph = jev_graph(FakeJev(), defaultdict(list))

    dup = await process_event(
        {"Records": [record("m1", 1, "김치찌개")]}, graph, FakeKbap(DuplicateIngestError("FOOD-004")), concurrency=1
    )
    other = await process_event(
        {"Records": [record("m2", 2, "김치찌개")]}, graph, FakeKbap(RuntimeError("409 다른 코드")), concurrency=1
    )

    assert dup == [] and other == ["m2"]


# ===== 엔진 선택 =====


def test_llm_engine_is_default_and_returns_fns_untouched(monkeypatch):
    monkeypatch.delenv("REVIEW_ENGINE", raising=False)
    fns = build_fns(defaultdict(list))
    assert apply_review_engine(fns, {}) is fns
    assert apply_review_engine(fns, {"review_engine": "llm"}) is fns


def test_env_overrides_config_and_jev_requires_key(monkeypatch):
    fns = build_fns(defaultdict(list))
    monkeypatch.setenv("REVIEW_ENGINE", "jev")
    monkeypatch.delenv("TYPESAFE_API_KEY", raising=False)
    with pytest.raises(KeyError):
        apply_review_engine(fns, {"review_engine": "llm"})

    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    jev_fns = apply_review_engine(fns, {"jev": {"model": "jev-1.13.0", "policy": {"food_min": 0.9}}})
    assert jev_fns.review_name.__self__.settings.model == "jev-1.13.0"
    assert jev_fns.review_name.__self__.policy.food_min == 0.9
    assert jev_fns.generate_description is fns.generate_description  # 생성은 그대로


@pytest.mark.parametrize("spec, expected", [
    ("llm", {"name": "llm", "description": "llm", "ingredients": "llm"}),
    ("jev", {"name": "jev", "description": "jev", "ingredients": "jev"}),
    ("description=jev", {"name": "llm", "description": "jev", "ingredients": "llm"}),
    ("description=jev, name=jev", {"name": "jev", "description": "jev", "ingredients": "llm"}),
])
def test_review_engine_spec_per_node(spec, expected):
    assert parse_review_engine(spec) == expected


@pytest.mark.parametrize("spec", ["gpt", "description=gpt", "desc=jev", "description"])
def test_bad_review_engine_spec_rejected(spec):
    with pytest.raises(ValueError):
        parse_review_engine(spec)


def test_description_only_keeps_llm_name_and_ingredient_reviews(monkeypatch):
    # R-174: 설명만 Jev — 이름·기피성분 검수는 기존 LLM 함수 그대로(결과 무변).
    monkeypatch.setenv("REVIEW_ENGINE", "description=jev")
    monkeypatch.setenv("TYPESAFE_API_KEY", "k")
    fns = build_fns(defaultdict(list))
    mixed = apply_review_engine(fns, {})
    assert mixed.review_description.__self__.settings.api_key == "k"
    assert mixed.review_name is fns.review_name and mixed.review_ingredients is fns.review_ingredients


async def test_mixed_engine_keeps_outage_fallback_and_failure_policy():
    # 노드별로 섞여도 대체·실패 정책은 그대로 — 장애는 LLM 대체, 429 소진은 메시지 실패.
    fake = FakeJev(desc=[(503, {}), (529, {})])
    rec = defaultdict(list)
    state = await jev_graph(fake, rec, nodes=("description",)).ainvoke({"food_name": "김치찌개"})
    assert dict(fake.requests) == {"desc": 2}
    assert rec["llm_review_description"] == ["김치찌개"]
    assert len(rec["llm_review_name"]) == 1 and len(rec["llm_review_ingredients"]) == 1
    assert [t["fallback"] for t in state["review_traces"]] == ["outage"]

    fake = FakeJev(desc=[(429, {}), (429, {})])
    kbap = FakeKbap()
    failures = await process_event(
        {"Records": [record("m1", 1, "김치찌개")]}, jev_graph(fake, defaultdict(list), nodes=("description",)), kbap, concurrency=1
    )
    assert failures == ["m1"] and kbap.posts == []


def test_unknown_engine_rejected(monkeypatch):
    monkeypatch.delenv("REVIEW_ENGINE", raising=False)
    with pytest.raises(ValueError):
        apply_review_engine(build_fns(defaultdict(list)), {"review_engine": "gpt"})


PAYLOAD_KEYS = {
    "outboxId", "foodId", "displayName", "passed", "description", "spiciness",
    "nameTranslations", "descriptionTranslations", "ingredients", "longDescription",
}


async def test_llm_engine_keeps_nine_calls_and_payload(monkeypatch):
    # 기본 엔진(llm)은 머지돼도 운영 동작이 그대로다 — 호출 9회, 판정 근거 없음, payload 키 동일.
    monkeypatch.delenv("REVIEW_ENGINE", raising=False)
    calls = defaultdict(int)
    state = await build_content_graph(apply_review_engine(counting_fns(calls), {})).ainvoke({"food_name": "김치찌개"})

    assert sum(calls.values()) == 9
    assert not state.get("review_traces")
    assert set(build_ingest_payload(state, 1, 1)) == PAYLOAD_KEYS


async def test_jev_engine_payload_has_no_jev_fields():
    # Jev 응답·trace 는 그래프 상태에만 남고 적재 payload 로 새지 않는다.
    state, _ = await run_jev(FakeJev())
    payload = build_ingest_payload(state, 1, 1)
    assert set(payload) == PAYLOAD_KEYS
    assert "jev" not in json.dumps(payload, ensure_ascii=False)


async def test_retry_after_beyond_budget_is_not_retried_early():
    # 서버가 30초 뒤를 요구하면 4초 만에 마지막 시도를 버리지 않는다 — 바로 일시 실패(SQS 재시도)로.
    slept = []

    async def sleep(s):
        slept.append(s)

    fake = FakeJev(ing=[(429, {"retry-after": "30"})])
    llm = build_fns(defaultdict(list))
    reviews = JevReviews(JevSettings(api_key="test-key"), llm, transport=httpx.MockTransport(fake.handler), sleep=sleep)

    with pytest.raises(JevTransientError):
        await reviews.review_ingredients("김치찌개", {"substances": [], "spiciness": 3})
    assert fake.requests["ing"] == 1 and slept == [] and reviews.requests == 1


async def test_request_count_includes_retries():
    fake = FakeJev(name=[(503, {})])
    llm = build_fns(defaultdict(list))
    reviews = JevReviews(JevSettings(api_key="test-key"), llm, transport=httpx.MockTransport(fake.handler), sleep=no_sleep)
    r = await reviews.review_name("김치찌게", "김치찌개")

    assert reviews.requests == 2 and r._trace["requests"] == 2
