"""콘텐츠 도메인(kbap.content) — 그래프 연결·재시도 규칙·형식 검증·SQS 핸들러."""

import json
from collections import defaultdict

import pytest
from pydantic import ValidationError

from kbap.content import (
    VALID_CODES,
    ContentFns,
    DescGen,
    JudgeVerdict,
    Translations,
    build_content_graph,
    process_event,
    valid_substances,
)
from kbap.review import TARGET_LANGS, FieldScore, Thresholds

TH = Thresholds(description=70, translations=70, avoidance=70)


# ===== 그래프 배선·재시도 =====


def build_fns(rec, name_translation_seq=(90,), description_seq=(90,), ingredient_seq=(90,)):
    """호출 기록(rec)과 검수 점수 시퀀스로 테스트용 노드 함수 모음을 만든다."""
    name_translation_seq_scores, description_seq_scores, ingredient_scores = list(name_translation_seq), list(description_seq), list(ingredient_seq)

    async def clean_name(name):
        rec["clean"].append(name)
        return {"name": "김치찌개", "reason": "오타 교정"}

    async def generate_name_translations(name, feedback):
        rec["gen_nt"].append((name, feedback))
        return {"en": "Kimchi Stew"}

    async def generate_description(name, feedback):
        rec["generate_description"].append((name, feedback))
        return "돼지고기와 김치를 끓인 찌개."

    async def generate_description_translations(name, desc):
        rec["generate_description_translations"].append((name, desc))
        return {"en": "A stew of pork and kimchi."}

    async def generate_ingredients(name, feedback):
        rec["generate_ingredients"].append((name, feedback))
        return {"substances": [{"code": "PORK", "inclusionPercent": 95}], "spiciness": 3}

    async def review_name_translations(name, translations):
        score = name_translation_seq_scores.pop(0)
        return FieldScore(score=score, reason=f"이름번역 {score}")

    async def review_description(name, desc, desc_tr):
        score = description_seq_scores.pop(0)
        return FieldScore(score=score, reason=f"설명 {score}")

    async def review_ingredients(name, ingredients):
        score = ingredient_scores.pop(0)
        return FieldScore(score=score, reason=f"기피 {score}")

    async def judge(state):
        rec["judge"].append(state)
        scores = [state["name_translation_score"], state["description_score"], state["ingredient_score"]]
        passed = all(s.score >= 70 for s in scores)
        return JudgeVerdict(reason="종합", passed=passed, rejected_fields=[])

    return ContentFns(
        clean_name=clean_name,
        generate_name_translations=generate_name_translations,
        generate_description=generate_description,
        generate_description_translations=generate_description_translations,
        generate_ingredients=generate_ingredients,
        review_name_translations=review_name_translations,
        review_description=review_description,
        review_ingredients=review_ingredients,
        judge=judge,
    )


async def run(rec, **seqs):
    graph = build_content_graph(build_fns(rec, **seqs), TH)
    return await graph.ainvoke({"food_name": "김치찌게 8,000원"})


async def test_happy_path_populates_all_content():
    rec = defaultdict(list)
    state = await run(rec)

    assert state["cleaned_name"] == "김치찌개"
    assert state["name_translations"] == {"en": "Kimchi Stew"}
    assert state["description"].startswith("돼지고기")
    assert state["description_translations"]["en"].startswith("A stew")
    assert state["ingredients"]["spiciness"] == 3
    assert state["verdict"].passed is True
    # 생성은 각각 한 번, 종합 판정은 join 후 정확히 한 번 실행한다
    assert len(rec["generate_description"]) == 1
    assert len(rec["judge"]) == 1


async def test_generators_receive_cleaned_name():
    rec = defaultdict(list)
    await run(rec)
    # 생성 단계에는 원본("김치찌게 8,000원")이 아닌 정제된 이름을 전달해야 한다.
    assert rec["gen_nt"][0][0] == "김치찌개"
    assert rec["generate_description"][0][0] == "김치찌개"
    assert rec["generate_ingredients"][0][0] == "김치찌개"


async def test_review_fail_retries_generation_once_with_feedback():
    rec = defaultdict(list)
    state = await run(rec, description_seq=(30, 90))

    assert len(rec["generate_description"]) == 2
    # 재시도 프롬프트에 탈락 사유를 포함해야 한다.
    assert rec["generate_description"][1][1] == "설명 30"
    # 설명을 재생성하면 설명 번역도 다시 만든다(순차 분기).
    assert len(rec["generate_description_translations"]) == 2
    assert state["description_attempts"] == 2
    assert state["verdict"].passed is True
    assert len(rec["judge"]) == 1


async def test_retry_exhausted_flows_failure_to_judge():
    rec = defaultdict(list)
    state = await run(rec, description_seq=(30, 40))

    # 한 번만 재시도해 총 생성 횟수가 2회를 넘지 않는다.
    assert len(rec["generate_description"]) == 2
    assert len(rec["judge"]) == 1
    # 종합 판정에는 실패 점수와 사유를 그대로 전달한다.
    judged = rec["judge"][0]
    assert judged["description_score"].score == 40
    assert judged["description_feedback"] == "설명 40"
    assert state["verdict"].passed is False


async def test_independent_branches_do_not_retry_each_other():
    rec = defaultdict(list)
    await run(rec, ingredient_seq=(30, 90))

    assert len(rec["generate_ingredients"]) == 2
    assert len(rec["generate_description"]) == 1
    assert len(rec["gen_nt"]) == 1


# ===== 기피성분 후보 필터 =====


def test_valid_codes_covers_all_81_candidates():
    assert len(VALID_CODES) == 81
    assert "PORK" in VALID_CODES
    assert "SALTED_SHRIMP" in VALID_CODES


def test_valid_substances_drops_out_of_candidate_codes():
    # 과거 모델이 후보 밖 코드(김치 등)를 반환한 사례가 있어 저장 전에 걸러낸다.
    items = [
        {"code": "PORK", "inclusionPercent": 95},
        {"code": "KIMCHI", "inclusionPercent": 90},
    ]
    assert valid_substances(items) == [{"code": "PORK", "inclusionPercent": 95}]


def test_valid_substances_keeps_first_on_duplicate():
    items = [
        {"code": "PORK", "inclusionPercent": 95},
        {"code": "PORK", "inclusionPercent": 10},
    ]
    assert valid_substances(items) == [{"code": "PORK", "inclusionPercent": 95}]


# ===== 생성 결과 형식 검증 (스프링 LLM 경계 require 이관) =====


def test_desc_gen_rejects_over_255_chars():
    with pytest.raises(ValidationError):
        DescGen(description="가" * 256)
    assert DescGen(description="가" * 255).description == "가" * 255


def test_desc_gen_rejects_blank_and_placeholder():
    with pytest.raises(ValidationError):
        DescGen(description="   ")
    with pytest.raises(ValidationError):
        DescGen(description="설명 준비 중")


def test_translations_require_all_nine_languages():
    full = [{"lang": lang, "text": "t"} for lang in TARGET_LANGS]
    assert len(Translations(reason="ok", items=full).items) == 9
    with pytest.raises(ValidationError):
        Translations(reason="ok", items=full[:8])  # 언어 누락
    with pytest.raises(ValidationError):
        Translations(reason="ok", items=full + [{"lang": "fr", "text": "t"}])  # 목록 밖 언어


def test_translations_reject_blank_text():
    items = [{"lang": lang, "text": "t"} for lang in TARGET_LANGS[:-1]]
    items.append({"lang": TARGET_LANGS[-1], "text": "  "})
    with pytest.raises(ValidationError):
        Translations(reason="ok", items=items)


def test_valid_substances_drops_zero_percent():
    # 스프링은 0% 항목을 저장 전에 걸렀다(RiskLevel 은 1~100만 허용) — 동일하게 제거한다.
    items = [
        {"code": "PORK", "inclusionPercent": 95},
        {"code": "ONION", "inclusionPercent": 0},
    ]
    assert valid_substances(items) == [{"code": "PORK", "inclusionPercent": 95}]


# ===== SQS Lambda 핸들러 =====


class FakeGraph:
    def __init__(self, fail_names=()):
        self.calls = []
        self.fail_names = set(fail_names)

    async def ainvoke(self, state, config=None):
        self.calls.append(state["food_name"])
        if state["food_name"] in self.fail_names:
            raise RuntimeError("LLM down")
        return {**state, "verdict": JudgeVerdict(reason="ok", passed=True)}


def record(message_id: str, food_id: int, name: str) -> dict:
    return {"messageId": message_id, "body": json.dumps({"foodId": food_id, "scannedName": name})}


async def test_all_success_reports_no_failures():
    graph = FakeGraph()
    event = {"Records": [record("m1", 1, "김치찌개"), record("m2", 2, "불고기")]}

    failures = await process_event(event, graph, concurrency=20)

    assert failures == []
    assert sorted(graph.calls) == ["김치찌개", "불고기"]


async def test_partial_failure_reports_only_failed_message():
    # 10건 묶음에서 1건만 실패하면 해당 메시지만 다시 수신해야 한다.
    # 전체를 다시 수신하면 성공한 9건의 LLM 비용이 중복으로 발생한다.
    graph = FakeGraph(fail_names={"불고기"})
    event = {"Records": [record("m1", 1, "김치찌개"), record("m2", 2, "불고기")]}

    failures = await process_event(event, graph, concurrency=20)

    assert failures == ["m2"]


async def test_malformed_body_is_reported_as_failure():
    # 계약 위반 메시지는 버리지 않고 실패로 보고해 DLQ로 보낸다.
    graph = FakeGraph()
    event = {"Records": [{"messageId": "bad", "body": "not-json"}]}

    failures = await process_event(event, graph, concurrency=20)

    assert failures == ["bad"]
    assert graph.calls == []
