"""콘텐츠 도메인(kbap.content) — 그래프 연결·재시도 규칙·형식 검증·SQS 핸들러."""

import json
from collections import defaultdict

import pytest
from pydantic import ValidationError

from kbap.content import (
    VALID_CODES,
    ContentFns,
    DESC_PASS_SCORE,
    DescGen,
    DescReview,
    IngredientsReview,
    JudgeVerdict,
    LongDescGen,
    NameReview,
    Translations,
    build_content_graph,
    build_ingest_payload,
    process_event,
    valid_substances,
)
from kbap.review import TARGET_LANGS


# ===== 그래프 배선·재시도 =====


def build_fns(rec, name_review_seq=(True,), description_seq=(3,), ingredient_seq=(True,)):
    """호출 기록(rec)과 검수 통과/점수 시퀀스로 테스트용 노드 함수 모음을 만든다."""
    name_review_results, description_seq_scores, ingredient_results = (
        list(name_review_seq), list(description_seq), list(ingredient_seq)
    )

    async def clean_name(name, feedback):
        rec["clean"].append((name, feedback))
        return {"name": "김치찌개", "reason": "오타 교정"}

    async def review_name(original, cleaned):
        rec["review_name"].append((original, cleaned))
        passed = name_review_results.pop(0)
        return NameReview(reason=f"이름검수{len(rec['review_name'])}", is_food=True, passed=passed)

    async def generate_name_translations(name):
        rec["gen_nt"].append(name)
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

    async def generate_long_description(name):
        rec["generate_long_description"].append(name)
        return "돼지고기와 신김치를 넣고 끓인 한국의 대표 찌개 요리. 얼큰하고 시원한 국물이 특징이다."

    async def review_description(name, desc):
        score = description_seq_scores.pop(0)
        return DescReview(reason=f"설명 {score}", score=score)

    async def review_ingredients(name, ingredients):
        passed = ingredient_results.pop(0)
        return IngredientsReview(reason=f"기피 {'통과' if passed else '불통과'}", passed=passed)

    async def judge(state):
        rec["judge"].append(state)
        passed = (
            state["description_score"].score >= DESC_PASS_SCORE
            and state["ingredient_review"].passed
        )
        return JudgeVerdict(reason="종합", passed=passed, rejected_fields=[])

    return ContentFns(
        clean_name=clean_name,
        review_name=review_name,
        generate_name_translations=generate_name_translations,
        generate_description=generate_description,
        generate_description_translations=generate_description_translations,
        generate_ingredients=generate_ingredients,
        generate_long_description=generate_long_description,
        review_description=review_description,
        review_ingredients=review_ingredients,
        judge=judge,
    )


async def run(rec, **seqs):
    graph = build_content_graph(build_fns(rec, **seqs))
    return await graph.ainvoke({"food_name": "김치찌게 8,000원"})


async def test_happy_path_populates_all_content():
    rec = defaultdict(list)
    state = await run(rec)

    assert state["cleaned_name"] == "김치찌개"
    assert state["name_translations"] == {"en": "Kimchi Stew"}
    assert state["description"].startswith("돼지고기")
    assert state["description_translations"]["en"].startswith("A stew")
    assert state["ingredients"]["spiciness"] == 3
    assert state["long_description"].startswith("돼지고기와 신김치")
    assert state["verdict"].passed is True
    # 생성은 각각 한 번, 종합 판정은 join 후 정확히 한 번 실행한다
    assert len(rec["generate_description"]) == 1
    assert len(rec["generate_long_description"]) == 1
    assert len(rec["judge"]) == 1
    # 이름 번역은 검수 없이 1회 생성 — 9키 전수는 pydantic(Translations)이 보장한다
    assert rec["gen_nt"] == ["김치찌개"]


async def test_failed_ingredient_review_fails_even_if_judge_passes():
    # 안전 fail-closed: LLM judge가 통과를 줘도 기피성분 검수 불통과면 코드가 탈락시킨다.
    rec = defaultdict(list)

    async def lenient_judge(state):
        return JudgeVerdict(reason="관대한 판정", passed=True, rejected_fields=[])

    fns = build_fns(rec, ingredient_seq=(False, False))._replace(judge=lenient_judge)
    graph = build_content_graph(fns)
    state = await graph.ainvoke({"food_name": "김치찌개"})

    assert state["verdict"].passed is False
    assert state["verdict"].rejected_fields == ["avoidance"]
    assert state["verdict"].failure_kind == "INGREDIENT_GUARD"


async def test_non_food_ends_graph_without_generation_or_retry():
    # "사리 추가" 같은 옵션·판독 불가 입력은 이름 검수에서 그래프를 끝내 생성 8콜을 아낀다.
    # 비음식은 이름을 다시 고쳐도 음식이 안 되므로 재정제 3회 루프도 타지 않는다.
    rec = defaultdict(list)

    async def non_food_review(original, cleaned):
        rec["review_name"].append((original, cleaned))
        return NameReview(reason="옵션 항목", is_food=False, passed=False)

    fns = build_fns(rec)._replace(review_name=non_food_review)
    graph = build_content_graph(fns)
    state = await graph.ainvoke({"food_name": "사리 추가"})

    assert state["verdict"].passed is False
    assert "부적합" in state["verdict"].reason
    assert state["verdict"].failure_kind == "NOT_FOOD"
    assert len(rec["clean"]) == 1  # 재정제 없이 즉시 종료
    assert rec["gen_nt"] == []
    assert rec["generate_description"] == []
    assert rec["generate_ingredients"] == []
    assert rec["generate_long_description"] == []
    assert rec["judge"] == []  # LLM 종합 판정도 건너뛴다


async def test_name_review_fail_retries_clean_with_feedback():
    # 이름 검수 탈락 사유가 재정제 프롬프트로 전달돼야 한다.
    rec = defaultdict(list)
    state = await run(rec, name_review_seq=(False, True))

    assert [feedback for (_, feedback) in rec["clean"]] == ["", "이름검수1"]
    assert len(rec["review_name"]) == 2
    assert state["verdict"].passed is True
    # 검수를 통과한 뒤에만 후행 분기가 정확히 한 번 실행된다.
    assert len(rec["gen_nt"]) == 1
    assert len(rec["generate_long_description"]) == 1


async def test_name_review_exhausted_rejects_as_not_food():
    # 정제 3회(초회 포함) 후에도 검수 불통과면 NOT_FOOD 로 종료하고 생성은 시작하지 않는다.
    rec = defaultdict(list)
    state = await run(rec, name_review_seq=(False, False, False))

    assert len(rec["clean"]) == 3
    assert state["verdict"].passed is False
    assert state["verdict"].failure_kind == "NOT_FOOD"
    assert rec["gen_nt"] == []
    assert rec["judge"] == []


async def test_generators_receive_cleaned_name():
    rec = defaultdict(list)
    await run(rec)
    # 생성 단계에는 원본("김치찌게 8,000원")이 아닌 정제된 이름을 전달해야 한다.
    assert rec["gen_nt"][0] == "김치찌개"
    assert rec["generate_description"][0][0] == "김치찌개"
    assert rec["generate_ingredients"][0][0] == "김치찌개"
    assert rec["generate_long_description"][0] == "김치찌개"


async def test_review_fail_retries_generation_once_with_feedback():
    rec = defaultdict(list)
    state = await run(rec, description_seq=(2, 3))

    assert len(rec["generate_description"]) == 2
    # 재시도 프롬프트에 탈락 사유를 포함해야 한다.
    assert rec["generate_description"][1][1] == "설명 2"
    # 검수는 한국어 설명만 보므로, 번역은 설명이 확정된 뒤 정확히 한 번만 만든다.
    assert len(rec["generate_description_translations"]) == 1
    assert state["description_attempts"] == 2
    assert state["verdict"].passed is True
    assert len(rec["judge"]) == 1


async def test_retry_exhausted_flows_failure_to_judge():
    rec = defaultdict(list)
    state = await run(rec, description_seq=(2, 2))

    # 한 번만 재시도해 총 생성 횟수가 2회를 넘지 않는다.
    assert len(rec["generate_description"]) == 2
    # 재시도 소진 후에도 번역은 1회 생성해 judge 로 간다 (judge 가 통과시키면 payload 에 필요).
    assert len(rec["generate_description_translations"]) == 1
    assert len(rec["judge"]) == 1
    # 종합 판정에는 실패 점수와 사유를 그대로 전달한다.
    judged = rec["judge"][0]
    assert judged["description_score"].score == 2
    assert judged["description_feedback"] == "설명 2"
    assert state["verdict"].passed is False
    # LLM judge 가 failure_kind 를 안 채워도(테스트 fns 처럼) 노드 코드가 확정적으로 찍는다.
    assert state["verdict"].failure_kind == "JUDGE_REJECTED"


async def test_independent_branches_do_not_retry_each_other():
    rec = defaultdict(list)
    await run(rec, ingredient_seq=(False, True))

    assert len(rec["generate_ingredients"]) == 2
    assert len(rec["generate_description"]) == 1
    assert len(rec["gen_nt"]) == 1
    # 검수 루프가 없는 긴 설명은 다른 분기의 재시도에 휘말리지 않는다.
    assert len(rec["generate_long_description"]) == 1


# ===== kbap 적재 페이로드 =====
# 계약: agenthub wiki/langchain-food-ingest-contract.md


def test_ingest_payload_passed_maps_contract_fields():
    state = {
        "food_name": "김치찌게 8,000원",
        "cleaned_name": "김치찌개",
        "description": "돼지고기와 김치를 끓인 찌개",
        "name_translations": {"en": "Kimchi Stew"},
        "description_translations": {"en": "A stew"},
        "ingredients": {"substances": [{"code": "PORK", "inclusionPercent": 95}], "spiciness": 3},
        "long_description": "돼지고기와 신김치를 넣고 끓인 찌개. 얼큰한 국물이 특징이다.",
        "verdict": JudgeVerdict(reason="ok", passed=True),
    }

    assert build_ingest_payload(state, food_id=1234) == {
        "foodId": 1234,  # 큐 메시지 값 왕복 — 서버는 foodId 로만 대상을 찾는다
        "displayName": "김치찌개",  # 스캔 원본이 아닌 정제된 이름
        "passed": True,
        "description": "돼지고기와 김치를 끓인 찌개",
        "spiciness": 3,
        "nameTranslations": {"en": "Kimchi Stew"},
        "descriptionTranslations": {"en": "A stew"},
        "ingredients": [{"code": "PORK", "inclusion_percent": 95}],  # 계약 키는 snake_case
        "longDescription": "돼지고기와 신김치를 넣고 끓인 찌개. 얼큰한 국물이 특징이다.",
    }


def test_ingest_payload_failed_sends_kind_and_reason_only():
    state = {
        "food_name": "사리 추가",
        "cleaned_name": "사리 추가",
        "verdict": JudgeVerdict(
            reason="콘텐츠 생성 부적합: 옵션 항목", passed=False, failure_kind="NOT_FOOD"
        ),
    }

    assert build_ingest_payload(state, food_id=99) == {
        "foodId": 99,
        "displayName": "사리 추가",
        "passed": False,
        "failureKind": "NOT_FOOD",
        "reason": "콘텐츠 생성 부적합: 옵션 항목",
    }


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


def test_long_desc_gen_rejects_over_500_chars():
    with pytest.raises(ValidationError):
        LongDescGen(long_description="가" * 501)


def test_long_desc_gen_rejects_blank():
    with pytest.raises(ValidationError):
        LongDescGen(long_description="   ")


def test_long_desc_gen_keeps_multi_sentence_periods():
    # 디스플레이 설명과 달리 여러 문장이므로 마침표를 제거하지 않는다.
    text = "돼지고기와 신김치를 넣고 끓인 찌개. 얼큰한 국물이 특징이다."
    assert LongDescGen(long_description=f"  {text}  ").long_description == text


def test_desc_gen_rejects_over_255_chars():
    with pytest.raises(ValidationError):
        DescGen(description="가" * 256)
    assert DescGen(description="가" * 255).description == "가" * 255


def test_desc_gen_rejects_blank_and_placeholder():
    with pytest.raises(ValidationError):
        DescGen(description="   ")
    with pytest.raises(ValidationError):
        DescGen(description="설명 준비 중")


def test_translation_item_strips_trailing_period():
    from kbap.content import TranslationItem

    assert TranslationItem(lang="en", text="Fried cheese balls.").text == "Fried cheese balls"
    assert TranslationItem(lang="ja", text="チーズボール。").text == "チーズボール"
    assert TranslationItem(lang="zh-Hant", text="起司球．").text == "起司球"  # 전각 마침표


def test_desc_gen_strips_trailing_period():
    # 마침표 금지는 콘텐츠 정책 — 모델이 붙여도 재생성 없이 잘라낸다.
    assert DescGen(description="치즈를 넣어 튀긴 사이드 메뉴.").description == "치즈를 넣어 튀긴 사이드 메뉴"
    with pytest.raises(ValidationError):
        DescGen(description="...")  # 마침표뿐인 설명은 빈 값으로 취급
    # 길이 검사는 마침표 제거 후 — 255자 + 마침표는 탈락이 아니라 255자로 정리된다.
    assert DescGen(description="가" * 255 + ".").description == "가" * 255


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
    def __init__(self, fail_names=(), verdict=None):
        self.calls = []
        self.fail_names = set(fail_names)
        self.verdict = verdict or JudgeVerdict(reason="ok", passed=True)

    async def ainvoke(self, state, config=None):
        name = state["food_name"]
        self.calls.append(name)
        if name in self.fail_names:
            raise RuntimeError("LLM down")
        # 실제 그래프처럼 완결된 최종 상태를 돌려준다 — 페이로드 조립이 이 키들을 쓴다.
        return {
            **state,
            "cleaned_name": name,
            "description": f"{name} 설명",
            "name_translations": {"en": "x"},
            "description_translations": {"en": "y"},
            "ingredients": {"substances": [{"code": "PORK", "inclusionPercent": 95}], "spiciness": 3},
            "long_description": f"{name} 상세 설명. 여러 문장이다.",
            "verdict": self.verdict,
        }


class FakeKbap:
    def __init__(self, fail=False):
        self.posts = []
        self.fail = fail

    async def post_food_content(self, payload):
        if self.fail:
            raise RuntimeError("kbap 5xx")
        self.posts.append(payload)


def record(message_id: str, food_id: int, name: str) -> dict:
    return {"messageId": message_id, "body": json.dumps({"foodId": food_id, "scannedName": name})}


# 적재 POST 임시 비활성(프롬프트 튜닝 기간 — content.py process_event 주석 참조).
# POST 를 되살릴 때 이 마커를 지우고 아래 skip 두 개와 주석 처리된 단언을 복원한다.
_POST_DISABLED = pytest.mark.skip(reason="적재 POST 임시 비활성 — 프롬프트 튜닝 기간")


async def test_all_success_posts_each_food_and_reports_no_failures():
    graph, kbap = FakeGraph(), FakeKbap()
    event = {"Records": [record("m1", 1, "김치찌개"), record("m2", 2, "불고기")]}

    failures = await process_event(event, graph, kbap, concurrency=20)

    assert failures == []
    assert sorted(graph.calls) == ["김치찌개", "불고기"]
    # POST 임시 비활성 동안은 아무것도 전송하지 않는다.
    assert kbap.posts == []
    # assert sorted(p["displayName"] for p in kbap.posts) == ["김치찌개", "불고기"]
    # assert all(p["passed"] for p in kbap.posts)


@_POST_DISABLED
async def test_failed_verdict_is_posted_with_failure_kind():
    # 판정 실패도 kbap에 적재한다(FAILED 상태 저장) — 메시지 재시도 대상이 아니다.
    verdict = JudgeVerdict(reason="번역 미달", passed=False, failure_kind="JUDGE_REJECTED")
    graph, kbap = FakeGraph(verdict=verdict), FakeKbap()
    event = {"Records": [record("m1", 1, "김치찌개")]}

    failures = await process_event(event, graph, kbap, concurrency=20)

    assert failures == []
    assert kbap.posts == [
        {"foodId": 1, "displayName": "김치찌개", "passed": False, "failureKind": "JUDGE_REJECTED", "reason": "번역 미달"}
    ]


@_POST_DISABLED
async def test_post_failure_reports_message_for_retry():
    # 네트워크·5xx·409 전부 — POST 실패면 재시도(→3회 후 DLQ)로 보낸다.
    graph, kbap = FakeGraph(), FakeKbap(fail=True)
    event = {"Records": [record("m1", 1, "김치찌개")]}

    failures = await process_event(event, graph, kbap, concurrency=20)

    assert failures == ["m1"]


async def test_partial_failure_reports_only_failed_message():
    # 10건 묶음에서 1건만 실패하면 해당 메시지만 다시 수신해야 한다.
    # 전체를 다시 수신하면 성공한 9건의 LLM 비용이 중복으로 발생한다.
    graph, kbap = FakeGraph(fail_names={"불고기"}), FakeKbap()
    event = {"Records": [record("m1", 1, "김치찌개"), record("m2", 2, "불고기")]}

    failures = await process_event(event, graph, kbap, concurrency=20)

    assert failures == ["m2"]
    # 그래프 런타임 예외는 POST하지 않는다 — FAILED에 인프라 장애를 섞지 않는다.
    # (POST 임시 비활성 동안은 성공 건도 전송하지 않는다)
    assert kbap.posts == []
    # assert [p["displayName"] for p in kbap.posts] == ["김치찌개"]


async def test_name_only_message_is_contract_violation():
    # 서버가 foodId 로만 대상을 찾으므로(2026-08-11 개정) foodId 없는 메시지는
    # 적재 불가 — 그래프를 태우지 않고 실패로 보고해 DLQ로 보낸다.
    graph = FakeGraph()
    event = {"Records": [{"messageId": "m1", "body": json.dumps({"scannedName": "김치찌개"})}]}

    failures = await process_event(event, graph, FakeKbap(), concurrency=20)

    assert failures == ["m1"]
    assert graph.calls == []


async def test_malformed_body_is_reported_as_failure():
    # 계약 위반 메시지는 버리지 않고 실패로 보고해 DLQ로 보낸다.
    graph, kbap = FakeGraph(), FakeKbap()
    event = {"Records": [{"messageId": "bad", "body": "not-json"}]}

    failures = await process_event(event, graph, kbap, concurrency=20)

    assert failures == ["bad"]
    assert graph.calls == []
    assert kbap.posts == []
