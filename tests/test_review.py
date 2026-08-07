"""검수 도메인(kbap.review) — 설정·클라이언트·프롬프트·판정·그래프·러너."""

import json
import textwrap

import httpx
import pytest
from pydantic import ValidationError

from kbap.review import (
    AVOIDANCE_CODES,
    MAX_NOTE_CHARS,
    TARGET_LANGS,
    FieldScore,
    KbapClient,
    Scorers,
    Thresholds,
    TranslationLangScore,
    TranslationScores,
    Verdict,
    avoidance_prompt,
    build_graph,
    decide,
    description_prompt,
    lang_scores,
    load_config,
    run_batch,
    translations_prompt,
)

TH = Thresholds(description=70, translations=70, avoidance=70)


# ===== 설정 =====


def test_load_config(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
          avoidance_model: gpt-5-mini
        thresholds:
          description: 70
          translations: 75
          avoidance: 80
        concurrency: 3
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "secret-token")

    cfg = load_config(str(cfg_file))

    assert cfg.kbap_base_url == "http://kbap.example.com"
    assert cfg.kbap_token == "secret-token"
    assert cfg.model == "gemini-2.5-flash"
    assert cfg.avoidance_model == "gpt-5-mini"
    assert cfg.thresholds.description == 70
    assert cfg.thresholds.translations == 75
    assert cfg.thresholds.avoidance == 80
    assert cfg.concurrency == 3


def test_avoidance_model_defaults_to_model(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    assert load_config(str(cfg_file)).avoidance_model == "gemini-2.5-flash"


def test_timeout_seconds_defaults_to_120(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    assert load_config(str(cfg_file)).timeout_seconds == 120


def test_timeout_seconds_is_configurable(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
          timeout_seconds: 45
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    assert load_config(str(cfg_file)).timeout_seconds == 45


def _write(tmp_path, thresholds: str, concurrency: int):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        "kbap_api:\n"
        "  base_url: http://kbap.example.com\n"
        "llm:\n"
        "  model: openai:gpt-5-mini\n"
        f"thresholds:\n{thresholds}"
        f"concurrency: {concurrency}\n"
    )
    return str(cfg)


OK_THRESHOLDS = "  description: 70\n  translations: 70\n  avoidance: 70\n"


def test_rejects_nonpositive_concurrency(tmp_path, monkeypatch):
    # Semaphore(0) 은 모든 코루틴을 영구히 막아 무인 배치가 조용히 멈춘다.
    monkeypatch.setenv("KBAP_API_TOKEN", "t")
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, OK_THRESHOLDS, 0))


def test_rejects_threshold_outside_score_range(tmp_path, monkeypatch):
    # 음수면 전부 통과, 100 초과면 전부 탈락한다.
    monkeypatch.setenv("KBAP_API_TOKEN", "t")
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, "  description: -1\n  translations: 70\n  avoidance: 70\n", 5))
    with pytest.raises(ValidationError):
        load_config(_write(tmp_path, "  description: 70\n  translations: 70\n  avoidance: 101\n", 5))


# ===== kbap 클라이언트 =====

PATH = "/api/v1/admin/foods/content-reviews"


def make_client(handler) -> KbapClient:
    client = KbapClient(base_url="http://kbap.test", token="tok")
    client._client = httpx.AsyncClient(
        base_url="http://kbap.test",
        headers={"Authorization": "Bearer tok"},
        transport=httpx.MockTransport(handler),
    )
    return client


async def test_fetch_unwraps_base_response_payload():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == PATH
        assert request.url.params["limit"] == "50"
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(
            200,
            json={
                "success": True,
                "payload": {"items": [{"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0}]},
            },
        )

    client = make_client(handler)
    foods = await client.fetch_review_candidates(limit=50)
    assert foods == [{"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0}]


async def test_post_passed_result():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "payload": {"foodId": 7, "contentStatus": "REVIEWED"}})

    client = make_client(handler)
    applied = await client.post_review_result(
        7, Verdict(passed=True, rejected_fields=[], scores={"description": 90})
    )

    assert applied == {"foodId": 7, "contentStatus": "REVIEWED"}
    assert captured["path"] == f"{PATH}/7"
    # 통과 결과에는 rejectedFields·reason 을 보내지 않는다.
    assert captured["body"] == {"passed": True}


async def test_post_rejected_result():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "payload": {"foodId": 7, "contentStatus": "REVIEWED"}})

    client = make_client(handler)
    await client.post_review_result(
        7,
        Verdict(
            passed=False,
            rejected_fields=["AVOIDANCE_SUBSTANCES", "SPICINESS"],
            scores={"avoidance": 40},
            reason="- 기피성분·매운맛(40점): 돼지고기 누락",
        ),
    )

    assert captured["body"] == {
        "passed": False,
        "rejectedFields": ["AVOIDANCE_SUBSTANCES", "SPICINESS"],
        "reason": "- 기피성분·매운맛(40점): 돼지고기 누락",
    }


async def test_post_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409)

    client = make_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await client.post_review_result(7, Verdict(passed=True, rejected_fields=[], scores={}))


async def test_fetch_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = make_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_review_candidates(limit=50)


# ===== 채점 프롬프트·스키마 =====

PROMPT_FOOD = {
    "id": 1,
    "koreanName": "김치찌개",
    "description": "돼지고기와 김치를 넣고 끓인 얼큰한 찌개",
    "nameTranslations": {"en": "Kimchi Stew", "ja": "キムチチゲ"},
    "descriptionTranslations": {"en": "Spicy stew with pork and kimchi"},
    "spiciness": 7,
    "avoidanceSubstances": [{"code": "PORK", "inclusion_percent": 95}],
    "reviewAttempts": 0,
}


def test_target_langs():
    assert TARGET_LANGS == ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]


def test_field_score_rejects_out_of_range():
    with pytest.raises(ValidationError):
        FieldScore(score=101, reason="r")
    with pytest.raises(ValidationError):
        FieldScore(score=-1, reason="r")


def test_translation_scores_schema():
    ts = TranslationScores(items=[{"lang": "en", "score": 90, "reason": "ok"}])
    assert ts.items[0].lang == "en"


def test_description_prompt_contains_food():
    p = description_prompt(PROMPT_FOOD)
    assert "김치찌개" in p
    assert PROMPT_FOOD["description"] in p


def test_translations_prompt_lists_all_target_langs():
    p = translations_prompt(PROMPT_FOOD)
    for lang in TARGET_LANGS:
        assert lang in p
    assert "Kimchi Stew" in p


def test_avoidance_prompt_contains_substances_and_spiciness():
    p = avoidance_prompt(PROMPT_FOOD)
    assert "PORK" in p
    assert "95" in p
    assert "7" in p


def test_avoidance_prompt_carries_generator_contract():
    """생성기(SpringAiFoodAvoidanceAssessmentClient)와 같은 척도·후보 목록을 실어야 한다.

    이게 빠지면 검수기가 제 감각으로 판단해, 규격대로 생성된 데이터를 깎고(매운맛 척도 불일치)
    후보에 없어 넣을 수 없던 성분을 누락으로 감점한다 — 스모크에서 실제로 나온 실패.
    """
    p = avoidance_prompt(PROMPT_FOOD)
    assert AVOIDANCE_CODES in p
    assert "1~3 약간 매콤" in p  # 생성기 척도. 없으면 김치찌개 3점을 "너무 낮다"고 깎는다.
    assert "양(量)이 아니라 포함 여부의 확률" in p


def test_field_score_puts_reason_before_score():
    """structured output 은 필드 순서대로 생성된다 — 근거가 점수보다 먼저 나와야 한다."""
    assert list(FieldScore.model_fields) == ["reason", "score"]


def test_lang_scores_backfills_missing_languages_with_zero():
    result = TranslationScores(items=[TranslationLangScore(lang="en", score=90, reason="ok")])

    scores = lang_scores(result)

    assert set(scores.keys()) == set(TARGET_LANGS)
    assert scores["en"].score == 90
    assert scores["ja"].score == 0
    assert scores["ja"].reason == "모델 응답에서 언어 누락"


def test_lang_scores_keeps_all_scores_when_response_is_complete():
    items = [TranslationLangScore(lang=lang, score=80, reason="ok") for lang in TARGET_LANGS]

    scores = lang_scores(TranslationScores(items=items))

    assert {lang: s.score for lang, s in scores.items()} == {lang: 80 for lang in TARGET_LANGS}


# ===== 종합판정(decide) =====


def fs(score: int, reason: str = "이유") -> FieldScore:
    return FieldScore(score=score, reason=reason)


def all_pass_translations(score: int = 90) -> dict[str, FieldScore]:
    return {lang: fs(score) for lang in TARGET_LANGS}


def test_all_pass():
    v = decide(fs(80), all_pass_translations(), fs(75), TH)
    assert v.passed is True
    assert v.rejected_fields == []
    assert v.reason is None
    assert v.scores["description"] == 80
    assert v.scores["translations"]["en"] == 90
    assert v.scores["avoidance"] == 75


def test_threshold_is_inclusive():
    # 임계값과 같으면 통과 (70 >= 70)
    v = decide(fs(70), all_pass_translations(70), fs(70), TH)
    assert v.passed is True


def test_description_fail_maps_to_kbap_field():
    v = decide(fs(50), all_pass_translations(), fs(90), TH)
    assert v.passed is False
    assert v.rejected_fields == ["DESCRIPTION"]


def test_one_failing_language_rejects_both_translation_fields():
    # 배치가 이름·설명 번역을 통짜로 재생성하므로 언어 하나만 나빠도 둘 다 비운다.
    translations = all_pass_translations() | {"th": fs(30, "태국어 번역이 다른 음식을 지칭")}
    v = decide(fs(90), translations, fs(90), TH)
    assert v.passed is False
    assert v.rejected_fields == ["NAME_TRANSLATIONS", "DESCRIPTION_TRANSLATIONS"]


def test_avoidance_fail_clears_spiciness_too():
    # spiciness 는 기피성분과 한 번에 산출되므로 함께 비운다.
    v = decide(fs(90), all_pass_translations(), fs(40, "돼지고기 누락"), TH)
    assert v.passed is False
    assert v.rejected_fields == ["AVOIDANCE_SUBSTANCES", "SPICINESS"]


def test_incomplete_translation_map_does_not_pass():
    # 언어가 빠진 채로 들어와도 통과시키지 않는다(fail-closed).
    v = decide(fs(90), {"en": fs(90)}, fs(90), TH)
    assert v.passed is False
    assert "NAME_TRANSLATIONS" in v.rejected_fields


def test_reason_lists_every_failed_group():
    v = decide(
        fs(50, "설명이 다른 음식을 설명함"), all_pass_translations(), fs(40, "돼지고기 누락"), TH
    )
    assert "설명이 다른 음식을 설명함" in v.reason
    assert "돼지고기 누락" in v.reason


def test_reason_capped():
    translations = {lang: fs(10, "사" * 300) for lang in TARGET_LANGS}
    v = decide(fs(10, "설명 문제"), translations, fs(10, "성분 문제"), TH)
    assert len(v.reason) <= MAX_NOTE_CHARS


def test_reason_keeps_description_and_avoidance_when_many_langs_fail():
    # 단일 줄 그룹(설명·기피성분)은 언어 줄보다 먼저 들어가 잘리지 않는다.
    translations = {lang: fs(10, "사" * 300) for lang in TARGET_LANGS}
    v = decide(fs(10, "설명 문제"), translations, fs(10, "성분 문제"), TH)
    assert "설명 문제" in v.reason
    assert "성분 문제" in v.reason


def test_all_groups_fail():
    v = decide(fs(10), {lang: fs(10) for lang in TARGET_LANGS}, fs(10), TH)
    assert v.rejected_fields == [
        "DESCRIPTION",
        "NAME_TRANSLATIONS",
        "DESCRIPTION_TRANSLATIONS",
        "AVOIDANCE_SUBSTANCES",
        "SPICINESS",
    ]


# ===== 검수 그래프 =====

FOOD = {"foodId": 7, "koreanName": "김치찌개", "contentReviewAttempts": 0}


class FakeClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append((food_id, verdict))
        return {"foodId": food_id, "contentStatus": "REVIEWED" if verdict.passed else "INCOMPLETE"}


def make_scorers(desc=85, trans=90, avoid=80) -> Scorers:
    async def d(food):
        return FieldScore(score=desc, reason="설명 사유")

    async def t(food):
        return {lang: FieldScore(score=trans, reason="번역 사유") for lang in TARGET_LANGS}

    async def a(food):
        return FieldScore(score=avoid, reason="성분 사유")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_pass_path_posts_with_food_id():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].passed is True
    assert client.posts == [(7, state["verdict"])]


async def test_fail_path_posts_rejected_fields():
    client = FakeClient()
    graph = build_graph(make_scorers(desc=30), client, TH)

    await graph.ainvoke({"food": FOOD})

    ((food_id, verdict),) = client.posts
    assert food_id == 7
    assert verdict.passed is False
    assert verdict.rejected_fields == ["DESCRIPTION"]
    assert "설명 사유" in verdict.reason


async def test_dry_run_does_not_post():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH, dry_run=True)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].passed is True
    assert client.posts == []


async def test_scorer_exception_propagates():
    # LLM 실패는 그래프 실행 실패로 전파 — 실행기가 보류 처리 (POST 없음)
    async def boom(food):
        raise RuntimeError("LLM down")

    scorers = make_scorers()._replace(description=boom)
    client = FakeClient()
    graph = build_graph(scorers, client, TH)

    with pytest.raises(RuntimeError):
        await graph.ainvoke({"food": FOOD})
    assert client.posts == []


async def test_parse_failure_is_retried():
    # structured output 파싱 실패는 ValueError 하위라 LangGraph 기본 정책이 제외한다.
    # retry_on 을 덮어썼으므로 두 번 시도돼야 한다.
    from langchain_core.exceptions import OutputParserException

    calls = 0

    async def flaky(food):
        nonlocal calls
        calls += 1
        raise OutputParserException("malformed structured output")

    scorers = make_scorers()._replace(description=flaky)
    client = FakeClient()
    graph = build_graph(scorers, client, TH)

    with pytest.raises(OutputParserException):
        await graph.ainvoke({"food": FOOD})
    assert calls == 2
    assert client.posts == []


# ===== 러너(run_batch) =====


class CountingClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append(food_id)
        return {"foodId": food_id, "contentStatus": "REVIEWED" if verdict.passed else "INCOMPLETE"}


def scorers_failing_for(bad_id: int | None) -> Scorers:
    async def d(food):
        if bad_id is not None and food.get("foodId") == bad_id:
            raise RuntimeError("LLM down")
        if food.get("fail_desc"):
            return FieldScore(score=30, reason="설명 문제")
        return FieldScore(score=90, reason="ok")

    async def t(food):
        return {lang: FieldScore(score=90, reason="ok") for lang in TARGET_LANGS}

    async def a(food):
        return FieldScore(score=90, reason="ok")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_run_batch_counts_and_isolates_failures():
    client = CountingClient()
    graph = build_graph(scorers_failing_for(bad_id=2), client, TH)
    foods = [
        {"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0},
        {"foodId": 2, "koreanName": "불고기", "contentReviewAttempts": 0},
        {"foodId": 3, "koreanName": "비빔밥", "contentReviewAttempts": 0},
    ]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    # foodId=2 는 LLM 실패 → 보류(HELD), POST 없음. 나머지는 PASS + POST.
    assert counts == {"PASS": 2, "FAIL": 0, "HELD": 1}
    assert sorted(client.posts) == [1, 3]


async def test_run_batch_counts_all_outcomes():
    client = CountingClient()
    graph = build_graph(scorers_failing_for(bad_id=2), client, TH)
    foods = [
        {"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0},
        {"foodId": 2, "koreanName": "불고기", "contentReviewAttempts": 0},
        {"foodId": 3, "koreanName": "비빔밥", "contentReviewAttempts": 0, "fail_desc": True},
    ]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    # 탈락 건이 재생성으로 갈지 REVIEW_REJECTED 로 갈지는 kbap 이 정하므로 여기선 FAIL 하나로 센다.
    assert counts == {"PASS": 1, "FAIL": 1, "HELD": 1}
    assert sorted(client.posts) == [1, 3]


async def test_run_batch_missing_food_id_is_held():
    # kbap 응답에 foodId 가 없으면(계약 위반) 조용히 넘어가지 않고 보류로 떨어진다.
    client = CountingClient()
    graph = build_graph(scorers_failing_for(bad_id=None), client, TH)
    foods = [{"koreanName": "떡볶이", "contentReviewAttempts": 0}]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    assert counts == {"PASS": 0, "FAIL": 0, "HELD": 1}
    assert client.posts == []
