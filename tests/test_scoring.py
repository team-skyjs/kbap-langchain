import pytest
from pydantic import ValidationError

from kbap_review.scoring import (
    AVOIDANCE_CODES,
    TARGET_LANGS,
    FieldScore,
    TranslationLangScore,
    TranslationScores,
    avoidance_prompt,
    description_prompt,
    lang_scores,
    translations_prompt,
)

FOOD = {
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
    p = description_prompt(FOOD)
    assert "김치찌개" in p
    assert FOOD["description"] in p


def test_translations_prompt_lists_all_target_langs():
    p = translations_prompt(FOOD)
    for lang in TARGET_LANGS:
        assert lang in p
    assert "Kimchi Stew" in p


def test_avoidance_prompt_contains_substances_and_spiciness():
    p = avoidance_prompt(FOOD)
    assert "PORK" in p
    assert "95" in p
    assert "7" in p


def test_avoidance_prompt_carries_generator_contract():
    """생성기(SpringAiFoodAvoidanceAssessmentClient)와 같은 척도·후보 목록을 실어야 한다.

    이게 빠지면 검수기가 제 감각으로 판단해, 규격대로 생성된 데이터를 깎고(매운맛 척도 불일치)
    후보에 없어 넣을 수 없던 성분을 누락으로 감점한다 — 스모크에서 실제로 나온 실패.
    """
    p = avoidance_prompt(FOOD)
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
