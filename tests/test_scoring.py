import pytest
from pydantic import ValidationError

from kbap_review.scoring import (
    TARGET_LANGS,
    FieldScore,
    TranslationScores,
    avoidance_prompt,
    description_prompt,
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
