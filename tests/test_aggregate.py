from kbap_review.aggregate import MAX_NOTE_CHARS, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import TARGET_LANGS, FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


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
