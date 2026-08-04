from kbap_review.aggregate import MAX_NOTE_LINES, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


def fs(score: int, reason: str = "이유") -> FieldScore:
    return FieldScore(score=score, reason=reason)


def all_pass_translations() -> dict[str, FieldScore]:
    return {lang: fs(90) for lang in ["zh-Hans", "en", "ja"]}


def test_all_pass():
    v = decide(0, fs(80), all_pass_translations(), fs(75), TH)
    assert v.verdict == "PASS"
    assert v.failed_fields == []
    assert v.review_note is None
    assert v.scores["description"] == 80
    assert v.scores["translations"]["en"] == 90
    assert v.scores["avoidance"] == 75


def test_threshold_is_inclusive():
    # 임계값과 같으면 통과 (70 >= 70)
    v = decide(0, fs(70), {"en": fs(70)}, fs(70), TH)
    assert v.verdict == "PASS"


def test_retry_on_description_fail():
    v = decide(0, fs(50), all_pass_translations(), fs(90), TH)
    assert v.verdict == "RETRY"
    assert v.failed_fields == ["description"]


def test_retry_when_one_language_fails():
    translations = all_pass_translations() | {"th": fs(30, "태국어 번역이 다른 음식을 지칭")}
    v = decide(1, fs(90), translations, fs(90), TH)
    assert v.verdict == "RETRY"
    assert v.failed_fields == ["translations"]


def test_reject_at_two_attempts():
    v = decide(2, fs(50, "설명이 다른 음식을 설명함"), all_pass_translations(), fs(40, "돼지고기 누락"), TH)
    assert v.verdict == "REJECT"
    assert v.failed_fields == ["description", "avoidance"]
    assert v.review_note is not None
    assert "설명이 다른 음식을 설명함" in v.review_note
    assert "돼지고기 누락" in v.review_note


def test_reject_note_capped_at_10_lines():
    translations = {f"l{i}": fs(10, f"사유 {i}") for i in range(15)}
    v = decide(2, fs(10, "설명 문제"), translations, fs(10, "성분 문제"), TH)
    assert len(v.review_note.splitlines()) <= MAX_NOTE_LINES


def test_pass_at_two_attempts_still_passes():
    # attempts가 몇이든 점수가 되면 PASS
    v = decide(5, fs(90), all_pass_translations(), fs(90), TH)
    assert v.verdict == "PASS"
