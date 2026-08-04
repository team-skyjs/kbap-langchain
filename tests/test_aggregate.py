from kbap_review.aggregate import MAX_NOTE_CHARS, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import TARGET_LANGS, FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


def fs(score: int, reason: str = "이유") -> FieldScore:
    return FieldScore(score=score, reason=reason)


def all_pass_translations() -> dict[str, FieldScore]:
    return {lang: fs(90) for lang in TARGET_LANGS}


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
    translations = {lang: fs(70) for lang in TARGET_LANGS}
    v = decide(0, fs(70), translations, fs(70), TH)
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


def test_reject_note_capped_at_1000_chars():
    translations = {f"l{i}": fs(10, "사" * 200) for i in range(15)}
    v = decide(2, fs(10, "설명 문제"), translations, fs(10, "성분 문제"), TH)
    assert len(v.review_note) <= MAX_NOTE_CHARS


def test_reject_note_keeps_description_and_avoidance_when_many_langs_fail():
    # 단일 줄 그룹(설명·기피성분)은 언어 줄보다 먼저 들어가 잘리지 않는다.
    translations = {f"l{i}": fs(10, "사" * 200) for i in range(15)}
    v = decide(2, fs(10, "설명 문제"), translations, fs(10, "성분 문제"), TH)
    assert "설명 문제" in v.review_note
    assert "성분 문제" in v.review_note


def test_pass_at_two_attempts_still_passes():
    # attempts가 몇이든 점수가 되면 PASS
    v = decide(5, fs(90), all_pass_translations(), fs(90), TH)
    assert v.verdict == "PASS"


def test_retry_on_avoidance_only_fail():
    v = decide(0, fs(90), all_pass_translations(), fs(40, "돼지고기 누락"), TH)
    assert v.verdict == "RETRY"
    assert v.failed_fields == ["avoidance"]


def test_empty_translations_does_not_pass():
    # decide()가 받은 언어만 보고 판단하면 빈 맵도 통과해버린다 — fail-closed 방어.
    v = decide(0, fs(90), {}, fs(90), TH)
    assert v.verdict != "PASS"
    assert "translations" in v.failed_fields


def test_long_description_reason_does_not_evict_avoidance_line():
    # 설명 reason 하나가 2000자면 줄 단위 상한이 없을 때 note 전체(1000자)를 잡아먹어
    # 기피성분 줄이 통째로 사라진다 — 안전 직결 정보라 반드시 남아야 한다.
    v = decide(2, fs(10, "설" * 2000), all_pass_translations(), fs(10, "성분 문제"), TH)
    assert "성분 문제" in v.review_note


def test_partial_translations_does_not_pass():
    # 9개 중 8개만 있어도(1개 언어 누락) 통과해서는 안 된다.
    translations = {lang: fs(90) for lang in TARGET_LANGS[:-1]}
    v = decide(0, fs(90), translations, fs(90), TH)
    assert v.verdict != "PASS"
    assert "translations" in v.failed_fields
