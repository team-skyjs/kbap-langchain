from pydantic import BaseModel

from kbap_review.config import Thresholds
from kbap_review.scoring import TARGET_LANGS, FieldScore

# 사유 상한. kbap 도 Food.MAX_REJECTION_REASON_LINES(10) / _LENGTH(1000) 로 자르지만,
# 서버는 "앞 10줄 → 앞 1000자" 순으로 자르므로 여기서 미리 다듬어 잘림이 줄 중간에서
# 일어나지 않게 한다.
MAX_NOTE_CHARS = 1000
MAX_REASON_CHARS = 200

# 채점 필드군 → kbap FoodContentReviewField.
# 배치가 함께 재생성하는 단위에 맞춘다:
#   DESCRIPTION 만 비우면 배치가 설명과 설명 번역을 같이 다시 만든다(FoodContentItemProcessor).
#   AVOIDANCE_SUBSTANCES 는 spiciness 와 한 번에 산출되므로 둘 다 비운다.
REJECTED_FIELDS = {
    "description": ["DESCRIPTION"],
    "translations": ["NAME_TRANSLATIONS", "DESCRIPTION_TRANSLATIONS"],
    "avoidance": ["AVOIDANCE_SUBSTANCES", "SPICINESS"],
}


class Verdict(BaseModel):
    passed: bool
    rejected_fields: list[str]
    scores: dict
    reason: str | None = None


def decide(
    description_score: FieldScore,
    translation_scores: dict[str, FieldScore],
    avoidance_score: FieldScore,
    thresholds: Thresholds,
) -> Verdict:
    """필드군별 점수를 kbap 검수 결과로 바꾼다.

    재시도를 몇 번까지 허용할지는 kbap 이 정한다(Food.rejectContentReview 가
    contentReviewAttempts 를 보고 컬럼을 비울지 REVIEW_REJECTED 로 갈지 고른다).
    여기서는 통과 여부와 문제 필드만 판단한다.
    """
    failed: list[str] = []
    if description_score.score < thresholds.description:
        failed.append("description")

    failed_langs = {
        lang: s for lang, s in translation_scores.items() if s.score < thresholds.translations
    }
    # 응답에 아예 없는 언어는 0점과 동일하게 취급 — fail-closed 방어선(scoring.lang_scores 가
    # 이미 채우지만, decide()가 받은 값만 보고도 통과시키지 않는다).
    for lang in TARGET_LANGS:
        if lang not in translation_scores:
            failed_langs[lang] = FieldScore(score=0, reason="번역 점수 누락")
    if failed_langs:
        failed.append("translations")

    if avoidance_score.score < thresholds.avoidance:
        failed.append("avoidance")

    scores = {
        "description": description_score.score,
        "translations": {lang: s.score for lang, s in translation_scores.items()},
        "avoidance": avoidance_score.score,
    }

    if not failed:
        return Verdict(passed=True, rejected_fields=[], scores=scores)

    rejected_fields = [f for group in failed for f in REJECTED_FIELDS[group]]
    return Verdict(
        passed=False,
        rejected_fields=rejected_fields,
        scores=scores,
        reason=_reason(failed, failed_langs, description_score, avoidance_score),
    )


def _reason(
    failed: list[str],
    failed_langs: dict[str, FieldScore],
    description_score: FieldScore,
    avoidance_score: FieldScore,
) -> str:
    """재시도가 소진됐을 때 사람이 읽는 개조식 사유.

    단일 줄 그룹(설명·기피성분)을 먼저 넣는다 — kbap 이 앞 10줄만 남기므로, 언어 줄이
    많으면 뒤에 있는 기피성분 줄이 통째로 사라진다.
    """
    lines: list[str] = []
    if "description" in failed:
        lines.append(
            f"- 설명({description_score.score}점): {description_score.reason[:MAX_REASON_CHARS]}"
        )
    if "avoidance" in failed:
        lines.append(
            f"- 기피성분·매운맛({avoidance_score.score}점): "
            f"{avoidance_score.reason[:MAX_REASON_CHARS]}"
        )
    for lang, s in failed_langs.items():
        lines.append(f"- 번역 {lang}({s.score}점): {s.reason[:MAX_REASON_CHARS]}")
    return "\n".join(lines)[:MAX_NOTE_CHARS]
