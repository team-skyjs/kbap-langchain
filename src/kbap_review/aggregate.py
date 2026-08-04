from typing import Literal

from pydantic import BaseModel

from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore

MAX_NOTE_CHARS = 1000

# 재검수(컬럼 비움 + INCOMPLETE 롤백) 허용 횟수 — 스펙: 2회까지, 이후 REJECT.
MAX_RETRY_ATTEMPTS = 2


class Verdict(BaseModel):
    verdict: Literal["PASS", "RETRY", "REJECT"]
    failed_fields: list[str]
    scores: dict
    review_note: str | None = None


def decide(
    review_attempts: int,
    description_score: FieldScore,
    translation_scores: dict[str, FieldScore],
    avoidance_score: FieldScore,
    thresholds: Thresholds,
) -> Verdict:
    failed: list[str] = []
    if description_score.score < thresholds.description:
        failed.append("description")
    failed_langs = {
        lang: s for lang, s in translation_scores.items() if s.score < thresholds.translations
    }
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
        return Verdict(verdict="PASS", failed_fields=[], scores=scores)
    if review_attempts < MAX_RETRY_ATTEMPTS:
        return Verdict(verdict="RETRY", failed_fields=failed, scores=scores)

    # 단일 줄 그룹(설명·기피성분)을 먼저 넣어 언어 줄이 많아도 잘려나가지 않게 한다.
    note_lines: list[str] = []
    if "description" in failed:
        note_lines.append(f"- 설명({description_score.score}점): {description_score.reason}")
    if "avoidance" in failed:
        note_lines.append(f"- 기피성분·매운맛({avoidance_score.score}점): {avoidance_score.reason}")
    for lang, s in failed_langs.items():
        note_lines.append(f"- 번역 {lang}({s.score}점): {s.reason}")
    return Verdict(
        verdict="REJECT",
        failed_fields=failed,
        scores=scores,
        review_note="\n".join(note_lines)[:MAX_NOTE_CHARS],
    )
