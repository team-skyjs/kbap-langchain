from typing import Literal

from pydantic import BaseModel

from kbap_review.config import Thresholds
from kbap_review.scoring import TARGET_LANGS, FieldScore

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
    # 응답에 아예 없는 언어는 0점과 동일하게 취급 — fail-closed 방어선(설계 의도는
    # scoring.lang_scores가 이미 채우지만, decide()가 받는 값만 보고도 통과시키지 않는다).
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
        return Verdict(verdict="PASS", failed_fields=[], scores=scores)
    if review_attempts < MAX_RETRY_ATTEMPTS:
        return Verdict(verdict="RETRY", failed_fields=failed, scores=scores)

    # 단일 줄 그룹(설명·기피성분)을 먼저 넣어 언어 줄이 많아도 잘려나가지 않게 한다.
    # 줄마다 reason을 상한선(200자)으로 자른다 — 안 그러면 reason 하나가 길 때
    # 전체 1000자 예산을 다 먹어 뒤 줄(기피성분 등)이 통째로 사라진다.
    note_lines: list[str] = []
    if "description" in failed:
        note_lines.append(f"- 설명({description_score.score}점): {description_score.reason[:200]}")
    if "avoidance" in failed:
        note_lines.append(
            f"- 기피성분·매운맛({avoidance_score.score}점): {avoidance_score.reason[:200]}"
        )
    for lang, s in failed_langs.items():
        note_lines.append(f"- 번역 {lang}({s.score}점): {s.reason[:200]}")
    return Verdict(
        verdict="REJECT",
        failed_fields=failed,
        scores=scores,
        review_note="\n".join(note_lines)[:MAX_NOTE_CHARS],
    )
