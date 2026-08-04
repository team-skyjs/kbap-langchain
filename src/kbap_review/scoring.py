import json

from pydantic import BaseModel, Field

# kbap LanguageCode에서 ko 제외 9개 — 순서 포함 일치해야 한다.
TARGET_LANGS = ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]


class FieldScore(BaseModel):
    score: int = Field(ge=0, le=100)
    reason: str


class TranslationLangScore(BaseModel):
    lang: str
    score: int = Field(ge=0, le=100)
    reason: str


class TranslationScores(BaseModel):
    items: list[TranslationLangScore]


def description_prompt(food: dict) -> str:
    return f"""당신은 한국 음식 콘텐츠 검수자입니다. 아래 음식 설명이 외국인 관광객에게
제공하기에 적합한지 0~100점으로 채점하세요.

채점 기준:
- 설명이 실제로 이 음식을 정확히 설명하는가 (다른 음식 설명이 아닌가)
- 재료·조리법·맛 서술에 환각이나 오기가 없는가
- 외국인 관광객 기준으로 이해 가능한 설명인가

음식 이름: {food["koreanName"]}
설명: {food["description"]}

score(0~100)와 reason(한국어 한 문장)을 반환하세요."""


def translations_prompt(food: dict) -> str:
    return f"""당신은 다국어 번역 검수자입니다. 한국 음식의 이름·설명 번역을 언어별로
0~100점으로 채점하세요.

채점 기준 (언어별로 각각):
- 이름 번역이 원문 음식을 정확히 지칭하는가
- 설명 번역이 한국어 원문과 의미가 일치하는가 (누락·왜곡·환각 없음)
- 해당 언어 태그와 실제 표기 언어가 일치하는가

음식 이름(한국어): {food["koreanName"]}
설명(한국어): {food["description"]}
이름 번역: {json.dumps(food["nameTranslations"], ensure_ascii=False)}
설명 번역: {json.dumps(food["descriptionTranslations"], ensure_ascii=False)}

대상 언어 {len(TARGET_LANGS)}개 전부에 대해 items 배열로 반환하세요: {", ".join(TARGET_LANGS)}
각 항목은 lang, score(0~100), reason(한국어 한 문장)입니다.
번역이 아예 없는 언어는 score 0으로 채점하세요."""


def avoidance_prompt(food: dict) -> str:
    return f"""당신은 식품 안전 검수자입니다. 아래 음식의 기피성분 목록과 매운맛 등급이
일반적인 레시피 기준으로 타당한지 0~100점으로 채점하세요.

채점 기준:
- 기피성분 목록(성분 코드 + 포함 확률 %)이 이 음식의 일반적인 레시피와 부합하는가
- 명백히 포함되는 주요 성분이 목록에서 빠지지 않았는가 (알레르기·비건·종교 안전 직결)
- 매운맛 등급(0~10)이 이 음식에 타당한가

이것은 안전 직결 판단입니다. 확신이 없으면 낮은 점수를 주세요.

음식 이름: {food["koreanName"]}
기피성분 목록: {json.dumps(food["avoidanceSubstances"], ensure_ascii=False)}
매운맛 등급: {food["spiciness"]}

score(0~100)와 reason(한국어 한 문장)을 반환하세요."""
