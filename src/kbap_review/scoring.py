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


def lang_scores(result: TranslationScores) -> dict[str, FieldScore]:
    """모델 응답을 언어별 점수 맵으로 변환하고, 누락된 언어는 0점으로 채운다.

    make_scorers 밖으로 뺀 이유: make_scorers는 API 키 없이 생성할 수 없어 이 백필
    로직(누락=fail-closed)을 단위 테스트할 방법이 없었다.
    """
    scores = {i.lang: FieldScore(score=i.score, reason=i.reason) for i in result.items}
    # 모델이 언어를 누락하면 0점 처리 — 누락을 통과로 취급하지 않는다(fail-closed).
    for lang in TARGET_LANGS:
        scores.setdefault(lang, FieldScore(score=0, reason="모델 응답에서 언어 누락"))
    return scores


def make_scorers(config):
    """실 LLM 기반 스코어러. import를 함수 안에 두어 테스트가 LLM 패키지 없이 돌게 한다."""
    from langchain.chat_models import init_chat_model

    from kbap_review.graph import Scorers

    def _model(name: str):
        # "gemini-*"는 자동 추론이 안 되는 버전이 있어 provider를 명시한다.
        # gpt-* 등 타 벤더로 바꾸면 "openai:gpt-5-mini"처럼 "provider:model" 형식으로 설정.
        # timeout 미설정 시 SDK 기본값을 쓰는데, 무인 배치에서 한 콜이 멈추면 그
        # 세마포어 슬롯을 영원히 붙잡아 gather 전체가 멎는다 — 반드시 설정한다.
        if ":" in name:
            provider, model = name.split(":", 1)
            return init_chat_model(model, model_provider=provider, timeout=config.timeout_seconds)
        if name.startswith("gemini"):
            return init_chat_model(
                name, model_provider="google_genai", timeout=config.timeout_seconds
            )
        return init_chat_model(name, timeout=config.timeout_seconds)

    base = _model(config.model)
    avoid = _model(config.avoidance_model)
    desc_llm = base.with_structured_output(FieldScore)
    trans_llm = base.with_structured_output(TranslationScores)
    avoid_llm = avoid.with_structured_output(FieldScore)

    async def description(food: dict) -> FieldScore:
        return await desc_llm.ainvoke(description_prompt(food))

    async def translations(food: dict) -> dict[str, FieldScore]:
        result: TranslationScores = await trans_llm.ainvoke(translations_prompt(food))
        return lang_scores(result)

    async def avoidance(food: dict) -> FieldScore:
        return await avoid_llm.ainvoke(avoidance_prompt(food))

    return Scorers(description=description, translations=translations, avoidance=avoidance)
