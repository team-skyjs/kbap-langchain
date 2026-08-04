import json

from pydantic import BaseModel, Field

# kbap LanguageCode에서 ko 제외 9개 — 순서 포함 일치해야 한다.
TARGET_LANGS = ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]

# 형식 검증(글자 수, 9개 언어 존재 여부, 언어 코드, spiciness 범위)은 업스트림 kbap 배치가
# PENDING_REVIEW 로 올리기 전에 이미 끝냈다(Food.needsNameTranslations / assessAvoidance).
# 여기서 다시 보면 모델 주의력만 나눠 쓰고 배치가 보장한 걸 깎을 위험이 있어 명시적으로 배제한다.
_CONTENT_ONLY = """형식 검증은 이미 끝났습니다 — 글자 수, 번역 누락 여부, 언어 코드 표기, 등급 범위는
보지 마세요. 오직 내용이 맞는가만 판단하세요."""


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

{_CONTENT_ONLY}

채점 기준:
- 설명이 실제로 이 음식을 정확히 설명하는가 (다른 음식 설명이 아닌가)
- 재료·조리법·맛 서술에 사실과 다른 내용이 없는가 (들어가지 않는 재료를 지어내지 않았는가,
  조리법을 다른 음식의 것과 섞지 않았는가)
- 이 음식을 처음 보는 외국인이 읽고 무슨 음식인지 그려지는가

음식 이름: {food["koreanName"]}
설명: {food["description"]}

score(0~100)와 reason(한국어 한 문장)을 반환하세요."""


def translations_prompt(food: dict) -> str:
    return f"""당신은 다국어 번역 검수자입니다. 한국 음식의 이름·설명 번역을 언어별로
0~100점으로 채점하세요.

{_CONTENT_ONLY}

채점 기준 (언어별로 각각) — 오역을 잡는 것이 목적입니다:
- 이름 번역이 이 음식을 제대로 가리키는가. 글자만 옮겨 뜻이 달라지지 않았는가
  (예: 다른 요리 이름이 되어버림, 재료명을 엉뚱하게 옮김)
- 설명 번역이 한국어 원문과 같은 내용인가. 원문에 없는 재료·조리법을 지어내거나,
  원문에 있는 핵심 정보를 빠뜨리거나, 뜻을 뒤집지 않았는가
- 그 언어 화자가 읽었을 때 말이 되는가 (기계번역 티가 나는 어색한 직역인가)

음식 이름(한국어): {food["koreanName"]}
설명(한국어): {food["description"]}
이름 번역: {json.dumps(food["nameTranslations"], ensure_ascii=False)}
설명 번역: {json.dumps(food["descriptionTranslations"], ensure_ascii=False)}

대상 언어 {len(TARGET_LANGS)}개 전부에 대해 items 배열로 반환하세요: {", ".join(TARGET_LANGS)}
각 항목은 lang, score(0~100), reason(한국어 한 문장)입니다."""


def avoidance_prompt(food: dict) -> str:
    return f"""당신은 식품 안전 검수자입니다. 아래 음식의 기피성분 목록과 매운맛 등급이
일반적인 레시피 기준으로 타당한지 0~100점으로 채점하세요.

{_CONTENT_ONLY}

채점 기준:
- **생뚱맞은 성분이 섞여 있지 않은가** — 이 음식 레시피와 아무 상관 없는 성분이
  목록에 올라와 있거나, 실제보다 터무니없이 높은 확률이 붙어 있지 않은가
  (예: 김치찌개에 갑각류 90%). 관광객이 먹을 수 있는 음식을 못 먹는다고 잘못 걸러낸다.
- 명백히 포함되는 주요 성분이 목록에서 빠지지 않았는가 (알레르기·비건·종교 안전 직결)
- 남은 성분들의 포함 확률이 일반적인 레시피 감각과 맞는가
- 매운맛 등급이 이 음식에 타당한가 (예: 물냉면 8, 불닭 1 이면 이상하다)

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
