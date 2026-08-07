"""콘텐츠 그래프의 실 LLM 노드 구현.

검수 쪽은 kbap_review.scoring 의 프롬프트·모델을 재사용하고,
이름 정제는 kbap_namefix 를 그대로 쓴다. 생성 프롬프트만 여기서 새로 정의한다.
"""

import json

from pydantic import BaseModel, Field

from kbap_review.scoring import (
    AVOIDANCE_CODES,
    TARGET_LANGS,
    FieldScore,
    TranslationScores,
    avoidance_prompt,
)

LANGS = ", ".join(TARGET_LANGS)


# reason 을 결과보다 앞에 둔다 — scoring.FieldScore 와 같은 교훈(근거를 먼저 세우게 한다).
class TranslationItem(BaseModel):
    lang: str
    text: str


class Translations(BaseModel):
    reason: str
    items: list[TranslationItem]


class AvoidanceItem(BaseModel):
    code: str
    inclusionPercent: int = Field(ge=0, le=100)


class AvoidanceGen(BaseModel):
    reason: str
    items: list[AvoidanceItem]
    spiciness: int = Field(ge=0, le=10)


class DescGen(BaseModel):
    description: str


def _feedback_block(feedback: str) -> str:
    if not feedback:
        return ""
    return f"""

# 이전 시도 탈락 사유 — 반드시 반영해 다시 만드세요
{feedback}"""


def _translation_rules() -> str:
    return f"""출력 규칙 — 반드시 지키세요:
- items 배열은 정확히 {len(TARGET_LANGS)}개, 아래 lang 값을 문자 그대로 사용: {LANGS}
- 각 항목은 반드시 그 lang 이 가리키는 언어로 실제 표기할 것"""


def name_tr_prompt(name: str, feedback: str) -> str:
    return f"""당신은 한국 음식 이름 번역가입니다. 외국인 관광객이 메뉴에서 알아볼 수 있도록
음식 이름을 각 언어로 번역하세요. 음차보다 뜻이 전달되는 번역을 우선하되,
널리 알려진 이름(Kimchi, Bibimbap 등)은 관용 표기를 씁니다.

음식 이름(한국어): {name}

{_translation_rules()}{_feedback_block(feedback)}"""


def desc_prompt(name: str, feedback: str) -> str:
    return f"""당신은 한국 음식 콘텐츠 작가입니다. 이 음식을 처음 보는 외국인 관광객에게
무슨 음식인지 그려지도록 한국어 설명을 2~3문장으로 쓰세요.
주재료·조리법·맛을 사실대로 담고, 들어가지 않는 재료를 지어내지 마세요.

음식 이름: {name}

설명 텍스트만 반환하세요.{_feedback_block(feedback)}"""


def desc_tr_prompt(name: str, description: str) -> str:
    return f"""당신은 다국어 번역가입니다. 한국 음식 설명을 각 언어로 번역하세요.
원문에 없는 내용을 더하거나 빼지 말고, 그 언어 화자가 자연스럽게 읽히게 옮기세요.

음식 이름: {name}
설명(한국어): {description}

{_translation_rules()}"""


def avoid_gen_prompt(name: str, feedback: str) -> str:
    return f"""당신은 식품 안전 조사원입니다. 이 한국 음식의 일반적인 레시피 기준으로
기피성분과 매운맛 등급을 산출하세요. 알레르기·비건·종교 안전에 직결되므로
주요 성분 누락이 가장 위험합니다.

# inclusionPercent — "아무 식당에서나 시켰을 때 한 접시에 이 성분이 들어 있을 확률"
95~100 정의상 반드시 / 80~95 표준 레시피 핵심 재료 / 55~80 대부분 넣지만 집집마다 다름 /
30~55 흔한 선택 재료·고명·양념 / 10~30 일부 식당·변형만 / 1~10 미량·교차오염

# spiciness (0~10)
0 맵지 않음(계란말이) / 1~3 약간 매콤(김치찌개) / 4~6 보통 매움(떡볶이) / 7~10 매우 매움(불닭)

# 후보 성분 코드 — 이 목록 안에서만 고르세요
{AVOIDANCE_CODES}

음식 이름: {name}

레시피에 거의 항상 들어가는 재료부터 후보 목록과 대조해 빠짐없이 넣으세요.{_feedback_block(feedback)}"""


def name_tr_review_prompt(name: str, translations: dict) -> str:
    return f"""당신은 다국어 번역 검수자입니다. 한국 음식 이름의 번역을 언어별로
0~100점으로 채점하세요. 오역을 잡는 것이 목적입니다:
- 번역이 이 음식을 제대로 가리키는가 (다른 요리 이름이 되지 않았는가)
- lang 이 가리키는 언어로 실제로 쓰여 있는가 (아니면 0점)

음식 이름(한국어): {name}
이름 번역: {json.dumps(translations, ensure_ascii=False)}

items 배열은 정확히 {len(TARGET_LANGS)}개({LANGS}), 각각 lang·score·reason(한국어 한 문장)."""


def desc_review_prompt(name: str, description: str, translations: dict) -> str:
    return f"""당신은 한국 음식 콘텐츠 검수자입니다. 설명과 설명 번역을 함께 0~100점
하나로 채점하세요:
- 설명이 이 음식을 사실대로 정확히 설명하는가 (없는 재료·다른 음식 조리법 금지)
- 번역들이 원문과 같은 내용인가, 각 lang 의 언어로 자연스럽게 쓰였는가
- 하나라도 심각한 문제가 있으면 그 항목 기준으로 낮게 매기세요

음식 이름: {name}
설명(한국어): {description}
설명 번역: {json.dumps(translations, ensure_ascii=False)}

score(0~100)와 reason(한국어 한 문장, 문제 항목 명시)을 반환하세요."""


def judge_prompt(state: dict, thresholds) -> str:
    scores = {
        "이름 번역": (state["nt_score"], thresholds.translations),
        "설명·설명 번역": (state["desc_score"], thresholds.description),
        "기피성분·매운맛": (state["avoid_score"], thresholds.avoidance),
    }
    lines = "\n".join(
        f"- {field}: {s.score}점 (임계값 {th}) — {s.reason}" for field, (s, th) in scores.items()
    )
    return f"""당신은 한국 음식 콘텐츠의 최종 판정자입니다. 필드별 검수 점수와 사유를 보고
이 음식 콘텐츠를 통과(passed=true)시킬지 판정하세요.

원칙:
- 임계값 미달 필드가 있으면 원칙적으로 탈락이며, rejected_fields 에 해당 필드를 담으세요.
  필드 이름은 다음 중에서만: translations, description, avoidance
- 점수가 임계값을 넘어도 사유에 안전 문제(기피성분 누락 등)가 보이면 탈락시키세요.
- 애매한 감점(문체·사소한 표현)만으로 임계값 근처에서 탈락시키지는 마세요.

음식 이름: {state["cleaned_name"]}
검수 결과:
{lines}

reason(한국어 1~2문장), passed, rejected_fields 를 반환하세요."""


def make_fns(model: str, timeout: int, thresholds, judge_model: str | None = None):
    """실 LLM 기반 노드 세트. import 를 함수 안에 두어 테스트가 LLM 패키지 없이 돌게 한다."""
    from kbap_content.graph import ContentFns, JudgeVerdict
    from kbap_namefix.pipeline import clean_one, make_normalizer
    from kbap_review.scoring import init_model, lang_scores

    base = init_model(model, timeout)
    judge_llm = init_model(judge_model or model, timeout).with_structured_output(JudgeVerdict)
    tr_llm = base.with_structured_output(Translations)
    desc_llm = base.with_structured_output(DescGen)
    avoid_llm = base.with_structured_output(AvoidanceGen)
    score_llm = base.with_structured_output(FieldScore)
    tr_score_llm = base.with_structured_output(TranslationScores)
    normalize = make_normalizer(model, timeout, callbacks=[])

    async def clean_name(name: str) -> dict:
        # ponytail: 앵커 없이 시작 — 수집 데이터가 쌓이면 확정 음식명을 앵커로 주입
        return await clean_one(name, [], normalize)

    async def gen_name_tr(name: str, feedback: str) -> dict:
        result: Translations = await tr_llm.ainvoke(name_tr_prompt(name, feedback))
        return {i.lang: i.text for i in result.items}

    async def gen_desc(name: str, feedback: str) -> str:
        result: DescGen = await desc_llm.ainvoke(desc_prompt(name, feedback))
        return result.description

    async def gen_desc_tr(name: str, description: str) -> dict:
        result: Translations = await tr_llm.ainvoke(desc_tr_prompt(name, description))
        return {i.lang: i.text for i in result.items}

    async def gen_avoid(name: str, feedback: str) -> dict:
        # ponytail: 모델 지식만 사용 — 희귀 메뉴 정확도가 부족해지면 web_search 도구 바인딩
        result: AvoidanceGen = await avoid_llm.ainvoke(avoid_gen_prompt(name, feedback))
        return {
            "substances": [i.model_dump() for i in result.items],
            "spiciness": result.spiciness,
        }

    async def rev_name_tr(name: str, translations: dict) -> FieldScore:
        result: TranslationScores = await tr_score_llm.ainvoke(
            name_tr_review_prompt(name, translations)
        )
        scores = lang_scores(result)  # 누락 언어 0점 fail-closed 재사용
        worst = min(scores.values(), key=lambda s: s.score)
        failing = [f"{lang} {s.score}점: {s.reason}" for lang, s in scores.items() if s.score < 100]
        return FieldScore(score=worst.score, reason="; ".join(failing) or "이상 없음")

    async def rev_desc(name: str, description: str, translations: dict) -> FieldScore:
        return await score_llm.ainvoke(desc_review_prompt(name, description, translations))

    async def rev_avoid(name: str, avoidance: dict) -> FieldScore:
        food = {
            "koreanName": name,
            "avoidanceSubstances": avoidance["substances"],
            "spiciness": avoidance["spiciness"],
        }
        return await score_llm.ainvoke(avoidance_prompt(food))

    async def judge(state: dict) -> JudgeVerdict:
        return await judge_llm.ainvoke(judge_prompt(state, thresholds))

    return ContentFns(
        clean_name=clean_name,
        gen_name_tr=gen_name_tr,
        gen_desc=gen_desc,
        gen_desc_tr=gen_desc_tr,
        gen_avoid=gen_avoid,
        rev_name_tr=rev_name_tr,
        rev_desc=rev_desc,
        rev_avoid=rev_avoid,
        judge=judge,
    )
