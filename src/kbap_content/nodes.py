"""콘텐츠 그래프의 실 LLM 노드 구현.

검수 쪽은 kbap_review.scoring 의 프롬프트·모델을 재사용하고,
이름 정제는 kbap_namefix 를 그대로 쓴다.

생성 프롬프트는 kbap(Spring) infra/llm/food 의 운영 프롬프트를 이식한 것이다:
SpringAiFoodNameTranslationClient / SpringAiFoodDescriptionClient(설명·번역을 두 노드로 분리)
/ SpringAiFoodAvoidanceAssessmentClient. JSON 출력 형식 지시는 structured output 이
대체하므로 뺐고, 의미 규칙·예시·휴리스틱은 그대로 가져왔다. 원본이 바뀌면 여기도 맞출 것.
"""

import json
import re

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


# 모델 응답에서 후보 밖 코드를 저장 전에 걸러낸다 — kbap KB-236(후보 밖 코드 유출) 방어를
# 프롬프트에만 맡기지 않는다. 중복 코드는 첫 값만 남긴다.
_CODE = re.compile(r"([A-Z_]+)\(")
VALID_CODES = frozenset(_CODE.findall(AVOIDANCE_CODES))


def valid_substances(items: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out = []
    for item in items:
        code = item["code"]
        if code not in VALID_CODES or code in seen:
            continue
        seen.add(code)
        out.append(item)
    return out


def _feedback_block(feedback: str) -> str:
    if not feedback:
        return ""
    return f"""

## 이전 시도 탈락 사유 — 반드시 반영해 다시 만드세요
{feedback}"""


def _translation_rules() -> str:
    return f"""- 언어(9개, 순서 고정): {LANGS}. 9개 전수 채우고 빈 값 금지.
- 각 언어의 문자 체계를 따른다 (ja 는 일본어 표기, ru 는 키릴 문자, th 는 태국 문자)."""


def name_tr_prompt(name: str, feedback: str) -> str:
    return f"""당신은 한식 메뉴 데이터베이스 담당자입니다. 아래 음식명을 외국인 손님이 메뉴판에서 읽을
번역명으로 만드세요.
음식명: "{name}"

## 생성 항목

items: 음식명을 9개 언어로 번역한 값
{_translation_rules()}

## 번역 규칙

- 널리 알려진 한식은 그 언어권에서 통용되는 표기를 쓴다 (Kimchi, Bibimbap, キンパ, 泡菜).
- 통용 표기가 없으면 음식을 알아볼 수 있게 짧게 의역한다 (된장술밥 → Soybean Paste Rice).
- 메뉴판에 올릴 이름이므로 간결하게. 문장·설명·괄호 병기 금지.
- 예시(치즈볼): en "Cheese Balls", es "Bolas de queso", ja "チーズボール", zh-Hans "芝士球"

## 입력이 불완전할 때

- 오탈자·띄어쓰기 오류로 보이면 가장 유사한 실제 한식 메뉴로 추론해 고친 이름 기준으로
  번역한다 (김치찌게 → 김치찌개, 짜장면/자장면 동일 취급).
- 상호·수식어가 붙어 있으면 음식 본체 기준으로 번역한다 (원조할매국밥 → 국밥,
  왕돈까스(대) → 왕돈까스).
- 로제떡볶이·마라탕면처럼 합성·변형 메뉴는 구성 요소를 조합해 번역한다.
- 파스타·피자 같은 외래 음식은 각 언어의 통용 표기를 그대로 쓴다.
- 그래도 모르는 음식이면 건너뛰지 말고 음식명의 구성 요소를 기준으로 번역한다.{_feedback_block(feedback)}"""


def desc_prompt(name: str, feedback: str) -> str:
    return f"""당신은 한식 메뉴 데이터베이스 담당자입니다. 아래 음식의 한국어 한 줄 설명을 생성하세요.
음식명: "{name}"

## 생성 항목

description: 한국어 한 줄 설명
- 요리법·주재료가 드러나는 한 문장. 과장 없이 사실적으로.
- 반드시 255자 이하. 빈 값·"설명 준비 중" 같은 템플릿 문구 금지.
- 예시(치즈볼): "치즈를 넣은 반죽을 둥글게 튀긴 사이드 메뉴"

## 규칙

- 음식명에 오탈자·띄어쓰기 오류가 보이면 가장 유사한 실제 한식 메뉴로 추론해 그 음식
  기준으로 작성하세요 (김치찌게 → 김치찌개). 상호·수식어가 붙어 있으면 음식 본체 기준.
- 모르는 음식이어도 건너뛰지 말고 일반적인 한식 지식 기준으로 작성하세요.{_feedback_block(feedback)}"""


def desc_tr_prompt(name: str, description: str) -> str:
    return f"""당신은 한식 메뉴 데이터베이스 담당자입니다. 아래 음식 설명을 9개 언어로 번역하세요.
음식명: "{name}"
설명(한국어): "{description}"

## 생성 항목

items: 설명을 9개 언어로 실제 번역한 값 (템플릿 문구·원문 복사 금지)
{_translation_rules()}
- 원문에 없는 내용을 더하거나 빼지 않는다.
- 예시(치즈볼 "치즈를 넣은 반죽을 둥글게 튀긴 사이드 메뉴"):
  en "Round fried dough balls filled with cheese.", ja "チーズを入れた生地を丸く揚げたサイドメニュー。",
  zh-Hans "面团包入芝士后炸成圆球的小吃。\""""


def avoid_gen_prompt(name: str, feedback: str) -> str:
    return f"""너는 한국 음식 레시피와 알레르기·기피성분 전문가다. 아래 메뉴의 대표 레시피를 기준으로
기피성분의 포함 확률을 1~100 정수로 매기고, 음식의 맵기를 0~10 정수로 판정하라.
음식명: "{name}"

# spiciness (맵기) 의 의미
- 0: 맵지 않음 (계란말이, 치즈볼 등)
- 1~3: 약간 매콤 (제육볶음 순한맛, 김치찌개 등)
- 4~6: 보통 매움 (떡볶이, 닭갈비 등)
- 7~10: 매우 매움 (불닭, 매운 갈비찜, 마라 계열 등)

# inclusionPercent 의 의미
"손님이 아무 식당에서나 이 메뉴를 시켰을 때, 그 한 접시에 이 성분이 들어 있을 확률."
양(量)이 아니라 포함 여부의 확률이다. 값 기준:
- 95~100: 정의상 반드시 들어감 (김밥의 쌀, 라떼의 우유)
- 80~95 : 표준 레시피 핵심 재료 (떡볶이의 고추장→SOY)
- 55~80 : 대부분 넣지만 집집마다 다름 (김밥의 계란)
- 30~55 : 흔한 선택 재료·고명·양념 (부침의 쪽파)
- 10~30 : 일부 식당·지역·변형에서만 (감자탕의 들깨)
- 1~10  : 미량·교차오염·드문 변형
present(들어갈 가능성 있는)만 나열한다. 사실상 0%인 성분은 뺀다. 애매하면 낮은 값으로
포함하되, 5 미만이면 대개 생략. 기피성분이 사실상 없는 메뉴(아메리카노, 공기밥 등)는
빈 배열([])을 반환한다.

# 반드시 추적할 숨은·파생 성분 (겉에 안 보여도 양념·육수·가공품 속에 있음)
- 양조간장(거의 모든 볶음·조림·양념): SOY 90+, WHEAT 75+ (한국 양조간장엔 밀)
- 고추장·된장·쌈장·춘장: SOY 90+
- 어묵·맛살·게맛살·크래미: FISH 90+, WHEAT 70+
- 멸치육수·다시: ANCHOVY 60~85, FISH 70+, DASHI 40+
- 사골·고기육수: BEEF 또는 PORK 70+, BROTH
- 액젓·멸치액젓(김치·양념): FISH_SAUCE 80+, ANCHOVY, FISH
- 새우젓(돼지·순대·만두양념): SALTED_SHRIMP, SHRIMP
- 굴소스: OYSTER_SAUCE, OYSTER
- 밀가루 반죽·튀김옷·부침·면·빵·만두피: WHEAT 90+
- 튀김옷·부침 반죽: EGG 40~70
- 마요네즈(샐러드·핫도그·양념): EGG 85+
- 치즈: MILK 95, DAIRY 95, CHEESE 95, RENNET 40
- 버터·크림·우유·라떼: MILK/DAIRY 95+, 버터일 때 BUTTER
- 카라멜소스·연유: MILK/DAIRY
- 떡볶이·라볶이의 떡: 밀떡 흔함 → WHEAT 40~70
- 소시지·햄·베이컨·스팸: PORK 90+
- 맥주·매실주·청주·막걸리: ALCOHOL 100 (맥주엔 BARLEY·WHEAT, 막걸리엔 WHEAT 흔함)
- 미림·맛술: MIRIN, ALCOHOL, COOKING_WINE
- 김치(반찬·찌개·볶음밥): FISH_SAUCE 70+, SALTED_SHRIMP 50+, GARLIC, SCALLION
- 파·대파·쪽파·마늘·양파: SCALLION/GARLIC/ONION, 한식 양념 베이스 대개 60~90
- 참기름·깨소금(거의 모든 한식 마무리): SESAME 70~95

# 후보 성분 코드 (이 목록 밖 code 절대 금지)
{AVOIDANCE_CODES}
출력의 모든 code 는 반드시 위 후보 목록 안에 있어야 한다. 위 휴리스틱이 가리키는 성분이라도
후보 목록에 없으면 절대 출력하지 마라(확률이 높아도 뺀다).

# 규칙
- 음식명에 명백한 오탈자가 보이면 가장 유사한 실제 한식 메뉴로 추론해 판단하라
  (김치찌게 → 김치찌개). 단 어떤 음식인지 애매하면 지어내지 말고 이름 그대로 보수적으로
  판단하라 — 안전 데이터이므로 잘못된 추론이 누락보다 위험하다.
- SEAFOOD/FISH/POULTRY 는 총칭이다. 구체 종(SHRIMP, SALMON, CHICKEN 등)을 넣을 땐
  총칭도 함께 넣되, 총칭 확률 ≥ 구체 종 확률이 되게 하라.
- ASAFOETIDA, LUPIN, GHEE, GOAT_MILK, RYE, BRAZIL_NUT 등은 한식에 거의 없다.
  근거 없이 넣지 마라.
- 확신 없는 성분은 지어내지 말고 낮은 값으로 두거나 생략하라.
- 같은 code 를 중복하지 마라.{_feedback_block(feedback)}"""


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
        # ponytail: 단일 모델 — kbap 은 다중 모델 fanout+minAgreement(2)로 종합한다.
        # 안전 데이터 정확도가 아쉬우면 그 합의 구조(또는 web_search 도구)를 이식할 것.
        result: AvoidanceGen = await avoid_llm.ainvoke(avoid_gen_prompt(name, feedback))
        return {
            "substances": valid_substances([i.model_dump() for i in result.items]),
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
