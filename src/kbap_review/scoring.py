import json

from pydantic import BaseModel, Field

# kbap LanguageCode에서 ko 제외 9개 — 순서 포함 일치해야 한다.
TARGET_LANGS = ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]

# 기피성분 후보 코드 — kbap AvoidanceSubstanceCode enum(= avoidance_substance 시드)과 같아야 한다.
# 생성기(SpringAiFoodAvoidanceAssessmentClient)는 이 안에서만 코드를 고르므로, 검수기도 같은
# 목록을 알아야 "목록에 없어서 못 넣은 성분"을 누락으로 오인해 깎지 않는다.
# 카탈로그를 내려주는 API 가 없어 하드코딩한다 — enum 이 바뀌면 여기도 갱신할 것.
AVOIDANCE_CODES = """EGG(계란) MILK(우유) DAIRY(유제품) GOAT_MILK(산양유) BUTTER(버터) GHEE(기버터)
CHEESE(치즈) GELATIN(젤라틴) RENNET(레닛) HONEY(꿀) CARMINE(카민) PEANUT(땅콩) WALNUT(호두)
PINE_NUT(잣) ALMOND(아몬드) CASHEW(캐슈넛) PISTACHIO(피스타치오) HAZELNUT(헤이즐넛)
MACADAMIA(마카다미아) PECAN(피칸) BRAZIL_NUT(브라질너트) CHESTNUT(밤) SESAME(참깨)
SUNFLOWER_SEED(해바라기씨) MUSTARD(겨자) WHEAT(밀) BUCKWHEAT(메밀) BARLEY(보리) RYE(호밀)
OAT(귀리) CORN(옥수수) SOY(대두) LUPIN(루핀) PEA(완두콩) CHICKPEA(병아리콩) LENTIL(렌틸콩)
SHRIMP(새우) SALTED_SHRIMP(새우젓) CRAB(게) CRAYFISH(가재) LOBSTER(랍스터) SQUID(오징어)
OCTOPUS(문어) OYSTER(굴) OYSTER_SAUCE(굴소스) ABALONE(전복) MUSSEL(홍합) CLAM(조개)
SHORT_NECK_CLAM(바지락) SCALLOP(가리비) SEAFOOD(해산물) FISH(생선) MACKEREL(고등어) SALMON(연어)
TUNA(참치) COD(대구) ANCHOVY(멸치) FISH_SAUCE(액젓) BROTH(육수) DASHI(다시) BEEF(소고기)
PORK(돼지고기) LARD(라드) TALLOW(우지) CHICKEN(닭고기) POULTRY(가금류) PEACH(복숭아)
TOMATO(토마토) CELERY(셀러리) POTATO(감자) CARROT(당근) ONION(양파) GARLIC(마늘) SCALLION(파)
CHIVE(부추) WILD_CHIVE(달래) ASAFOETIDA(흥거) ALCOHOL(알코올) MIRIN(미림) COOKING_WINE(맛술)
SULFITES(아황산류)"""

# 형식 검증(글자 수, 9개 언어 존재 여부, spiciness 범위)은 업스트림 kbap 배치가
# PENDING_REVIEW 로 올리기 전에 이미 끝냈다(Food.needsNameTranslations / assessAvoidance).
# 여기서 다시 보면 모델 주의력만 나눠 쓰고 배치가 보장한 걸 깎을 위험이 있어 명시적으로 배제한다.
#
# 단, "언어 태그와 실제 표기 언어가 맞는가"는 여기 해당하지 않는다 — 배치는 9개 키에 값이
# 있는지만 보장하고, 태국어 자리에 영어가 들어가도 통과시킨다. 그건 내용 검증이라 남긴다.
_CONTENT_ONLY = """형식 검증은 이미 끝났습니다 — 글자 수, 번역 누락 여부, 등급 범위는
보지 마세요. 오직 내용이 맞는가만 판단하세요."""


# reason 을 score 보다 앞에 둔다 — structured output 은 필드 순서대로 생성되므로, score 가
# 먼저면 모델이 근거를 세우기 전에 숫자부터 뱉는다. 기피성분 스모크에서 주요 성분(PORK) 누락을
# 6회 중 2회만 잡던 것이 순서를 뒤집자 개선됐다.
class FieldScore(BaseModel):
    reason: str
    score: int = Field(ge=0, le=100)


class TranslationLangScore(BaseModel):
    lang: str
    reason: str
    score: int = Field(ge=0, le=100)


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
- lang 이 가리키는 언어로 실제로 쓰여 있는가 (예: th 자리에 영어가 들어가 있으면 0점)

음식 이름(한국어): {food["koreanName"]}
설명(한국어): {food["description"]}
이름 번역: {json.dumps(food["nameTranslations"], ensure_ascii=False)}
설명 번역: {json.dumps(food["descriptionTranslations"], ensure_ascii=False)}

출력 규칙 — 반드시 지키세요:
- items 배열은 **정확히 {len(TARGET_LANGS)}개** 항목이어야 합니다. 하나라도 빠지면 안 됩니다.
- 아래 순서 그대로, 이 lang 값을 문자 그대로 사용하세요: {", ".join(TARGET_LANGS)}
- 여러 언어를 한 항목으로 합치거나, 점수가 같다는 이유로 생략하지 마세요.
  점수가 같아도 {len(TARGET_LANGS)}개를 각각 적으세요.
- 판단이 어려운 언어도 건너뛰지 말고, 확신이 없으면 낮은 점수를 주세요.
  빠뜨린 언어는 0점으로 간주되어 멀쩡한 번역까지 폐기됩니다.

각 항목은 lang, score(0~100), reason(한국어 한 문장)입니다."""


def avoidance_prompt(food: dict) -> str:
    return f"""당신은 식품 안전 검수자입니다. 아래 음식의 기피성분 목록과 매운맛 등급이
일반적인 레시피 기준으로 타당한지 0~100점으로 채점하세요.

{_CONTENT_ONLY}

# inclusionPercent 의 의미 (생성 규격)
"손님이 아무 식당에서나 이 메뉴를 시켰을 때, 그 한 접시에 이 성분이 들어 있을 확률."
양(量)이 아니라 포함 여부의 확률입니다. 95~100 정의상 반드시 / 80~95 표준 레시피 핵심 재료 /
55~80 대부분 넣지만 집집마다 다름 / 30~55 흔한 선택 재료·고명·양념 / 10~30 일부 식당·변형만 /
1~10 미량·교차오염. 핵심 재료에 90~100 이 붙는 것은 규격대로이니 과대평가로 깎지 마세요.

# spiciness 의 의미 (생성 규격 — 이 척도로만 판단)
0 맵지 않음(계란말이) / 1~3 약간 매콤(제육볶음 순한맛, **김치찌개**) /
4~6 보통 매움(떡볶이, 닭갈비) / 7~10 매우 매움(불닭, 마라 계열).
체감이 아니라 이 척도 기준으로 어긋날 때만 감점하세요.

# 후보 성분 코드 (생성기가 고를 수 있는 전체 목록)
{AVOIDANCE_CODES}
이 목록에 없는 성분(김치·고춧가루·된장 등)은 애초에 표기할 수 없습니다.
목록 밖 성분이 빠졌다는 이유로 절대 감점하지 마세요.

채점 절차 — reason 에 이 순서대로 쓰고 마지막에 score 를 매기세요:
1. **누락 대조 (가장 중요)** — 먼저 이 음식의 대표 레시피에 거의 항상 들어가는 재료를
   떠올리고, 그중 후보 코드 목록에 있는 것을 하나씩 위 기피성분 목록과 대조하세요.
   목록에 있어야 할 주요 성분이 빠져 있으면 그 코드를 reason 에 적고 크게 감점하세요
   (50점 이하). 알레르기·비건·종교 안전 직결이라 누락이 가장 위험합니다.
   예: 돼지고기를 넣고 끓이는 음식에 PORK 가 없음, 밀가루 면 요리에 WHEAT 가 없음.
2. **생뚱맞은 성분** — 이 음식 레시피와 아무 상관 없는 성분이 올라와 있거나, 실제보다
   터무니없이 높은 확률이 붙어 있지 않은가 (예: 김치찌개에 갑각류 90%).
   관광객이 먹을 수 있는 음식을 못 먹는다고 잘못 걸러낸다.
3. **확률·매운맛** — 남은 성분의 포함 확률과 spiciness 가 위 구간 정의와 맞는가

특정 브랜드·식당 레시피가 아니라 한국 음식의 일반적인 레시피를 기준으로 판단합니다.
관광객이 먹을 수 있는 음식인지, 알레르기·비건·종교 안전에 문제가 없는지 판단하는 것이 목적입니다.

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
