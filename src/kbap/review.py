"""food 최종 검수 배치 — 설정·kbap 클라이언트·채점 프롬프트·판정·그래프·러너."""

from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict
import asyncio
import httpx
import json
import logging
import os
import yaml

from dotenv import load_dotenv
from langchain_core.exceptions import OutputParserException
from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy, default_retry_on
from pydantic import BaseModel
from pydantic import BaseModel, Field
from pydantic import ValidationError


class Thresholds(BaseModel):
    # 모델 점수가 0~100 이므로 임계값도 그 범위여야 한다. 음수면 전부 통과해
    # 기피성분 미달까지 REVIEWED 로 나가고, 100 초과면 전부 탈락한다.
    description: int = Field(ge=0, le=100)
    translations: int = Field(ge=0, le=100)
    avoidance: int = Field(ge=0, le=100)


class AppConfig(BaseModel):
    kbap_base_url: str
    kbap_token: str
    model: str
    avoidance_model: str
    thresholds: Thresholds
    # 0 이면 Semaphore(0) 이 모든 코루틴을 영구히 막아 무인 배치가 조용히 멈춘다.
    concurrency: int = Field(ge=1)
    timeout_seconds: int = Field(default=120, ge=1)


def load_config(path: str = "config.yaml") -> AppConfig:
    # .env 를 상위 디렉터리까지 훑어 읽는다(노트북은 notebooks/ 에서 실행돼도 루트 .env 를 찾는다).
    # override=False 라 이미 설정된 환경변수가 우선 — cron/CI 가 준 값을 .env 가 덮지 않는다.
    load_dotenv()
    with open(path) as f:
        raw = yaml.safe_load(f)
    llm = raw["llm"]
    return AppConfig(
        kbap_base_url=raw["kbap_api"]["base_url"],
        kbap_token=os.environ["KBAP_API_TOKEN"],
        model=llm["model"],
        avoidance_model=llm.get("avoidance_model", llm["model"]),
        thresholds=Thresholds(**raw["thresholds"]),
        concurrency=raw["concurrency"],
        timeout_seconds=llm.get("timeout_seconds", 120),
    )


# kbap ApiPaths.ADMIN = "/api/v1/admin". base_url 은 호스트까지만 준다.
CONTENT_REVIEWS = "/api/v1/admin/foods/content-reviews"


class KbapClient:
    def __init__(self, base_url: str, token: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )

    async def fetch_review_candidates(self, limit: int) -> list[dict]:
        """PENDING_REVIEW 상태 음식 목록. 항목 키는 foodId / contentReviewAttempts."""
        resp = await self._client.get(CONTENT_REVIEWS, params={"limit": limit})
        resp.raise_for_status()
        # BaseResponse<AdminFoodContentReviewTargetsResponse> — {success, payload:{items:[...]}}
        return resp.json()["payload"]["items"]

    async def post_review_result(self, food_id: int, verdict: Verdict) -> dict:
        """검수 결과 반영. 재시도 소진 여부 판단과 컬럼 비우기는 kbap 이 한다.

        반영 후 상태를 돌려준다 — {foodId, contentStatus, contentReviewAttempts,
        contentReviewRejectionReason}. 탈락 건이 재생성으로 갔는지 REVIEW_REJECTED 로
        갔는지는 이 값으로만 알 수 있다.
        """
        body: dict = {"passed": verdict.passed}
        if not verdict.passed:
            body["rejectedFields"] = verdict.rejected_fields
            body["reason"] = verdict.reason
        resp = await self._client.post(f"{CONTENT_REVIEWS}/{food_id}", json=body)
        resp.raise_for_status()
        return resp.json()["payload"]

    async def aclose(self) -> None:
        await self._client.aclose()


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


def init_model(name: str, timeout: int):
    """모델명 문자열로 chat model 생성. import를 함수 안에 두어 테스트가 LLM 패키지 없이 돌게 한다.

    "gemini-*"는 자동 추론이 안 되는 버전이 있어 provider를 명시한다.
    gpt-* 등 타 벤더로 바꾸면 "openai:gpt-5-mini"처럼 "provider:model" 형식으로 설정.
    timeout 미설정 시 SDK 기본값을 쓰는데, 무인 배치에서 한 콜이 멈추면 그
    세마포어 슬롯을 영원히 붙잡아 gather 전체가 멎는다 — 반드시 설정한다.
    """
    from langchain.chat_models import init_chat_model

    if ":" in name:
        provider, model = name.split(":", 1)
        return init_chat_model(model, model_provider=provider, timeout=timeout)
    if name.startswith("gemini"):
        return init_chat_model(name, model_provider="google_genai", timeout=timeout)
    return init_chat_model(name, timeout=timeout)


def make_scorers(config):
    """실 LLM 기반 스코어러."""
    base = init_model(config.model, config.timeout_seconds)
    avoid = init_model(config.avoidance_model, config.timeout_seconds)
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


def _retryable(exc: Exception) -> bool:
    """LangGraph 기본 정책 + structured output 파싱 실패."""
    if isinstance(exc, (OutputParserException, ValidationError)):
        return True
    return default_retry_on(exc)


class Scorers(NamedTuple):
    description: Callable[[dict], Awaitable[FieldScore]]
    translations: Callable[[dict], Awaitable[dict[str, FieldScore]]]
    avoidance: Callable[[dict], Awaitable[FieldScore]]


class ReviewState(TypedDict, total=False):
    food: dict
    description_score: FieldScore
    translation_scores: dict[str, FieldScore]
    avoidance_score: FieldScore
    verdict: Verdict
    applied: dict  # kbap 반영 후 상태. dry_run 이면 없다.


def build_graph(scorers: Scorers, client, thresholds: Thresholds, dry_run: bool = False):
    # 설명 검수 — 설명이 이 음식을 사실대로 설명하는지 0~100 채점 (팬아웃 갈래 1)
    async def score_description(state: ReviewState):
        return {"description_score": await scorers.description(state["food"])}

    # 번역 검수 — 이름·설명 번역을 9개 언어별로 채점, 누락 언어는 0점 (팬아웃 갈래 2)
    async def score_translations(state: ReviewState):
        return {"translation_scores": await scorers.translations(state["food"])}

    # 기피성분·매운맛 검수 — 주요 성분 누락을 최우선으로 채점 (팬아웃 갈래 3)
    async def score_avoidance(state: ReviewState):
        return {"avoidance_score": await scorers.avoidance(state["food"])}

    # 종합판정 — 세 점수를 임계값과 대조해 통과/탈락과 문제 필드를 결정 (순수 함수, LLM 없음)
    def aggregate(state: ReviewState):
        return {
            "verdict": decide(
                description_score=state["description_score"],
                translation_scores=state["translation_scores"],
                avoidance_score=state["avoidance_score"],
                thresholds=thresholds,
            )
        }

    # 결과 반영 — 판정을 kbap API 로 POST 해 DB 상태를 바꾼다 (dry_run 이면 건너뜀)
    async def report(state: ReviewState):
        if dry_run:
            return {}
        applied = await client.post_review_result(state["food"]["foodId"], state["verdict"])
        return {"applied": applied}

    # LLM 노드만 재시도 — aggregate는 순수 함수, report 실패는 실행기의 보류 처리로 충분.
    # 기본 retry_on 은 ValueError 계열을 제외하는데, structured output 파싱 실패
    # (OutputParserException·pydantic ValidationError)가 전부 ValueError 하위라 한 번도
    # 재시도되지 않는다. 모델이 다시 뽑으면 통과하는 경우가 많아 명시적으로 포함시킨다.
    retry = RetryPolicy(max_attempts=2, retry_on=_retryable)
    g = StateGraph(ReviewState)
    g.add_node("score_description", score_description, retry_policy=retry)
    g.add_node("score_translations", score_translations, retry_policy=retry)
    g.add_node("score_avoidance", score_avoidance, retry_policy=retry)
    g.add_node("aggregate", aggregate)
    g.add_node("report", report)

    g.add_edge(START, "score_description")
    g.add_edge(START, "score_translations")
    g.add_edge(START, "score_avoidance")
    # 리스트 엣지 = join: 세 채점이 모두 끝난 뒤 aggregate 실행
    g.add_edge(["score_description", "score_translations", "score_avoidance"], "aggregate")
    g.add_edge("aggregate", "report")
    g.add_edge("report", END)
    return g.compile()


log = logging.getLogger("kbap.review")


async def run_batch(graph, foods: list[dict], concurrency: int, callbacks: list) -> dict[str, int]:
    # concurrency는 동시 실행 "그래프" 수를 제한한다 — 그래프 하나가 3개 필드군을
    # 팬아웃하므로 실제 동시 LLM 콜 상한은 concurrency * 3. Gemini RPM 쿼터는 이 값 기준으로 확인.
    sem = asyncio.Semaphore(concurrency)

    async def one(food: dict):
        async with sem:
            return await graph.ainvoke({"food": food}, config={"callbacks": callbacks})

    results = await asyncio.gather(*(one(f) for f in foods), return_exceptions=True)

    # 탈락 건이 재생성으로 돌아갈지 REVIEW_REJECTED 로 갈지는 kbap 이 정한다 —
    # 여기서는 통과/탈락/보류만 센다.
    counts = {"PASS": 0, "FAIL": 0, "HELD": 0}
    for food, result in zip(foods, results):
        if isinstance(result, BaseException):
            # LLM/POST 실패 — 판정 보류. PENDING_REVIEW에 남아 다음 실행에서 자연 재시도.
            counts["HELD"] += 1
            # foodId 자체가 없는 계약 위반도 여기로 떨어지므로 get 으로 읽는다.
            log.warning(
                "보류 foodId=%s (%s): %s", food.get("foodId"), food.get("koreanName"), result
            )
        else:
            verdict = result["verdict"]
            key = "PASS" if verdict.passed else "FAIL"
            counts[key] += 1
            applied = result.get("applied") or {}
            log.info(
                "%s foodId=%s (%s) %s -> %s",
                key,
                food.get("foodId"),
                food.get("koreanName"),
                verdict.rejected_fields or "",
                applied.get("contentStatus", "dry-run"),
            )
    return counts


def make_callbacks() -> list:
    # LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY / LANGFUSE_HOST 환경변수로 연결
    if not os.environ.get("LANGFUSE_PUBLIC_KEY"):
        return []
    from langfuse.langchain import CallbackHandler

    return [CallbackHandler()]
