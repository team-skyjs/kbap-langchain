import asyncio
import difflib
import unicodedata
from collections.abc import Awaitable, Callable

from pydantic import BaseModel


# reason 을 corrected 보다 앞에 둔다 — structured output 은 필드 순서대로 생성되므로
# 모델이 근거를 세운 뒤 교정명을 뱉게 한다(scoring.FieldScore 와 같은 교훈).
class NameFix(BaseModel):
    reason: str
    corrected: str


def _jamo(s: str) -> str:
    # NFD 정규화가 한글 음절을 자모로 분해한다 — "김치찌게"와 "김치찌개"의 차이가
    # 음절 1글자가 아니라 자모 1글자가 되어 OCR 오타에 훨씬 민감하게 반응한다.
    return unicodedata.normalize("NFD", s)


def snap(name: str, anchors: list[str], threshold: float = 0.85) -> str | None:
    """자모 편집거리로 앵커(기수집 확정 음식명)에 스냅. 못 미치면 None — 억지 스냅 금지."""
    if name in anchors:
        return name
    # ponytail: difflib 선형 스캔 — 앵커 수만 건 이상이면 rapidfuzz 로 교체
    j = _jamo(name)
    best, best_ratio = None, threshold
    for anchor in anchors:
        ratio = difflib.SequenceMatcher(None, j, _jamo(anchor)).ratio()
        if ratio >= best_ratio:
            best, best_ratio = anchor, ratio
    return best


Normalizer = Callable[[str], Awaitable[NameFix]]


async def clean_one(
    name: str, anchors: list[str], normalize: Normalizer, threshold: float = 0.85
) -> dict:
    snapped = snap(name, anchors, threshold)
    if snapped is not None:
        return {"original": name, "name": snapped, "method": "snap", "reason": ""}
    fix = await normalize(name)
    corrected = fix.corrected.strip()
    if not corrected or corrected == name:
        # 빈 출력·무변경은 원본 유지 — 수집 데이터를 지우거나 과교정하지 않는다.
        return {"original": name, "name": name, "method": "unchanged", "reason": fix.reason}
    return {"original": name, "name": corrected, "method": "llm", "reason": fix.reason}


async def clean_batch(
    names: list[str], anchors: list[str], normalize: Normalizer, concurrency: int
) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)

    async def one(name: str) -> dict:
        async with sem:
            return await clean_one(name, anchors, normalize)

    results = await asyncio.gather(*(one(n) for n in names), return_exceptions=True)
    return [
        {"original": name, "name": name, "method": "held", "reason": str(result)}
        if isinstance(result, BaseException)
        else result
        for name, result in zip(names, results)
    ]


def normalize_prompt(name: str) -> str:
    return f"""당신은 메뉴판 OCR로 수집된 한국 음식 이름을 정제하는 도구입니다.
아래 이름에서 다음만 고치세요:
- 명백한 오타·OCR 오인식 (예: 김치찌게 → 김치찌개, 재육볶음 → 제육볶음)
- 음식 이름이 아닌 노이즈 제거: 가격, 메뉴 번호, 장식 문자, 과도한 공백

하지 말 것:
- 다른 음식으로 바꾸거나 새 이름을 짓지 마세요
- 수식어를 지우지 마세요 ("할매손맛 김치찌개"의 "할매손맛"은 이름의 일부입니다)
- 확신이 없으면 원본을 그대로 반환하세요. 원본 유지가 잘못된 교정보다 낫습니다.

이름: {name}

reason(한국어 한 문장)과 corrected(정제된 이름)를 반환하세요."""


def make_normalizer(model_name: str, timeout: int, callbacks: list) -> Normalizer:
    """실 LLM 기반 정규화기. import를 함수 안에 두어 테스트가 LLM 패키지 없이 돌게 한다."""
    from kbap_review.scoring import init_model

    llm = init_model(model_name, timeout).with_structured_output(NameFix)

    async def normalize(name: str) -> NameFix:
        return await llm.ainvoke(normalize_prompt(name), config={"callbacks": callbacks})

    return normalize
