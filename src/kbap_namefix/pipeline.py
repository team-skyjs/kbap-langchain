import asyncio
import difflib
import re
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


def _jamo_edits(a: str, b: str) -> int:
    """자모 삽입+삭제 수. 치환 1자모 = 삭제+삽입 = 2."""
    matches = sum(bl.size for bl in difflib.SequenceMatcher(None, a, b).get_matching_blocks())
    return len(a) + len(b) - 2 * matches


def snap(name: str, anchors: list[str], max_edits: int = 2) -> str | None:
    """자모 편집 수로 앵커(기수집 확정 음식명)에 스냅. 예산 초과·동률이면 None.

    비율(ratio)이 아니라 절대 편집 수를 쓴다 — 비율 기준은 공유 접두어가 길면
    "왕김치찌개"→"김치찌개"(수식어 삭제), "돼지고기 김치찜"→"돼지고기 김치찌개"(다른 요리)
    까지 통과시킨다. OCR 오타는 길이와 무관하게 1~2자모라 예산 2가 정확히 가른다.
    """
    if name in anchors:
        return name
    # ponytail: difflib 선형 스캔 — 앵커 수만 건 이상이면 rapidfuzz 로 교체
    j = _jamo(name)
    best, best_edits, tied = None, 0, False
    for anchor in anchors:
        edits = _jamo_edits(j, _jamo(anchor))
        if edits > max_edits:
            continue
        if best is None or edits < best_edits:
            best, best_edits, tied = anchor, edits, False
        elif edits == best_edits:
            tied = True  # 어느 앵커인지 모른다 — 앵커 순서로 결과가 뒤집히면 안 된다
    return None if tied else best


Normalizer = Callable[[str], Awaitable[NameFix]]

_NON_HANGUL = re.compile(r"[^가-힣 ]")


def _plausible(corrected: str, original: str) -> bool:
    """LLM 교정이 원본의 한글 부분과 닮았는지 결정적 가드.

    프롬프트의 보존 규칙은 강제력이 없다 — 모델이(또는 프롬프트 인젝션이) 전혀 다른
    이름을 내놔도 여기서 막는다. 원본에서 노이즈(가격·번호·기호)를 뺀 한글만 남기고
    비교하므로, 정상 교정(오타 1~2자모, 노이즈 제거)은 통과한다.
    """
    base = " ".join(_NON_HANGUL.sub(" ", original).split()) or original
    return difflib.SequenceMatcher(None, _jamo(corrected), _jamo(base)).ratio() >= 0.5


async def clean_one(name: str, anchors: list[str], normalize: Normalizer) -> dict:
    snapped = snap(name, anchors)
    if snapped is not None:
        return {"original": name, "name": snapped, "method": "snap", "reason": ""}
    fix = await normalize(name)
    corrected = fix.corrected.strip()
    if not corrected or corrected == name or not _plausible(corrected, name):
        # 빈 출력·무변경·원본과 동떨어진 출력은 원본 유지 — 지우거나 과교정하지 않는다.
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
