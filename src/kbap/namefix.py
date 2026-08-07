"""스캔 음식 이름 정제 — 자모 스냅·LLM 보수적 정규화·배치 실행.

콘텐츠 파이프라인의 첫 단계다: 여기서 정제된 이름이 생성·검수 전체의 입력이 된다.
kbap.content 그래프가 clean_one/make_normalizer 를 그대로 쓰며, 단독 배치
(uv run kbap namefix)는 로컬 JSON 으로 정제만 돌려보는 디버깅용이다."""

from collections.abc import Awaitable, Callable
import asyncio
import difflib
import re
import unicodedata

from pydantic import BaseModel


# structured output은 필드 순서대로 생성되므로 reason을 corrected보다 앞에 둔다.
# 모델이 근거를 먼저 세운 뒤 교정명을 생성하게 한다(scoring.FieldScore와 같은 이유).
class NameFix(BaseModel):
    reason: str
    # 단일 음식 메뉴명이 아니면(옵션·카테고리 제목·판독 불가) False — 그래프가 생성 없이 끝낸다.
    # 기본 True: 구버전 프롬프트가 이 필드를 지시하지 않아도 기존 동작(전부 통과)을 유지한다.
    is_food: bool = True
    corrected: str


def _jamo(s: str) -> str:
    # NFD 정규화로 한글 음절을 자모로 분해하면 "김치찌게"와 "김치찌개"의 차이가
    # 음절 1자가 아닌 자모 1자가 되어 OCR 오타를 더 세밀하게 감지할 수 있다.
    return unicodedata.normalize("NFD", s)


def _jamo_edits(a: str, b: str) -> int:
    """자모 삽입·삭제 횟수. 자모 치환 1회는 삭제 1회와 삽입 1회로 계산하므로 결과는 2다."""
    matches = sum(bl.size for bl in difflib.SequenceMatcher(None, a, b).get_matching_blocks())
    return len(a) + len(b) - 2 * matches


def snap(name: str, anchors: list[str], max_edits: int = 2) -> str | None:
    """자모 편집 횟수로 앵커(이미 수집해 확정한 음식명)에 스냅한다. 예산 초과·동률이면 None.

    비율(ratio)이 아닌 절대 편집 횟수를 사용한다. 비율 기준은 공통 접두사가 길면
    "왕김치찌개"→"김치찌개"(수식어 삭제), "돼지고기 김치찜"→"돼지고기 김치찌개"(다른 요리)
    까지 통과시킨다. OCR 오타는 이름 길이와 무관하게 1~2자모 차이이므로 예산 2로 구분할 수 있다.
    """
    if name in anchors:
        return name
    # ponytail: difflib 선형 스캔 — 앵커 수가 수만 건을 넘으면 rapidfuzz로 교체
    j = _jamo(name)
    best, best_edits, tied = None, 0, False
    for anchor in anchors:
        edits = _jamo_edits(j, _jamo(anchor))
        if edits > max_edits:
            continue
        if best is None or edits < best_edits:
            best, best_edits, tied = anchor, edits, False
        elif edits == best_edits:
            tied = True  # 어느 앵커가 맞는지 알 수 없으며 앵커 순서에 따라 결과가 바뀌면 안 된다
    return None if tied else best


Normalizer = Callable[[str], Awaitable[NameFix]]

_NON_HANGUL = re.compile(r"[^가-힣 ]")


def _plausible(corrected: str, original: str) -> bool:
    """LLM 교정 결과가 원본의 한글 부분과 유사한지 확인하는 결정론적 방어선.

    프롬프트의 보존 규칙은 강제력이 없다. 모델이나 프롬프트 인젝션으로 전혀 다른
    이름이 나와도 여기서 막는다. 원본에서 노이즈(가격·번호·기호)를 제거한 한글만
    비교하므로 정상적인 교정(오타 1~2자모, 노이즈 제거)은 통과한다.
    """
    base = " ".join(_NON_HANGUL.sub(" ", original).split()) or original
    return difflib.SequenceMatcher(None, _jamo(corrected), _jamo(base)).ratio() >= 0.5


# 정제된 이름의 길이 상한(공백 포함) — 콘텐츠 정책. 프롬프트 지시와 함께 코드로도 강제한다.
MAX_NAME_LENGTH = 20


async def clean_one(name: str, anchors: list[str], normalize: Normalizer) -> dict:
    snapped = snap(name, anchors)
    if snapped is not None:
        return {"original": name, "name": snapped, "method": "snap", "reason": ""}
    fix = await normalize(name)
    if not fix.is_food:
        # 옵션("사리 추가")·판독 불가 텍스트 — 콘텐츠 생성 대상이 아니므로 호출부가 걸러낸다.
        return {"original": name, "name": name, "method": "rejected", "reason": fix.reason}
    corrected = fix.corrected.strip()
    if (
        not corrected
        or corrected == name
        or len(corrected) > MAX_NAME_LENGTH
        or not _plausible(corrected, name)
    ):
        # 빈 출력·변경 없음·길이 초과·원본과 동떨어진 출력은 원본을 유지해 삭제나 과교정을 막는다.
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
    from kbap.prompts import NAMEFIX_TEMPLATE, render_prompt

    return render_prompt("food-namefix", NAMEFIX_TEMPLATE, name=name)


def make_normalizer(model_name: str, timeout: int, callbacks: list) -> Normalizer:
    """실제 LLM 기반 정규화기. 테스트가 LLM 패키지 없이 실행되도록 함수 안에서 가져온다."""
    from kbap.review import init_model

    llm = init_model(model_name, timeout).with_structured_output(NameFix)

    async def normalize(name: str) -> NameFix:
        return await llm.ainvoke(normalize_prompt(name), config={"callbacks": callbacks})

    return normalize
