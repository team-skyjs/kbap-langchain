"""스캔 음식 이름 정제 — LLM 보수적 정규화·결정론적 가드·배치 실행.

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
# 정제는 고치는 일만 한다 — 비음식 판정은 그래프의 이름 검수(content.NameReview)가 맡는다.
class NameFix(BaseModel):
    reason: str
    corrected: str


def _jamo(s: str) -> str:
    # NFD 정규화로 한글 음절을 자모로 분해하면 "김치찌게"와 "김치찌개"의 차이가
    # 음절 1자가 아닌 자모 1자가 되어 OCR 오타를 더 세밀하게 감지할 수 있다.
    return unicodedata.normalize("NFD", s)


Normalizer = Callable[[str, str], Awaitable[NameFix]]

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


async def clean_one(name: str, normalize: Normalizer, feedback: str = "") -> dict:
    """feedback 은 이름 검수 탈락 사유 — 재정제 시 프롬프트에 들어가 개선을 유도한다."""
    fix = await normalize(name, feedback)
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


async def clean_batch(names: list[str], normalize: Normalizer, concurrency: int) -> list[dict]:
    sem = asyncio.Semaphore(concurrency)

    async def one(name: str) -> dict:
        async with sem:
            return await clean_one(name, normalize)

    results = await asyncio.gather(*(one(n) for n in names), return_exceptions=True)
    return [
        {"original": name, "name": name, "method": "held", "reason": str(result)}
        if isinstance(result, BaseException)
        else result
        for name, result in zip(names, results)
    ]


def normalize_prompt(name: str, feedback: str = "") -> str:
    from kbap.prompts import NAMEFIX_TEMPLATE, render_prompt

    block = f"\n\n## 이전 시도 탈락 사유 — 반드시 반영해 다시 정제하세요\n{feedback}" if feedback else ""
    return render_prompt("food-namefix", NAMEFIX_TEMPLATE, name=name, feedback_block=block)


def make_normalizer(model_name: str, timeout: int, callbacks: list) -> Normalizer:
    """실제 LLM 기반 정규화기. 테스트가 LLM 패키지 없이 실행되도록 함수 안에서 가져온다."""
    from kbap.review import init_model

    llm = init_model(model_name, timeout).with_structured_output(NameFix)
    # callbacks 가 비면 config 를 넘기지 않는다 — 빈 리스트를 명시하면 그래프 실행이
    # 전파하는 트레이싱 콜백을 덮어써 이 LLM 콜만 트레이스에서 사라진다(content._bind 와 동일 규칙).
    if callbacks:
        llm = llm.with_config(callbacks=callbacks)

    async def normalize(name: str, feedback: str = "") -> NameFix:
        return await llm.ainvoke(normalize_prompt(name, feedback))

    return normalize
