from kbap_namefix.pipeline import NameFix, clean_batch, clean_one, snap

ANCHORS = ["김치찌개", "된장찌개", "비빔밥", "돼지고기 김치찌개"]


# --- snap: 자모 편집거리 스냅 (LLM 없음) ---


def test_snap_fixes_jamo_typo():
    assert snap("김치찌게", ANCHORS) == "김치찌개"


def test_snap_exact_match():
    assert snap("비빔밥", ANCHORS) == "비빔밥"


def test_snap_rejects_distant_name():
    # 앵커에 없는 신규 음식은 억지로 스냅하지 않는다 — 과교정 방지가 최우선.
    assert snap("마라탕", ANCHORS) is None


def test_snap_empty_anchors():
    assert snap("김치찌개", []) is None


# --- clean_one: 스냅 → LLM 폴백 판정 ---


def make_normalizer(corrected: str, calls: list | None = None):
    async def normalize(name: str) -> NameFix:
        if calls is not None:
            calls.append(name)
        return NameFix(reason="테스트", corrected=corrected)

    return normalize


async def test_clean_one_snap_skips_llm():
    calls: list = []
    result = await clean_one("김치찌게", ANCHORS, make_normalizer("무관", calls))
    assert result == {"original": "김치찌게", "name": "김치찌개", "method": "snap", "reason": ""}
    assert calls == []  # 스냅되면 LLM 콜 자체가 없다


async def test_clean_one_llm_correction():
    result = await clean_one("냉             면 8,000원", [], make_normalizer("냉면"))
    assert result["name"] == "냉면"
    assert result["method"] == "llm"


async def test_clean_one_llm_no_change():
    result = await clean_one("할매손맛 김치찌개", [], make_normalizer("할매손맛 김치찌개"))
    assert result["name"] == "할매손맛 김치찌개"
    assert result["method"] == "unchanged"


async def test_clean_one_llm_empty_output_keeps_original():
    # 모델이 빈 문자열을 뱉으면 원본 유지 — 수집 데이터를 지우는 일은 없어야 한다.
    result = await clean_one("떡볶이", [], make_normalizer("  "))
    assert result["name"] == "떡볶이"
    assert result["method"] == "unchanged"


# --- clean_batch: 배치 실행, 실패 격리 ---


async def test_clean_batch_isolates_failures():
    async def flaky(name: str) -> NameFix:
        if name == "폭탄":
            raise RuntimeError("LLM down")
        return NameFix(reason="ok", corrected=name)

    results = await clean_batch(["김치찌게", "폭탄", "마라탕"], ANCHORS, flaky, concurrency=2)

    by_original = {r["original"]: r for r in results}
    assert by_original["김치찌게"]["method"] == "snap"
    assert by_original["폭탄"]["method"] == "held"
    assert by_original["마라탕"]["method"] == "unchanged"
