from kbap.namefix import NameFix, clean_batch, clean_one, snap

ANCHORS = ["김치찌개", "된장찌개", "비빔밥", "돼지고기 김치찌개"]


# --- snap: 자모 편집 거리 스냅(LLM 없음) ---


def test_snap_fixes_jamo_typo():
    assert snap("김치찌게", ANCHORS) == "김치찌개"


def test_snap_exact_match():
    assert snap("비빔밥", ANCHORS) == "비빔밥"


def test_snap_rejects_distant_name():
    # 앵커에 없는 새 음식명은 무리하게 스냅하지 않는다. 과교정 방지가 우선이다.
    assert snap("마라탕", ANCHORS) is None


def test_snap_empty_anchors():
    assert snap("김치찌개", []) is None


def test_snap_preserves_modifier():
    # "왕김치찌개"는 오타가 아니라 수식어가 붙은 별도 이름이므로 스냅으로 지우면 안 된다.
    assert snap("왕김치찌개", ANCHORS) is None


def test_snap_rejects_different_dish():
    # 찜과 찌개는 다른 요리이므로 공통 접두어("돼지고기 김치")가 길어도 스냅하지 않는다.
    assert snap("돼지고기 김치찜", ["돼지고기 김치찌개"]) is None


def test_snap_ambiguous_tie_rejected():
    # 두 앵커까지의 거리가 같으면 정답을 알 수 없으므로 앵커 순서에 따라 결과가 바뀌면 안 된다.
    assert snap("김치찌갸", ["김치찌개", "김치찌게"]) is None


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
    assert calls == []  # 스냅에 성공하면 LLM을 호출하지 않는다


async def test_clean_one_llm_correction():
    result = await clean_one("냉             면 8,000원", [], make_normalizer("냉면"))
    assert result["name"] == "냉면"
    assert result["method"] == "llm"


async def test_clean_one_llm_no_change():
    result = await clean_one("할매손맛 김치찌개", [], make_normalizer("할매손맛 김치찌개"))
    assert result["name"] == "할매손맛 김치찌개"
    assert result["method"] == "unchanged"


async def test_clean_one_llm_wholesale_rename_rejected():
    # 프롬프트 규칙이 무시되어 모델이 전혀 다른 이름을 반환해도 결정론적 방어선이 막는다.
    result = await clean_one("할매손맛 김치찌개", [], make_normalizer("마라탕"))
    assert result["name"] == "할매손맛 김치찌개"
    assert result["method"] == "unchanged"


async def test_clean_one_llm_empty_output_keeps_original():
    # 모델이 빈 문자열을 반환하면 원본을 유지해 수집 데이터가 삭제되지 않게 한다.
    result = await clean_one("떡볶이", [], make_normalizer("  "))
    assert result["name"] == "떡볶이"
    assert result["method"] == "unchanged"


# --- clean_batch: 배치 실행과 실패 격리 ---


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


def test_normalize_prompt_instructs_spacing():
    # 붙어 쓴 이름의 띄어쓰기 교정도 정제 범위다 — 지시가 빠지면 모델이 건드리지 않는다.
    from kbap.namefix import normalize_prompt

    p = normalize_prompt("돼지고기김치찌개")
    assert "띄어쓰기" in p
