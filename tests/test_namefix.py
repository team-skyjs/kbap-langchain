from kbap.namefix import NameFix, clean_batch, clean_one


# --- clean_one: LLM 교정 → 결정론적 가드 ---


def make_normalizer(corrected: str, calls: list | None = None):
    async def normalize(name: str, feedback: str = "") -> NameFix:
        if calls is not None:
            calls.append((name, feedback))
        return NameFix(reason="테스트", corrected=corrected)

    return normalize


async def test_clean_one_passes_feedback_to_normalizer():
    # 이름 검수 탈락 사유가 재정제 프롬프트에 들어가야 한다 — 비음식 판정은 검수 노드로 이관됐다.
    calls: list = []
    await clean_one("김치찌게", make_normalizer("김치찌개", calls), feedback="오타가 남아 있다")
    assert calls == [("김치찌게", "오타가 남아 있다")]


async def test_clean_one_llm_correction():
    result = await clean_one("냉             면 8,000원", make_normalizer("냉면"))
    assert result["name"] == "냉면"
    assert result["method"] == "llm"


async def test_clean_one_llm_no_change():
    result = await clean_one("할매손맛 김치찌개", make_normalizer("할매손맛 김치찌개"))
    assert result["name"] == "할매손맛 김치찌개"
    assert result["method"] == "unchanged"


async def test_clean_one_llm_wholesale_rename_rejected():
    # 프롬프트 규칙이 무시되어 모델이 전혀 다른 이름을 반환해도 결정론적 방어선이 막는다.
    result = await clean_one("할매손맛 김치찌개", make_normalizer("마라탕"))
    assert result["name"] == "할매손맛 김치찌개"
    assert result["method"] == "unchanged"


async def test_clean_one_llm_empty_output_keeps_original():
    # 모델이 빈 문자열을 반환하면 원본을 유지해 수집 데이터가 삭제되지 않게 한다.
    result = await clean_one("떡볶이", make_normalizer("  "))
    assert result["name"] == "떡볶이"
    assert result["method"] == "unchanged"


# --- clean_batch: 배치 실행과 실패 격리 ---


async def test_clean_batch_isolates_failures():
    async def flaky(name: str, feedback: str = "") -> NameFix:
        if name == "폭탄":
            raise RuntimeError("LLM down")
        return NameFix(reason="ok", corrected="김치찌개" if name == "김치찌게" else name)

    results = await clean_batch(["김치찌게", "폭탄", "마라탕"], flaky, concurrency=2)

    by_original = {r["original"]: r for r in results}
    assert by_original["김치찌게"]["method"] == "llm"
    assert by_original["폭탄"]["method"] == "held"
    assert by_original["마라탕"]["method"] == "unchanged"


def test_normalize_prompt_instructs_name_length_limit():
    from kbap.namefix import normalize_prompt

    assert "20자" in normalize_prompt("김치찌개")


async def test_clean_one_rejects_overlong_correction():
    # 정책: 정제된 이름은 20자 이내. 원본과 닮아 유사도 가드는 통과하는
    # 띄어쓰기 교정이라도 20자를 넘으면 원본 유지로 떨어뜨린다.
    original = "할매손맛묵은지김치찜명품장인정성가득"  # 18자
    corrected = "할매손맛 묵은지 김치찜 명품 장인 정성 가득"  # 24자
    result = await clean_one(original, make_normalizer(corrected))
    assert result["name"] == original
    assert result["method"] == "unchanged"


def test_normalize_prompt_instructs_spacing():
    # 붙어 쓴 이름의 띄어쓰기 교정도 정제 범위다 — 지시가 빠지면 모델이 건드리지 않는다.
    from kbap.namefix import normalize_prompt

    p = normalize_prompt("돼지고기김치찌개")
    assert "띄어쓰기" in p
