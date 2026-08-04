# KB-286 food 최종 검수(scoring) LangGraph 구현 계획

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `PENDING_REVIEW` 상태 food를 LLM으로 필드군별 채점해 PASS/RETRY/REJECT 판정을 kbap API로 반영하는 CLI 배치.

**Architecture:** 음식 1건 = LangGraph 1회 실행. 채점 노드 3개(설명/번역/기피성분) 팬아웃 → aggregate(순수 함수) → report(kbap POST). 실행기는 그래프 밖 평범한 asyncio 코드. LLM·kbap 클라이언트는 그래프 팩토리에 주입해 테스트에서 fake로 교체.

**Tech Stack:** Python 3.12+, uv, langgraph, langchain(`init_chat_model`), langchain-google-genai, httpx, pydantic, pyyaml, langfuse, pytest + pytest-asyncio.

**스펙:** `docs/superpowers/specs/2026-08-04-food-review-scoring-design.md` — kbap(Kotlin) 쪽 작업은 범위 밖(별도 지라 태스크).

## Global Constraints

- verdict 문자열은 정확히 `"PASS"` / `"RETRY"` / `"REJECT"`.
- failedFields 값은 정확히 `"description"` / `"translations"` / `"avoidance"`.
- 대상 언어 9개(순서 포함): `["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]` (kbap `LanguageCode`에서 ko 제외).
- REJECT의 `reviewNote`는 개조식 최대 10줄.
- RETRY/REJECT 분기 기준: `food["reviewAttempts"] >= 2`면 REJECT.
- LLM 실패는 판정 보류(POST 안 함) — 탈락은 오직 점수 미달만.
- config 키: `kbap_api.base_url`, `llm.model`, `llm.avoidance_model`, `thresholds.{description,translations,avoidance}`, `concurrency`. 인증 토큰은 `KBAP_API_TOKEN` 환경변수.
- 모든 커밋 메시지 끝에 `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`.

---

### Task 1: 프로젝트 스캐폴딩 + 설정 로더

**Files:**
- Create: `pyproject.toml`
- Create: `config.yaml`
- Create: `src/kbap_review/__init__.py` (빈 파일)
- Create: `src/kbap_review/config.py`
- Test: `tests/test_config.py`

**Interfaces:**
- Consumes: 없음 (최초 태스크)
- Produces: `load_config(path: str = "config.yaml") -> AppConfig`.
  `AppConfig` 필드: `kbap_base_url: str`, `kbap_token: str`, `model: str`, `avoidance_model: str`, `thresholds: Thresholds`, `concurrency: int`.
  `Thresholds` 필드: `description: int`, `translations: int`, `avoidance: int`.

- [ ] **Step 1: pyproject.toml 작성**

```toml
[project]
name = "kbap-review"
version = "0.1.0"
description = "KB-286 food 최종 검수(scoring) LangGraph 배치"
requires-python = ">=3.12"
dependencies = [
    "langgraph>=1.0",
    "langchain>=1.0",
    "langchain-google-genai>=2.0",
    "httpx>=0.27",
    "pydantic>=2.7",
    "pyyaml>=6.0",
    "langfuse>=3.0",
]

[dependency-groups]
dev = ["pytest>=8.0", "pytest-asyncio>=0.24"]

[tool.pytest.ini_options]
asyncio_mode = "auto"
testpaths = ["tests"]

[tool.uv]
package = true

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/kbap_review"]
```

- [ ] **Step 2: config.yaml 작성**

```yaml
kbap_api:
  base_url: http://localhost:8080   # 홈서버 배포 시 실제 kbap 주소로 교체. 인증: KBAP_API_TOKEN 환경변수
llm:
  model: gemini-2.5-flash           # 기본 채점 모델. 언제든 교체 가능
  avoidance_model: gemini-2.5-flash # 안전 직결 노드만 별도 오버라이드 (판정 이상 시 gpt-5-mini 등)
thresholds:                         # 필드군별 통과 임계값 — 운영하며 튜닝
  description: 70
  translations: 70
  avoidance: 70
concurrency: 5
```

- [ ] **Step 3: 실패하는 테스트 작성** (`tests/test_config.py`)

```python
import textwrap

from kbap_review.config import load_config


def test_load_config(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
          avoidance_model: gpt-5-mini
        thresholds:
          description: 70
          translations: 75
          avoidance: 80
        concurrency: 3
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "secret-token")

    cfg = load_config(str(cfg_file))

    assert cfg.kbap_base_url == "http://kbap.example.com"
    assert cfg.kbap_token == "secret-token"
    assert cfg.model == "gemini-2.5-flash"
    assert cfg.avoidance_model == "gpt-5-mini"
    assert cfg.thresholds.description == 70
    assert cfg.thresholds.translations == 75
    assert cfg.thresholds.avoidance == 80
    assert cfg.concurrency == 3


def test_avoidance_model_defaults_to_model(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
        thresholds:
          description: 70
          translations: 70
          avoidance: 70
        concurrency: 5
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "t")

    cfg = load_config(str(cfg_file))

    assert cfg.avoidance_model == "gemini-2.5-flash"
```

- [ ] **Step 4: 실패 확인**

Run: `uv run pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'kbap_review.config'` (uv가 먼저 의존성을 설치하므로 몇 분 걸릴 수 있음)

- [ ] **Step 5: 구현** (`src/kbap_review/config.py`)

```python
import os

import yaml
from pydantic import BaseModel


class Thresholds(BaseModel):
    description: int
    translations: int
    avoidance: int


class AppConfig(BaseModel):
    kbap_base_url: str
    kbap_token: str
    model: str
    avoidance_model: str
    thresholds: Thresholds
    concurrency: int


def load_config(path: str = "config.yaml") -> AppConfig:
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
    )
```

- [ ] **Step 6: 통과 확인**

Run: `uv run pytest tests/test_config.py -v`
Expected: PASS 2건

- [ ] **Step 7: .gitignore 작성 후 커밋**

`.gitignore`:
```
.venv/
__pycache__/
*.pyc
.pytest_cache/
.env
```

```bash
git add pyproject.toml uv.lock config.yaml .gitignore src tests
git commit -m "feat: 프로젝트 스캐폴딩 + 설정 로더

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: 채점 스키마 + 프롬프트

**Files:**
- Create: `src/kbap_review/scoring.py`
- Test: `tests/test_scoring.py`

**Interfaces:**
- Consumes: 없음
- Produces:
  - `TARGET_LANGS: list[str]` — Global Constraints의 9개 언어 리스트.
  - `class FieldScore(BaseModel)`: `score: int` (0~100 검증), `reason: str`
  - `class TranslationLangScore(BaseModel)`: `lang: str`, `score: int` (0~100), `reason: str`
  - `class TranslationScores(BaseModel)`: `items: list[TranslationLangScore]`
  - `description_prompt(food: dict) -> str`, `translations_prompt(food: dict) -> str`, `avoidance_prompt(food: dict) -> str`
  - `food` dict는 kbap 조회 응답 형식: `id`, `koreanName`, `description`, `nameTranslations`(dict), `descriptionTranslations`(dict), `spiciness`(int), `avoidanceSubstances`(list of `{code, inclusion_percent}`), `reviewAttempts`(int)

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/test_scoring.py`)

```python
import pytest
from pydantic import ValidationError

from kbap_review.scoring import (
    TARGET_LANGS,
    FieldScore,
    TranslationScores,
    avoidance_prompt,
    description_prompt,
    translations_prompt,
)

FOOD = {
    "id": 1,
    "koreanName": "김치찌개",
    "description": "돼지고기와 김치를 넣고 끓인 얼큰한 찌개",
    "nameTranslations": {"en": "Kimchi Stew", "ja": "キムチチゲ"},
    "descriptionTranslations": {"en": "Spicy stew with pork and kimchi"},
    "spiciness": 7,
    "avoidanceSubstances": [{"code": "PORK", "inclusion_percent": 95}],
    "reviewAttempts": 0,
}


def test_target_langs():
    assert TARGET_LANGS == ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]


def test_field_score_rejects_out_of_range():
    with pytest.raises(ValidationError):
        FieldScore(score=101, reason="r")
    with pytest.raises(ValidationError):
        FieldScore(score=-1, reason="r")


def test_translation_scores_schema():
    ts = TranslationScores(items=[{"lang": "en", "score": 90, "reason": "ok"}])
    assert ts.items[0].lang == "en"


def test_description_prompt_contains_food():
    p = description_prompt(FOOD)
    assert "김치찌개" in p
    assert FOOD["description"] in p


def test_translations_prompt_lists_all_target_langs():
    p = translations_prompt(FOOD)
    for lang in TARGET_LANGS:
        assert lang in p
    assert "Kimchi Stew" in p


def test_avoidance_prompt_contains_substances_and_spiciness():
    p = avoidance_prompt(FOOD)
    assert "PORK" in p
    assert "95" in p
    assert "7" in p
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/test_scoring.py -v`
Expected: FAIL — `ModuleNotFoundError` 또는 `ImportError`

- [ ] **Step 3: 구현** (`src/kbap_review/scoring.py`)

```python
import json

from pydantic import BaseModel, Field

# kbap LanguageCode에서 ko 제외 9개 — 순서 포함 일치해야 한다.
TARGET_LANGS = ["zh-Hans", "en", "ja", "zh-Hant", "vi", "id", "th", "ru", "es"]


class FieldScore(BaseModel):
    score: int = Field(ge=0, le=100)
    reason: str


class TranslationLangScore(BaseModel):
    lang: str
    score: int = Field(ge=0, le=100)
    reason: str


class TranslationScores(BaseModel):
    items: list[TranslationLangScore]


def description_prompt(food: dict) -> str:
    return f"""당신은 한국 음식 콘텐츠 검수자입니다. 아래 음식 설명이 외국인 관광객에게
제공하기에 적합한지 0~100점으로 채점하세요.

채점 기준:
- 설명이 실제로 이 음식을 정확히 설명하는가 (다른 음식 설명이 아닌가)
- 재료·조리법·맛 서술에 환각이나 오기가 없는가
- 외국인 관광객 기준으로 이해 가능한 설명인가

음식 이름: {food["koreanName"]}
설명: {food["description"]}

score(0~100)와 reason(한국어 한 문장)을 반환하세요."""


def translations_prompt(food: dict) -> str:
    return f"""당신은 다국어 번역 검수자입니다. 한국 음식의 이름·설명 번역을 언어별로
0~100점으로 채점하세요.

채점 기준 (언어별로 각각):
- 이름 번역이 원문 음식을 정확히 지칭하는가
- 설명 번역이 한국어 원문과 의미가 일치하는가 (누락·왜곡·환각 없음)
- 해당 언어 태그와 실제 표기 언어가 일치하는가

음식 이름(한국어): {food["koreanName"]}
설명(한국어): {food["description"]}
이름 번역: {json.dumps(food["nameTranslations"], ensure_ascii=False)}
설명 번역: {json.dumps(food["descriptionTranslations"], ensure_ascii=False)}

대상 언어 {len(TARGET_LANGS)}개 전부에 대해 items 배열로 반환하세요: {", ".join(TARGET_LANGS)}
각 항목은 lang, score(0~100), reason(한국어 한 문장)입니다.
번역이 아예 없는 언어는 score 0으로 채점하세요."""


def avoidance_prompt(food: dict) -> str:
    return f"""당신은 식품 안전 검수자입니다. 아래 음식의 기피성분 목록과 매운맛 등급이
일반적인 레시피 기준으로 타당한지 0~100점으로 채점하세요.

채점 기준:
- 기피성분 목록(성분 코드 + 포함 확률 %)이 이 음식의 일반적인 레시피와 부합하는가
- 명백히 포함되는 주요 성분이 목록에서 빠지지 않았는가 (알레르기·비건·종교 안전 직결)
- 매운맛 등급(0~10)이 이 음식에 타당한가

이것은 안전 직결 판단입니다. 확신이 없으면 낮은 점수를 주세요.

음식 이름: {food["koreanName"]}
기피성분 목록: {json.dumps(food["avoidanceSubstances"], ensure_ascii=False)}
매운맛 등급: {food["spiciness"]}

score(0~100)와 reason(한국어 한 문장)을 반환하세요."""
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/test_scoring.py -v`
Expected: PASS 6건

- [ ] **Step 5: 커밋**

```bash
git add src/kbap_review/scoring.py tests/test_scoring.py
git commit -m "feat: 채점 스키마 + 필드군별 프롬프트

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: aggregate 판정 로직 (순수 함수)

**Files:**
- Create: `src/kbap_review/aggregate.py`
- Test: `tests/test_aggregate.py`

**Interfaces:**
- Consumes: Task 2의 `FieldScore`, `Thresholds`(Task 1)
- Produces:
  - `class Verdict(BaseModel)`: `verdict: Literal["PASS", "RETRY", "REJECT"]`, `failed_fields: list[str]`, `scores: dict`, `review_note: str | None`
    - `scores` 형식: `{"description": int, "translations": {lang: int}, "avoidance": int}`
  - `decide(review_attempts: int, description_score: FieldScore, translation_scores: dict[str, FieldScore], avoidance_score: FieldScore, thresholds: Thresholds) -> Verdict`
  - `MAX_NOTE_LINES = 10`

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/test_aggregate.py`)

```python
from kbap_review.aggregate import MAX_NOTE_LINES, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


def fs(score: int, reason: str = "이유") -> FieldScore:
    return FieldScore(score=score, reason=reason)


def all_pass_translations() -> dict[str, FieldScore]:
    return {lang: fs(90) for lang in ["zh-Hans", "en", "ja"]}


def test_all_pass():
    v = decide(0, fs(80), all_pass_translations(), fs(75), TH)
    assert v.verdict == "PASS"
    assert v.failed_fields == []
    assert v.review_note is None
    assert v.scores["description"] == 80
    assert v.scores["translations"]["en"] == 90
    assert v.scores["avoidance"] == 75


def test_threshold_is_inclusive():
    # 임계값과 같으면 통과 (70 >= 70)
    v = decide(0, fs(70), {"en": fs(70)}, fs(70), TH)
    assert v.verdict == "PASS"


def test_retry_on_description_fail():
    v = decide(0, fs(50), all_pass_translations(), fs(90), TH)
    assert v.verdict == "RETRY"
    assert v.failed_fields == ["description"]


def test_retry_when_one_language_fails():
    translations = all_pass_translations() | {"th": fs(30, "태국어 번역이 다른 음식을 지칭")}
    v = decide(1, fs(90), translations, fs(90), TH)
    assert v.verdict == "RETRY"
    assert v.failed_fields == ["translations"]


def test_reject_at_two_attempts():
    v = decide(2, fs(50, "설명이 다른 음식을 설명함"), all_pass_translations(), fs(40, "돼지고기 누락"), TH)
    assert v.verdict == "REJECT"
    assert v.failed_fields == ["description", "avoidance"]
    assert v.review_note is not None
    assert "설명이 다른 음식을 설명함" in v.review_note
    assert "돼지고기 누락" in v.review_note


def test_reject_note_capped_at_10_lines():
    translations = {f"l{i}": fs(10, f"사유 {i}") for i in range(15)}
    v = decide(2, fs(10, "설명 문제"), translations, fs(10, "성분 문제"), TH)
    assert len(v.review_note.splitlines()) <= MAX_NOTE_LINES


def test_pass_at_two_attempts_still_passes():
    # attempts가 몇이든 점수가 되면 PASS
    v = decide(5, fs(90), all_pass_translations(), fs(90), TH)
    assert v.verdict == "PASS"
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/test_aggregate.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'kbap_review.aggregate'`

- [ ] **Step 3: 구현** (`src/kbap_review/aggregate.py`)

```python
from typing import Literal

from pydantic import BaseModel

from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore

MAX_NOTE_LINES = 10

# 재검수(컬럼 비움 + INCOMPLETE 롤백) 허용 횟수 — 스펙: 2회까지, 이후 REJECT.
MAX_RETRY_ATTEMPTS = 2


class Verdict(BaseModel):
    verdict: Literal["PASS", "RETRY", "REJECT"]
    failed_fields: list[str]
    scores: dict
    review_note: str | None = None


def decide(
    review_attempts: int,
    description_score: FieldScore,
    translation_scores: dict[str, FieldScore],
    avoidance_score: FieldScore,
    thresholds: Thresholds,
) -> Verdict:
    failed: list[str] = []
    if description_score.score < thresholds.description:
        failed.append("description")
    failed_langs = {
        lang: s for lang, s in translation_scores.items() if s.score < thresholds.translations
    }
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
        return Verdict(verdict="PASS", failed_fields=[], scores=scores)
    if review_attempts < MAX_RETRY_ATTEMPTS:
        return Verdict(verdict="RETRY", failed_fields=failed, scores=scores)

    note_lines: list[str] = []
    if "description" in failed:
        note_lines.append(f"- 설명({description_score.score}점): {description_score.reason}")
    for lang, s in failed_langs.items():
        note_lines.append(f"- 번역 {lang}({s.score}점): {s.reason}")
    if "avoidance" in failed:
        note_lines.append(f"- 기피성분·매운맛({avoidance_score.score}점): {avoidance_score.reason}")
    return Verdict(
        verdict="REJECT",
        failed_fields=failed,
        scores=scores,
        review_note="\n".join(note_lines[:MAX_NOTE_LINES]),
    )
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/test_aggregate.py -v`
Expected: PASS 7건

- [ ] **Step 5: 커밋**

```bash
git add src/kbap_review/aggregate.py tests/test_aggregate.py
git commit -m "feat: aggregate 판정 로직 (PASS/RETRY/REJECT + 사유 조립)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: kbap API 클라이언트

**Files:**
- Create: `src/kbap_review/kbap_client.py`
- Test: `tests/test_kbap_client.py`

**Interfaces:**
- Consumes: Task 3의 `Verdict`
- Produces:
  - `class KbapClient`: `__init__(self, base_url: str, token: str)`
  - `async fetch_review_candidates(self, limit: int) -> list[dict]` — `GET /admin/foods/review-candidates?limit={limit}`
  - `async post_review_result(self, food_id: int, verdict: Verdict) -> None` — `POST /admin/foods/{food_id}/review-result`
    - 전송 body: `{"verdict": ..., "scores": ...}` + RETRY면 `"failedFields"`, REJECT면 `"reviewNote"` (camelCase — kbap API 계약)
  - `async aclose(self) -> None`

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/test_kbap_client.py`)

httpx 내장 `MockTransport` 사용 — 추가 의존성 없음.

```python
import json

import httpx
import pytest

from kbap_review.aggregate import Verdict
from kbap_review.kbap_client import KbapClient


def make_client(handler) -> KbapClient:
    client = KbapClient(base_url="http://kbap.test", token="tok")
    client._client = httpx.AsyncClient(
        base_url="http://kbap.test",
        headers={"Authorization": "Bearer tok"},
        transport=httpx.MockTransport(handler),
    )
    return client


async def test_fetch_review_candidates():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/admin/foods/review-candidates"
        assert request.url.params["limit"] == "50"
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(200, json=[{"id": 1, "koreanName": "김치찌개"}])

    client = make_client(handler)
    foods = await client.fetch_review_candidates(limit=50)
    assert foods == [{"id": 1, "koreanName": "김치찌개"}]


async def test_post_pass_result():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    client = make_client(handler)
    v = Verdict(verdict="PASS", failed_fields=[], scores={"description": 90})
    await client.post_review_result(1, v)

    assert captured["path"] == "/admin/foods/1/review-result"
    assert captured["body"] == {"verdict": "PASS", "scores": {"description": 90}}


async def test_post_retry_result_includes_failed_fields():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    client = make_client(handler)
    v = Verdict(verdict="RETRY", failed_fields=["description"], scores={"description": 50})
    await client.post_review_result(1, v)

    assert captured["body"]["failedFields"] == ["description"]
    assert "reviewNote" not in captured["body"]


async def test_post_reject_result_includes_review_note():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    client = make_client(handler)
    v = Verdict(
        verdict="REJECT",
        failed_fields=["avoidance"],
        scores={"avoidance": 40},
        review_note="- 기피성분(40점): 돼지고기 누락",
    )
    await client.post_review_result(1, v)

    assert captured["body"]["reviewNote"] == "- 기피성분(40점): 돼지고기 누락"
    assert captured["body"]["failedFields"] == ["avoidance"]


async def test_post_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409)

    client = make_client(handler)
    v = Verdict(verdict="PASS", failed_fields=[], scores={})
    with pytest.raises(httpx.HTTPStatusError):
        await client.post_review_result(1, v)
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/test_kbap_client.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'kbap_review.kbap_client'`

- [ ] **Step 3: 구현** (`src/kbap_review/kbap_client.py`)

```python
import httpx

from kbap_review.aggregate import Verdict


class KbapClient:
    def __init__(self, base_url: str, token: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )

    async def fetch_review_candidates(self, limit: int) -> list[dict]:
        resp = await self._client.get("/admin/foods/review-candidates", params={"limit": limit})
        resp.raise_for_status()
        return resp.json()

    async def post_review_result(self, food_id: int, verdict: Verdict) -> None:
        body: dict = {"verdict": verdict.verdict, "scores": verdict.scores}
        if verdict.verdict == "RETRY":
            body["failedFields"] = verdict.failed_fields
        elif verdict.verdict == "REJECT":
            body["failedFields"] = verdict.failed_fields
            body["reviewNote"] = verdict.review_note
        resp = await self._client.post(f"/admin/foods/{food_id}/review-result", json=body)
        resp.raise_for_status()

    async def aclose(self) -> None:
        await self._client.aclose()
```

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/test_kbap_client.py -v`
Expected: PASS 5건

- [ ] **Step 5: 커밋**

```bash
git add src/kbap_review/kbap_client.py tests/test_kbap_client.py
git commit -m "feat: kbap API 클라이언트 (조회 + 검수 결과 반영)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: LangGraph 그래프 조립

**Files:**
- Create: `src/kbap_review/graph.py`
- Test: `tests/test_graph.py`

**Interfaces:**
- Consumes: Task 2 `FieldScore`/`TranslationScores`/`TARGET_LANGS`, Task 3 `decide`/`Verdict`, Task 4 `KbapClient`(덕 타이핑 — post_review_result만 사용), Task 1 `Thresholds`
- Produces:
  - `class Scorers(NamedTuple)`: `description: Callable[[dict], Awaitable[FieldScore]]`, `translations: Callable[[dict], Awaitable[dict[str, FieldScore]]]`, `avoidance: Callable[[dict], Awaitable[FieldScore]]` — 각각 food dict를 받는 async 함수
  - `build_graph(scorers: Scorers, client, thresholds: Thresholds, dry_run: bool = False)` — 컴파일된 그래프 반환. `ainvoke({"food": food_dict})` 결과 state에 `verdict: Verdict` 포함. dry_run이면 report 노드가 POST 생략.

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/test_graph.py`)

```python
from kbap_review.config import Thresholds
from kbap_review.graph import Scorers, build_graph
from kbap_review.scoring import FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)

FOOD = {"id": 7, "koreanName": "김치찌개", "reviewAttempts": 0}


class FakeClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append((food_id, verdict))


def make_scorers(desc=85, trans=90, avoid=80) -> Scorers:
    async def d(food):
        return FieldScore(score=desc, reason="설명 사유")

    async def t(food):
        return {"en": FieldScore(score=trans, reason="영어 사유")}

    async def a(food):
        return FieldScore(score=avoid, reason="성분 사유")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_pass_path_posts_pass():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].verdict == "PASS"
    assert client.posts == [(7, state["verdict"])]


async def test_retry_path_posts_failed_fields():
    client = FakeClient()
    graph = build_graph(make_scorers(desc=30), client, TH)

    state = await graph.ainvoke({"food": FOOD})

    (food_id, verdict), = client.posts
    assert verdict.verdict == "RETRY"
    assert verdict.failed_fields == ["description"]


async def test_reject_path_when_attempts_exhausted():
    client = FakeClient()
    graph = build_graph(make_scorers(avoid=10), client, TH)

    state = await graph.ainvoke({"food": {**FOOD, "reviewAttempts": 2}})

    (_, verdict), = client.posts
    assert verdict.verdict == "REJECT"
    assert "성분 사유" in verdict.review_note


async def test_dry_run_does_not_post():
    client = FakeClient()
    graph = build_graph(make_scorers(), client, TH, dry_run=True)

    state = await graph.ainvoke({"food": FOOD})

    assert state["verdict"].verdict == "PASS"
    assert client.posts == []


async def test_scorer_exception_propagates():
    # LLM 실패는 그래프 실행 실패로 전파 — 실행기가 보류 처리 (POST 없음)
    async def boom(food):
        raise RuntimeError("LLM down")

    scorers = make_scorers()._replace(description=boom)
    client = FakeClient()
    graph = build_graph(scorers, client, TH)

    import pytest

    with pytest.raises(RuntimeError):
        await graph.ainvoke({"food": FOOD})
    assert client.posts == []
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/test_graph.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'kbap_review.graph'`

- [ ] **Step 3: 구현** (`src/kbap_review/graph.py`)

```python
from collections.abc import Awaitable, Callable
from typing import NamedTuple, TypedDict

from langgraph.graph import END, START, StateGraph
from langgraph.types import RetryPolicy

from kbap_review.aggregate import Verdict, decide
from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore


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


def build_graph(scorers: Scorers, client, thresholds: Thresholds, dry_run: bool = False):
    async def score_description(state: ReviewState):
        return {"description_score": await scorers.description(state["food"])}

    async def score_translations(state: ReviewState):
        return {"translation_scores": await scorers.translations(state["food"])}

    async def score_avoidance(state: ReviewState):
        return {"avoidance_score": await scorers.avoidance(state["food"])}

    def aggregate(state: ReviewState):
        return {
            "verdict": decide(
                review_attempts=state["food"].get("reviewAttempts", 0),
                description_score=state["description_score"],
                translation_scores=state["translation_scores"],
                avoidance_score=state["avoidance_score"],
                thresholds=thresholds,
            )
        }

    async def report(state: ReviewState):
        if not dry_run:
            await client.post_review_result(state["food"]["id"], state["verdict"])
        return {}

    # LLM 노드만 재시도 — aggregate는 순수 함수, report 실패는 실행기의 보류 처리로 충분.
    retry = RetryPolicy(max_attempts=2)
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
```

주의: `RetryPolicy` import 경로와 `add_node`의 `retry_policy` 파라미터명은 langgraph 버전에 따라 다를 수 있다(구버전은 `retry=`). 설치된 버전에서 `ImportError`/`TypeError`가 나면 `uv run python -c "from langgraph.types import RetryPolicy; help(RetryPolicy)"`와 `StateGraph.add_node` 시그니처를 확인해 맞춘다.

- [ ] **Step 4: 통과 확인**

Run: `uv run pytest tests/test_graph.py -v`
Expected: PASS 5건

- [ ] **Step 5: 전체 테스트 확인 후 커밋**

Run: `uv run pytest -v`
Expected: 전부 PASS

```bash
git add src/kbap_review/graph.py tests/test_graph.py
git commit -m "feat: 검수 그래프 조립 (채점 팬아웃 → aggregate → report)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: 실 LLM 스코어러 + CLI 실행기 + Langfuse

**Files:**
- Modify: `src/kbap_review/scoring.py` (끝에 `make_scorers` 추가)
- Create: `src/kbap_review/__main__.py`
- Test: `tests/test_runner.py`

**Interfaces:**
- Consumes: 전 태스크 전부
- Produces:
  - `make_scorers(config: AppConfig) -> Scorers` (scoring.py) — 실 LLM 기반
  - `run_batch(graph, foods: list[dict], concurrency: int, callbacks: list) -> dict[str, int]` (\_\_main\_\_.py) — `{"PASS": n, "RETRY": n, "REJECT": n, "HELD": n}` 반환
  - CLI: `uv run python -m kbap_review --limit 50 [--dry-run] [--config config.yaml]`

- [ ] **Step 1: 실패하는 테스트 작성** (`tests/test_runner.py`)

run_batch만 테스트 — make_scorers(실 LLM)와 main()은 스모크로 검증.

```python
from kbap_review.__main__ import run_batch
from kbap_review.config import Thresholds
from kbap_review.graph import Scorers, build_graph
from kbap_review.scoring import FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


class FakeClient:
    def __init__(self):
        self.posts = []

    async def post_review_result(self, food_id, verdict):
        self.posts.append(food_id)


def scorers_failing_for(bad_id: int) -> Scorers:
    async def d(food):
        if food["id"] == bad_id:
            raise RuntimeError("LLM down")
        return FieldScore(score=90, reason="ok")

    async def t(food):
        return {"en": FieldScore(score=90, reason="ok")}

    async def a(food):
        return FieldScore(score=90, reason="ok")

    return Scorers(description=d, translations=t, avoidance=a)


async def test_run_batch_counts_and_isolates_failures():
    client = FakeClient()
    graph = build_graph(scorers_failing_for(bad_id=2), client, TH)
    foods = [
        {"id": 1, "koreanName": "김치찌개", "reviewAttempts": 0},
        {"id": 2, "koreanName": "불고기", "reviewAttempts": 0},
        {"id": 3, "koreanName": "비빔밥", "reviewAttempts": 0},
    ]

    counts = await run_batch(graph, foods, concurrency=2, callbacks=[])

    # id=2는 LLM 실패 → 보류(HELD), POST 없음. 나머지는 PASS + POST.
    assert counts == {"PASS": 2, "RETRY": 0, "REJECT": 0, "HELD": 1}
    assert sorted(client.posts) == [1, 3]
```

- [ ] **Step 2: 실패 확인**

Run: `uv run pytest tests/test_runner.py -v`
Expected: FAIL — `ModuleNotFoundError` 또는 `ImportError: cannot import name 'run_batch'`

- [ ] **Step 3: make_scorers 구현** (`src/kbap_review/scoring.py` 끝에 추가)

```python
def make_scorers(config):
    """실 LLM 기반 스코어러. import를 함수 안에 두어 테스트가 LLM 패키지 없이 돌게 한다."""
    from langchain.chat_models import init_chat_model

    from kbap_review.graph import Scorers

    def _model(name: str):
        # "gemini-*"는 자동 추론이 안 되는 버전이 있어 provider를 명시한다.
        # gpt-* 등 타 벤더로 바꾸면 "openai:gpt-5-mini"처럼 "provider:model" 형식으로 설정.
        if ":" in name:
            provider, model = name.split(":", 1)
            return init_chat_model(model, model_provider=provider)
        if name.startswith("gemini"):
            return init_chat_model(name, model_provider="google_genai")
        return init_chat_model(name)

    base = _model(config.model)
    avoid = _model(config.avoidance_model)
    desc_llm = base.with_structured_output(FieldScore)
    trans_llm = base.with_structured_output(TranslationScores)
    avoid_llm = avoid.with_structured_output(FieldScore)

    async def description(food: dict) -> FieldScore:
        return await desc_llm.ainvoke(description_prompt(food))

    async def translations(food: dict) -> dict[str, FieldScore]:
        result: TranslationScores = await trans_llm.ainvoke(translations_prompt(food))
        scores = {i.lang: FieldScore(score=i.score, reason=i.reason) for i in result.items}
        # 모델이 언어를 누락하면 0점 처리 — 누락을 통과로 취급하지 않는다(fail-closed).
        for lang in TARGET_LANGS:
            scores.setdefault(lang, FieldScore(score=0, reason="모델 응답에서 언어 누락"))
        return scores

    async def avoidance(food: dict) -> FieldScore:
        return await avoid_llm.ainvoke(avoidance_prompt(food))

    return Scorers(description=description, translations=translations, avoidance=avoidance)
```

- [ ] **Step 4: CLI 실행기 구현** (`src/kbap_review/__main__.py`)

```python
import argparse
import asyncio
import logging
import os

from kbap_review.config import load_config
from kbap_review.graph import build_graph
from kbap_review.kbap_client import KbapClient
from kbap_review.scoring import make_scorers

log = logging.getLogger("kbap_review")


async def run_batch(graph, foods: list[dict], concurrency: int, callbacks: list) -> dict[str, int]:
    sem = asyncio.Semaphore(concurrency)

    async def one(food: dict):
        async with sem:
            return await graph.ainvoke({"food": food}, config={"callbacks": callbacks})

    results = await asyncio.gather(*(one(f) for f in foods), return_exceptions=True)

    counts = {"PASS": 0, "RETRY": 0, "REJECT": 0, "HELD": 0}
    for food, result in zip(foods, results):
        if isinstance(result, BaseException):
            # LLM/POST 실패 — 판정 보류. PENDING_REVIEW에 남아 다음 실행에서 자연 재시도.
            counts["HELD"] += 1
            log.warning("보류 id=%s (%s): %s", food["id"], food.get("koreanName"), result)
        else:
            verdict = result["verdict"]
            counts[verdict.verdict] += 1
            log.info("%s id=%s (%s)", verdict.verdict, food["id"], food.get("koreanName"))
    return counts


def make_callbacks() -> list:
    # LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY / LANGFUSE_HOST 환경변수로 연결
    if not os.environ.get("LANGFUSE_PUBLIC_KEY"):
        return []
    from langfuse.langchain import CallbackHandler

    return [CallbackHandler()]


async def main() -> None:
    parser = argparse.ArgumentParser(description="KB-286 food 최종 검수 배치")
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument("--dry-run", action="store_true", help="kbap에 결과를 반영하지 않고 판정만 출력")
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    config = load_config(args.config)
    client = KbapClient(config.kbap_base_url, config.kbap_token)
    try:
        foods = await client.fetch_review_candidates(limit=args.limit)
        if not foods:
            log.info("검수 대상 없음")
            return
        log.info("검수 대상 %d건 (dry_run=%s)", len(foods), args.dry_run)
        graph = build_graph(make_scorers(config), client, config.thresholds, dry_run=args.dry_run)
        counts = await run_batch(graph, foods, config.concurrency, make_callbacks())
        log.info("완료: %s", counts)
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
```

- [ ] **Step 5: 통과 확인**

Run: `uv run pytest -v`
Expected: 전부 PASS (신규 1건 포함)

- [ ] **Step 6: 커밋**

```bash
git add src/kbap_review/scoring.py src/kbap_review/__main__.py tests/test_runner.py
git commit -m "feat: 실 LLM 스코어러 + CLI 실행기 + Langfuse 연동

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: 실 LLM 스모크 (수동 — 자동화 안 함)

**Files:** 없음 (검증만)

**전제:** kbap 쪽 API(별도 지라 태스크)가 아직 없으면 이 태스크는 **API 준비 후로 미룬다** — 그 전까지는 아래 "그래프 단독 스모크"만 수행.

- [ ] **Step 1: 그래프 단독 스모크 (kbap API 불필요)**

`GOOGLE_API_KEY` 설정 후 임시 스크립트로 실제 음식 1건을 그래프에 직접 투입:

```bash
GOOGLE_API_KEY=... uv run python - <<'EOF'
import asyncio
from kbap_review.config import load_config
from kbap_review.graph import build_graph
from kbap_review.scoring import make_scorers

FOOD = {
    "id": 0,
    "koreanName": "김치찌개",
    "description": "돼지고기와 잘 익은 김치를 넣고 끓인 얼큰한 국물 요리입니다.",
    "nameTranslations": {"zh-Hans": "泡菜汤", "en": "Kimchi Stew", "ja": "キムチチゲ", "zh-Hant": "泡菜鍋", "vi": "Canh kim chi", "id": "Sup Kimchi", "th": "ซุปกิมจิ", "ru": "Кимчи-чиге", "es": "Estofado de kimchi"},
    "descriptionTranslations": {"zh-Hans": "加入猪肉和熟成泡菜炖煮的辣汤", "en": "A spicy stew made with pork and well-fermented kimchi", "ja": "豚肉と熟成キムチを煮込んだ辛いスープ料理", "zh-Hant": "加入豬肉與熟成泡菜燉煮的辣湯", "vi": "Món canh cay nấu với thịt heo và kim chi lên men", "id": "Sup pedas dengan daging babi dan kimchi fermentasi", "th": "ซุปรสเผ็ดต้มกับหมูและกิมจิหมัก", "ru": "Острый суп со свининой и квашеной кимчи", "es": "Estofado picante con cerdo y kimchi fermentado"},
    "spiciness": 7,
    "avoidanceSubstances": [{"code": "PORK", "inclusion_percent": 95}, {"code": "GARLIC", "inclusion_percent": 90}],
    "reviewAttempts": 0,
}

class NullClient:
    async def post_review_result(self, food_id, verdict): pass

async def main():
    config = load_config()
    graph = build_graph(make_scorers(config), NullClient(), config.thresholds, dry_run=True)
    state = await graph.ainvoke({"food": FOOD})
    v = state["verdict"]
    print(v.verdict, v.scores)
    if v.review_note:
        print(v.review_note)

asyncio.run(main())
EOF
```

확인 사항:
- 정상 데이터가 PASS로 나오는가 (verdict + 필드별 점수 출력)
- `avoidanceSubstances`에서 PORK를 빼고 다시 돌리면 avoidance 점수가 떨어지는가 (안전 직결 판정 품질)
- `descriptionTranslations`의 en을 엉뚱한 문장으로 바꾸면 translations가 탈락하는가

- [ ] **Step 2: (kbap API 준비 후) 엔드투엔드 dry-run**

```bash
KBAP_API_TOKEN=... GOOGLE_API_KEY=... uv run python -m kbap_review --limit 3 --dry-run
```

확인: 조회 → 채점 → 판정 로그가 나오고 kbap에 반영은 안 되는 것. Langfuse 키를 주면 트레이스에 노드 3개 팬아웃이 보이는 것.

- [ ] **Step 3: 스모크에서 프롬프트/임계값 조정이 나오면 수정 후 커밋**

```bash
git add -A && git commit -m "chore: 스모크 결과 반영

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```
