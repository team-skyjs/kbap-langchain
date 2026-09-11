# KB-549 Lambda 환경 분리 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 같은 컨테이너 이미지로 dev·prod 두 Lambda 를 띄울 수 있게 적재 API 주소를 환경변수로 분리하고, deploy.sh 가 두 함수를 모두 갱신하며, 이미지 태그를 git SHA 로 고정한다.

**Architecture:** `KbapClient` 를 만드는 두 지점(`content._consume` Lambda 경로, `review.load_config` 배치 경로)이 공통 헬퍼 `kbap_base_url(raw)` 로 `KBAP_API_BASE_URL` 환경변수 → `config.yaml` 순으로 주소를 고른다. 이미지는 `:<git sha>`(불변)와 `:latest`(이동 포인터) 두 태그로 push 하고, 함수 갱신은 SHA 태그로만 한다 — 어떤 커밋이 떠 있는지 함수에서 바로 읽히고 롤백은 이전 SHA 로 `update-function-code` 한 번이다. prod 함수(`kbap-generate-content-prod`)는 콘솔에서 같은 이미지·다른 환경변수로 만들고 큐 트리거는 붙이지 않는다.

**Tech Stack:** Python 3.12, pytest, bash, AWS CLI(Lambda·ECR), Docker(arm64)

**Spec:** Jira [KB-549](https://simhani1.atlassian.net/browse/KB-549) — 별도 설계 문서 없음. 티켓 DoD 4항이 곧 스펙이다:
1. 적재 API 주소를 환경변수에서 읽되, config.yaml 값은 기본값으로만 사용한다.
2. 같은 이미지로 prod Lambda 를 만들고 prod 적재 주소와 prod 관리자 토큰을 설정한다. prod 큐 트리거는 아직 연결하지 않는다.
3. deploy.sh 가 dev 와 prod 의 두 함수를 모두 갱신한다.
4. prod 이미지 태그 전략을 정해 반영한다.

## Global Constraints

- 리전 `ap-northeast-2`, 계정 `118178010621`, ECR 리포 `kbap/langchain`, 로컬 프로필 `kbap-lambda-deployer`(CI 는 env 자격증명)
- dev 함수명 `kbap-generate-content`(기존, 이름 유지), prod 함수명 `kbap-generate-content-prod`(신규 — 티켓이 이름을 정하지 않아 여기서 확정)
- 환경변수 이름 `KBAP_API_BASE_URL` (기존 `KBAP_API_TOKEN` 과 접두사 통일). 비어 있거나 없으면 `config.yaml` 의 `kbap_api.base_url` 을 쓴다
- 이미지 태그: `<git rev-parse --short=12 HEAD>` (+ 작업트리가 더러우면 `-dirty`) 와 `latest` 를 함께 push, 함수는 SHA 태그로 갱신
- 아키텍처 arm64 · `--provenance=false` 유지 (Lambda 는 단일 매니페스트만 허용)
- `.env` 는 이미지에 넣지 않는다. dev·prod 차이는 Lambda 환경변수뿐이다
- 티켓 배경 그대로: dev·prod JWT 서명 키가 같아 토큰은 환경을 구분하지 못한다 — 환경을 가르는 것은 적재 주소다. prod 함수에 dev 토큰이 들어가도 동작하므로 **콘솔에서 prod 토큰을 넣었는지 눈으로 확인**한다

## 파일 구조

| 파일 | 책임 | 변경 |
|---|---|---|
| `src/kbap/review.py` | 설정 로드·`KbapClient` — 헬퍼 `kbap_base_url(raw)` 추가, `load_config` 가 사용 | 수정 |
| `src/kbap/content.py` | Lambda 경로 `_consume` 이 같은 헬퍼 사용 | 수정 |
| `tests/test_review.py` | 헬퍼·`load_config` 환경변수 우선 테스트 | 수정 |
| `config.yaml` | `base_url` 주석을 "기본값, `KBAP_API_BASE_URL` 로 덮음" 으로 | 수정 |
| `.env.example` | `KBAP_API_BASE_URL` 항목(선택) 추가 | 수정 |
| `deploy.sh` | SHA+latest 태그, 함수 2개 루프 | 수정 |
| `README.md`, `CLAUDE.md` | 배포 설명 갱신 | 수정 |
| `kbap-agenthub/wiki/langchain-lambda-deploy-pitfalls.md`, `INDEX.md` | dev·prod 함수 구성·태그 전략·콘솔 절차 기록 | 수정(다른 repo) |

---

### Task 1: 적재 주소 환경변수 우선 — 헬퍼 + 두 호출 지점

**Files:**
- Modify: `src/kbap/review.py:46-63` (`load_config` 바로 위에 헬퍼 추가, `load_config` 안에서 사용)
- Modify: `src/kbap/content.py:678-689` (`_consume`)
- Modify: `config.yaml:1-3`, `.env.example`
- Test: `tests/test_review.py` (`test_load_config` 근처)

**Interfaces:**
- Consumes: `os.environ`, `yaml.safe_load` 결과 dict
- Produces: `kbap.review.kbap_base_url(raw: dict) -> str` — `KBAP_API_BASE_URL` 이 비어 있지 않으면 그 값, 아니면 `raw["kbap_api"]["base_url"]`. Task 2·문서가 이 이름을 그대로 쓴다.

- [ ] **Step 1: 실패하는 테스트 작성**

`tests/test_review.py` 의 import 목록에 `kbap_base_url` 을 추가하고(알파벳순으로 `decide` 뒤·`load_config` 앞), `test_load_config` 바로 아래에 추가:

```python
def test_kbap_base_url_env_overrides_config(monkeypatch):
    raw = {"kbap_api": {"base_url": "http://from-yaml"}}
    monkeypatch.setenv("KBAP_API_BASE_URL", "http://from-env")
    assert kbap_base_url(raw) == "http://from-env"


@pytest.mark.parametrize("env", [None, ""])
def test_kbap_base_url_falls_back_to_config(monkeypatch, env):
    # Lambda 콘솔에 빈 값으로 등록되는 경우까지 config 폴백이어야 한다.
    raw = {"kbap_api": {"base_url": "http://from-yaml"}}
    if env is None:
        monkeypatch.delenv("KBAP_API_BASE_URL", raising=False)
    else:
        monkeypatch.setenv("KBAP_API_BASE_URL", env)
    assert kbap_base_url(raw) == "http://from-yaml"


def test_load_config_base_url_from_env(tmp_path, monkeypatch):
    cfg_file = tmp_path / "config.yaml"
    cfg_file.write_text(textwrap.dedent("""\
        kbap_api:
          base_url: http://kbap.example.com
        llm:
          model: gemini-2.5-flash
        thresholds:
          description: 70
          avoidance: 80
        concurrency: 3
    """))
    monkeypatch.setenv("KBAP_API_TOKEN", "secret-token")
    monkeypatch.setenv("KBAP_API_BASE_URL", "https://prod.kbap.site")

    assert load_config(str(cfg_file)).kbap_base_url == "https://prod.kbap.site"
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `uv run pytest tests/test_review.py -k "kbap_base_url or base_url_from_env" -v`
Expected: ImportError `cannot import name 'kbap_base_url'` (수집 단계 실패)

- [ ] **Step 3: 헬퍼 구현 + `load_config` 적용**

`src/kbap/review.py` 의 `load_config` 정의 바로 위에:

```python
def kbap_base_url(raw: dict) -> str:
    """적재 API 주소 — 환경변수 KBAP_API_BASE_URL 이 config.yaml 값을 덮는다.

    config.yaml 은 이미지에 구워지므로 dev·prod 가 같은 이미지를 쓰려면 주소는
    Lambda 환경변수로 갈라야 한다(KB-549). 빈 문자열은 미설정으로 본다.
    """
    return os.environ.get("KBAP_API_BASE_URL") or raw["kbap_api"]["base_url"]
```

`load_config` 안의 `kbap_base_url=raw["kbap_api"]["base_url"],` 를 `kbap_base_url=kbap_base_url(raw),` 로 바꾼다.

- [ ] **Step 4: `content._consume` 적용**

`src/kbap/content.py` `_consume` 의 import 와 클라이언트 생성을:

```python
    from kbap.review import KbapClient, kbap_base_url

    with open(os.environ.get("CONFIG_PATH", "config.yaml")) as f:
        raw = yaml.safe_load(f)
    kbap = KbapClient(kbap_base_url(raw), os.environ["KBAP_API_TOKEN"])
```

- [ ] **Step 5: 설정 파일 주석 갱신**

`config.yaml` 상단을:

```yaml
kbap_api:
  # 호스트까지만 입력한다. /api/admin/... 경로는 클라이언트가 붙인다.
  # 이 값은 기본값(dev)이며 Lambda 환경변수 KBAP_API_BASE_URL 이 있으면 그쪽이 이긴다 —
  # 이미지에 구워지는 파일이라 prod 함수는 환경변수로 주소를 가른다(KB-549).
  base_url: https://dev.kbap.site # 인증: KBAP_API_TOKEN 환경변수
```

`.env.example` 의 `KBAP_API_TOKEN=` 아래에 추가:

```
# 적재 API 주소 (선택) — 비우면 config.yaml 의 kbap_api.base_url(dev)
# KBAP_API_BASE_URL=https://dev.kbap.site
```

- [ ] **Step 6: 전체 테스트 통과 확인**

Run: `uv run pytest -q`
Expected: 전부 PASS (신규 4케이스 포함)

- [ ] **Step 7: 커밋**

```bash
git add src/kbap/review.py src/kbap/content.py tests/test_review.py config.yaml .env.example
git commit -m "feat: 적재 API 주소를 KBAP_API_BASE_URL 환경변수로 덮어쓰기 — config.yaml 은 기본값 (KB-549)"
```

---

### Task 2: deploy.sh — SHA 태그 + dev·prod 두 함수 갱신

**Files:**
- Modify: `deploy.sh` (전체 교체)

**Interfaces:**
- Consumes: git 작업트리(`git rev-parse`), Docker, AWS CLI. CI 는 `.github/workflows/deploy.yml` 이 그대로 `./deploy.sh` 를 부른다(수정 없음 — `actions/checkout@v4` 기본 depth 1 에서도 `HEAD` 는 있다).
- Produces: ECR `kbap/langchain:<sha>` 와 `:latest`, 두 함수 코드 갱신. 롤백 명령(README 에 기록):
  `aws lambda update-function-code --function-name <fn> --image-uri 118178010621.dkr.ecr.ap-northeast-2.amazonaws.com/kbap/langchain:<이전 sha> --region ap-northeast-2`

- [ ] **Step 1: deploy.sh 교체**

```bash
#!/usr/bin/env bash
# 반복 배포 전용: ECR 로그인 -> build(arm64) -> push -> dev·prod Lambda 코드 업데이트.
# 1회성 작업(ECR 리포 생성, Lambda 함수 생성, SQS 트리거 연결)은 콘솔에서 한다 —
# 함수 생성 시 아키텍처 arm64 를 명시할 것(기본값 x86_64 면 Runtime.InvalidEntrypoint).
set -euo pipefail

# 로컬: 배포 전용 최소권한 유저 프로필 (default 프로필은 다른 계정이다).
# CI(GitHub Actions): 프로필 없이 env 자격증명(AWS_ACCESS_KEY_ID 등)을 쓴다.
if [ -z "${CI:-}" ]; then
  export AWS_PROFILE="${AWS_PROFILE:-kbap-lambda-deployer}"
fi
REGION=ap-northeast-2
ACCOUNT=118178010621
REPO=kbap/langchain
# dev·prod 는 같은 이미지를 쓰고 Lambda 환경변수(KBAP_API_BASE_URL·KBAP_API_TOKEN)만 다르다.
# prod 큐 트리거는 아직 미연결 — 함수 코드는 main 푸시마다 같이 올라가지만 소비는 안 한다(KB-549).
FUNCTIONS=(kbap-generate-content kbap-generate-content-prod)
REGISTRY="$ACCOUNT.dkr.ecr.$REGION.amazonaws.com"
# 태그 전략: 커밋 SHA(불변)로 함수를 갱신해 "어느 커밋이 떠 있나"를 함수에서 읽고,
# 롤백은 이전 SHA 로 update-function-code 한 번. latest 는 로컬 RIE·수동 생성용 이동 포인터.
TAG="$(git rev-parse --short=12 HEAD)$(git diff --quiet HEAD || echo -dirty)"
IMAGE="$REGISTRY/$REPO:$TAG"
LATEST="$REGISTRY/$REPO:latest"

aws ecr get-login-password --region "$REGION" |
  docker login --username AWS --password-stdin "$REGISTRY"

# provenance 매니페스트가 붙으면 Lambda가 이미지를 거부한다(단일 매니페스트만 허용)
docker build --platform linux/arm64 --provenance=false -t "$IMAGE" -t "$LATEST" .
docker push "$IMAGE"
docker push "$LATEST"

update_function() {
  local fn=$1
  # "함수 없음"과 권한 에러를 구분한다 — AccessDenied 를 삼키면 배포가 조용히 누락된다.
  if err=$(aws lambda get-function --function-name "$fn" --region "$REGION" 2>&1 >/dev/null); then
    aws lambda update-function-code --function-name "$fn" \
      --image-uri "$IMAGE" --region "$REGION" --no-cli-pager >/dev/null
    echo "배포 완료: $fn <- $IMAGE"
  elif grep -q ResourceNotFound <<<"$err"; then
    echo "Lambda 함수($fn)가 아직 없다 — 콘솔에서 $IMAGE 로 생성(arm64)한 뒤 다시 실행하면 코드 업데이트까지 수행된다."
  else
    echo "$err" >&2
    exit 1
  fi
}

for fn in "${FUNCTIONS[@]}"; do
  update_function "$fn"
done
```

- [ ] **Step 2: 문법·태그 계산 확인 (AWS 호출 없이)**

Run:
```bash
bash -n deploy.sh && echo syntax-ok
bash -c 'TAG="$(git rev-parse --short=12 HEAD)$(git diff --quiet HEAD || echo -dirty)"; echo "$TAG"'
```
Expected: `syntax-ok`, 그리고 12자리 SHA(작업트리에 미커밋 변경이 있으면 `-dirty` 접미).

- [ ] **Step 3: 로컬 실배포로 dev 갱신 + prod 부재 메시지 확인**

Run: `./deploy.sh`
Expected(prod 함수 생성 전):
```
배포 완료: kbap-generate-content <- ...kbap/langchain:<sha>
Lambda 함수(kbap-generate-content-prod)가 아직 없다 — 콘솔에서 ... 생성(arm64)한 뒤 ...
```
`get-function` 이 AccessDenied 로 실패하면 `kbap-lambda-deployer` IAM 정책의 리소스에 prod 함수 ARN 이 아직 없는 것 — Task 3 Step 2 를 먼저 한다(정책이 `kbap-generate-content*` 와일드카드가 아니면 ResourceNotFound 대신 AccessDenied 가 나서 스크립트가 exit 1 한다. 이 경우 정책 수정 전까지 배포 자체가 막히므로 Task 3 Step 2 를 이 Step 보다 먼저 수행해도 된다).

- [ ] **Step 4: 커밋**

```bash
git add deploy.sh
git commit -m "feat: deploy.sh — SHA 태그로 dev·prod 두 Lambda 함수 갱신 (KB-549)"
```

---

### Task 3: prod Lambda 생성·IAM (콘솔, 사용자 작업) + 검증

코드 변경 없음. 실행자는 아래를 체크리스트로 사용자에게 넘기고, 완료 신호를 받은 뒤 Step 4 검증만 수행한다. 프로필 3종은 `lambda:GetFunctionConfiguration`·IAM 권한이 없어(agenthub wiki `langchain-lambda-deploy-pitfalls.md` 권한 메모) CLI 로 대신할 수 없다.

**Files:** 없음

**Interfaces:**
- Consumes: Task 2 가 push 한 `kbap/langchain:<sha>`
- Produces: Lambda `kbap-generate-content-prod` (arm64, 트리거 없음)

- [ ] **Step 1: prod 함수 생성 (Lambda 콘솔)**

dev 함수 `kbap-generate-content` 의 설정을 그대로 복제하되 아래만 다르게:

| 항목 | 값 |
|---|---|
| 함수 이름 | `kbap-generate-content-prod` |
| 이미지 URI | `118178010621.dkr.ecr.ap-northeast-2.amazonaws.com/kbap/langchain:<Task 2 가 찍은 sha>` |
| 아키텍처 | **arm64** (기본값 x86_64 그대로 두면 Runtime.InvalidEntrypoint) |
| 메모리/타임아웃 | dev 와 동일 (2026-08-26 실조회 기준 1024MB / 300초) |
| 실행 역할 | dev 와 같은 역할 재사용 가능 (SQS 소비 권한 포함, 트리거는 안 붙임) |
| 환경변수 `KBAP_API_BASE_URL` | `https://prod.kbap.site` |
| 환경변수 `KBAP_API_TOKEN` | **prod 관리자 토큰** (dev 토큰도 서명 키가 같아 동작하므로 값 출처를 눈으로 확인) |
| 환경변수 `OPENAI_API_KEY`, `LANGFUSE_PUBLIC_KEY`, `LANGFUSE_SECRET_KEY`, `LANGFUSE_HOST`, `GRAPH_CONCURRENCY` | dev 와 동일 |
| 트리거 | **없음** — prod 큐 이벤트 소스 매핑은 이 티켓 범위 밖 |

dev 함수에는 `KBAP_API_BASE_URL` 을 넣지 않아도 된다(config.yaml 기본값이 dev). 명시하고 싶으면 `https://dev.kbap.site`.

- [ ] **Step 2: 배포 자격증명에 prod 함수 권한 추가 (IAM 콘솔, `kbap-infra` 관리자 키로)**

`kbap-lambda-deployer` 유저 정책(CI 가 같은 키를 쓰면 하나로 끝)의 `lambda:GetFunction`·`lambda:UpdateFunctionCode` Resource 에
`arn:aws:lambda:ap-northeast-2:118178010621:function:kbap-generate-content-prod` 를 추가한다.
기존 리소스가 `...:function:kbap-generate-content*` 와일드카드면 추가 불필요.

- [ ] **Step 3: deploy.sh 재실행으로 두 함수 갱신 확인**

Run: `./deploy.sh`
Expected:
```
배포 완료: kbap-generate-content <- ...:<sha>
배포 완료: kbap-generate-content-prod <- ...:<sha>
```

- [ ] **Step 4: prod 함수가 prod 주소로 적재하는지 검증 (콘솔 Test 탭, LLM 비용 0)**

prod 함수 콘솔 → Test → 계약 위반 이벤트로 기동만 확인(그래프·POST 없이 batchItemFailures 반환):

```json
{"Records": [{"messageId": "smoke-1", "body": "not-json"}]}
```
Expected: 응답 `{"batchItemFailures": [{"itemIdentifier": "smoke-1"}]}`, 로그에 `계약 위반 메시지 smoke-1`. `ImportModuleError`·`InvalidEntrypoint` 가 없으면 이미지·아키텍처 OK.

주소 자체의 검증은 실제 메시지 1건(`{"scannedName": "...", "foodId": <prod 의 실제 food id>, "outboxId": <해당 outbox id>}`)을 Test 로 넣고 **prod kbap 서버 로그**에 `/api/admin/foods/contents` POST 가 찍히는지로 확인한다 — LLM 비용 1건분이 든다. prod 에 준비된 outbox 행이 없으면 생략하고 환경변수 값 육안 확인으로 대신한다(400 COMMON-002 가 와도 prod 서버에 도달했다는 증거이므로 주소 검증엔 충분).

---

### Task 4: 문서 — README·CLAUDE.md·agenthub 위키

**Files:**
- Modify: `README.md:70-76` (배포 절)
- Modify: `CLAUDE.md:12`
- Modify: `/Users/simjonghan/source_code/swm-kbap/kbap-agenthub/wiki/langchain-lambda-deploy-pitfalls.md` (새 절 추가)
- Modify: `/Users/simjonghan/source_code/swm-kbap/kbap-agenthub/INDEX.md:16`

**Interfaces:**
- Consumes: Task 1 의 `KBAP_API_BASE_URL`, Task 2 의 함수명·태그 전략

- [ ] **Step 1: README 배포 절 교체**

`README.md` 의 `## 배포` 절 첫 문단(`./deploy.sh` … 프로필 … 까지 2줄)을:

```markdown
`./deploy.sh` — arm64 컨테이너 이미지를 빌드해 ECR `kbap/langchain` 에 `:<git sha>`·`:latest` 로
푸시하고 Lambda `kbap-generate-content`(dev)·`kbap-generate-content-prod`(prod) 코드를 SHA 태그로
갱신한다(프로필 `kbap-lambda-deployer`, CI 는 main 푸시 시 자동). 두 함수는 같은 이미지이며
환경변수 `KBAP_API_BASE_URL`·`KBAP_API_TOKEN` 만 다르다 — `config.yaml` 의 `base_url` 은 기본값(dev).
prod 큐 트리거는 아직 붙이지 않았다(KB-549).
롤백: `aws lambda update-function-code --function-name <fn> --image-uri <registry>/kbap/langchain:<이전 sha> --region ap-northeast-2`
```

- [ ] **Step 2: CLAUDE.md 배포 줄 교체**

```markdown
- 배포: `./deploy.sh` — arm64 컨테이너 이미지 → ECR `kbap/langchain:<sha>` → Lambda `kbap-generate-content`(dev)·`kbap-generate-content-prod`(prod, 트리거 미연결) 둘 다 갱신 (프로필 `kbap-lambda-deployer`). 환경 차이는 Lambda 환경변수 `KBAP_API_BASE_URL`·`KBAP_API_TOKEN` 뿐
```

- [ ] **Step 3: agenthub 위키에 절 추가**

`wiki/langchain-lambda-deploy-pitfalls.md` 의 `## 권한 메모` 앞에:

```markdown
## dev·prod 함수 분리와 이미지 태그 (KB-549, 2026-09-12)

- 함수 2개: `kbap-generate-content`(dev) · `kbap-generate-content-prod`(prod). **같은 이미지**, 차이는 Lambda 환경변수 `KBAP_API_BASE_URL`(prod: `https://prod.kbap.site`)·`KBAP_API_TOKEN`(prod 관리자 토큰) 뿐. 환경변수가 없으면 이미지에 구워진 config.yaml 의 dev 주소로 간다 — **prod 함수에 `KBAP_API_BASE_URL` 이 빠지면 prod 결과가 조용히 dev 로 적재된다.**
- dev·prod 는 JWT 서명 키가 같아 dev 토큰이 prod 에서도 통한다. 환경을 가르는 건 토큰이 아니라 적재 주소 — 토큰이 어느 환경 것인지는 콘솔에서 눈으로만 확인 가능.
- 태그 전략: deploy.sh 가 `:<git sha 12자리>`(불변, `-dirty` 접미 가능)와 `:latest` 를 함께 push 하고 함수는 **SHA 태그로** 갱신한다. 함수 이미지 URI 로 어느 커밋이 떠 있는지 읽고, 롤백은 이전 SHA 로 `update-function-code`. main 푸시마다 두 함수가 같이 갱신되지만 **prod 큐 이벤트 소스 매핑은 미연결** — 트리거를 붙이는 순간부터 main 푸시 = prod 배포가 되므로, 그때 prod 만 수동/승인 게이트로 가를지 다시 정한다.
- 배포 자격증명(`kbap-lambda-deployer`)의 `GetFunction`·`UpdateFunctionCode` 리소스에 prod 함수 ARN 이 있어야 한다. 없으면 deploy.sh 가 `ResourceNotFound` 가 아닌 AccessDenied 로 exit 1 한다(의도된 동작 — 조용한 누락 방지).
```

- [ ] **Step 4: INDEX.md 한 줄 갱신**

16행을:

```markdown
- [랭체인 Lambda 배포 함정](wiki/langchain-lambda-deploy-pitfalls.md) — 콘솔 수동 생성 함수의 x86_64 기본값 vs arm64 이미지(Runtime.InvalidEntrypoint), DLQ redrive 체크리스트와 서버 데이터 전제(outbox 행 삭제 시 영구 COMMON-002→purge+재발행), 팀 프로필 조회 권한 부재, KB-549 dev·prod 함수 분리(같은 이미지·`KBAP_API_BASE_URL` 로 주소만 분기·SHA 태그·prod 트리거 미연결)
```

- [ ] **Step 5: 커밋 (repo 별로)**

```bash
# kbap-langchain
git add README.md CLAUDE.md
git commit -m "docs: dev·prod Lambda 분리와 SHA 태그 배포 설명 (KB-549)"

# kbap-agenthub
cd /Users/simjonghan/source_code/swm-kbap/kbap-agenthub
git add wiki/langchain-lambda-deploy-pitfalls.md INDEX.md
git commit -m "wiki: 랭체인 Lambda dev·prod 분리·태그 전략 (KB-549)"
```

---

## 스킵한 것 (YAGNI)

- **prod 전용 배포 게이트(workflow_dispatch·승인)** — prod 트리거가 없어 지금은 코드가 올라가도 소비되지 않는다. 트리거 연결 티켓에서 정한다.
- **Lambda 함수 생성·IAM 을 Terraform/CLI 로** — 함수 2개, 프로필에 권한도 없다. 기존 방침(콘솔 1회성) 유지.
- **Langfuse 환경 태그** — prod 트레이스가 같은 프로젝트로 섞이지만 트리거 미연결이라 실트래픽 0. 필요해지면 `LANGFUSE_ENVIRONMENT` 환경변수 한 줄.
- **ECR 태그 정리 lifecycle** — SHA 태그가 쌓이지만 이미지당 수백 MB·배포 빈도 낮음. 비용이 보이면 lifecycle policy(untagged·N개 초과 만료) 콘솔 1회.

## Self-Review

- **Spec coverage:** DoD 1 → Task 1. DoD 2 → Task 3 Step 1(트리거 미연결 명시). DoD 3 → Task 2. DoD 4 → Task 2(SHA+latest, 함수는 SHA) + Task 4 기록.
- **Placeholder scan:** 없음. Task 3 는 콘솔 작업이라 코드 대신 값 표로 명세.
- **Type consistency:** `kbap_base_url(raw: dict) -> str` — Task 1 정의, Task 1 Step 4·테스트·문서에서 같은 이름. 함수명 `kbap-generate-content-prod` — Task 2·3·4 동일. 환경변수 `KBAP_API_BASE_URL` — 전 Task 동일.
