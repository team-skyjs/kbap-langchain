# 검수 엔진 오프라인 A/B 평가 (KB-913)

A = 기존 LLM 검수, B = Jev 검수 + 확정된 LLM 대체 정책. 최종 판정은 둘 다 KB-897 코드 규칙이다.
생성 모델·프롬프트·재생성 상한은 양쪽이 같다.

```bash
# 고정 입력 — 저장된 생성물에 A/B 검수만 적용 (같은 입력을 두 엔진이 본다)
uv run python evals/decision/run.py fixed --dataset D.json --split tune --out evals/decision/runs/<id>
# 전체 파이프라인 — 같은 음식명으로 A/B 그래프 실행. SQS·적재 API 호출 없음, 파일 출력만
uv run python evals/decision/run.py pipeline --dataset D.json --out evals/decision/runs/<id>
# 집계만 다시
uv run python evals/decision/run.py summarize --out evals/decision/runs/<id>
```

- **기본은 모의 응답이다.** 네트워크·유료 호출이 없다. 모의 결과는 도구가 도는지 확인할 뿐이고, 정확도·지연·비용의 증거가 아니다.
- `--live` 일 때만 실제 LLM과 Jev를 부른다. 보내는 필드를 stderr에 먼저 출력한다. 유료 호출 전에는 사용자 허가와 예산을 받는다. 키(`OPENAI_API_KEY`, `TYPESAFE_API_KEY`)는 어떤 산출물에도 쓰지 않는다.
- 산출물은 `manifest.json`·`results.jsonl` 또는 `pipeline.jsonl`·`summary.md`·`disagreements.csv` 다.
  - `manifest.json`: commit, 데이터셋 해시, 모델·설정, Jev 질문 본문과 해시, 정책 버전, 피드백 문구, 프롬프트 출처와 버전
  - `summary.md`: §6.4 지표표(분모 포함, real/synthetic 분리)
  - `disagreements.csv`: A/B 불일치 목록(입력·정답·각 판정·Jev 확률)
- `runs/` 는 gitignore 대상이다. 결과 원본은 spec 레포 `reports/jev-decision-*` 로 옮긴다.

## dev Lambda 에서 실행 (평가 모드)

키를 로컬에 두지 않고 dev 함수(`kbap-generate-content`) 안에서 A/B를 돌린다. 로직은 `kbap.evaluate` 를 로컬 CLI와 같이 쓴다.

- 이벤트는 Records 없이 최상위 `eval` 키다. SQS 메시지와 섞이지 않는다. 적재와 SQS 응답이 없고, 결과 JSON을 반환한다.
- **`KBAP_EVAL_ENABLED=true` 인 함수에서만 돈다.** prod 함수에는 이 환경변수가 없어서 평가 이벤트는 무시되고 `{"eval": "disabled"}` 가 남는다.
- `engine`: `A`(기존 LLM) / `B`(Jev+대체) / `both`. 함수의 `REVIEW_ENGINE` 과 무관하다. `A` 만 돌리면 Jev 키를 읽지 않는다.
- 응답이 5MB를 넘으면 행을 CloudWatch 로그에 `EVAL_ROW ` 접두 JSON 줄로 내보내고 응답의 `rows` 는 `null` 이 된다. 조각을 작게 보내 피한다.
- 한 번 invoke가 함수 타임아웃 안에 끝나야 한다. fixed는 20~25건, pipeline은 5~10건씩 나눠 보내는 것부터 시작한다.

```bash
# 1) 조각 만들기 — labels 는 collect 가 원본 데이터셋에서 다시 읽는다
jq '{dataset_version, items: .items[0:25]}' D.json > chunk01.json
# 2) 평가 이벤트로 감싸 invoke (AWS CLI v2)
jq '{eval: {mode: "fixed", engine: "both", concurrency: 4, live: true, dataset: .}}' chunk01.json > ev01.json
aws lambda invoke --function-name kbap-generate-content --region ap-northeast-2 \
  --cli-binary-format raw-in-base64-out --cli-read-timeout 900 \
  --payload file://ev01.json resp01.json
# 3) 응답들을 로컬 실행과 같은 파일 구성으로 합치기
uv run python evals/decision/run.py collect --dataset D.json --out evals/decision/runs/<id> resp01.json resp02.json
```

`mode: "pipeline"` 은 `scanned_name` 이 있는 항목으로 그래프 전체를 돈다. collect 는 fixed·pipeline 응답을 섞지 않는다.

## 데이터셋 (`Dataset` 스키마, `run.py`)

선정 기준·변형·라벨 정의는 [DATASET.md](DATASET.md), 기계용 스키마는 `dataset.schema.json`(`run.py schema`).

- `split`: `tune`(질문·임계값 조정) / `final`(고정 평가). **같은 `group`(같은 음식의 정상·오류 변형)이 두 split에 나뉘면 로드가 실패한다.**
- `source`: `real` / `synthetic` — 지표를 따로 집계한다. 섞은 평균을 운영 정확도로 읽지 않는다.
- `name_review` / `description` / `ingredients`: fixed 의 검수 입력(저장된 생성물). `scanned_name`: pipeline 입력.
- `labels`: 사람이 검토한 정답. 모르면 `null` 로 두고, 해당 지표의 분모에서 빠진다. LLM이나 Jev의 답을 정답으로 쓰지 않는다.
- `sample_dataset.json` 은 도구 확인용 합성 5건이다. 평가용이 아니다.

최종 결과를 보고 정책을 바꿨다면, 그 final 은 조정에 쓴 셈이다. 새로 떼어 둔 데이터로 다시 확인한다.
