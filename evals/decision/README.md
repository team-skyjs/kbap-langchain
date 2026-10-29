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

## 데이터셋 (`Dataset` 스키마, `run.py`)

- `split`: `tune`(질문·임계값 조정) / `final`(고정 평가). **같은 `group`(같은 음식의 정상·오류 변형)이 두 split에 나뉘면 로드가 실패한다.**
- `source`: `real` / `synthetic` — 지표를 따로 집계한다. 섞은 평균을 운영 정확도로 읽지 않는다.
- `name_review` / `description` / `ingredients`: fixed 의 검수 입력(저장된 생성물). `scanned_name`: pipeline 입력.
- `labels`: 사람이 검토한 정답. 모르면 `null` 로 두고, 해당 지표의 분모에서 빠진다. LLM이나 Jev의 답을 정답으로 쓰지 않는다.
- `sample_dataset.json` 은 도구 확인용 합성 5건이다. 평가용이 아니다.

최종 결과를 보고 정책을 바꿨다면, 그 final 은 조정에 쓴 셈이다. 새로 떼어 둔 데이터로 다시 확인한다.
