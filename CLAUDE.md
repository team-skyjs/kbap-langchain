# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 프로젝트

음식 콘텐츠 생성·검수 LangGraph 파이프라인. kbap(Spring)이 SQS로 발행한 음식 이름을
Lambda가 소비해 콘텐츠(번역·설명·기피성분)를 완성하고, 결과를 kbap API로 적재한다.

- 본체: `src/kbap/content.py` — 콘텐츠 그래프 + SQS Lambda 핸들러(`kbap.main.handler`)
- 실행: `uv run kbap review|namefix` (이관 과도기 보조 배치), 테스트: `uv run pytest`
- 배포: `./deploy.sh` — arm64 컨테이너 이미지 → ECR `kbap/langchain:<sha>` → Lambda `kbap-generate-content`(dev)·`kbap-generate-content-prod`(prod, 트리거 미연결) 둘 다 갱신 (프로필 `kbap-lambda-deployer`). 환경 차이는 Lambda 환경변수 `KBAP_API_BASE_URL`·`KBAP_API_TOKEN` 뿐

## kbap-agenthub — 공유 지식 위키 (필수 참조)

`/Users/simjonghan/source_code/swm-kbap/kbap-agenthub` 는 kbap(Spring)·kbap-langchain
두 repo 가 공유하는 지식 위키다. **작업 시작 전 `INDEX.md` 를 훑고 관련 wiki 문서를 읽는다.**

- 두 repo 에 걸친 계약·도메인 지식은 여기에 산다. 특히 kbap API 호출을 만들거나 바꿀 때는
  `wiki/langchain-food-ingest-contract.md` (적재 계약)가 단일 진실 원천이다.
- 계약·도메인 지식이 바뀌면 해당 wiki 문서를 갱신하고 INDEX.md 한 줄도 맞춘다.
  기록 규칙은 agenthub 의 CLAUDE.md 를 따른다 (daily 는 훅이 기록, 과거 파일 수정 금지).
- 코드를 읽으면 알 수 있는 것, 이 repo 전용 구현 세부는 넣지 않는다 (여기 `docs/`).
