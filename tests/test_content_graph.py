from collections import defaultdict

from kbap_content.graph import ContentFns, JudgeVerdict, build_content_graph
from kbap_review.config import Thresholds
from kbap_review.scoring import FieldScore

TH = Thresholds(description=70, translations=70, avoidance=70)


def build_fns(rec, nt_seq=(90,), desc_seq=(90,), avoid_seq=(90,)):
    """호출 기록(rec)과 검수 점수 시퀀스로 페이크 노드 함수 세트를 만든다."""
    nt_scores, desc_scores, avoid_scores = list(nt_seq), list(desc_seq), list(avoid_seq)

    async def clean_name(name):
        rec["clean"].append(name)
        return {"name": "김치찌개", "reason": "오타 교정"}

    async def gen_name_tr(name, feedback):
        rec["gen_nt"].append((name, feedback))
        return {"en": "Kimchi Stew"}

    async def gen_desc(name, feedback):
        rec["gen_desc"].append((name, feedback))
        return "돼지고기와 김치를 끓인 찌개."

    async def gen_desc_tr(name, desc):
        rec["gen_desc_tr"].append((name, desc))
        return {"en": "A stew of pork and kimchi."}

    async def gen_avoid(name, feedback):
        rec["gen_avoid"].append((name, feedback))
        return {"substances": [{"code": "PORK", "inclusionPercent": 95}], "spiciness": 3}

    async def rev_name_tr(name, translations):
        score = nt_scores.pop(0)
        return FieldScore(score=score, reason=f"이름번역 {score}")

    async def rev_desc(name, desc, desc_tr):
        score = desc_scores.pop(0)
        return FieldScore(score=score, reason=f"설명 {score}")

    async def rev_avoid(name, avoidance):
        score = avoid_scores.pop(0)
        return FieldScore(score=score, reason=f"기피 {score}")

    async def judge(state):
        rec["judge"].append(state)
        scores = [state["nt_score"], state["desc_score"], state["avoid_score"]]
        passed = all(s.score >= 70 for s in scores)
        return JudgeVerdict(reason="종합", passed=passed, rejected_fields=[])

    return ContentFns(
        clean_name=clean_name,
        gen_name_tr=gen_name_tr,
        gen_desc=gen_desc,
        gen_desc_tr=gen_desc_tr,
        gen_avoid=gen_avoid,
        rev_name_tr=rev_name_tr,
        rev_desc=rev_desc,
        rev_avoid=rev_avoid,
        judge=judge,
    )


async def run(rec, **seqs):
    graph = build_content_graph(build_fns(rec, **seqs), TH)
    return await graph.ainvoke({"food_name": "김치찌게 8,000원"})


async def test_happy_path_populates_all_content():
    rec = defaultdict(list)
    state = await run(rec)

    assert state["cleaned_name"] == "김치찌개"
    assert state["name_translations"] == {"en": "Kimchi Stew"}
    assert state["description"].startswith("돼지고기")
    assert state["description_translations"]["en"].startswith("A stew")
    assert state["avoidance"]["spiciness"] == 3
    assert state["verdict"].passed is True
    # 생성은 각 1회, 종합판정은 join 후 정확히 1회
    assert len(rec["gen_desc"]) == 1
    assert len(rec["judge"]) == 1


async def test_generators_receive_cleaned_name():
    rec = defaultdict(list)
    await run(rec)
    # 원본("김치찌게 8,000원")이 아니라 정제된 이름이 생성 입력이어야 한다.
    assert rec["gen_nt"][0][0] == "김치찌개"
    assert rec["gen_desc"][0][0] == "김치찌개"
    assert rec["gen_avoid"][0][0] == "김치찌개"


async def test_review_fail_retries_generation_once_with_feedback():
    rec = defaultdict(list)
    state = await run(rec, desc_seq=(30, 90))

    assert len(rec["gen_desc"]) == 2
    # 재시도 프롬프트에 탈락 사유가 실려야 한다.
    assert rec["gen_desc"][1][1] == "설명 30"
    # 설명이 재생성되면 설명 번역도 다시 만든다(순차 갈래).
    assert len(rec["gen_desc_tr"]) == 2
    assert state["desc_attempts"] == 2
    assert state["verdict"].passed is True
    assert len(rec["judge"]) == 1


async def test_retry_exhausted_flows_failure_to_judge():
    rec = defaultdict(list)
    state = await run(rec, desc_seq=(30, 40))

    # 재시도는 1회 한 — 생성 2회를 넘지 않는다.
    assert len(rec["gen_desc"]) == 2
    assert len(rec["judge"]) == 1
    # 종합판정은 실패 점수와 사유를 그대로 받는다.
    judged = rec["judge"][0]
    assert judged["desc_score"].score == 40
    assert judged["desc_feedback"] == "설명 40"
    assert state["verdict"].passed is False


async def test_independent_branches_do_not_retry_each_other():
    rec = defaultdict(list)
    await run(rec, avoid_seq=(30, 90))

    assert len(rec["gen_avoid"]) == 2
    assert len(rec["gen_desc"]) == 1
    assert len(rec["gen_nt"]) == 1
