"""벡터 임베딩 기반 음식 추천 예제.

흐름: 음식 설명 → OpenAI 임베딩 → Qdrant 저장 → 질의 임베딩 → 유사도 검색.
Qdrant 는 저장·검색만 한다 — 임베딩 계산은 항상 이쪽(OpenAI API)에서 하고 결과 벡터를 넣는다.

    docker compose up -d   # qdrant 먼저
    uv run python examples/recommend.py "얼큰한 국물 요리"
"""

import sys

from dotenv import load_dotenv

load_dotenv()  # langfuse/openai 임포트 전에 환경변수 로드

from langchain_openai import OpenAIEmbeddings  # noqa: E402
from langchain_qdrant import QdrantVectorStore  # noqa: E402
from langfuse import get_client, observe  # noqa: E402

QDRANT_URL = "http://localhost:6333"
COLLECTION = "foods"

# ponytail: 예제용 인라인 데이터 — 실전에서는 kbap API 의 음식 설명을 넣는다
FOODS = [
    ("김치찌개", "김치와 돼지고기를 넣고 얼큰하게 끓인 매운 국물 요리"),
    ("된장찌개", "된장을 풀어 두부와 야채를 넣고 구수하게 끓인 국물 요리"),
    ("비빔밥", "밥 위에 나물과 고추장을 올려 비벼 먹는 한 그릇 요리"),
    ("불고기", "간장 양념에 재운 소고기를 달콤짭짤하게 구운 고기 요리"),
    ("삼계탕", "닭 속에 찹쌀과 인삼을 넣고 푹 고아낸 보양 국물 요리"),
    ("냉면", "차가운 육수에 메밀면을 말아 먹는 시원한 여름 면 요리"),
    ("떡볶이", "떡을 고추장 소스에 볶은 매콤달콤한 분식"),
    ("잡채", "당면과 야채, 고기를 간장에 볶아 무친 잔치 요리"),
    ("갈비탕", "소갈비를 오래 끓여 맑고 깊은 맛을 낸 국물 요리"),
    ("파전", "파와 해물을 반죽에 넣어 기름에 부친 전"),
    ("순두부찌개", "부드러운 순두부를 고춧가루 양념에 끓인 매운 찌개"),
    ("김밥", "밥과 재료를 김에 말아 한 입에 먹는 간편식"),
]

embeddings = OpenAIEmbeddings(model="text-embedding-3-small")


@observe(name="index-foods")
def index_foods() -> QdrantVectorStore:
    # from_texts 가 임베딩 호출 + 컬렉션 생성 + upsert 를 한 번에 한다.
    # force_recreate 로 재실행해도 멱등 — 예제라 매번 새로 만든다.
    return QdrantVectorStore.from_texts(
        texts=[desc for _, desc in FOODS],
        embedding=embeddings,
        metadatas=[{"name": name} for name, _ in FOODS],
        url=QDRANT_URL,
        collection_name=COLLECTION,
        force_recreate=True,
    )


@observe(name="recommend-food")
def recommend(store: QdrantVectorStore, query: str, k: int = 3):
    # @observe 기본 입력은 함수 인자 전부(store 객체 포함) — 질의만 남긴다
    get_client().update_current_span(input={"query": query, "k": k})
    results = store.similarity_search_with_score(query, k=k)
    return [
        {"name": doc.metadata["name"], "description": doc.page_content, "score": round(score, 4)}
        for doc, score in results
    ]


if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "얼큰한 국물 요리"
    store = index_foods()
    for i, r in enumerate(recommend(store, query), 1):
        print(f"{i}. {r['name']} (유사도 {r['score']}) — {r['description']}")
    get_client().flush()  # 짧은 스크립트 — 종료 전 트레이스 전송
