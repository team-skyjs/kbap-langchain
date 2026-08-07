"""벡터 임베딩 기반 음식 추천 예제 — 완전 로컬, API 비용 0원.

흐름: 음식 설명 → FastEmbed(로컬 ONNX 모델) → Qdrant 저장 → 질의 임베딩 → 유사도 검색.
Qdrant 는 저장·검색만 한다 — 임베딩 계산은 qdrant-client 에 내장된 FastEmbed 가
로컬 CPU 에서 수행한다. 첫 실행 때 모델(~220MB)을 내려받아 캐시한다.

    docker compose up -d   # qdrant 먼저
    uv run python examples/recommend.py "얼큰한 국물 요리"
"""

import sys

from dotenv import load_dotenv

load_dotenv()  # langfuse 임포트 전에 환경변수 로드

from langfuse import get_client, observe  # noqa: E402
from qdrant_client import QdrantClient, models  # noqa: E402

QDRANT_URL = "http://localhost:6333"
COLLECTION = "foods"
# 한국어를 다루므로 다국어 모델. MiniLM(220MB)·mpnet(1GB)은 "얼큰한"·"보양식" 같은
# 한국어 뉘앙스를 못 잡아 e5-large(1024차원, ~2.2GB)를 쓴다. FastEmbed 지원 모델 중 최강.
# e5 계열 규약: 문서는 "passage: ", 질의는 "query: " 접두사를 붙여야 제 성능이 난다.
EMBED_MODEL = "intfloat/multilingual-e5-large"

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

client = QdrantClient(url=QDRANT_URL)


@observe(name="index-foods")
def index_foods() -> None:
    # models.Document 를 벡터 자리에 넣으면 클라이언트가 FastEmbed 로 로컬 임베딩 후 upsert 한다.
    # 예제라 매번 지우고 새로 만든다 — 멱등.
    if client.collection_exists(COLLECTION):
        client.delete_collection(COLLECTION)
    client.create_collection(
        collection_name=COLLECTION,
        vectors_config=models.VectorParams(size=1024, distance=models.Distance.COSINE),
    )
    client.upsert(
        collection_name=COLLECTION,
        points=[
            models.PointStruct(
                id=i,
                vector=models.Document(text=f"passage: {desc}", model=EMBED_MODEL),
                payload={"name": name, "description": desc},
            )
            for i, (name, desc) in enumerate(FOODS)
        ],
    )


@observe(name="recommend-food")
def recommend(query: str, k: int = 3):
    # @observe 기본 입력은 함수 인자 전부 — 질의만 남긴다
    get_client().update_current_span(input={"query": query, "k": k})
    hits = client.query_points(
        collection_name=COLLECTION,
        query=models.Document(text=f"query: {query}", model=EMBED_MODEL),
        limit=k,
    ).points
    return [
        {
            "name": h.payload["name"],
            "description": h.payload["description"],
            "score": round(h.score, 4),
        }
        for h in hits
    ]


if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "얼큰한 국물 요리"
    index_foods()
    for i, r in enumerate(recommend(query), 1):
        print(f"{i}. {r['name']} (유사도 {r['score']}) — {r['description']}")
    get_client().flush()  # 짧은 스크립트 — 종료 전 트레이스 전송
