import httpx

from kbap_review.aggregate import Verdict

# kbap ApiPaths.ADMIN = "/api/v1/admin". base_url 은 호스트까지만 준다.
CONTENT_REVIEWS = "/api/v1/admin/foods/content-reviews"


class KbapClient:
    def __init__(self, base_url: str, token: str):
        self._client = httpx.AsyncClient(
            base_url=base_url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=30.0,
        )

    async def fetch_review_candidates(self, limit: int) -> list[dict]:
        """PENDING_REVIEW 상태 음식 목록. 항목 키는 foodId / contentReviewAttempts."""
        resp = await self._client.get(CONTENT_REVIEWS, params={"limit": limit})
        resp.raise_for_status()
        # BaseResponse<AdminFoodContentReviewTargetsResponse> — {success, payload:{items:[...]}}
        return resp.json()["payload"]["items"]

    async def post_review_result(self, food_id: int, verdict: Verdict) -> dict:
        """검수 결과 반영. 재시도 소진 여부 판단과 컬럼 비우기는 kbap 이 한다.

        반영 후 상태를 돌려준다 — {foodId, contentStatus, contentReviewAttempts,
        contentReviewRejectionReason}. 탈락 건이 재생성으로 갔는지 REVIEW_REJECTED 로
        갔는지는 이 값으로만 알 수 있다.
        """
        body: dict = {"passed": verdict.passed}
        if not verdict.passed:
            body["rejectedFields"] = verdict.rejected_fields
            body["reason"] = verdict.reason
        resp = await self._client.post(f"{CONTENT_REVIEWS}/{food_id}", json=body)
        resp.raise_for_status()
        return resp.json()["payload"]

    async def aclose(self) -> None:
        await self._client.aclose()
