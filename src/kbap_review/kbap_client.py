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
