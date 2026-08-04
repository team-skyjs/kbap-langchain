import json

import httpx
import pytest

from kbap_review.aggregate import Verdict
from kbap_review.kbap_client import KbapClient

PATH = "/api/v1/admin/foods/content-reviews"


def make_client(handler) -> KbapClient:
    client = KbapClient(base_url="http://kbap.test", token="tok")
    client._client = httpx.AsyncClient(
        base_url="http://kbap.test",
        headers={"Authorization": "Bearer tok"},
        transport=httpx.MockTransport(handler),
    )
    return client


async def test_fetch_unwraps_base_response_payload():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == PATH
        assert request.url.params["limit"] == "50"
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(
            200,
            json={
                "success": True,
                "payload": {"items": [{"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0}]},
            },
        )

    client = make_client(handler)
    foods = await client.fetch_review_candidates(limit=50)
    assert foods == [{"foodId": 1, "koreanName": "김치찌개", "contentReviewAttempts": 0}]


async def test_post_passed_result():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "payload": {}})

    client = make_client(handler)
    await client.post_review_result(7, Verdict(passed=True, rejected_fields=[], scores={"description": 90}))

    assert captured["path"] == f"{PATH}/7"
    # 통과 결과에는 rejectedFields·reason 을 보내지 않는다.
    assert captured["body"] == {"passed": True}


async def test_post_rejected_result():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200, json={"success": True, "payload": {}})

    client = make_client(handler)
    await client.post_review_result(
        7,
        Verdict(
            passed=False,
            rejected_fields=["AVOIDANCE_SUBSTANCES", "SPICINESS"],
            scores={"avoidance": 40},
            reason="- 기피성분·매운맛(40점): 돼지고기 누락",
        ),
    )

    assert captured["body"] == {
        "passed": False,
        "rejectedFields": ["AVOIDANCE_SUBSTANCES", "SPICINESS"],
        "reason": "- 기피성분·매운맛(40점): 돼지고기 누락",
    }


async def test_post_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409)

    client = make_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await client.post_review_result(7, Verdict(passed=True, rejected_fields=[], scores={}))


async def test_fetch_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = make_client(handler)
    with pytest.raises(httpx.HTTPStatusError):
        await client.fetch_review_candidates(limit=50)
