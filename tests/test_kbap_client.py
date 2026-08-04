import json

import httpx
import pytest

from kbap_review.aggregate import Verdict
from kbap_review.kbap_client import KbapClient


def make_client(handler) -> KbapClient:
    client = KbapClient(base_url="http://kbap.test", token="tok")
    client._client = httpx.AsyncClient(
        base_url="http://kbap.test",
        headers={"Authorization": "Bearer tok"},
        transport=httpx.MockTransport(handler),
    )
    return client


async def test_fetch_review_candidates():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/admin/foods/review-candidates"
        assert request.url.params["limit"] == "50"
        assert request.headers["Authorization"] == "Bearer tok"
        return httpx.Response(200, json=[{"id": 1, "koreanName": "김치찌개"}])

    client = make_client(handler)
    foods = await client.fetch_review_candidates(limit=50)
    assert foods == [{"id": 1, "koreanName": "김치찌개"}]


async def test_post_pass_result():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["path"] = request.url.path
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    client = make_client(handler)
    v = Verdict(verdict="PASS", failed_fields=[], scores={"description": 90})
    await client.post_review_result(1, v)

    assert captured["path"] == "/admin/foods/1/review-result"
    assert captured["body"] == {"verdict": "PASS", "scores": {"description": 90}}


async def test_post_retry_result_includes_failed_fields():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    client = make_client(handler)
    v = Verdict(verdict="RETRY", failed_fields=["description"], scores={"description": 50})
    await client.post_review_result(1, v)

    assert captured["body"]["failedFields"] == ["description"]
    assert "reviewNote" not in captured["body"]


async def test_post_reject_result_includes_review_note():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content)
        return httpx.Response(200)

    client = make_client(handler)
    v = Verdict(
        verdict="REJECT",
        failed_fields=["avoidance"],
        scores={"avoidance": 40},
        review_note="- 기피성분(40점): 돼지고기 누락",
    )
    await client.post_review_result(1, v)

    assert captured["body"]["reviewNote"] == "- 기피성분(40점): 돼지고기 누락"
    assert captured["body"]["failedFields"] == ["avoidance"]


async def test_post_raises_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409)

    client = make_client(handler)
    v = Verdict(verdict="PASS", failed_fields=[], scores={})
    with pytest.raises(httpx.HTTPStatusError):
        await client.post_review_result(1, v)
