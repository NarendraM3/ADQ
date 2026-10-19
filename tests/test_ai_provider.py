import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException

from app import main
from app.config import Settings, settings


def test_negative_age_profile_adds_range_even_if_provider_omits_it():
    profile = [{"dataset_id": 7, "columns": {"age": {
        "inferred_type": "integer", "distinct_sample": ["30", "-5"]}}}]

    rules = main._negative_age_range_suggestions(profile, [])

    assert rules == [{"dataset_id": 7, "column_name": "age", "rule_type": "RANGE",
        "parameters": {"min": 0}, "severity": "high",
        "rationale": "Observed negative age values; age must be non-negative."}]


def test_groq_endpoint_override_is_respected():
    configured = Settings(ai_provider="groq", ai_groq_base_url="https://groq.example/v1")
    assert configured.resolved_ai_base_url == "https://groq.example/v1"


def test_groq_request_generates_and_parses_rules(monkeypatch):
    monkeypatch.setattr(settings, "ai_provider", "groq")
    monkeypatch.setattr(settings, "ai_api_key", None)
    monkeypatch.setattr(settings, "groq_api_key", "test-key")
    monkeypatch.setattr(settings, "ai_model", None)
    monkeypatch.setattr(settings, "groq_model", "openai/gpt-oss-20b")
    monkeypatch.setattr(settings, "ai_base_url", None)
    monkeypatch.setattr(settings, "ai_groq_base_url", None)

    dataset = SimpleNamespace(
        id=1, name="customers", source_type="snowflake",
        schema_json={"columns": {"email": {"inferred_type": "string"}}},
    )
    result_body = {"suggestions": [{
        "dataset_id": 1, "column_name": "email", "rule_type": "EMAIL_FORMAT",
        "parameters": {}, "severity": "high", "rationale": "Email format check",
    }]}

    class FakeResponse:
        def raise_for_status(self):
            return None

        def json(self):
            return {"choices": [{"message": {"content": json.dumps(result_body)}}]}

    captured = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured["timeout"] = kwargs["timeout"]

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, url, *, headers, json):
            captured.update(url=url, headers=headers, payload=json)
            return FakeResponse()

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    suggestions = asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert captured["url"] == "https://api.groq.com/openai/v1/chat/completions"
    assert captured["headers"] == {"Authorization": "Bearer test-key"}
    assert captured["payload"]["model"] == "openai/gpt-oss-20b"
    assert captured["payload"]["response_format"] == {"type": "json_object"}
    assert "contains a negative sample, include a RANGE rule with min 0" in captured["payload"]["messages"][0]["content"]
    assert suggestions[0]["rule_type"] == "EMAIL_FORMAT"
    assert suggestions[0]["severity"] == "high"


def test_groq_authentication_failure_is_clear_and_does_not_leak_key(monkeypatch):
    monkeypatch.setattr(settings, "ai_provider", "groq")
    monkeypatch.setattr(settings, "groq_api_key", "never-return-this")
    monkeypatch.setattr(settings, "ai_api_key", None)
    monkeypatch.setattr(settings, "groq_model", "openai/gpt-oss-20b")
    monkeypatch.setattr(settings, "ai_model", None)
    monkeypatch.setattr(settings, "ai_base_url", None)
    monkeypatch.setattr(settings, "ai_groq_base_url", None)

    class FakeResponse:
        status_code = 401
        headers = {"x-groq-id": "test-request-id"}

        def json(self):
            return {"error": {"message": "Invalid key: never-return-this"}}

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return None

        async def post(self, *args, **kwargs):
            raise httpx.HTTPStatusError("unauthorized", request=httpx.Request("POST", "https://api.groq.com"), response=FakeResponse())

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    dataset = SimpleNamespace(id=1, name="customers", source_type="snowflake", schema_json={"columns": {"email": {}}})
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert error.value.status_code == 502
    assert "invalid API key" in error.value.detail
    assert "never-return-this" not in error.value.detail


@pytest.mark.parametrize(("status", "expected"), [
    (400, "invalid request or unsupported parameter"),
    (401, "invalid API key"),
    (403, "permission or model access issue"),
    (404, "endpoint or model unavailable"),
    (429, "rate limit or quota issue"),
    (503, "provider-side failure"),
])
def test_provider_status_errors_are_specific_and_scrubbed(monkeypatch, status, expected):
    monkeypatch.setattr(settings, "ai_provider", "groq")
    monkeypatch.setattr(settings, "groq_api_key", "secret-test-key")
    monkeypatch.setattr(settings, "groq_model", "openai/gpt-oss-20b")
    monkeypatch.setattr(settings, "ai_base_url", None)
    monkeypatch.setattr(settings, "ai_groq_base_url", None)

    class FakeResponse:
        status_code = status
        headers = {}
        text = '{"error":{"message":"bad request secret-test-key"}}'

        def json(self):
            return {"error": {"message": "bad request secret-test-key"}}

    class FakeClient:
        def __init__(self, **kwargs): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *args): return None
        async def post(self, *args, **kwargs):
            raise httpx.HTTPStatusError("provider error", request=httpx.Request("POST", "https://api.groq.com"), response=FakeResponse())

    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)
    dataset = SimpleNamespace(id=1, name="customers", source_type="snowflake", schema_json={"columns": {"email": {}}})
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.generate_rule_suggestions([dataset], None))
    assert expected in error.value.detail
    assert "secret-test-key" not in error.value.detail
