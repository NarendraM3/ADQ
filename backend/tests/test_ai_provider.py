import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from fastapi import HTTPException
from openai import APIConnectionError, APIStatusError, APITimeoutError

from app import main
from app.config import Settings, settings


def _openai_status_error(status, message):
    body = {"error": {"message": message}}
    response = httpx.Response(
        status,
        request=httpx.Request("POST", "https://api.openai.com/v1/chat/completions"),
        json=body,
        headers={"x-request-id": "test-request-id"},
    )
    return APIStatusError(message, response=response, body=body)


def test_openai_model_defaults_and_key_are_resolved():
    configured = Settings(openai_api_key="test-key")

    assert configured.resolved_ai_api_key == "test-key"
    assert configured.resolved_ai_model == "gpt-5.5"
    assert configured.resolved_ai_base_url == "https://api.openai.com/v1"


def test_openai_request_generates_and_parses_rules(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    dataset = SimpleNamespace(
        id=1, name="customers", source_type="snowflake",
        schema_json={"columns": {"email": {"inferred_type": "string"}}},
    )
    result_body = {"suggestions": [{
        "dataset_id": 1, "column_name": "email", "rule_type": "EMAIL_FORMAT",
        "parameters": {}, "severity": "high", "rationale": "Email format check",
    }]}
    captured = {}

    class FakeMessage:
        content = json.dumps(result_body)

    class FakeChoice:
        message = FakeMessage()

    class FakeCompletions:
        async def create(self, **kwargs):
            captured["payload"] = kwargs
            return SimpleNamespace(choices=[FakeChoice()])

    class FakeOpenAI:
        def __init__(self, **kwargs):
            captured["client"] = kwargs
            self.chat = SimpleNamespace(completions=FakeCompletions())

    monkeypatch.setattr(main, "AsyncOpenAI", FakeOpenAI)
    suggestions = asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert captured["client"]["api_key"] == "test-key"
    assert captured["client"]["max_retries"] == 2
    assert captured["payload"]["model"] == "gpt-5.5"
    assert "temperature" not in captured["payload"]
    assert captured["payload"]["response_format"]["type"] == "json_schema"
    assert captured["payload"]["response_format"]["json_schema"]["strict"] is True
    assert "Do not infer business ranges from column names" in captured["payload"]["messages"][1]["content"]
    assert suggestions[0]["rule_type"] == "EMAIL_FORMAT"
    assert suggestions[0]["severity"] == "high"


def test_openai_rule_generation_accepts_markdown_wrapped_json(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    dataset = SimpleNamespace(
        id=3, name="inventory", source_type="local",
        schema_json={"columns": {"sku": {"inferred_type": "string"}}},
    )

    async def fake_chat(*args, **kwargs):
        return """```json
{"suggestions":[{"dataset_id":3,"column_name":"sku","rule_type":"UNIQUE","parameters":{},"severity":"medium","rationale":"Duplicate SKU values would identify repeated inventory items."}]}
```"""

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    suggestions = asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert suggestions == [{
        "dataset_id": 3,
        "column_name": "sku",
        "rule_type": "UNIQUE",
        "parameters": {},
        "severity": "medium",
        "rationale": "Duplicate SKU values would identify repeated inventory items.",
    }]


def test_openai_empty_suggestions_do_not_create_static_fallbacks(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    dataset = SimpleNamespace(
        id=7, name="arbitrary", source_type="local",
        schema_json={"columns": {"foo": {"inferred_type": "integer", "nullable": True}}},
    )

    async def fake_chat(*args, **kwargs):
        return json.dumps({"suggestions": []})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    assert asyncio.run(main.generate_rule_suggestions([dataset], None)) == []


def test_openai_rule_generation_retries_invalid_regex_with_feedback(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    dataset = SimpleNamespace(
        id=8, name="products", source_type="local",
        schema_json={"columns": {"sku": {"inferred_type": "string", "distinct_sample": ["AB-100", "CD-200"]}}},
    )
    calls = []

    async def fake_chat(messages, *, timeout, response_format=None):
        calls.append(messages)
        if len(calls) == 1:
            return json.dumps({"suggestions": [{
                "dataset_id": 8, "column_name": "sku", "rule_type": "REGEX",
                "parameters": {"pattern": None}, "severity": "medium",
                "rationale": "SKU values follow a visible pattern.",
            }]})
        assert "REGEX requires a pattern" in messages[-1]["content"]
        return json.dumps({"suggestions": [{
            "dataset_id": 8, "column_name": "sku", "rule_type": "REGEX",
            "parameters": {"pattern": "^[A-Z]{2}-\\d{3}$"}, "severity": "medium",
            "rationale": "SKU samples use two uppercase letters, a dash, and three digits.",
        }]})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    suggestions = asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert len(calls) == 2
    assert suggestions == [{
        "dataset_id": 8,
        "column_name": "sku",
        "rule_type": "REGEX",
        "parameters": {"pattern": "^[A-Z]{2}-\\d{3}$"},
        "severity": "medium",
        "rationale": "SKU samples use two uppercase letters, a dash, and three digits.",
    }]


def test_openai_rule_generation_reports_clear_error_after_retry_failures(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    dataset = SimpleNamespace(id=9, name="products", source_type="local", schema_json={"columns": {"sku": {}}})
    calls = []

    async def fake_chat(messages, *, timeout, response_format=None):
        calls.append(messages)
        return json.dumps({"suggestions": [{
            "dataset_id": 9, "column_name": "sku", "rule_type": "REGEX",
            "parameters": {"pattern": None}, "severity": "medium",
            "rationale": "Invalid missing regex.",
        }]})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert len(calls) == main.RULE_GENERATION_MAX_ATTEMPTS
    assert error.value.status_code == 502
    assert "REGEX requires a pattern" in error.value.detail
    assert "failed after 3 attempts" in error.value.detail
    assert "no rules were created" in error.value.detail


def test_openai_rule_generation_reports_malformed_json_detail(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    dataset = SimpleNamespace(id=1, name="customers", source_type="local", schema_json={"columns": {"email": {}}})

    async def fake_chat(*args, **kwargs):
        return '{"suggestions": ['

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert error.value.status_code == 502
    assert "malformed or truncated JSON" in error.value.detail
    assert "no rules were created" in error.value.detail


def test_rule_generation_uses_actual_dataset_profiles_for_different_schemas(monkeypatch):
    from pathlib import Path
    from shutil import rmtree
    from uuid import uuid4

    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")
    root = Path(__file__).resolve().parents[1] / ".ai-provider-runtime" / uuid4().hex
    root.mkdir(parents=True)
    try:
        monkeypatch.setattr(settings, "local_data_dir", str(root))
        orders_path = root / "orders.csv"
        readings_path = root / "readings.csv"
        orders_path.write_text("order_code,status_flag\nA1,new\nA1,done\nB2,new\n", encoding="utf-8")
        readings_path.write_text("sensor_ref,temperature_c\nS1,21.5\nS2,error\nS3,22.0\n", encoding="utf-8")
        orders = SimpleNamespace(
            id=11, name="orders", source_type="local", filename="orders.csv", path=str(orders_path),
            schema_json={"columns": {"order_code": {}, "status_flag": {}}},
        )
        readings = SimpleNamespace(
            id=12, name="readings", source_type="local", filename="readings.csv", path=str(readings_path),
            schema_json={"columns": {"sensor_ref": {}, "temperature_c": {}}},
        )

        async def fake_chat(messages, *, timeout, response_format=None):
            profile = json.loads(messages[1]["content"])["profile"]
            assert profile[0]["columns"]["order_code"]["duplicate_count"] == 1
            assert profile[1]["columns"]["temperature_c"]["non_numeric_count"] == 1
            return json.dumps({"suggestions": [
                {"dataset_id": 11, "column_name": "order_code", "rule_type": "UNIQUE",
                 "parameters": {}, "severity": "high", "rationale": "Duplicate order_code values appear in the uploaded data."},
                {"dataset_id": 12, "column_name": "temperature_c", "rule_type": "DATA_TYPE",
                 "parameters": {"type": "number"}, "severity": "medium", "rationale": "The reading column contains a non-numeric sampled value."},
            ]})

        monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
        suggestions = asyncio.run(main.generate_rule_suggestions([orders, readings], None))

        assert {(item["dataset_id"], item["column_name"], item["rule_type"]) for item in suggestions} == {
            (11, "order_code", "UNIQUE"),
            (12, "temperature_c", "DATA_TYPE"),
        }
    finally:
        rmtree(root.parent, ignore_errors=True)


def test_openai_authentication_failure_is_clear_and_does_not_leak_key(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "never-return-this")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    async def fail(*args, **kwargs):
        raise _openai_status_error(401, "Invalid key: never-return-this")

    monkeypatch.setattr(main, "_openai_json_chat", fail)
    dataset = SimpleNamespace(id=1, name="customers", source_type="snowflake", schema_json={"columns": {"email": {}}})
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.generate_rule_suggestions([dataset], None))

    assert error.value.status_code == 502
    assert "invalid API key" in error.value.detail
    assert "never-return-this" not in error.value.detail


def test_openai_correction_selects_only_evidence_backed_candidate(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "test-model")

    async def fake_chat(messages, *, timeout):
        assert "allowed_candidates" in messages[0]["content"]
        return json.dumps({"suggested_value":"eng@example.com", "explanation":"Exact employee match.", "confidence":"high", "reason":"Matching source record.", "safe_to_apply":True})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    result=asyncio.run(main._openai_correction_suggestion(record={"name":"A"}, column="email", error="missing",
        column_metadata={"inferred_type":"string"}, approved_rules=[],
        candidates=[{"value":"eng@example.com","source":"matching_source_record"}], matching_records=[{"name":"A","email":"eng@example.com"}]))
    assert result["suggested_value"] == "eng@example.com"
    assert result["safe_to_apply"] is True and result["confidence"] == "high"


@pytest.mark.parametrize("failure", ["timeout", "api"])
def test_openai_correction_failure_recommends_manual_without_inventing(monkeypatch, failure):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "test-model")

    async def fail(*args, **kwargs):
        request = httpx.Request("POST", "https://api.openai.com/v1/chat/completions")
        if failure == "timeout":
            raise APITimeoutError(request)
        raise APIConnectionError(message="offline", request=request)

    monkeypatch.setattr(main, "_openai_json_chat", fail)
    result=asyncio.run(main._openai_correction_suggestion(record={"name":"A"}, column="email", error="missing",
        column_metadata={}, approved_rules=[], candidates=[{"value":"known@example.com"}], matching_records=[]))
    assert result["suggested_value"] is None
    assert result["safe_to_apply"] is False
    assert "manually" in result["explanation"]


def test_openai_correction_accepts_contextual_unverified_model_candidate(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "test-model")

    async def fake_chat(*args, **kwargs):
        return json.dumps({"suggested_value":"invented@example.com", "safe_to_apply":True})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    result=asyncio.run(main._openai_correction_suggestion(record={}, column="email", error="missing", column_metadata={}, approved_rules=[],
        candidates=[{"value":"verified@example.com"}], matching_records=[]))
    assert result["suggested_value"] == "invented@example.com"
    assert result["safe_to_apply"] is False
    assert result["verification_status"] == "UNVERIFIED"


def test_openai_correction_is_called_without_preexisting_candidates(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "test-model")

    async def fake_chat(messages, *, timeout):
        assert "column_names" in messages[0]["content"]
        return json.dumps({"suggested_value":"42", "confidence":"medium", "explanation":"Contextual value", "reason":"Record context"})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    result = asyncio.run(main._openai_correction_suggestion(record={"name":"A", "age":None}, column="age", error="missing",
        column_metadata={"inferred_type":"integer"}, approved_rules=[], candidates=[], matching_records=[]))
    assert result["suggested_value"] == "42"
    assert result["safe_to_apply"] is False


def test_openai_correction_retries_transient_rate_limit(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "test-model")
    calls = {"count": 0}

    async def fake_chat(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise _openai_status_error(429, "rate limited")
        return json.dumps({"suggested_value":"42","confidence":"medium"})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    result=asyncio.run(main._openai_correction_suggestion(record={"age":None},column="age",error="missing",
        column_metadata={},approved_rules=[],candidates=[],matching_records=[]))
    assert result["suggested_value"]=="42"
    assert calls["count"] == 2


def test_openai_correction_invalid_schema_falls_back_to_manual_review(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "test-model")

    async def fake_chat(*args, **kwargs):
        return json.dumps({"suggested_value": ["invalid"]})

    monkeypatch.setattr(main, "_openai_json_chat", fake_chat)
    result = asyncio.run(main._openai_correction_suggestion(record={}, column="email", error="missing",
        column_metadata={}, approved_rules=[], candidates=[{"value":"verified@example.com"}], matching_records=[]))
    assert result["suggested_value"] is None
    assert result["safe_to_apply"] is False
    assert "invalid response" in result["reason"]


def test_name_based_email_draft_is_readable_and_never_verified():
    draft = main._name_based_email_draft({"employee_id": 324, "name": " Neha   Patel ", "email": "employee324gmail.com"})
    assert draft == "neha.patel@gmail.com"
    assert "employee324" not in draft
    assert main._name_based_email_draft({"employee_id": 324, "email": ""}) is None


def test_batch_correction_surfaces_unverified_email_repair_without_provider(monkeypatch):
    monkeypatch.setattr(settings, "openai_api_key", None)
    monkeypatch.setattr(settings, "openai_model", None)

    result = asyncio.run(main._openai_correction_suggestions_batch([{
        "key": "6:email",
        "row_index": 6,
        "column": "email",
        "current_value": "employee7gmail.com",
        "reason": "Invalid email format",
        "record": {"employee_id": 7, "email": "employee7gmail.com"},
        "metadata": {},
        "approved_rules": [{"type": "EMAIL_FORMAT", "parameters": {}}],
        "candidates": [{"value": "employee7@gmail.com", "source": "original_value_repair_unverified"}],
        "matching": [],
    }]))

    suggestion = result["6:email"]
    assert suggestion["suggested_value"] == "employee7@gmail.com"
    assert suggestion["safe_to_apply"] is False
    assert suggestion["verification_status"] == "UNVERIFIED"
    assert suggestion["confidence"] == "low"


@pytest.mark.parametrize(("status", "expected"), [
    (400, "invalid request or unsupported parameter"),
    (401, "invalid API key"),
    (403, "permission or model access issue"),
    (404, "endpoint or model unavailable"),
    (429, "rate limit or quota issue"),
    (503, "provider-side failure"),
])
def test_provider_status_errors_are_specific_and_scrubbed(monkeypatch, status, expected):
    monkeypatch.setattr(settings, "openai_api_key", "secret-test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    async def fail(*args, **kwargs):
        raise _openai_status_error(status, "bad request secret-test-key")

    monkeypatch.setattr(main, "_openai_json_chat", fail)
    dataset = SimpleNamespace(id=1, name="customers", source_type="snowflake", schema_json={"columns": {"email": {}}})
    with pytest.raises(HTTPException) as error:
        asyncio.run(main.generate_rule_suggestions([dataset], None))
    assert expected in error.value.detail
    assert "secret-test-key" not in error.value.detail
