import json

import httpx
import pytest
import respx

from actions import Plan
from conftest import FakeLLM
from planner import LLMError, OpenAICompatLLM, Planner, parse

FACTS = {"transfer_id": "s-1", "status": "STUCK", "step": "CALL_RAIL", "rail": [{"rail_ref": "s-1", "state": "PAID"}]}


# ---------- parse ----------

def test_parse_valid_json():
    p = parse('{"action": "retry_step", "reason": "rail already paid"}')
    assert p.action == "retry_step" and p.reason == "rail already paid"


def test_parse_accepts_code_fences_and_surrounding_text():
    p = parse('Sure!\n```json\n{"action": "compensate", "reason": "rail rejected it"}\n```')
    assert p.action == "compensate"


def test_parse_invalid_json():
    with pytest.raises(ValueError, match="invalid JSON"):
        parse('{"action": "retry_step", "reason": }')


def test_parse_no_json_at_all():
    with pytest.raises(ValueError, match="no JSON"):
        parse("I think you should retry.")


def test_parse_unknown_action():
    with pytest.raises(ValueError, match="action"):
        parse('{"action": "delete_wallet", "reason": "why not"}')


def test_parse_missing_reason():
    with pytest.raises(ValueError, match="reason"):
        parse('{"action": "retry_step"}')


def test_partial_refund_needs_amount_and_others_drop_extras():
    with pytest.raises(ValueError, match="amount_paise"):
        parse('{"action": "partial_refund", "reason": "refund some"}')
    p = parse('{"action": "retry_step", "reason": "ok then", "amount_paise": 5, "rail": "x"}')
    assert p.amount_paise is None and p.rail is None


def test_executor_body():
    p = Plan(action="reroute", rail="backup", reason="primary rail rejected")
    assert p.to_executor("llm_no_verifier") == {
        "action": "reroute", "reason": "primary rail rejected", "actor": "llm_no_verifier", "rail": "backup"}


# ---------- planner retry / fallback ----------

def test_planner_first_reply_ok():
    llm = FakeLLM('{"action": "retry_step", "reason": "rail says PAID"}')
    out = Planner(llm).plan(FACTS)
    assert out.plan.action == "retry_step" and out.attempts == 1 and out.parse_failures == 0
    assert not out.fallback and out.prompt_tokens == 100
    # The facts are sent as the user message.
    assert json.loads(llm.calls[0][1]["content"])["transfer_id"] == "s-1"


def test_planner_repairs_once_with_the_error():
    llm = FakeLLM("not json", '{"action": "compensate", "reason": "rail rejected"}')
    out = Planner(llm).plan(FACTS)
    assert out.plan.action == "compensate" and out.attempts == 2 and out.parse_failures == 1
    assert "not usable" in llm.calls[1][-1]["content"]


def test_planner_escalates_after_two_bad_replies():
    llm = FakeLLM('{"action": "nuke"}', "still not json")
    out = Planner(llm).plan(FACTS)
    assert out.fallback and out.plan.action == "escalate"
    assert out.attempts == 2 and out.parse_failures == 2


# ---------- HTTP client (no real network) ----------

URL = "https://llm.test/v1/chat/completions"


def _ok(content):
    return httpx.Response(200, json={"choices": [{"message": {"content": content}}],
                                     "usage": {"prompt_tokens": 50, "completion_tokens": 10}})


@respx.mock
def test_llm_client_sends_temperature_zero_and_json_mode():
    route = respx.post(URL).mock(return_value=_ok('{"action":"escalate","reason":"x y z"}'))
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "secret-key", min_interval_s=0)
    reply = llm.complete([{"role": "user", "content": "hi"}])
    sent = json.loads(route.calls[0].request.content)
    assert sent["temperature"] == 0 and sent["response_format"] == {"type": "json_object"}
    assert route.calls[0].request.headers["authorization"] == "Bearer secret-key"
    assert reply.prompt_tokens == 50


@respx.mock
def test_llm_client_retries_429(monkeypatch):
    monkeypatch.setattr("planner.time.sleep", lambda s: None)
    respx.post(URL).mock(side_effect=[httpx.Response(429, headers={"retry-after": "1"}), _ok("{}")])
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "k", min_interval_s=0)
    assert llm.complete([]).text == "{}"


@respx.mock
def test_llm_client_drops_json_mode_if_unsupported():
    route = respx.post(URL).mock(side_effect=[httpx.Response(400, text="response_format not supported"), _ok("{}")])
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "k", min_interval_s=0)
    llm.complete([])
    assert "response_format" not in json.loads(route.calls[1].request.content)


@respx.mock
def test_llm_client_bad_key_fails_fast_without_leaking_it():
    respx.post(URL).mock(return_value=httpx.Response(401, text="invalid api key"))
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "super-secret", min_interval_s=0)
    with pytest.raises(LLMError) as e:
        llm.complete([])
    assert "super-secret" not in str(e.value)


@respx.mock
def test_reasoning_effort_sent_and_dropped_if_unsupported():
    route = respx.post(URL).mock(side_effect=[_ok("{}"), httpx.Response(400, text="reasoning_effort is not supported"), _ok("{}")])
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "k", min_interval_s=0, max_tokens=999, reasoning_effort="low")
    llm.complete([])
    first = json.loads(route.calls[0].request.content)
    assert first["reasoning_effort"] == "low" and first["max_tokens"] == 999
    llm.complete([])  # provider now rejects it -> retried without
    assert "reasoning_effort" not in json.loads(route.calls[2].request.content)


@respx.mock
def test_reasoning_model_out_of_tokens_is_an_unusable_reply_not_a_crash():
    respx.post(URL).mock(return_value=httpx.Response(200, json={
        "choices": [{"message": {"content": ""}, "finish_reason": "length"}], "usage": {"completion_tokens": 300}}))
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "k", min_interval_s=0)
    out = Planner(llm).plan(FACTS)
    assert out.fallback and out.parse_failures == 2 and "max_tokens" in out.errors[0]


@respx.mock
def test_ping_reports_quota_errors_without_retrying():
    route = respx.post(URL).mock(return_value=httpx.Response(429, json={"error": {"message": "tokens per day"}},
                                                                headers={"retry-after": "600"}))
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "secret-key", min_interval_s=0)
    res = llm.ping()
    assert res["ok"] is False and res["status"] == 429 and res["retry_after"] == "600"
    assert len(route.calls) == 1 and llm.rate_limited == 1 and "secret-key" not in str(res)


@respx.mock
def test_rate_limit_is_counted_and_shown_without_key(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    respx.post(URL).mock(side_effect=[httpx.Response(429, text="slow down"), _ok('{"action":"escalate","reason":"x y z"}')])
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "secret-key", min_interval_s=0)
    llm.complete([{"role": "user", "content": "hi"}])
    assert llm.rate_limited == 1 and "429" in llm.last_http_error and "secret-key" not in llm.last_http_error
