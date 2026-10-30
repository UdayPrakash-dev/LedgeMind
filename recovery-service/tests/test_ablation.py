"""Cached-token capture (KV cache) and the ablation switches B (hide_rail) and C (inject_rate)."""
import json

import fakeredis
import httpx
import pytest
import respx

from ablation import HIDDEN_RAIL, Ablation, AblationConfig
from actions import Plan
from conftest import WALLET, FakeLLM, mock_wallet
from consumer import Consumer, StrategyHolder
from decisions import DecisionLog
from memory import Memory
from planner import OpenAICompatLLM, Planner, cached_tokens
from wallet import WalletClient

RETRY = '{"action":"retry_step","reason":"rail says PAID"}'
URL = "https://llm.test/v1/chat/completions"


@pytest.fixture
def r():
    return fakeredis.FakeRedis()


def make(r, path, llm, strategy, **ablation):
    return Consumer(r, WalletClient(WALLET), Planner(llm), DecisionLog(path), StrategyHolder(strategy),
                    memory=Memory(r), ablation=Ablation(AblationConfig(**ablation)))


def executed(route):
    return [json.loads(c.request.content)["action"] for c in route.calls]


# ---------- cached tokens (provider prompt / KV cache) ----------

def test_cached_tokens_from_each_provider_format():
    assert cached_tokens({"prompt_tokens": 900, "prompt_tokens_details": {"cached_tokens": 768}}) == 768  # OpenAI, Groq
    assert cached_tokens({"prompt_cache_hit_tokens": 512}) == 512  # DeepSeek style
    assert cached_tokens({"cached_tokens": 128}) == 128
    assert cached_tokens({"prompt_tokens": 900}) == 0  # not reported
    assert cached_tokens({"prompt_tokens_details": None}) == 0


@respx.mock
def test_llm_client_and_planner_report_cached_tokens():
    respx.post(URL).mock(return_value=httpx.Response(200, json={
        "choices": [{"message": {"content": RETRY}}],
        "usage": {"prompt_tokens": 900, "completion_tokens": 30, "prompt_tokens_details": {"cached_tokens": 640}}}))
    llm = OpenAICompatLLM("https://llm.test/v1", "m", "k", min_interval_s=0)
    out = Planner(llm).plan({"step": "CALL_RAIL"})
    assert out.cached_tokens == 640 and out.prompt_tokens == 900


@respx.mock
def test_decision_log_has_cached_tokens(r, decisions_path):
    mock_wallet(respx.mock, rail_state="PAID")
    llm = FakeLLM(RETRY)
    d = make(r, decisions_path, llm, "llm_verified_memory").handle("s-1")
    assert d["cached_tokens"] == 0 and d["prompt_tokens"] == 100 and d["ablation"] == "main"


# ---------- B: hide_rail ----------

@respx.mock
def test_hide_rail_hides_rail_from_ai_only(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID")
    llm = FakeLLM('{"action":"compensate","reason":"guessing: refund"}', RETRY)
    d = make(r, decisions_path, llm, "llm_verified_memory", hide_rail=True).handle("s-1")
    seen = json.loads(llm.calls[0][1]["content"])
    assert seen["rail"] == HIDDEN_RAIL and "PAID" not in llm.calls[0][1]["content"]
    # the verifier still used the real rail state and blocked the refund
    assert d["rejections"][0]["action"] == "compensate" and executed(ex) == ["retry_step"]
    assert d["rail"] == [{"rail_ref": "s-1", "state": "PAID"}] and d["ablation"] == "hide_rail"


@respx.mock
def test_hide_rail_without_verifier_lets_the_guess_through(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID")
    llm = FakeLLM('{"action":"compensate","reason":"guessing: refund"}')
    make(r, decisions_path, llm, "llm_no_verifier", hide_rail=True).handle("s-1")
    assert executed(ex) == ["compensate"]


# ---------- C: injected errors ----------

def test_injection_is_seeded_and_never_escalate():
    plan = Plan(action="retry_step", reason="ok fine")
    a, b = Ablation(AblationConfig(inject_rate=0.5, seed=7)), Ablation(AblationConfig(inject_rate=0.5, seed=7))
    seq_a = [a.maybe_inject(plan, {"amount_paise": 1000}) for _ in range(50)]
    seq_b = [b.maybe_inject(plan, {"amount_paise": 1000}) for _ in range(50)]
    assert [(p.action, hit) for p, hit in seq_a] == [(p.action, hit) for p, hit in seq_b]
    hits = [p for p, hit in seq_a if hit]
    assert 10 < len(hits) < 40
    assert all(p.action not in ("retry_step", "escalate") for p in hits)
    assert all(p.amount_paise == 500 for p in hits if p.action == "partial_refund")
    assert Ablation().maybe_inject(plan, {}) == (plan, False)
    with pytest.raises(ValueError):
        Ablation(AblationConfig(inject_rate=1.5))


@respx.mock
def test_injected_error_reaches_the_bank_without_verifier(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID")
    c = make(r, decisions_path, FakeLLM(RETRY), "llm_no_verifier", inject_rate=1.0, seed=1)
    d = c.handle("s-1")
    assert executed(ex)[0] != "retry_step"
    assert d["injected"][0]["ai_action"] == "retry_step" and d["injected"][0]["executed"] is True
    assert c.stats.snapshot()["injected_executed"] == 1 and d["ablation"] == "inject100"


@respx.mock
def test_verifier_blocks_injected_errors_and_nothing_bad_is_remembered(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID")
    c = make(r, decisions_path, FakeLLM(RETRY, RETRY), "llm_verified_memory", inject_rate=1.0, seed=1)
    d = c.handle("s-1")
    # with rail PAID every wrong choice (compensate, reroute, partial_refund) is unsafe -> both blocked
    assert executed(ex) == ["escalate"] and d["source"] == "verifier_escalation"
    assert [i["blocked"] for i in d["injected"]] == [True, True]
    assert not any(i["executed"] for i in d["injected"])
    s = c.stats.snapshot()
    assert s["injected_errors"] == 2 and s["injected_blocked"] == 2 and s["injected_executed"] == 0
    assert Memory(r).all() == {}


@respx.mock
def test_injected_plan_is_never_stored_in_memory(r, decisions_path):
    # rail REJECTED: an injected compensate is actually correct, executes, but must not be remembered
    ex = mock_wallet(respx.mock, rail_state="REJECTED", final_status="COMPENSATED")
    c = make(r, decisions_path, FakeLLM('{"action":"escalate","reason":"unsure here"}'), "llm_verified_memory")
    c.ablation.maybe_inject = lambda plan, f: (Plan(action="compensate", reason="INJECTED"), True)
    d = c.handle("s-1")
    assert executed(ex) == ["compensate"] and d["injected"][0]["executed"] is True
    assert d["stored_in_memory"] is False and Memory(r).all() == {}


# ---------- memory off ----------

@respx.mock
def test_use_memory_false_skips_memory(r, decisions_path):
    mock_wallet(respx.mock, rail_state="PAID")
    Memory(r).put("rail|CALL_RAIL|timeout|PAID", Plan(action="retry_step", reason="remembered"), "old")
    llm = FakeLLM(RETRY)
    d = make(r, decisions_path, llm, "llm_verified_memory", use_memory=False).handle("s-1")
    assert d["memory_hit"] is False and d["source"] == "llm" and len(llm.calls) == 1
    assert d["stored_in_memory"] is False


# ---------- endpoint ----------

def test_ablation_endpoint(monkeypatch):
    from fastapi.testclient import TestClient
    from config import Settings
    from main import create_app
    monkeypatch.setenv("START_CONSUMER", "false")
    app = create_app(Settings(), llm=FakeLLM(), redis_client=fakeredis.FakeRedis())
    with TestClient(app) as c:
        assert c.get("/admin/ablation").json()["label"] == "main"
        got = c.post("/admin/ablation", json={"hide_rail": True, "inject_rate": 0.2, "seed": 3}).json()
        assert got["label"] == "hide_rail+inject20" and got["seed"] == 3
        assert c.get("/health").json()["ablation"] == "hide_rail+inject20"
        assert c.post("/admin/ablation", json={"inject_rate": 2}).status_code == 400
        assert c.post("/admin/ablation", json={}).json()["label"] == "main"
