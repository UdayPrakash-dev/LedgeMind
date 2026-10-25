"""The llm_verified_memory strategy end to end (wallet faked, LLM scripted)."""
import json

import fakeredis
import pytest
import respx

from conftest import WALLET, FakeLLM, events_posted, mock_wallet
from consumer import Consumer, StrategyHolder
from decisions import DecisionLog
from memory import Memory
from planner import Planner
from wallet import WalletClient

RETRY = '{"action":"retry_step","reason":"rail says PAID"}'
REFUND = '{"action":"compensate","reason":"refund the sender"}'


@pytest.fixture
def r():
    return fakeredis.FakeRedis()


def make(r, decisions_path, llm, strategy="llm_verified_memory"):
    return Consumer(r, WalletClient(WALLET), Planner(llm), DecisionLog(decisions_path), StrategyHolder(strategy),
                    memory=Memory(r))


def executed_body(route):
    return json.loads(route.calls[-1].request.content)


@respx.mock
def test_safe_plan_is_approved_executed_and_remembered(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID")
    c = make(r, decisions_path, FakeLLM(RETRY))
    d = c.handle("s-1")
    assert executed_body(ex)["action"] == "retry_step" and executed_body(ex)["actor"] == "llm_verified_memory"
    assert d["verdict"] == "approved" and d["source"] == "llm" and d["stored_in_memory"] is True
    assert events_posted(respx.mock) == ["RecoveryProposed", "RecoveryVerified", "RecoveryExecuted"]
    assert Memory(r).all()["rail|CALL_RAIL|timeout|PAID"]["action"] == "retry_step"


@respx.mock
def test_unsafe_plan_is_blocked_then_ai_corrects_itself(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID")
    llm = FakeLLM(REFUND, RETRY)  # first an unsafe refund, then the right answer
    d = make(r, decisions_path, llm).handle("s-1")
    assert executed_body(ex)["action"] == "retry_step"  # the refund never reached the bank
    assert len(ex.calls) == 1
    assert d["verdict"] == "approved_after_rejection" and d["source"] == "llm_retry"
    assert d["proposed"]["action"] == "compensate" and d["final"]["action"] == "retry_step"
    assert "PAID" in d["rejections"][0]["reasons"][0]
    assert "REJECTED your choice 'compensate'" in llm.calls[1][-1]["content"]  # reason fed back
    assert events_posted(respx.mock) == ["RecoveryProposed", "RecoveryRejected",
                                         "RecoveryProposed", "RecoveryVerified", "RecoveryExecuted"]


@respx.mock
def test_two_unsafe_plans_escalate_to_a_human(r, decisions_path):
    ex = mock_wallet(respx.mock, rail_state="PAID", final_status="ESCALATED")
    d = make(r, decisions_path, FakeLLM(REFUND, REFUND)).handle("s-1")
    body = executed_body(ex)
    assert body["action"] == "escalate" and "safety checker rejected" in body["reason"]
    assert d["verdict"] == "rejected_escalated" and len(d["rejections"]) == 2
    assert d["stored_in_memory"] is False


@respx.mock
def test_memory_hit_skips_the_ai(r, decisions_path):
    mock_wallet(respx.mock, rail_state="PAID")
    llm = FakeLLM(RETRY)
    c = make(r, decisions_path, llm)
    c.handle("s-1")                      # learns
    assert len(llm.calls) == 1
    d = c.handle("s-1")                  # same kind of failure again
    assert len(llm.calls) == 1           # no new AI call
    assert d["source"] == "memory" and d["memory_hit"] is True and d["attempt"] == 0
    assert c.stats.snapshot()["memory_hits"] == 1


@respx.mock
def test_remembered_plan_still_goes_through_the_verifier(r, decisions_path):
    mock_wallet(respx.mock, rail_state="PAID")
    m = Memory(r)
    # Poison the memory with an unsafe fix for this exact key.
    from actions import Plan
    m.put("rail|CALL_RAIL|timeout|PAID", Plan(action="compensate", reason="bad memory"), "old")
    llm = FakeLLM(RETRY)
    d = make(r, decisions_path, llm).handle("s-1")
    assert d["memory_hit"] is True and d["source"] == "llm"   # memory rejected, AI asked instead
    assert d["rejections"][0]["source"] == "memory"
    assert len(llm.calls) == 1


@respx.mock
def test_not_remembered_if_invariants_fail_afterwards(r, decisions_path):
    mock_wallet(respx.mock, rail_state="PAID", invariants_ok=False)
    d = make(r, decisions_path, FakeLLM(RETRY)).handle("s-1")
    assert d["executed"] is True and d["stored_in_memory"] is False
    assert Memory(r).all() == {}


@respx.mock
def test_no_verifier_strategy_lets_the_unsafe_plan_through(r, decisions_path):
    """The contrast the paper measures: same unsafe plan, no checker."""
    ex = mock_wallet(respx.mock, rail_state="PAID")
    d = make(r, decisions_path, FakeLLM(REFUND), strategy="llm_no_verifier").handle("s-1")
    assert executed_body(ex)["action"] == "compensate"
    assert d["verdict"] == "not_checked"
    assert events_posted(respx.mock) == ["RecoveryProposed", "RecoveryExecuted"]
