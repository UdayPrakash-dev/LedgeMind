import json
import os
import uuid

import fakeredis
import pytest
import redis as redis_lib
import respx

from conftest import WALLET, FakeLLM, mock_wallet
from consumer import Consumer, StrategyHolder
from decisions import DecisionLog
from planner import LLMError, Planner
from wallet import WalletClient

STREAM, GROUP = "stuck-sagas", "recovery"


def make(r, decisions_path, llm=None, name="recovery-1", strategy="llm_no_verifier", **kw):
    return Consumer(r, WalletClient(WALLET), Planner(llm or FakeLLM()), DecisionLog(decisions_path),
                    StrategyHolder(strategy), stream=STREAM, group=GROUP, name=name, **kw)


def push(r, saga_id="s-1"):
    return r.xadd(STREAM, {"saga_id": saga_id, "step": "CALL_RAIL", "policy": "escalate", "error": "x"})


def pending(r):
    return r.xpending(STREAM, GROUP)["pending"]


@pytest.fixture
def r():
    """Real Redis if REDIS_ADDR is set (like the Go tests), else fakeredis. Each test gets its own stream."""
    global STREAM
    STREAM = f"stuck-test-{uuid.uuid4()}"
    addr = os.getenv("REDIS_ADDR")
    if addr:
        host, _, port = addr.partition(":")
        client = redis_lib.Redis(host=host, port=int(port or 6379))
        yield client
        client.delete(STREAM)
    else:
        yield fakeredis.FakeRedis()


def needs_real_redis(r):
    if isinstance(r, fakeredis.FakeRedis):
        pytest.skip("fakeredis does not return pending messages for XREADGROUP id 0; set REDIS_ADDR")


@respx.mock
def test_stuck_transfer_is_fixed_logged_and_acked(r, decisions_path):
    execute = mock_wallet(respx.mock, rail_state="PAID")
    c = make(r, decisions_path, FakeLLM('{"action":"retry_step","reason":"rail says PAID"}'))
    c.ensure_group()
    push(r)

    assert c.poll_once(block_ms=10) == 1
    sent = json.loads(execute.calls[0].request.content)
    assert sent == {"action": "retry_step", "reason": "rail says PAID", "actor": "llm_no_verifier"}
    assert pending(r) == 0  # acknowledged

    [d] = DecisionLog(decisions_path).read_all()
    assert d["saga_id"] == "s-1" and d["strategy"] == "llm_no_verifier" and d["source"] == "llm"
    assert d["proposed"]["action"] == "retry_step" and d["executed"] is True and d["final_status"] == "COMPLETED"
    assert d["verdict"] == "not_checked" and d["attempt"] == 1 and d["prompt_tokens"] == 100
    for key in ("latency_ms", "completion_tokens", "rail", "step"):
        assert key in d


@respx.mock
def test_already_fixed_transfer_is_skipped_without_llm(r, decisions_path):
    mock_wallet(respx.mock, status="COMPLETED")
    llm = FakeLLM()
    c = make(r, decisions_path, llm)
    c.ensure_group()
    push(r)
    c.poll_once(block_ms=10)
    assert llm.calls == [] and pending(r) == 0
    assert c.stats.snapshot()["skipped_not_stuck"] == 1


@respx.mock
def test_executor_409_is_recorded_not_retried(r, decisions_path):
    mock_wallet(respx.mock, exec_status=409)
    c = make(r, decisions_path, FakeLLM('{"action":"compensate","reason":"rail rejected"}'))
    c.ensure_group()
    push(r)
    c.poll_once(block_ms=10)
    [d] = DecisionLog(decisions_path).read_all()
    assert d["executed"] is False and d["executor_status"] == 409
    assert pending(r) == 0


class ExplodingLLM(FakeLLM):
    def complete(self, messages):
        raise LLMError("provider down")


@respx.mock
def test_crash_leaves_message_pending_and_restart_finishes_it(r, decisions_path):
    needs_real_redis(r)
    mock_wallet(respx.mock)
    first = make(r, decisions_path, ExplodingLLM())
    first.ensure_group()
    push(r)
    first.poll_once(block_ms=10)
    assert pending(r) == 1  # not acked: nothing lost

    # "Restart": a new consumer with the same name drains its own pending messages first.
    second = make(r, decisions_path, FakeLLM('{"action":"retry_step","reason":"rail says PAID"}'))
    assert second.drain_own_pending() == 1
    assert pending(r) == 0
    assert DecisionLog(decisions_path).read_all()[-1]["executed"] is True


@respx.mock
def test_abandoned_message_of_dead_consumer_is_claimed(r, decisions_path):
    mock_wallet(respx.mock)
    dead = make(r, decisions_path, ExplodingLLM(), name="recovery-dead")
    dead.ensure_group()
    push(r)
    dead.poll_once(block_ms=10)
    assert pending(r) == 1

    alive = make(r, decisions_path, FakeLLM('{"action":"retry_step","reason":"rail says PAID"}'),
                 name="recovery-2", claim_idle_ms=0)
    assert alive.claim_abandoned() == 1
    assert pending(r) == 0


@respx.mock
def test_poison_message_is_escalated_after_max_deliveries(r, decisions_path):
    execute = mock_wallet(respx.mock)
    c = make(r, decisions_path, ExplodingLLM(), claim_idle_ms=0, max_deliveries=2)
    c.ensure_group()
    push(r)
    c.poll_once(block_ms=10)          # delivery 1: fails, stays pending
    assert pending(r) == 1
    c.claim_abandoned()               # delivery 2: fails again -> escalate + ack
    assert pending(r) == 0
    body = json.loads(execute.calls[-1].request.content)
    assert body["action"] == "escalate" and "failed 2 times" in body["reason"]
    assert DecisionLog(decisions_path).read_all()[-1]["source"] == "dead_letter"


def test_off_strategy_reads_nothing(r, decisions_path):
    c = make(r, decisions_path, strategy="off")
    c.ensure_group()
    push(r)
    assert c.poll_once(block_ms=10) == 0
    assert r.xpending(STREAM, GROUP)["pending"] == 0  # still unread, waiting for a strategy


def test_strategy_holder_validates():
    h = StrategyHolder("off")
    h.set("llm_no_verifier")
    h.set("llm_verified_memory")
    with pytest.raises(ValueError, match="unknown"):
        h.set("yolo")
    assert h.get() == "llm_verified_memory"
