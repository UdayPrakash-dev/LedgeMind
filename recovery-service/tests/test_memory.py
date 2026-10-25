import fakeredis

from actions import Plan
from memory import Memory, error_type, key_for


def f(step="CALL_RAIL", err="payment rail unreachable: context deadline exceeded", rail="PAID"):
    return {"step": step, "last_error": err, "rail": [{"rail_ref": "x", "state": rail}]}


def test_error_types():
    assert error_type("payment rail unreachable: ... Client.Timeout exceeded") == "timeout"
    assert error_type('payment rail returned 422: {"error":"limit"}') == "http_422"
    assert error_type("payment rail returned 500: ERR_7004: settlement node ack mismatch") == "http_500"
    assert error_type("rail reports the payment was REVERSED after the transfer completed") == "reversed"
    assert error_type("insufficient funds") == "insufficient_funds"
    assert error_type("") == "other"


def test_key_includes_service_step_error_and_rail_state():
    assert key_for(f()) == "rail|CALL_RAIL|timeout|PAID"
    # Same error, different rail truth -> different key (the important part).
    paid = f(err="payment rail returned 500: ERR_1", rail="PAID")
    rejected = f(err="payment rail returned 500: ERR_1", rail="REJECTED")
    assert key_for(paid) != key_for(rejected)
    assert key_for({"step": "CREDIT_RECEIVER", "last_error": "boom", "rail": []}).startswith("wallet|")


def test_put_get_hits_and_clear():
    m = Memory(fakeredis.FakeRedis())
    k = key_for(f())
    assert m.get(k) is None
    assert m.put(k, Plan(action="retry_step", reason="rail paid"), "s-1")
    got = m.get(k)
    assert got.action == "retry_step" and "remembered fix" in got.reason
    m.get(k)
    assert m.all()[k]["hits"] == "2" and m.all()[k]["learned_from"] == "s-1"
    assert m.clear() == 1 and m.all() == {}


def test_escalate_and_partial_refund_are_not_remembered():
    m = Memory(fakeredis.FakeRedis())
    assert not m.put("k", Plan(action="escalate", reason="dunno"), "s")
    assert not m.put("k", Plan(action="partial_refund", amount_paise=5, reason="some"), "s")
    assert m.get("k") is None
