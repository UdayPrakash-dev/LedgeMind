"""One test pair (allowed / blocked) per verifier rule."""
from actions import Plan
from verifier import RULES, current_flows, simulate, verify

A = 1000


def facts(step="CALL_RAIL", rail="PAID", debited=True, credited=False, refunded=False, reversed_=False,
          partial=0, actions_so_far=0, sender=9000, receiver=0):
    return {
        "status": "STUCK", "step": step, "amount_paise": A, "recovery_actions_so_far": actions_so_far,
        "ledger": {"debited": debited, "credited": credited, "refunded": refunded, "reversed": reversed_,
                   "partial_refund_paise": partial},
        "rail": [] if rail is None else [{"rail_ref": "s-1", "state": rail}],
        "balances": {"sender": sender, "receiver": receiver},
    }


def plan(action, **kw):
    return Plan(action=action, reason="test plan", **kw)


def blocked_by(p, f):
    v = verify(p, f)
    return [name for name, result in v.checks.items() if result != "pass"]


# ---------- the scenarios that matter most for the paper ----------

def test_timeout_that_paid_retry_is_allowed():
    assert verify(plan("retry_step"), facts(rail="PAID")).ok


def test_timeout_that_paid_refund_is_blocked():
    """The Day 4 bug: refunding after the rail already paid."""
    v = verify(plan("compensate"), facts(rail="PAID"))
    assert not v.ok and "no_refund_after_rail_paid" in blocked_by(plan("compensate"), facts(rail="PAID"))
    assert "PAID" in v.reasons[0]


def test_reject_compensate_is_allowed():
    assert verify(plan("compensate"), facts(rail="REJECTED")).ok


def test_partial_reversal_compensate_is_allowed():
    f = facts(step="RECONCILE", rail="REVERSED", credited=True, receiver=1000)
    assert verify(plan("compensate"), f).ok


def test_escalate_always_allowed():
    assert verify(plan("escalate"), facts(actions_so_far=99, rail="PAID")).ok


# ---------- rule by rule ----------

def test_rule_loop_limit():
    assert "loop_limit" not in blocked_by(plan("retry_step"), facts(actions_so_far=2))
    assert "loop_limit" in blocked_by(plan("retry_step"), facts(actions_so_far=3))


def test_rule_action_fits_step():
    assert "action_fits_step" not in blocked_by(plan("reroute", rail="backup"), facts(rail="REJECTED"))
    assert "action_fits_step" in blocked_by(plan("reroute", rail="backup"), facts(step="CREDIT_RECEIVER", rail="PAID"))
    assert "action_fits_step" in blocked_by(plan("retry_step"), facts(step="RECONCILE", rail="REVERSED", credited=True))


def test_rule_money_conserved():
    # Already refunded once; a partial refund on top would give back more than was taken.
    f = facts(rail="REJECTED", refunded=True)
    assert "money_conserved" in blocked_by(plan("partial_refund", amount_paise=500), f)
    assert "money_conserved" not in blocked_by(plan("compensate"), facts(rail="REJECTED"))


def test_rule_no_negative_balance():
    receiver_spent = facts(step="RECONCILE", rail="REVERSED", credited=True, receiver=300)
    assert "no_negative_balance" in blocked_by(plan("compensate"), receiver_spent)
    has_money = facts(step="RECONCILE", rail="REVERSED", credited=True, receiver=1000)
    assert "no_negative_balance" not in blocked_by(plan("compensate"), has_money)
    broke_sender = facts(step="DEBIT_SENDER", rail=None, debited=False, sender=10)
    assert "no_negative_balance" in blocked_by(plan("retry_step"), broke_sender)


def test_rule_compensate_needs_commit():
    never_debited = facts(step="DEBIT_SENDER", rail=None, debited=False)
    assert "compensate_needs_commit" in blocked_by(plan("compensate"), never_debited)
    assert "compensate_needs_commit" not in blocked_by(plan("compensate"), facts(rail="REJECTED"))


def test_rule_refund_within_debited():
    assert "refund_within_debited" not in blocked_by(plan("partial_refund", amount_paise=400), facts(rail="REJECTED"))
    # 400 already partially refunded: a full compensate would refund 1000 of only 600 left.
    f = facts(rail="REJECTED", partial=400)
    assert "refund_within_debited" in blocked_by(plan("compensate"), f)


def test_rule_idempotency_key_unused():
    f = facts(rail="REJECTED", partial=200)
    assert "idempotency_key_unused" in blocked_by(plan("partial_refund", amount_paise=100), f)
    assert "idempotency_key_unused" in blocked_by(plan("compensate"), facts(rail="REJECTED", refunded=True))
    assert "idempotency_key_unused" not in blocked_by(plan("compensate"), facts(rail="REJECTED"))


def test_rule_no_refund_after_rail_paid():
    assert "no_refund_after_rail_paid" in blocked_by(plan("partial_refund", amount_paise=100), facts(rail="PAID"))
    assert "no_refund_after_rail_paid" not in blocked_by(plan("partial_refund", amount_paise=100), facts(rail="REJECTED"))


def test_rule_no_double_rail_payment():
    assert "no_double_rail_payment" in blocked_by(plan("reroute", rail="backup"), facts(rail="PAID"))
    assert "no_double_rail_payment" not in blocked_by(plan("reroute", rail="backup"), facts(rail="REJECTED"))
    assert "no_double_rail_payment" not in blocked_by(plan("reroute", rail="backup"), facts(rail="NONE"))


def test_rule_no_futile_retry():
    assert "no_futile_retry" in blocked_by(plan("retry_step"), facts(rail="REJECTED"))
    assert "no_futile_retry" not in blocked_by(plan("retry_step"), facts(rail="NONE"))
    assert "no_futile_retry" not in blocked_by(plan("retry_step"), facts(rail="PAID"))


def test_every_rule_reports_in_checks():
    v = verify(plan("retry_step"), facts())
    assert set(v.checks) == {name for name, _ in RULES}


# ---------- simulation ----------

def test_flows_and_simulation():
    f = facts(rail="REJECTED")
    assert current_flows(f).in_flight == A
    after = simulate(plan("compensate"), f)
    assert after.back == A and after.in_flight == 0
    after = simulate(plan("retry_step"), facts(rail="PAID"))
    assert after.delivered == A and after.in_flight == 0
