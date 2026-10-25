"""The safety checker. Deterministic, no AI: every rule is a small pure function.

A rule gets the proposed plan and the facts (read fresh from the wallet and the rail,
never from the AI) and returns None if the plan is fine, or a short reason if not.
"""
from dataclasses import dataclass, field
from typing import Callable, Optional

from actions import Plan

MAX_ACTIONS = 3  # after this many recovery actions on one transfer, only escalate is allowed


@dataclass(frozen=True)
class Flows:
    """Money movements of one transfer, in paise."""
    out: int        # taken from the sender
    back: int       # returned to the sender (refund + partial refund)
    delivered: int  # given to the receiver (credit minus reversal)

    @property
    def in_flight(self) -> int:
        return self.out - self.back - self.delivered


@dataclass
class Verdict:
    ok: bool
    reasons: list[str] = field(default_factory=list)
    checks: dict[str, str] = field(default_factory=dict)  # rule name -> "pass" or the reason


# ---------- helpers ----------

def _ledger(f: dict) -> dict:
    return f.get("ledger") or {}


def _amount(f: dict) -> int:
    return int(f.get("amount_paise") or 0)


def _rail_states(f: dict) -> list[str]:
    return [r.get("state", "") for r in f.get("rail") or []]


def current_flows(f: dict) -> Flows:
    L, a = _ledger(f), _amount(f)
    out = a if L.get("debited") else 0
    back = (a if L.get("refunded") else 0) + int(L.get("partial_refund_paise") or 0)
    delivered = (a if L.get("credited") else 0) - (a if L.get("reversed") else 0)
    return Flows(out, back, delivered)


def simulate(plan: Plan, f: dict) -> Flows:
    """Money movements AFTER the plan, assuming it succeeds (the worst case for money creation)."""
    L, a, step = _ledger(f), _amount(f), f.get("step")
    now = current_flows(f)
    out, back, delivered = now.out, now.back, now.delivered
    credited_now = L.get("credited") and not L.get("reversed")
    if plan.action == "retry_step":
        if step == "DEBIT_SENDER" and not L.get("debited"):
            out = a
        elif step in ("CALL_RAIL", "CREDIT_RECEIVER") and not L.get("credited"):
            delivered += a
        elif step == "REFUND_SENDER" and not L.get("refunded"):
            back += a
    elif plan.action == "compensate":
        if credited_now:
            delivered -= a
        if L.get("debited") and not L.get("refunded"):
            back += a
    elif plan.action == "reroute":
        if not L.get("credited"):
            delivered += a
    elif plan.action == "partial_refund":
        back += plan.amount_paise or 0
    return Flows(out, back, delivered)


# ---------- the rules ----------

def rule_loop_limit(plan: Plan, f: dict) -> Optional[str]:
    if plan.action != "escalate" and int(f.get("recovery_actions_so_far") or 0) >= MAX_ACTIONS:
        return f"{MAX_ACTIONS} recovery actions already tried; only escalate is allowed"
    return None


def rule_action_fits_step(plan: Plan, f: dict) -> Optional[str]:
    step = f.get("step")
    if plan.action == "reroute" and step != "CALL_RAIL":
        return f"reroute only applies at CALL_RAIL, not {step}"
    if plan.action == "retry_step" and step in ("RECONCILE", "DONE"):
        return f"nothing to retry at step {step}"
    return None


def rule_money_conserved(plan: Plan, f: dict) -> Optional[str]:
    """Simulate the action: the transfer may never give back or deliver more than it took."""
    a, after = _amount(f), simulate(plan, f)
    if after.in_flight < 0:
        return (f"would create money: took {after.out}, would give back {after.back} "
                f"and deliver {after.delivered}")
    if after.delivered > a or after.delivered < 0:
        return f"receiver would end with {after.delivered} of a {a} transfer"
    return None


def rule_no_negative_balance(plan: Plan, f: dict) -> Optional[str]:
    L, a, bal = _ledger(f), _amount(f), f.get("balances") or {}
    if plan.action == "compensate" and L.get("credited") and not L.get("reversed"):
        receiver = bal.get("receiver")
        if receiver is not None and receiver < a:
            return f"taking back {a} from the receiver would make its balance negative ({receiver})"
    if plan.action == "retry_step" and f.get("step") == "DEBIT_SENDER" and not L.get("debited"):
        sender = bal.get("sender")
        if sender is not None and sender < a:
            return f"debiting {a} would make the sender's balance negative ({sender})"
    return None


def rule_compensate_needs_commit(plan: Plan, f: dict) -> Optional[str]:
    if plan.action in ("compensate", "partial_refund") and not _ledger(f).get("debited"):
        return "nothing was debited from the sender, so there is nothing to undo or refund"
    return None


def rule_refund_within_debited(plan: Plan, f: dict) -> Optional[str]:
    now, a, L = current_flows(f), _amount(f), _ledger(f)
    held = now.in_flight  # money taken but neither returned nor delivered
    if plan.action == "partial_refund" and (plan.amount_paise or 0) > held:
        return f"partial refund of {plan.amount_paise} exceeds the {held} still held for this transfer"
    if plan.action == "compensate" and L.get("debited") and not L.get("refunded"):
        refundable = now.out - now.back
        if a > refundable:
            return f"compensate refunds {a} but only {refundable} is still refundable"
    return None


def rule_idempotency_key_unused(plan: Plan, f: dict) -> Optional[str]:
    """Each money step has one idempotency key per transfer; it cannot be used for a second, different action."""
    L = _ledger(f)
    if plan.action == "partial_refund" and int(L.get("partial_refund_paise") or 0) > 0:
        return "the partial-refund key of this transfer was already used"
    if plan.action == "compensate" and L.get("refunded") and not (L.get("credited") and not L.get("reversed")):
        return "the refund key of this transfer was already used; compensating again does nothing"
    return None


def rule_no_refund_after_rail_paid(plan: Plan, f: dict) -> Optional[str]:
    if plan.action in ("compensate", "partial_refund") and "PAID" in _rail_states(f):
        return "the rail says PAID: the money already left, refunding the sender would pay it out twice"
    return None


def rule_no_double_rail_payment(plan: Plan, f: dict) -> Optional[str]:
    if plan.action == "reroute" and "PAID" in _rail_states(f):
        return "the rail says PAID: rerouting would make the rail pay a second time"
    return None


def rule_no_futile_retry(plan: Plan, f: dict) -> Optional[str]:
    states = _rail_states(f)
    if (plan.action == "retry_step" and f.get("step") == "CALL_RAIL" and states
            and all(s in ("REJECTED", "REVERSED") for s in states)):
        return f"the rail already answered {states[-1]}; retrying the same payment will fail again"
    return None


RULES: list[tuple[str, Callable[[Plan, dict], Optional[str]]]] = [
    ("loop_limit", rule_loop_limit),
    ("action_fits_step", rule_action_fits_step),
    ("money_conserved", rule_money_conserved),
    ("no_negative_balance", rule_no_negative_balance),
    ("compensate_needs_commit", rule_compensate_needs_commit),
    ("refund_within_debited", rule_refund_within_debited),
    ("idempotency_key_unused", rule_idempotency_key_unused),
    ("no_refund_after_rail_paid", rule_no_refund_after_rail_paid),
    ("no_double_rail_payment", rule_no_double_rail_payment),
    ("no_futile_retry", rule_no_futile_retry),
]


def verify(plan: Plan, facts: dict) -> Verdict:
    """Run every rule. Escalate is always allowed: handing over to a human is never unsafe."""
    if plan.action == "escalate":
        return Verdict(ok=True, checks={name: "pass" for name, _ in RULES})
    v = Verdict(ok=True)
    for name, rule in RULES:
        reason = rule(plan, facts)
        v.checks[name] = reason or "pass"
        if reason:
            v.ok = False
            v.reasons.append(reason)
    return v
