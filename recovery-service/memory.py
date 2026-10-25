"""Resolution memory: remember fixes that worked, reuse them for the same kind of failure.

Key = service | step | error type | rail state. The rail state must be part of the key:
an "unknown" rail error that actually PAID needs a different fix than one that was
REJECTED, so without it memory would replay the wrong action.

A remembered plan is a suggestion only: it still goes through the verifier.
"""
import re
import time

import redis

from actions import Plan

PREFIX = "memory:"
NOT_REMEMBERED = ("escalate", "partial_refund")  # escalate fixes nothing; amounts are transfer-specific


def service_for(step: str) -> str:
    return "rail" if step in ("CALL_RAIL", "RECONCILE") else "wallet"


def error_type(last_error: str) -> str:
    e = (last_error or "").lower()
    if "timeout" in e or "deadline exceeded" in e:
        return "timeout"
    if "reversed" in e:
        return "reversed"
    if "insufficient funds" in e:
        return "insufficient_funds"
    m = re.search(r"returned (\d{3})", e)
    if m:
        return f"http_{m.group(1)}"
    if "unreachable" in e:
        return "unreachable"
    return "other"


def rail_state(facts: dict) -> str:
    states = sorted({r.get("state", "NONE") for r in facts.get("rail") or []})
    return "+".join(states) or "NONE"


def key_for(facts: dict) -> str:
    step = facts.get("step", "")
    return f"{service_for(step)}|{step}|{error_type(facts.get('last_error', ''))}|{rail_state(facts)}"


class Memory:
    def __init__(self, r: redis.Redis, prefix: str = PREFIX):
        self.r, self.prefix = r, prefix

    def get(self, key: str) -> Plan | None:
        data = self.r.hgetall(self.prefix + key)
        if not data:
            return None
        d = {k.decode() if isinstance(k, bytes) else k: v.decode() if isinstance(v, bytes) else v
             for k, v in data.items()}
        self.r.hincrby(self.prefix + key, "hits", 1)
        return Plan(action=d["action"], rail=d.get("rail") or None,
                    reason=f"remembered fix for {key}: {d.get('reason', '')}"[:1000])

    def put(self, key: str, plan: Plan, saga_id: str) -> bool:
        if plan.action in NOT_REMEMBERED:
            return False
        self.r.hset(self.prefix + key, mapping={
            "action": plan.action, "rail": plan.rail or "", "reason": plan.reason,
            "learned_from": saga_id, "stored_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
        self.r.hsetnx(self.prefix + key, "hits", 0)
        return True

    def all(self) -> dict[str, dict]:
        out = {}
        for k in self.r.scan_iter(match=self.prefix + "*"):
            name = (k.decode() if isinstance(k, bytes) else k)[len(self.prefix):]
            out[name] = {a.decode() if isinstance(a, bytes) else a: b.decode() if isinstance(b, bytes) else b
                         for a, b in self.r.hgetall(k).items()}
        return out

    def clear(self) -> int:
        keys = list(self.r.scan_iter(match=self.prefix + "*"))
        return self.r.delete(*keys) if keys else 0
