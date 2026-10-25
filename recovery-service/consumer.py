"""Reads stuck transfers from the Redis Stream and fixes them, one at a time.

Delivery guarantees:
- A message is acknowledged (XACK) only after it was fully handled.
- If the service crashes mid-way, the message stays pending: on restart this consumer
  re-reads its own pending messages, and XAUTOCLAIM picks up messages left by others.
- Handling is idempotent: a transfer that is no longer STUCK is skipped.
- A message that keeps failing (MAX_DELIVERIES) is escalated to a human and acknowledged.
"""
import logging
import threading
import time

import redis

import facts as facts_mod
import verifier
from actions import Plan, escalate
from decisions import DecisionLog
from memory import Memory, key_for
from planner import LLMError, Planner
from wallet import WalletClient, WalletError

log = logging.getLogger("recovery.consumer")

STRATEGIES = ("off", "llm_no_verifier", "llm_verified_memory")
GOOD_END_STATES = ("COMPLETED", "COMPENSATED", "FAILED")


class StrategyHolder:
    """The current strategy, switchable at runtime by /admin/strategy."""

    def __init__(self, initial: str):
        self._lock = threading.Lock()
        self._value = "off"
        self.set(initial)

    def get(self) -> str:
        with self._lock:
            return self._value

    def set(self, value: str) -> None:
        if value not in STRATEGIES:
            raise ValueError(f"unknown strategy {value!r}; use one of {', '.join(STRATEGIES)}")
        with self._lock:
            self._value = value


class Stats:
    FIELDS = ("received", "handled", "skipped_not_stuck", "executed", "executor_rejected",
              "llm_calls", "parse_failures", "fallback_escalations", "errors", "dead_lettered",
              "verifier_approved", "verifier_rejected", "verifier_escalations",
              "memory_hits", "memory_rejected", "memory_stored", "event_write_failures")

    def __init__(self):
        self._lock = threading.Lock()
        self._c = {k: 0 for k in self.FIELDS}
        self.last_error = ""

    def inc(self, key: str, n: int = 1) -> None:
        with self._lock:
            self._c[key] += n

    def snapshot(self) -> dict:
        with self._lock:
            return {**self._c, "last_error": self.last_error}


class Consumer:
    def __init__(self, r: redis.Redis, wallet: WalletClient, planner: Planner, decisions: DecisionLog,
                 strategy: StrategyHolder, stream: str = "stuck-sagas", group: str = "recovery",
                 name: str = "recovery-1", claim_idle_ms: int = 60_000, max_deliveries: int = 3,
                 memory: Memory | None = None):
        self.r, self.wallet, self.planner, self.decisions = r, wallet, planner, decisions
        self.memory = memory or Memory(r)
        self.strategy = strategy
        self.stream, self.group, self.name = stream, group, name
        self.claim_idle_ms, self.max_deliveries = claim_idle_ms, max_deliveries
        self.stats = Stats()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._last_claim = 0.0
        self._failures: dict = {}  # message id -> failed attempts seen by this process

    # ---------- lifecycle ----------

    def ensure_group(self) -> None:
        try:
            # "$": only messages added after the group exists. MKSTREAM creates the stream if needed.
            self.r.xgroup_create(self.stream, self.group, id="$", mkstream=True)
            log.info("created consumer group %s on %s", self.group, self.stream)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def start(self) -> None:
        self._thread = threading.Thread(target=self.run_forever, name="consumer", daemon=True)
        self._thread.start()

    def stop(self, timeout: float = 10) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout)

    @property
    def running(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    def run_forever(self) -> None:
        while not self._stop.is_set():
            try:
                self.ensure_group()
                self.drain_own_pending()
                break
            except redis.RedisError as e:
                log.warning("redis not ready (%s); retrying", e)
                self._stop.wait(2)
        while not self._stop.is_set():
            try:
                self.poll_once(block_ms=5000)
            except redis.RedisError as e:
                self.stats.last_error = f"redis: {e}"
                log.warning("redis error: %s", e)
                self._stop.wait(2)
            except Exception as e:  # never let the thread die
                self.stats.last_error = f"{type(e).__name__}: {e}"
                log.exception("unexpected error")
                self._stop.wait(1)

    # ---------- reading ----------

    def drain_own_pending(self) -> int:
        """After a restart: handle messages this consumer read but never acknowledged."""
        n = 0
        while True:
            resp = self.r.xreadgroup(self.group, self.name, {self.stream: "0"}, count=10)
            msgs = resp[0][1] if resp else []
            msgs = [(mid, f) for mid, f in msgs if f]  # deleted entries come back empty
            if not msgs:
                return n
            for mid, fields in msgs:
                self.process(mid, fields)
                n += 1

    def claim_abandoned(self) -> int:
        """Take over messages another (dead) consumer left pending for too long."""
        n = 0
        start = "0-0"
        while True:
            res = self.r.xautoclaim(self.stream, self.group, self.name, self.claim_idle_ms, start_id=start, count=10)
            start, msgs = res[0], res[1]
            for mid, fields in msgs:
                if fields:
                    self.process(mid, fields)
                    n += 1
            if start in ("0-0", b"0-0") or not msgs:
                return n

    def poll_once(self, block_ms: int = 5000) -> int:
        """Handle at most one new message (and, every 30s, abandoned ones). Returns messages handled."""
        if self.strategy.get() == "off":
            self._stop.wait(min(block_ms, 1000) / 1000)
            return 0
        n = 0
        if time.monotonic() - self._last_claim > 30:
            self._last_claim = time.monotonic()
            n += self.claim_abandoned()
        resp = self.r.xreadgroup(self.group, self.name, {self.stream: ">"}, count=1, block=block_ms)
        for _, msgs in resp or []:
            for mid, fields in msgs:
                self.process(mid, fields)
                n += 1
        return n

    # ---------- handling ----------

    def _deliveries(self, mid) -> int:
        """How many times this message was tried: Redis' delivery counter, or our own count."""
        local = self._failures.get(mid, 1)
        try:
            info = self.r.xpending_range(self.stream, self.group, min=mid, max=mid, count=1)
            remote = int(info[0].get("times_delivered", 0)) if info else 0
        except redis.RedisError:
            remote = 0
        return max(local, remote)

    def process(self, mid, fields: dict) -> None:
        self.stats.inc("received")
        saga_id = _s(fields.get(b"saga_id") or fields.get("saga_id"))
        if not saga_id:
            self.r.xack(self.stream, self.group, mid)
            return
        try:
            self.handle(saga_id)
            self.r.xack(self.stream, self.group, mid)
            self._failures.pop(mid, None)
        except (WalletError, LLMError) as e:
            self._failed(mid, saga_id, e)
        except Exception as e:  # a bug: same treatment, but log the stack
            log.exception("handling %s failed", saga_id)
            self._failed(mid, saga_id, e)

    def _failed(self, mid, saga_id: str, err: Exception) -> None:
        self.stats.inc("errors")
        self.stats.last_error = f"{saga_id}: {err}"
        self._failures[mid] = self._failures.get(mid, 0) + 1
        deliveries = self._deliveries(mid)
        log.warning("saga %s failed (delivery %d/%d): %s", saga_id, deliveries, self.max_deliveries, err)
        if deliveries < self.max_deliveries:
            return  # stays pending; claimed again after CLAIM_IDLE_MS
        # Give up: hand it to a human so it is not silently lost.
        strategy = self.strategy.get()
        plan = escalate(f"recovery service failed {deliveries} times: {str(err)[:200]}")
        executed, status_code = False, 0
        try:
            res = self.wallet.execute(saga_id, plan.to_executor(actor=strategy))
            executed, status_code = res.ok, res.status_code
        except WalletError:
            pass
        self.decisions.write(saga_id=saga_id, strategy=strategy, source="dead_letter",
                             proposed=plan.model_dump(), verdict="not_checked", executed=executed,
                             executor_status=status_code, error=str(err)[:300])
        self.stats.inc("dead_lettered")
        self.r.xack(self.stream, self.group, mid)
        self._failures.pop(mid, None)

    def handle(self, saga_id: str) -> dict | None:
        """Decide and execute one action for one stuck transfer. Returns the decision record."""
        strategy = self.strategy.get()
        f = facts_mod.gather(self.wallet, saga_id)
        if f["status"] != "STUCK":
            self.stats.inc("skipped_not_stuck")
            return None
        if strategy == "llm_verified_memory":
            return self._handle_verified(saga_id, f, strategy)
        return self._handle_unverified(saga_id, f, strategy)

    # ---------- audit events (stored in the wallet's event store) ----------

    def _event(self, saga_id: str, event_type: str, **data) -> None:
        if not self.wallet.post_event(saga_id, event_type, data):
            self.stats.inc("event_write_failures")

    def _count_llm(self, outcome) -> None:
        self.stats.inc("llm_calls", outcome.attempts)
        self.stats.inc("parse_failures", outcome.parse_failures)
        if outcome.fallback:
            self.stats.inc("fallback_escalations")

    def _execute(self, saga_id: str, plan: Plan, strategy: str, source: str):
        res = self.wallet.execute(saga_id, plan.to_executor(actor=strategy))
        self.stats.inc("executed" if res.ok else "executor_rejected")
        self.stats.inc("handled")
        self._event(saga_id, "RecoveryExecuted", action=plan.action, source=source, actor=strategy,
                    executor_status=res.status_code, ok=res.ok,
                    final_status=res.body.get("status") if res.ok else None,
                    error=None if res.ok else res.body.get("error"))
        return res

    # ---------- strategy: llm_no_verifier ----------

    def _handle_unverified(self, saga_id: str, f: dict, strategy: str) -> dict:
        outcome = self.planner.plan(f)
        self._count_llm(outcome)
        plan = outcome.plan
        self._event(saga_id, "RecoveryProposed", source="llm", attempt=1, actor=strategy,
                    plan=plan.model_dump(exclude_none=True))
        res = self._execute(saga_id, plan, strategy, "llm")
        return self._log(saga_id, strategy, f, source="llm", first=plan, final=plan, verdict="not_checked",
                         res=res, llm=[outcome])

    # ---------- strategy: llm_verified_memory ----------

    def _handle_verified(self, saga_id: str, f: dict, strategy: str) -> dict:
        key = key_for(f)
        rejections: list[dict] = []
        llm_outcomes = []
        first: Plan | None = None
        final: Plan | None = None
        source = ""

        def check(plan: Plan, src: str, attempt: int) -> bool:
            nonlocal first
            first = first or plan
            self._event(saga_id, "RecoveryProposed", source=src, attempt=attempt, actor=strategy,
                        plan=plan.model_dump(exclude_none=True), memory_key=key)
            v = verifier.verify(plan, f)
            if v.ok:
                self.stats.inc("verifier_approved")
                self._event(saga_id, "RecoveryVerified", source=src, attempt=attempt, action=plan.action)
            else:
                self.stats.inc("verifier_rejected")
                rejections.append({"source": src, "action": plan.action, "reasons": v.reasons})
                self._event(saga_id, "RecoveryRejected", source=src, attempt=attempt, action=plan.action,
                            reasons=v.reasons, checks=v.checks)
            return v.ok

        # 1. Memory first: a fix that worked before for the same kind of failure.
        remembered = self.memory.get(key)
        if remembered is not None:
            self.stats.inc("memory_hits")
            if check(remembered, "memory", 0):
                final, source = remembered, "memory"
            else:
                self.stats.inc("memory_rejected")

        # 2. Ask the AI; if the verifier rejects, ask once more with the reason.
        if final is None:
            out = self.planner.plan(f)
            llm_outcomes.append(out)
            self._count_llm(out)
            if check(out.plan, "llm", 1):
                final, source = out.plan, "llm"
            else:
                reasons = "; ".join(rejections[-1]["reasons"])
                feedback = (f"The safety checker REJECTED your choice {out.plan.action!r}: {reasons}. "
                            "Choose a different action that follows the bank rules.")
                out2 = self.planner.plan(f, feedback=feedback)
                llm_outcomes.append(out2)
                self._count_llm(out2)
                if check(out2.plan, "llm_retry", 2):
                    final, source = out2.plan, "llm_retry"

        # 3. Two rejections: hand it to a human.
        if final is None:
            self.stats.inc("verifier_escalations")
            summary = " | ".join(f"{r['action']}: {'; '.join(r['reasons'])}" for r in rejections)
            final = escalate(("safety checker rejected the AI's plans: " + summary)[:1000])
            source = "verifier_escalation"
            check(final, source, 3)

        res = self._execute(saga_id, final, strategy, source)

        # 4. Learn: remember an AI fix only if it worked AND the books are clean afterwards.
        stored = False
        if res.ok and source in ("llm", "llm_retry") and res.body.get("status") in GOOD_END_STATES:
            try:
                if self.wallet.invariants(saga_id).get("ok"):
                    stored = self.memory.put(key, final, saga_id)
            except WalletError:
                pass
            if stored:
                self.stats.inc("memory_stored")

        if not rejections:
            verdict = "approved"
        elif source == "verifier_escalation":
            verdict = "rejected_escalated"
        else:
            verdict = "approved_after_rejection"
        return self._log(saga_id, strategy, f, source=source, first=first, final=final, verdict=verdict,
                         res=res, llm=llm_outcomes, rejections=rejections, memory_key=key,
                         memory_hit=remembered is not None, stored_in_memory=stored)

    # ---------- decision log ----------

    def _log(self, saga_id, strategy, f, *, source, first, final, verdict, res, llm,
             rejections=None, memory_key=None, memory_hit=False, stored_in_memory=False) -> dict:
        return self.decisions.write(
            saga_id=saga_id, strategy=strategy, source=source,
            step=f["step"], last_error=f["last_error"][:300], rail=f.get("rail"),
            proposed=first.model_dump(exclude_none=True) if first else None,
            final=final.model_dump(exclude_none=True), verdict=verdict,
            rejections=rejections or [],
            executed=res.ok, executor_status=res.status_code,
            final_status=res.body.get("status") if res.ok else None,
            executor_error=None if res.ok else res.body.get("error"),
            memory_key=memory_key, memory_hit=memory_hit, stored_in_memory=stored_in_memory,
            latency_ms=sum(o.latency_ms for o in llm), prompt_tokens=sum(o.prompt_tokens for o in llm),
            completion_tokens=sum(o.completion_tokens for o in llm), attempt=sum(o.attempts for o in llm),
            parse_failures=sum(o.parse_failures for o in llm), fallback=any(o.fallback for o in llm),
            model=getattr(self.planner.llm, "model", ""),
        )


def _s(v) -> str:
    return v.decode() if isinstance(v, bytes) else (v or "")
