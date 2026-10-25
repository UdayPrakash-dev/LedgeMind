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
from actions import escalate
from decisions import DecisionLog
from planner import LLMError, Planner
from wallet import WalletClient, WalletError

log = logging.getLogger("recovery.consumer")

STRATEGIES = ("off", "llm_no_verifier")          # Day 7 adds "llm_verified_memory"
FUTURE_STRATEGIES = ("llm_verified_memory",)


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
        if value in FUTURE_STRATEGIES:
            raise ValueError(f"strategy {value!r} arrives on Day 7")
        if value not in STRATEGIES:
            raise ValueError(f"unknown strategy {value!r}; use one of {', '.join(STRATEGIES)}")
        with self._lock:
            self._value = value


class Stats:
    FIELDS = ("received", "handled", "skipped_not_stuck", "executed", "executor_rejected",
              "llm_calls", "parse_failures", "fallback_escalations", "errors", "dead_lettered")

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
                 name: str = "recovery-1", claim_idle_ms: int = 60_000, max_deliveries: int = 3):
        self.r, self.wallet, self.planner, self.decisions = r, wallet, planner, decisions
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

        outcome = self.planner.plan(f)
        self.stats.inc("llm_calls", outcome.attempts)
        self.stats.inc("parse_failures", outcome.parse_failures)
        if outcome.fallback:
            self.stats.inc("fallback_escalations")

        plan = outcome.plan
        res = self.wallet.execute(saga_id, plan.to_executor(actor=strategy))
        self.stats.inc("executed" if res.ok else "executor_rejected")
        self.stats.inc("handled")

        return self.decisions.write(
            saga_id=saga_id, strategy=strategy, source="llm",
            step=f["step"], last_error=f["last_error"][:300], rail=f.get("rail"),
            proposed=plan.model_dump(exclude_none=True), verdict="not_checked",
            executed=res.ok, executor_status=res.status_code,
            final_status=res.body.get("status") if res.ok else None,
            executor_error=None if res.ok else res.body.get("error"),
            latency_ms=outcome.latency_ms, prompt_tokens=outcome.prompt_tokens,
            completion_tokens=outcome.completion_tokens, attempt=outcome.attempts,
            parse_failures=outcome.parse_failures, fallback=outcome.fallback,
            model=getattr(self.planner.llm, "model", ""),
        )


def _s(v) -> str:
    return v.decode() if isinstance(v, bytes) else (v or "")
