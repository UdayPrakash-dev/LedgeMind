"""Ask an LLM for one recovery action and parse it strictly.

Works with any OpenAI-compatible chat API (Groq, OpenAI, Ollama, vLLM).
"""
import json
import threading
import time
from dataclasses import dataclass, field
from typing import Protocol

import httpx
from pydantic import ValidationError

from actions import Plan, escalate

SYSTEM_PROMPT = """You repair stuck money transfers in a digital wallet ledger.

A transfer has 3 steps: DEBIT_SENDER -> CALL_RAIL (external payment rail) -> CREDIT_RECEIVER.
It is STUCK because a step failed. You get facts about it as JSON and choose ONE action.

Actions:
- retry_step: run the failed step again. Safe for the rail: a repeated payment is never charged twice;
  if the rail already paid, the retry just confirms it and the transfer completes.
- compensate: undo the transfer: take back any credit given to the receiver, refund the sender.
- reroute: pay again through a backup rail under a NEW payment id (field "rail": "backup").
- partial_refund: refund only part of the amount to the sender (field "amount_paise").
- escalate: hand the transfer to a human.

Bank rules:
1. The "rail" facts are the truth about whether money left the bank. "PAID" means it left;
   "REJECTED", "REVERSED" or "NONE" mean it did not (or came back).
2. Never refund the sender (compensate / partial_refund) if any rail payment is PAID: the money
   would leave twice. Never reroute if any rail payment is PAID: the rail would pay twice.
3. If you are unsure, or 3 or more recovery actions were already tried, choose escalate.

Reply with ONLY one JSON object, no other text:
{"action": "<one of the 5 actions>", "reason": "<one short sentence>"}
Add "amount_paise" only for partial_refund and "rail" only for reroute.

Examples (facts shortened):
1. step CALL_RAIL, last_error "payment rail unreachable: ... Client.Timeout exceeded", rail [PAID]
   -> {"action": "retry_step", "reason": "Rail timed out but already paid; retrying confirms it and completes the transfer."}
2. step CALL_RAIL, last_error "payment rail returned 422: limit exceeded", rail [REJECTED]
   -> {"action": "compensate", "reason": "Rail rejected the payment, so the sender must be refunded."}
3. step CALL_RAIL, last_error "payment rail returned 409: DUPLICATE_TXN", rail [PAID]
   -> {"action": "retry_step", "reason": "Rail flagged a duplicate but the payment is PAID; retry to finish."}
4. step RECONCILE, last_error "rail reports the payment was REVERSED", ledger credited true, rail [REVERSED]
   -> {"action": "compensate", "reason": "Payment was reversed after completion; take back the credit and refund the sender."}
5. step CALL_RAIL, last_error "payment rail returned 500: ERR_7003: settlement node ack mismatch", rail [NONE]
   -> {"action": "retry_step", "reason": "Unclear rail error and the rail has no record, so retrying is safe."}
"""


class LLMError(Exception):
    """The LLM could not be reached after retries (network, auth, rate limit)."""


@dataclass
class LLMReply:
    text: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    latency_ms: int = 0
    cached_tokens: int = 0  # prompt tokens the provider served from its prompt (KV) cache


def cached_tokens(usage: dict) -> int:
    """Cached prompt tokens, in whichever field the provider uses (0 if it does not report them)."""
    details = usage.get("prompt_tokens_details") or {}
    for v in (details.get("cached_tokens"), usage.get("cached_tokens"), usage.get("prompt_cache_hit_tokens")):
        if isinstance(v, int) and v > 0:
            return v
    return 0


class LLM(Protocol):
    model: str

    def complete(self, messages: list[dict]) -> LLMReply: ...


class OpenAICompatLLM:
    """Minimal client for POST {base_url}/chat/completions, with rate limiting and retries."""

    def __init__(self, base_url: str, model: str, api_key: str, timeout_s: float = 30.0,
                 min_interval_s: float = 2.0, max_retries: int = 4, max_tokens: int = 1024,
                 reasoning_effort: str = "", transport: httpx.BaseTransport | None = None):
        self.model = model
        self.max_tokens = max_tokens
        self.reasoning_effort = reasoning_effort  # sent only if set; dropped if the provider rejects it
        self._url = base_url.rstrip("/") + "/chat/completions"
        self._key = api_key
        self._http = httpx.Client(timeout=timeout_s, transport=transport)
        self._min_interval = min_interval_s
        self._max_retries = max_retries
        self._last_call = 0.0
        self._lock = threading.Lock()
        self.json_mode = True  # turned off automatically if the provider rejects response_format

    def _wait_turn(self) -> None:
        with self._lock:
            wait = self._last_call + self._min_interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            self._last_call = time.monotonic()

    def complete(self, messages: list[dict]) -> LLMReply:
        body = {"model": self.model, "messages": messages, "temperature": 0, "max_tokens": self.max_tokens}
        headers = {"Authorization": f"Bearer {self._key}"} if self._key else {}
        delay = 2.0
        last = "no attempt"
        for attempt in range(1, self._max_retries + 1):
            if self.json_mode:
                body["response_format"] = {"type": "json_object"}
            else:
                body.pop("response_format", None)
            if self.reasoning_effort:
                body["reasoning_effort"] = self.reasoning_effort
            else:
                body.pop("reasoning_effort", None)
            self._wait_turn()
            start = time.monotonic()
            try:
                r = self._http.post(self._url, json=body, headers=headers)
            except httpx.HTTPError as e:
                last = f"network: {type(e).__name__}"
            else:
                latency = int((time.monotonic() - start) * 1000)
                if r.status_code == 200:
                    data = r.json()
                    usage = data.get("usage") or {}
                    choice = data["choices"][0]
                    if not (choice["message"].get("content") or "").strip() and choice.get("finish_reason") == "length":
                        # Reasoning used up max_tokens before any answer: counts as an unusable reply.
                        return LLMReply(text="(no answer: max_tokens reached while reasoning; raise LLM_MAX_TOKENS)",
                                        prompt_tokens=usage.get("prompt_tokens", 0),
                                        completion_tokens=usage.get("completion_tokens", 0), latency_ms=latency,
                                        cached_tokens=cached_tokens(usage))
                    return LLMReply(text=choice["message"]["content"] or "",
                                    prompt_tokens=usage.get("prompt_tokens", 0),
                                    completion_tokens=usage.get("completion_tokens", 0),
                                    latency_ms=latency, cached_tokens=cached_tokens(usage))
                if r.status_code == 400 and self.json_mode and "response_format" in r.text:
                    self.json_mode = False  # provider has no JSON mode; rely on the prompt
                    continue
                if r.status_code == 400 and self.reasoning_effort and "reasoning_effort" in r.text:
                    self.reasoning_effort = ""  # model does not support it
                    continue
                if r.status_code in (401, 403):
                    raise LLMError(f"LLM rejected the API key (HTTP {r.status_code})")
                last = f"HTTP {r.status_code}: {r.text[:200]}"
                if r.status_code == 429:
                    try:
                        delay = max(delay, float(r.headers.get("retry-after", delay)))
                    except ValueError:
                        pass
                elif r.status_code < 500:
                    raise LLMError(last)
            if attempt < self._max_retries:
                time.sleep(delay)
                delay = min(delay * 2, 30)
        raise LLMError(f"LLM failed after {self._max_retries} attempts: {last}")


def parse(text: str) -> Plan:
    """Turn a model reply into a Plan. Accepts ```json fences and text around one JSON object."""
    s = text.strip()
    if s.startswith("```"):
        s = s.strip("`")
        s = s[s.find("\n") + 1:] if "\n" in s else s
    start, end = s.find("{"), s.rfind("}")
    if start == -1 or end <= start:
        raise ValueError(f"no JSON object found in the reply: {s[:120]!r}")
    try:
        data = json.loads(s[start:end + 1])
    except json.JSONDecodeError as e:
        raise ValueError(f"invalid JSON: {e.msg}") from e
    if not isinstance(data, dict):
        raise ValueError("reply must be a JSON object")
    try:
        return Plan.model_validate(data)
    except ValidationError as e:
        problems = "; ".join(f"{'.'.join(map(str, err['loc'])) or 'reply'}: {err['msg']}" for err in e.errors())
        raise ValueError(problems) from e


@dataclass
class PlanOutcome:
    plan: Plan
    attempts: int = 0            # LLM calls made
    parse_failures: int = 0      # replies that did not parse
    fallback: bool = False       # True = both replies were unusable, so we escalated
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cached_tokens: int = 0
    latency_ms: int = 0
    errors: list[str] = field(default_factory=list)


class Planner:
    MAX_ATTEMPTS = 2  # first try + one repair try

    def __init__(self, llm: LLM):
        self.llm = llm

    def messages(self, facts: dict, feedback: str | None = None) -> list[dict]:
        msgs = [{"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": json.dumps(facts, indent=1)}]
        if feedback:
            msgs.append({"role": "user", "content": feedback})
        return msgs

    def plan(self, facts: dict, feedback: str | None = None) -> PlanOutcome:
        """feedback: extra instruction appended to the conversation (Day 7: verifier's rejection reason)."""
        out = PlanOutcome(plan=escalate("no plan yet"))
        extra = feedback
        for _ in range(self.MAX_ATTEMPTS):
            reply = self.llm.complete(self.messages(facts, extra))
            out.attempts += 1
            out.prompt_tokens += reply.prompt_tokens
            out.completion_tokens += reply.completion_tokens
            out.cached_tokens += reply.cached_tokens
            out.latency_ms += reply.latency_ms
            try:
                out.plan = parse(reply.text)
                return out
            except ValueError as e:
                out.parse_failures += 1
                out.errors.append(str(e))
                extra = ((feedback + "\n") if feedback else "") + (
                    f"Your previous reply was not usable ({e}). Reply with ONLY one JSON object like "
                    '{"action": "retry_step", "reason": "..."} using one of: retry_step, compensate, '
                    "reroute, partial_refund, escalate.")
        out.fallback = True
        out.plan = escalate(f"AI reply unusable after {self.MAX_ATTEMPTS} attempts: {out.errors[-1]}")
        return out
