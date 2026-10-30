"""Experiment switches for the paper's ablations. All off by default; set at runtime via /admin/ablation.

B. hide_rail: the AI is NOT shown the rail's state (it only sees the error text, ledger and balances).
   The verifier still checks the real rail state, so this measures how much the verifier saves
   when the AI has to guess.

C. inject_rate: with this probability, a plan the AI returns is replaced by a WRONG action
   (a different, non-escalate action chosen at random). Simulates a model that makes mistakes,
   so we can count how many reach the bank with and without the verifier. Seeded, so repeatable.

use_memory=False turns resolution memory off (isolates the verifier from the memory effect).
"""
import random
import threading
from dataclasses import dataclass

from actions import Plan

HIDDEN_RAIL = "hidden: you cannot see the payment rail's state in this run"
WRONG_CHOICES = ("retry_step", "compensate", "reroute", "partial_refund")  # escalate is never "wrong"


@dataclass
class AblationConfig:
    hide_rail: bool = False
    inject_rate: float = 0.0
    use_memory: bool = True
    seed: int = 0

    def label(self) -> str:
        parts = []
        if self.hide_rail:
            parts.append("hide_rail")
        if self.inject_rate > 0:
            parts.append(f"inject{int(round(self.inject_rate * 100))}")
        if not self.use_memory:
            parts.append("no_memory")
        return "+".join(parts) or "main"


class Ablation:
    def __init__(self, config: AblationConfig | None = None):
        self._lock = threading.Lock()
        self.set(config or AblationConfig())

    def set(self, config: AblationConfig) -> None:
        if not 0.0 <= config.inject_rate <= 1.0:
            raise ValueError("inject_rate must be between 0 and 1")
        with self._lock:
            self.config = config
            self._rng = random.Random(config.seed)  # restarts the injection sequence

    def get(self) -> AblationConfig:
        with self._lock:
            return self.config

    def facts_for_ai(self, facts: dict) -> dict:
        """What the AI sees. The verifier and memory always use the full facts."""
        if not self.get().hide_rail or "rail" not in facts:
            return facts
        return {**facts, "rail": HIDDEN_RAIL}

    def maybe_inject(self, plan: Plan, facts: dict) -> tuple[Plan, bool]:
        """Returns (plan to use, injected?)."""
        with self._lock:
            if self.config.inject_rate <= 0 or self._rng.random() >= self.config.inject_rate:
                return plan, False
            choices = [a for a in WRONG_CHOICES if a != plan.action]
            action = self._rng.choice(choices)
        amount = facts.get("amount_paise") or 2
        wrong = Plan(action=action, reason=f"INJECTED ERROR (ablation C); AI had chosen {plan.action}",
                     amount_paise=max(1, amount // 2) if action == "partial_refund" else None,
                     rail="backup" if action == "reroute" else None)
        return wrong, True
