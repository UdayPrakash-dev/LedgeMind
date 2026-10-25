"""The 5 actions the AI may choose. Anything else is rejected while parsing."""
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, model_validator

ActionName = Literal["retry_step", "compensate", "reroute", "partial_refund", "escalate"]
ACTIONS: tuple[str, ...] = ("retry_step", "compensate", "reroute", "partial_refund", "escalate")


class Plan(BaseModel):
    """One recovery action, exactly as the AI must return it."""

    model_config = ConfigDict(extra="ignore")  # tolerate extra keys the model adds

    action: ActionName
    amount_paise: Optional[int] = Field(default=None, gt=0)
    rail: Optional[str] = Field(default=None, max_length=50)
    reason: str = Field(min_length=3, max_length=1000)

    @model_validator(mode="after")
    def _amount_only_for_partial_refund(self) -> "Plan":
        if self.action == "partial_refund" and self.amount_paise is None:
            raise ValueError("partial_refund needs amount_paise")
        if self.action != "partial_refund":
            self.amount_paise = None
        if self.action != "reroute":
            self.rail = None
        return self

    def to_executor(self, actor: str) -> dict:
        """Body for POST /transfers/{id}/actions."""
        body = {"action": self.action, "reason": self.reason, "actor": actor}
        if self.amount_paise is not None:
            body["amount_paise"] = self.amount_paise
        if self.rail:
            body["rail"] = self.rail
        return body


def escalate(reason: str) -> Plan:
    return Plan(action="escalate", reason=reason)
