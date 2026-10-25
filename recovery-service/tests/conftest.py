import os
import sys

# Import the service modules (they live one folder up) and keep tests offline.
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ.setdefault("START_CONSUMER", "false")
os.environ.setdefault("DECISIONS_PATH", os.path.join(os.path.dirname(__file__), ".tmp-decisions.jsonl"))

import pytest  # noqa: E402

from planner import LLMReply  # noqa: E402

WALLET = "http://wallet.test"


class FakeLLM:
    """Returns scripted replies; never touches the network."""

    model = "fake-model"

    def __init__(self, *replies: str):
        self.replies = list(replies)
        self.calls: list[list[dict]] = []

    def complete(self, messages):
        self.calls.append(messages)
        text = self.replies.pop(0) if self.replies else '{"action":"escalate","reason":"script ran out"}'
        return LLMReply(text=text, prompt_tokens=100, completion_tokens=20, latency_ms=5)


def transfer(saga_id="s-1", status="STUCK", step="CALL_RAIL", history=None):
    return {
        "id": saga_id, "status": status, "current_step": step, "amount_paise": 1000, "policy": "escalate",
        "from_wallet": "w-sender", "to_wallet": "w-receiver",
        "history": history if history is not None else [
            {"step": "DEBIT_SENDER", "attempt": 1, "outcome": "ok"},
            {"step": "CALL_RAIL", "attempt": 1, "outcome": "error",
             "error": "payment rail unreachable: context deadline exceeded (Client.Timeout exceeded)"},
        ],
    }


MONEY = {"debited": True, "credited": False, "refunded": False, "reversed": False, "partial_refund_paise": 0}


def mock_wallet(respx_mock, saga_id="s-1", status="STUCK", rail_state="PAID", exec_status=200, history=None,
                step="CALL_RAIL", money=None, final_status="COMPLETED", invariants_ok=True):
    respx_mock.get(f"{WALLET}/transfers/{saga_id}").respond(json=transfer(saga_id, status, step, history=history))
    respx_mock.get(f"{WALLET}/transfers/{saga_id}/money").respond(json=money or MONEY)
    respx_mock.get(f"{WALLET}/transfers/{saga_id}/rail").respond(json=[{"rail_ref": saga_id, "state": rail_state}])
    respx_mock.get(f"{WALLET}/wallets/w-sender").respond(json={"id": "w-sender", "balance_paise": 9000})
    respx_mock.get(f"{WALLET}/wallets/w-receiver").respond(json={"id": "w-receiver", "balance_paise": 0})
    respx_mock.get(f"{WALLET}/admin/invariants").respond(json={"ok": invariants_ok, "violations": []})
    respx_mock.post(f"{WALLET}/transfers/{saga_id}/recovery-events").respond(201, json={"version": 1})
    final = {**transfer(saga_id, final_status, "DONE")} if exec_status == 200 else {"error": "not STUCK"}
    return respx_mock.post(f"{WALLET}/transfers/{saga_id}/actions").respond(exec_status, json=final)


def events_posted(respx_mock, saga_id="s-1") -> list[str]:
    import json as _json
    out = []
    for call in respx_mock.calls:
        if call.request.url.path == f"/transfers/{saga_id}/recovery-events":
            out.append(_json.loads(call.request.content)["type"])
    return out


@pytest.fixture
def decisions_path(tmp_path):
    return str(tmp_path / "decisions.jsonl")
