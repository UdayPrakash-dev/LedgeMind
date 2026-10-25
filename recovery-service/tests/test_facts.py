import respx

import facts
from conftest import WALLET, mock_wallet
from wallet import WalletClient, WalletError


@respx.mock
def test_gather_collects_everything():
    mock_wallet(respx.mock, rail_state="PAID", history=[
        {"step": "DEBIT_SENDER", "attempt": 1, "outcome": "ok"},
        {"step": "CALL_RAIL", "attempt": 1, "outcome": "error", "error": "payment rail returned 409: DUPLICATE_TXN"},
        {"step": "CALL_RAIL", "outcome": "ok", "action": "retry_step", "actor": "ai"},
    ])
    f = facts.gather(WalletClient(WALLET), "s-1")
    assert f["step"] == "CALL_RAIL" and f["amount_paise"] == 1000
    assert f["last_error"].startswith("payment rail returned 409")
    assert f["ledger"]["debited"] is True and f["ledger"]["credited"] is False
    assert f["rail"] == [{"rail_ref": "s-1", "state": "PAID"}]
    assert f["recovery_actions_so_far"] == 1
    assert len(f["recent_history"]) == 3


@respx.mock
def test_gather_keeps_only_last_5_history_entries():
    hist = [{"step": "CALL_RAIL", "attempt": i, "outcome": "error", "error": f"e{i}"} for i in range(1, 9)]
    mock_wallet(respx.mock, history=hist)
    f = facts.gather(WalletClient(WALLET), "s-1")
    assert [h["attempt"] for h in f["recent_history"]] == [4, 5, 6, 7, 8]
    assert f["last_error"] == "e8"


@respx.mock
def test_gather_skips_extra_calls_when_not_stuck():
    respx.get(f"{WALLET}/transfers/s-1").respond(json={
        "id": "s-1", "status": "COMPLETED", "current_step": "DONE", "amount_paise": 5, "history": []})
    f = facts.gather(WalletClient(WALLET), "s-1")
    assert f["status"] == "COMPLETED" and "rail" not in f


@respx.mock
def test_wallet_down_raises_retryable_error():
    respx.get(f"{WALLET}/transfers/s-1").respond(503, text="db unreachable")
    try:
        facts.gather(WalletClient(WALLET), "s-1")
    except WalletError:
        return
    raise AssertionError("expected WalletError")
