"""Collect everything the AI may see about one stuck transfer, as one JSON-ready dict.

Both AI strategies get exactly these facts, so the comparison is fair.
"""
from wallet import WalletClient

HISTORY_LIMIT = 5


def _short(entry: dict) -> dict:
    keep = ("step", "attempt", "outcome", "error", "action", "actor", "rail_ref")
    return {k: entry[k] for k in keep if entry.get(k) not in (None, "", 0)}


def gather(wallet: WalletClient, saga_id: str) -> dict:
    t = wallet.transfer(saga_id)
    history = t.get("history") or []
    last_error = next((h.get("error") for h in reversed(history) if h.get("error")), "")

    facts = {
        "transfer_id": t["id"],
        "status": t["status"],
        "step": t["current_step"],
        "amount_paise": t["amount_paise"],
        "last_error": last_error,
        "recent_history": [_short(h) for h in history[-HISTORY_LIMIT:]],
        "recovery_actions_so_far": sum(1 for h in history if h.get("action")),
    }
    if t["status"] != "STUCK":
        return facts  # nothing to decide; the caller skips it

    facts["ledger"] = wallet.money(saga_id)  # debited / credited / refunded / reversed
    facts["rail"] = [{"rail_ref": r["rail_ref"], "state": r["state"]} for r in wallet.rail(saga_id)]
    return facts
