"""Small HTTP client for the wallet service. The executor is the only way to change anything."""
from dataclasses import dataclass

import httpx


class WalletError(Exception):
    """The wallet service could not be reached or failed (5xx). Worth retrying later."""


@dataclass
class ExecResult:
    ok: bool
    status_code: int
    body: dict

    @property
    def not_actionable(self) -> bool:  # transfer no longer STUCK (someone fixed it)
        return self.status_code == 409


class WalletClient:
    def __init__(self, base_url: str, timeout_s: float = 30.0, transport: httpx.BaseTransport | None = None):
        # Executing a retry can take a few seconds (rail timeouts), hence the generous timeout.
        self._http = httpx.Client(base_url=base_url, timeout=timeout_s, transport=transport)

    def close(self) -> None:
        self._http.close()

    def _get(self, path: str):
        try:
            r = self._http.get(path)
        except httpx.HTTPError as e:
            raise WalletError(f"GET {path}: {e}") from e
        if r.status_code >= 500:
            raise WalletError(f"GET {path}: {r.status_code} {r.text[:200]}")
        r.raise_for_status()
        return r.json()

    def transfer(self, saga_id: str) -> dict:
        return self._get(f"/transfers/{saga_id}")

    def money(self, saga_id: str) -> dict:
        return self._get(f"/transfers/{saga_id}/money")

    def rail(self, saga_id: str) -> list[dict]:
        return self._get(f"/transfers/{saga_id}/rail")

    def balance(self, wallet_id: str) -> int:
        return int(self._get(f"/wallets/{wallet_id}")["balance_paise"])

    def invariants(self, saga_id: str) -> dict:
        """The wallet's own invariant check for one transfer (money conserved, ledger matches rail)."""
        return self._get(f"/admin/invariants?saga_id={saga_id}")

    def post_event(self, saga_id: str, event_type: str, data: dict) -> bool:
        """Append a RecoveryProposed/Verified/Rejected/Executed event to the audit trail."""
        try:
            r = self._http.post(f"/transfers/{saga_id}/recovery-events", json={"type": event_type, "data": data})
        except httpx.HTTPError:
            return False
        return r.status_code == 201

    def stuck_transfers(self, limit: int = 500) -> list[dict]:
        return self._get(f"/transfers?status=STUCK&limit={limit}")

    def execute(self, saga_id: str, body: dict) -> ExecResult:
        try:
            r = self._http.post(f"/transfers/{saga_id}/actions", json=body)
        except httpx.HTTPError as e:
            raise WalletError(f"execute {saga_id}: {e}") from e
        if r.status_code >= 500:
            raise WalletError(f"execute {saga_id}: {r.status_code} {r.text[:200]}")
        try:
            data = r.json()
        except ValueError:
            data = {"raw": r.text[:500]}
        return ExecResult(ok=r.status_code == 200, status_code=r.status_code, body=data)
