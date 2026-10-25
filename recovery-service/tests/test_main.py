import fakeredis
import respx
from fastapi.testclient import TestClient

from conftest import WALLET, FakeLLM, mock_wallet
from config import Settings
from main import create_app


def client(monkeypatch, key="sk-very-secret-123"):
    monkeypatch.setenv("LLM_API_KEY", key)
    monkeypatch.setenv("START_CONSUMER", "false")
    monkeypatch.setenv("WALLET_URL", WALLET)
    app = create_app(Settings(), llm=FakeLLM(), redis_client=fakeredis.FakeRedis())
    return TestClient(app)


def test_health_never_shows_the_key(monkeypatch):
    with client(monkeypatch) as c:
        r = c.get("/health")
        assert r.status_code == 200
        body = r.text
        assert "sk-very-secret-123" not in body
        assert r.json()["llm_key_set"] is True and r.json()["redis"] == "ok"
        assert "sk-very-secret-123" not in repr(Settings())  # not even in debug output


def test_strategy_switch(monkeypatch):
    with client(monkeypatch) as c:
        assert c.post("/admin/strategy", json={"strategy": "off"}).json()["strategy"] == "off"
        assert c.post("/admin/strategy", json={"strategy": "llm_no_verifier"}).status_code == 200
        assert c.post("/admin/strategy", json={"strategy": "llm_verified_memory"}).status_code == 200
        assert c.post("/admin/strategy", json={"strategy": "nope"}).status_code == 400
        assert c.get("/admin/strategy").json()["strategy"] == "llm_verified_memory"


def test_memory_endpoints(monkeypatch):
    with client(monkeypatch) as c:
        assert c.get("/admin/memory").json() == {}
        assert c.post("/admin/memory/clear").json() == {"deleted": 0}


def test_stats(monkeypatch):
    with client(monkeypatch) as c:
        s = c.get("/admin/stats").json()
        assert s["handled"] == 0 and s["model"] == "fake-model"


@respx.mock
def test_verify_dry_run(monkeypatch):
    respx.route(host="testserver").pass_through()
    execute = mock_wallet(respx.mock, rail_state="PAID")
    with client(monkeypatch) as c:
        bad = c.post("/admin/verify", json={"transfer_id": "s-1", "action": "compensate"}).json()
        assert bad["allowed"] is False and "PAID" in bad["reasons"][0]
        good = c.post("/admin/verify", json={"transfer_id": "s-1", "action": "retry_step"}).json()
        assert good["allowed"] is True
        assert c.post("/admin/verify", json={"transfer_id": "s-1", "action": "nuke"}).status_code == 400
    assert not execute.called  # dry run: nothing executed
