import fakeredis
from fastapi.testclient import TestClient

from conftest import FakeLLM
from config import Settings
from main import create_app


def client(monkeypatch, key="sk-very-secret-123"):
    monkeypatch.setenv("LLM_API_KEY", key)
    monkeypatch.setenv("START_CONSUMER", "false")
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
        r = c.post("/admin/strategy", json={"strategy": "llm_verified_memory"})
        assert r.status_code == 400 and "Day 7" in r.json()["detail"]
        assert c.post("/admin/strategy", json={"strategy": "nope"}).status_code == 400
        assert c.get("/admin/strategy").json()["strategy"] == "llm_no_verifier"


def test_stats(monkeypatch):
    with client(monkeypatch) as c:
        s = c.get("/admin/stats").json()
        assert s["handled"] == 0 and s["model"] == "fake-model"
