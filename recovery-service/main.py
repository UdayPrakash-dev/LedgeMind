"""Recovery service: reads stuck transfers, asks the AI for a fix, applies it through the executor."""
import logging
from contextlib import asynccontextmanager

import redis
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from config import Settings
from consumer import STRATEGIES, Consumer, StrategyHolder
import facts as facts_mod
import verifier
from actions import Plan
from decisions import DecisionLog
from memory import Memory
from planner import LLM, LLMError, OpenAICompatLLM, Planner
from wallet import WalletClient, WalletError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logging.getLogger("httpx").setLevel(logging.WARNING)  # request lines are noisy (and include URLs only)
log = logging.getLogger("recovery")


class StrategyBody(BaseModel):
    strategy: str


class VerifyBody(BaseModel):
    transfer_id: str
    action: str
    amount_paise: int | None = None
    rail: str | None = None
    reason: str = "manual check"


def create_app(settings: Settings | None = None, llm: LLM | None = None,
               redis_client: redis.Redis | None = None, wallet: WalletClient | None = None) -> FastAPI:
    s = settings or Settings()
    host, port = s.redis_host_port
    r = redis_client or redis.Redis(host=host, port=port, socket_timeout=10, socket_connect_timeout=3)
    w = wallet or WalletClient(s.wallet_url)
    model = llm or OpenAICompatLLM(s.llm_base_url, s.llm_model, s.llm_api_key,
                                   timeout_s=s.llm_timeout_s, min_interval_s=s.llm_min_interval_s,
                                   max_tokens=s.llm_max_tokens, reasoning_effort=s.llm_reasoning_effort)
    strategy = StrategyHolder(s.strategy)
    decisions = DecisionLog(s.decisions_path)
    memory = Memory(r)
    consumer = Consumer(r, w, Planner(model), decisions, strategy, stream=s.stream, group=s.group,
                        name=s.consumer, claim_idle_ms=s.claim_idle_ms, max_deliveries=s.max_deliveries,
                        memory=memory)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        log.info("strategy=%s model=%s llm_key_set=%s consumer=%s",
                 strategy.get(), model.model, bool(s.llm_api_key), s.consumer)
        if not s.llm_api_key:
            log.warning("LLM_API_KEY is empty: AI calls will fail unless the endpoint needs no key")
        if s.start_consumer:
            consumer.start()
        yield
        consumer.stop()

    app = FastAPI(title="LedgeMind recovery service", lifespan=lifespan)
    app.state.consumer = consumer

    @app.get("/health")
    def health():
        try:
            r.ping()
            redis_ok = "ok"
        except redis.RedisError:
            redis_ok = "down"
        return {
            "service": "recovery",
            "status": "ok" if redis_ok == "ok" else "degraded",
            "strategy": strategy.get(),
            "llm_model": model.model,
            "llm_key_set": bool(s.llm_api_key),  # never the key itself
            "consumer_running": consumer.running,
            "redis": redis_ok,
        }

    @app.get("/admin/strategy")
    def get_strategy():
        return {"strategy": strategy.get(), "available": list(STRATEGIES)}

    @app.post("/admin/strategy")
    def set_strategy(body: StrategyBody):
        try:
            strategy.set(body.strategy)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        log.info("strategy switched to %s", body.strategy)
        return {"strategy": strategy.get()}

    @app.get("/admin/stats")
    def stats():
        return {"strategy": strategy.get(), "model": model.model, **consumer.stats.snapshot()}

    @app.get("/admin/memory")
    def memory_list():
        return memory.all()

    @app.post("/admin/memory/clear")
    def memory_clear():
        """The benchmark calls this before every run, so no run gets a head start."""
        return {"deleted": memory.clear()}

    @app.post("/admin/verify")
    def verify_dry_run(body: VerifyBody):
        """Ask the verifier about a plan for a real transfer WITHOUT executing it (for demos and debugging)."""
        try:
            plan = Plan(action=body.action, amount_paise=body.amount_paise, rail=body.rail, reason=body.reason)
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        try:
            f = facts_mod.gather(w, body.transfer_id)
        except WalletError as e:
            raise HTTPException(status_code=502, detail=str(e))
        if f["status"] != "STUCK":
            raise HTTPException(status_code=409, detail=f"transfer is {f['status']}, not STUCK")
        v = verifier.verify(plan, f)
        return {"allowed": v.ok, "reasons": v.reasons, "checks": v.checks,
                "facts": {k: f.get(k) for k in ("step", "rail", "ledger", "balances")}}

    @app.post("/admin/sweep")
    def sweep():
        """Handle every currently STUCK transfer now (safety net if a stream message was missed)."""
        if strategy.get() == "off":
            raise HTTPException(status_code=409, detail="strategy is off")
        handled, failed = 0, 0
        try:
            stuck = w.stuck_transfers()
        except WalletError as e:
            raise HTTPException(status_code=502, detail=str(e))
        for t in stuck:
            try:
                if consumer.handle(t["id"]) is not None:
                    handled += 1
            except (WalletError, LLMError) as e:
                failed += 1
                consumer.stats.last_error = f"{t['id']}: {e}"
        return {"stuck_found": len(stuck), "handled": handled, "failed": failed}

    return app


app = create_app()  # uvicorn main:app
