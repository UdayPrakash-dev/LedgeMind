"""Settings, read once from environment variables. The API key is never printed."""
import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.getenv(name, default).strip()


@dataclass(frozen=True)
class Settings:
    redis_addr: str = field(default_factory=lambda: _env("REDIS_ADDR", "localhost:6379"))
    wallet_url: str = field(default_factory=lambda: _env("WALLET_URL", "http://localhost:8080").rstrip("/"))
    stream: str = field(default_factory=lambda: _env("STUCK_STREAM", "stuck-sagas"))
    group: str = field(default_factory=lambda: _env("CONSUMER_GROUP", "recovery"))
    consumer: str = field(default_factory=lambda: _env("CONSUMER_NAME", "recovery-1"))

    strategy: str = field(default_factory=lambda: _env("STRATEGY", "llm_no_verifier"))

    llm_base_url: str = field(default_factory=lambda: _env("LLM_BASE_URL", "https://generativelanguage.googleapis.com/v1beta/openai").rstrip("/"))
    llm_model: str = field(default_factory=lambda: _env("LLM_MODEL", "gemini-3.5-flash-lite"))
    llm_api_key: str = field(default_factory=lambda: _env("LLM_API_KEY"), repr=False)  # repr=False: never shown
    llm_timeout_s: float = field(default_factory=lambda: float(_env("LLM_TIMEOUT_S", "30")))
    llm_min_interval_s: float = field(default_factory=lambda: float(_env("LLM_MIN_INTERVAL_S", "4.5")))
    # Thinking models spend tokens reasoning before answering.
    llm_max_tokens: int = field(default_factory=lambda: int(_env("LLM_MAX_TOKENS", "1024")))
    llm_reasoning_effort: str = field(default_factory=lambda: _env("LLM_REASONING_EFFORT"))  # "", low, medium, high

    decisions_path: str = field(default_factory=lambda: _env("DECISIONS_PATH", "decisions.jsonl"))
    start_consumer: bool = field(default_factory=lambda: _env("START_CONSUMER", "true").lower() != "false")
    claim_idle_ms: int = field(default_factory=lambda: int(_env("CLAIM_IDLE_MS", "60000")))
    max_deliveries: int = field(default_factory=lambda: int(_env("MAX_DELIVERIES", "3")))

    @property
    def redis_host_port(self) -> tuple[str, int]:
        host, _, port = self.redis_addr.partition(":")
        return host or "localhost", int(port or 6379)
