"""Recovery service. Day 1: a stub that only answers /health.
The AI planner, safety checker and resolution memory arrive on Days 6-7."""
import os

from fastapi import FastAPI

app = FastAPI(title="LedgeMind recovery service")


@app.get("/health")
def health():
    return {
        "service": "recovery",
        "status": "ok",
        "llm_model": os.getenv("LLM_MODEL", ""),
        "llm_key_set": bool(os.getenv("LLM_API_KEY")),
    }
