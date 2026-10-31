"""LedgeMind demo console: one page to watch and drive the whole system.

Runs next to the other services (docker compose, port 8090, bound to localhost only).
- /api/status        health of every service + container state + current policy/strategy/experiment switches
- /api/feed          live event feed: transfers changing state, AI decisions, benchmark output, console actions
- /api/proxy/...     pass-through to wallet / rail / recovery (GET and POST)
- /api/containers    restart a container, read its logs (Docker socket; only ledgemind-* containers)
- /api/demo/transfer send transfers that fail in a chosen way
- /api/bench         start / stop the benchmark and stream its output
- /api/results       results tables from bench/results (same logic as bench/aggregate.py)
"""
import json
import os
import struct
import subprocess
import sys
import threading
import time
import uuid
from collections import deque
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

WALLET = os.getenv("WALLET_URL", "http://wallet-service:8080").rstrip("/")
RAIL = os.getenv("RAIL_URL", "http://fake-rail:9000").rstrip("/")
RECOVERY = os.getenv("RECOVERY_URL", "http://recovery-service:8000").rstrip("/")
BENCH_DIR = Path(os.getenv("BENCH_DIR", "/bench"))
RESULTS = BENCH_DIR / "results"
DECISIONS = Path(os.getenv("DECISIONS_PATH", str(RESULTS / "decisions.jsonl")))
DOCKER_SOCK = os.getenv("DOCKER_SOCK", "/var/run/docker.sock")
STATIC = Path(__file__).parent / "static"

SERVICES = {  # container name -> what it is
    "ledgemind-wallet": "Wallet service",
    "ledgemind-rail": "Payment rail",
    "ledgemind-recovery": "Recovery service",
    "ledgemind-postgres": "Postgres",
    "ledgemind-redis": "Redis",
}
UPSTREAM = {"wallet": WALLET, "rail": RAIL, "recovery": RECOVERY}
FAULTS = ("none", "timeout", "reject", "duplicate", "partial", "unknown")

http = httpx.Client(timeout=15)
app = FastAPI(title="LedgeMind console")


# ------------------------------------------------------------------ feed

class Feed:
    """In-memory event list with increasing ids; the page polls for entries after the last id it saw."""

    def __init__(self, size=3000):
        self._items = deque(maxlen=size)
        self._seq = 0
        self._lock = threading.Lock()

    def add(self, source: str, text: str, level: str = "info", **extra) -> None:
        with self._lock:
            self._seq += 1
            self._items.append({"id": self._seq, "at": time.strftime("%H:%M:%S"), "source": source,
                                "level": level, "text": text, **extra})

    def after(self, seq: int, limit: int = 400) -> list:
        with self._lock:
            items = [i for i in self._items if i["id"] > seq]
        return items[-limit:]


feed = Feed()


def get_json(url, default=None, timeout=5):
    try:
        r = http.get(url, timeout=timeout)
        return r.json() if r.status_code < 500 else default
    except (httpx.HTTPError, ValueError):
        return default


# ------------------------------------------------------------------ watchers (background threads)

STATUS_LEVEL = {"STUCK": "warn", "ESCALATED": "warn", "COMPLETED": "good", "COMPENSATED": "good", "FAILED": "bad"}


def transfer_line(t: dict, new: bool) -> str:
    """One sentence about a transfer, including what happened between two polls."""
    hist = t.get("history") or []
    head = f"Transfer {t['id'][-8:]} ({t['amount_paise']} paise)"
    if t["status"] in ("STUCK", "RUNNING"):
        err = next((h.get("error") for h in reversed(hist) if h.get("error")), "")
        return f"{head} is {t['status']} at {t['current_step']}" + (f": {err[:150]}" if err else "")
    acts = [h for h in hist if h.get("action")]
    errs = [h for h in hist if h.get("error") and not h.get("action")]
    if acts:
        a = acts[-1]
        stuck_at = errs[-1]["step"] if errs else a.get("step", "?")
        return (f"{head}: stuck at {stuck_at}, fixed by {a['action']} ({a.get('actor') or '?'})"
                f" -> {t['status']}")
    if errs and t.get("policy") in ("refund", "retry"):
        tries = sum(1 for h in hist if h.get("step") == "CALL_RAIL" and not h.get("action"))
        how = "refunded automatically" if any(h.get("step") == "REFUND_SENDER" for h in hist) else f"{tries} rail tries"
        return f"{head}: rail error ({errs[-1].get('error', '')[:80]}), {how} -> {t['status']}"
    return f"{head} {'finished' if new else 'is now'} {t['status']}"


def watch_transfers():
    """Turn transfer state changes into feed lines (the 60 most recent transfers, every 1.5 s)."""
    seen: dict = {}
    first = True
    while True:
        rows = get_json(f"{WALLET}/transfers?limit=60") or []
        if isinstance(rows, list):
            for t in reversed(rows):
                key = (t["status"], len([h for h in t.get("history") or [] if h.get("action")]))
                old = seen.get(t["id"])
                if old != key and not first:
                    feed.add("bank", transfer_line(t, new=old is None), STATUS_LEVEL.get(t["status"], "info"),
                             saga_id=t["id"])
                seen[t["id"]] = key
            if len(seen) > 2000:
                seen = {t["id"]: seen.get(t["id"]) for t in rows}
        first = False
        time.sleep(1.5)


def describe_decision(d: dict) -> tuple[str, str]:
    final = d.get("final") or {}
    proposed = d.get("proposed") or {}
    rail = ", ".join(r.get("state", "?") for r in d.get("rail") or []) if isinstance(d.get("rail"), list) else "?"
    src = {"llm": "AI", "llm_retry": "AI (2nd try)", "memory": "memory", "verifier_escalation": "verifier",
           "dead_letter": "failure handler"}.get(d.get("source"), d.get("source", "?"))
    sid = (d.get("saga_id") or "")[-8:]
    parts = [f"Transfer {sid}: {src} chose {final.get('action', '?')} [{d.get('strategy')}"
             + (f", {d['ablation']}" if d.get("ablation") not in (None, "main") else "") + "]"]
    if d.get("rejections"):
        rej = "; ".join(f"{r['action']} rejected ({', '.join(r.get('reasons') or [])[:120]})" for r in d["rejections"])
        parts.append(f"verifier: {rej}")
    if proposed and proposed.get("action") != final.get("action") and not d.get("rejections"):
        parts.append(f"first proposal {proposed.get('action')}")
    for i in d.get("injected") or []:
        parts.append(f"INJECTED {i['injected_action']} (AI said {i['ai_action']}): "
                     + ("blocked" if i.get("blocked") else "executed" if i.get("executed") else "not executed"))
    parts.append(f"rail {rail}")
    parts.append(f"-> {d.get('final_status') or d.get('executor_error') or 'not executed'}")
    if d.get("memory_hit"):
        parts.append("memory hit")
    if d.get("attempt"):
        parts.append(f"{d['attempt']} AI call(s), {d.get('latency_ms', 0)} ms")
    level = "bad" if not d.get("executed") else ("warn" if d.get("rejections") or d.get("injected") else "good")
    if d.get("final_status") == "ESCALATED":
        level = "warn"
    return " | ".join(parts), level


def watch_decisions():
    """Tail decisions.jsonl (written by the recovery service) into the feed."""
    pos = DECISIONS.stat().st_size if DECISIONS.exists() else 0
    while True:
        try:
            if DECISIONS.exists():
                size = DECISIONS.stat().st_size
                if size < pos:
                    pos = 0  # file was replaced
                if size > pos:
                    with DECISIONS.open(encoding="utf-8") as f:
                        f.seek(pos)
                        chunk = f.read()
                        pos = f.tell()
                    for line in chunk.splitlines():
                        try:
                            d = json.loads(line)
                        except ValueError:
                            continue
                        text, level = describe_decision(d)
                        feed.add("ai", text, level, saga_id=d.get("saga_id"))
        except OSError:
            pass
        time.sleep(1)


threading.Thread(target=watch_transfers, daemon=True).start()
threading.Thread(target=watch_decisions, daemon=True).start()
feed.add("console", "Console started. Watching transfers and AI decisions.")


# ------------------------------------------------------------------ docker

def docker() -> httpx.Client:
    if not os.path.exists(DOCKER_SOCK):
        raise HTTPException(503, "Docker socket not mounted: container controls are off")
    return httpx.Client(transport=httpx.HTTPTransport(uds=DOCKER_SOCK), base_url="http://docker", timeout=60)


def containers() -> dict:
    try:
        with docker() as d:
            rows = d.get("/containers/json", params={"all": "true"}).json()
    except (HTTPException, httpx.HTTPError):
        return {}
    out = {}
    for c in rows:
        for n in c.get("Names", []):
            n = n.lstrip("/")
            if n in SERVICES:
                out[n] = {"state": c.get("State"), "status": c.get("Status")}
    return out


def demux(raw: bytes) -> str:
    """Docker log stream without a TTY: 8-byte header (stream, 0,0,0, size) before each chunk."""
    out, i = [], 0
    while i + 8 <= len(raw):
        size = struct.unpack(">I", raw[i + 4:i + 8])[0]
        out.append(raw[i + 8:i + 8 + size])
        i += 8 + size
    text = b"".join(out) if out else raw
    return text.decode("utf-8", errors="replace")


@app.post("/api/containers/{name}/restart")
def restart(name: str):
    if name not in SERVICES:
        raise HTTPException(404, "only the LedgeMind service containers can be restarted here")
    feed.add("console", f"Restarting {SERVICES[name]} ({name})...", "warn")
    with docker() as d:
        r = d.post(f"/containers/{name}/restart", params={"t": 5})
    if r.status_code not in (204, 304):
        feed.add("console", f"Restart of {name} failed: HTTP {r.status_code} {r.text[:200]}", "bad")
        raise HTTPException(502, r.text[:300])
    note = {"ledgemind-wallet": " Wallet policy is back to its default (refund).",
            "ledgemind-recovery": " Experiment switches are back to off; strategy is back to its default.",
            "ledgemind-rail": " Rail faults and payment records were cleared."}.get(name, "")
    feed.add("console", f"{SERVICES[name]} restarted.{note}", "good")
    return {"restarted": name}


@app.get("/api/containers/{name}/logs")
def logs(name: str, tail: int = 200):
    if name not in SERVICES:
        raise HTTPException(404, "unknown container")
    with docker() as d:
        r = d.get(f"/containers/{name}/logs", params={"stdout": 1, "stderr": 1, "tail": min(tail, 2000)})
    if r.status_code != 200:
        raise HTTPException(502, r.text[:300])
    return {"name": name, "text": demux(r.content)}


# ------------------------------------------------------------------ status

@app.get("/api/status")
def status():
    wallet = get_json(f"{WALLET}/health", {"status": "down"}, timeout=3)
    rail = get_json(f"{RAIL}/health", {"status": "down"}, timeout=3)
    rec = get_json(f"{RECOVERY}/health", {"status": "down"}, timeout=3)
    return {
        "services": {
            "ledgemind-wallet": {"health": (wallet or {}).get("status", "down")},
            "ledgemind-rail": {"health": "ok" if rail and rail.get("status", "ok") in ("ok", True) else "down"},
            "ledgemind-recovery": {"health": (rec or {}).get("status", "down"), "redis": (rec or {}).get("redis")},
            "ledgemind-postgres": {"health": "ok" if (wallet or {}).get("status") == "ok" else "unknown"},
            "ledgemind-redis": {"health": (rec or {}).get("redis", "unknown")},
        },
        "containers": containers(),
        "policy": (get_json(f"{WALLET}/admin/policy", {}, 3) or {}).get("policy"),
        "recovery": rec,
        "ablation": get_json(f"{RECOVERY}/admin/ablation", {}, 3),
        "stats": get_json(f"{RECOVERY}/admin/stats", {}, 3),
        "wallet_stats": get_json(f"{WALLET}/admin/stats", {}, 3),
        "rail_stats": get_json(f"{RAIL}/admin/stats", {}, 3),
        "rail_config": get_json(f"{RAIL}/admin/config", {}, 3),
        "bench": bench.state(),
    }


@app.get("/api/feed")
def get_feed(after: int = 0):
    return feed.after(after)


# ------------------------------------------------------------------ proxy

@app.api_route("/api/proxy/{svc}/{path:path}", methods=["GET", "POST"])
async def proxy(svc: str, path: str, request: Request):
    if svc not in UPSTREAM:
        raise HTTPException(404, "unknown service")
    url = f"{UPSTREAM[svc]}/{path}"
    body = await request.body()
    headers = {"Content-Type": "application/json"}
    if "idempotency-key" in request.headers:
        headers["Idempotency-Key"] = request.headers["idempotency-key"]
    try:
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.request(request.method, url, params=request.query_params, content=body or None,
                                headers=headers)
    except httpx.HTTPError as e:
        return JSONResponse({"error": f"{svc} unreachable: {type(e).__name__}"}, status_code=502)
    if request.method == "POST" and not path.startswith("admin/verify"):
        feed.add("console", f"{svc}: POST /{path} {body.decode()[:160]} -> HTTP {r.status_code}",
                 "info" if r.status_code < 400 else "bad")
    try:
        return JSONResponse(r.json(), status_code=r.status_code)
    except ValueError:
        return JSONResponse({"raw": r.text[:2000]}, status_code=r.status_code)


# ------------------------------------------------------------------ demo transfers

class DemoBody(BaseModel):
    fault: str = "timeout"
    count: int = 1


_demo_amount = int(time.time()) % 5000 + 20000  # demo amounts stay clear of the benchmark's (1001-1100, 5000+)
_demo_lock = threading.Lock()


def _post(url, body, key=None):
    headers = {"Idempotency-Key": key} if key else {}
    r = http.post(url, json=body, headers=headers, timeout=60)
    if r.status_code >= 400:
        raise HTTPException(502, f"POST {url} -> {r.status_code}: {r.text[:200]}")
    return r.json()


def _reconcile_later(delay: float):
    time.sleep(delay)
    try:
        res = _post(f"{WALLET}/admin/reconcile", {})
        feed.add("console", f"Reconcile: {len(res.get('flagged') or [])} reversed payment(s) flagged STUCK at RECONCILE",
                 "info")
    except HTTPException as e:
        feed.add("console", f"Reconcile failed: {e.detail}", "bad")


@app.post("/api/demo/transfer")
def demo_transfer(body: DemoBody):
    global _demo_amount
    if body.fault not in FAULTS:
        raise HTTPException(400, f"fault must be one of {', '.join(FAULTS)}")
    if bench.running():
        raise HTTPException(409, "a benchmark is running; wait for it to finish")
    n = max(1, min(body.count, 20))
    with _demo_lock:
        amounts = list(range(_demo_amount, _demo_amount + n))
        _demo_amount += n
    if body.fault != "none":
        _post(f"{RAIL}/admin/faults", {"by_amount": {str(a): body.fault for a in amounts}})
    feed.add("console", f"Sending {n} transfer(s) with rail fault '{body.fault}'", "info")
    ids = []

    def send(amount):
        a = _post(f"{WALLET}/wallets", {"owner": "demo-sender"})["id"]
        b = _post(f"{WALLET}/wallets", {"owner": "demo-receiver"})["id"]
        _post(f"{WALLET}/wallets/{a}/credit", {"amount_paise": 100_000}, key=f"demo-seed-{a}")
        t = _post(f"{WALLET}/transfers", {"from_wallet": a, "to_wallet": b, "amount_paise": amount},
                  key=f"demo-{uuid.uuid4()}")
        ids.append(t["id"])

    threads = [threading.Thread(target=send, args=(a,)) for a in amounts]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    if body.fault == "partial":
        feed.add("console", "Partial payments are reversed by the rail ~3 s after success; reconciling in 5 s", "info")
        threading.Thread(target=_reconcile_later, args=(5,), daemon=True).start()
    return {"transfers": ids, "fault": body.fault}


# ------------------------------------------------------------------ benchmark runner

class Bench:
    def __init__(self):
        self.proc: subprocess.Popen | None = None
        self.args: list[str] = []
        self.started = 0.0
        self.exit_code = None
        self.lock = threading.Lock()

    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def state(self) -> dict:
        return {"running": self.running(), "args": " ".join(self.args), "exit_code": self.exit_code,
                "seconds": int(time.time() - self.started) if self.started else 0}

    def start(self, args: list[str]):
        with self.lock:
            if self.running():
                raise HTTPException(409, "a benchmark is already running")
            env = {**os.environ, "WALLET_URL": WALLET, "RAIL_URL": RAIL, "RECOVERY_URL": RECOVERY,
                   "DECISIONS_PATH": str(DECISIONS)}
            self.args, self.started, self.exit_code = args, time.time(), None
            self.proc = subprocess.Popen([sys.executable, "-u", str(BENCH_DIR / "run.py"), *args], cwd=str(BENCH_DIR),
                                         env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        feed.add("bench", "Benchmark started: run.py " + " ".join(args), "info")
        threading.Thread(target=self._pump, args=(self.proc,), daemon=True).start()

    def _pump(self, proc):
        for line in proc.stdout:
            line = line.rstrip()
            if not line:
                continue
            level = "bad" if ("INVALID" in line or "Traceback" in line or "Error" in line or "FAILED" in line) else \
                    "good" if ("resolved" in line and "unsafe 0" in line) or "done;" in line else "info"
            feed.add("bench", line, level)
        self.exit_code = proc.wait()
        feed.add("bench", f"Benchmark finished (exit code {self.exit_code})", "good" if self.exit_code == 0 else "bad")

    def stop(self):
        if self.running():
            self.proc.terminate()
            feed.add("bench", "Benchmark stopped by user. Leftover STUCK transfers stay as they are.", "warn")


bench = Bench()


class BenchBody(BaseModel):
    n: int = 10
    seed: int = 1
    strategies: list[str] = ["static", "retry", "llm_no_verifier", "llm_verified_memory"]
    hide_rail: bool = False
    inject_rate: float = 0.0
    no_memory: bool = False
    throughput: bool = False


@app.post("/api/bench/start")
def bench_start(b: BenchBody):
    if b.n % 5 or not 5 <= b.n <= 500:
        raise HTTPException(400, "n must be a multiple of 5 between 5 and 500")
    allowed = {"static", "retry", "llm_no_verifier", "llm_verified_memory"}
    if not b.strategies or not set(b.strategies) <= allowed:
        raise HTTPException(400, "pick at least one strategy")
    args = ["--n", str(b.n), "--seed", str(b.seed), "--strategies", ",".join(b.strategies)]
    if b.hide_rail:
        args.append("--hide-rail")
    if b.inject_rate > 0:
        args += ["--inject-rate", str(b.inject_rate)]
    if b.no_memory:
        args.append("--no-memory")
    if not b.throughput:
        args.append("--no-throughput")
    bench.start(args)
    return bench.state()


@app.post("/api/bench/stop")
def bench_stop():
    bench.stop()
    return bench.state()


# ------------------------------------------------------------------ results

def _aggregate_module():
    sys.path.insert(0, str(BENCH_DIR))
    import aggregate  # bench/aggregate.py: one source of truth for what counts as a valid run
    return aggregate


@app.get("/api/results")
def results():
    if not (RESULTS / "summary.csv").exists():
        return {"available": False}
    agg = _aggregate_module()
    runs = agg.valid_runs(str(RESULTS))
    all_rows = agg.read_csv(str(RESULTS / "summary.csv"))
    recent = [{k: r.get(k, "") for k in ("finished_at", "seed", "strategy", "variant", "n", "resolved", "unsafe",
                                         "escalated", "unresolved", "llm_calls", "memory_hits", "valid")}
              for r in all_rows[-25:]][::-1]
    paper = RESULTS / "paper"
    figures = sorted(p.name for p in paper.glob("*.png")) if paper.exists() else []
    return {"available": True, "main": agg.table_main(runs), "by_fault": agg.table_by_fault(str(RESULTS), runs),
            "ablation": agg.table_ablation(runs), "throughput": agg.table_throughput(str(RESULTS)),
            "recent": recent, "figures": figures,
            "invalid": sum(1 for r in all_rows if r.get("valid") == "0")}


@app.get("/api/results/figure/{name}")
def figure(name: str):
    path = (RESULTS / "paper" / name).resolve()
    if path.parent != (RESULTS / "paper").resolve() or path.suffix != ".png" or not path.exists():
        raise HTTPException(404, "no such figure")
    return FileResponse(path, headers={"Cache-Control": "no-store"})


@app.post("/api/results/aggregate")
def run_aggregate():
    feed.add("console", "Rebuilding paper tables and figures (aggregate.py)...", "info")
    r = subprocess.run([sys.executable, str(BENCH_DIR / "aggregate.py")], cwd=str(BENCH_DIR),
                       capture_output=True, text=True, timeout=300)
    ok = r.returncode == 0
    feed.add("console", "Tables and figures rebuilt in bench/results/paper" if ok else
             f"aggregate.py failed: {r.stderr[-300:]}", "good" if ok else "bad")
    return {"ok": ok, "output": (r.stdout + r.stderr)[-4000:]}


# ------------------------------------------------------------------ page

@app.get("/")
def index():
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store"})


@app.get("/expert")
def expert():
    return FileResponse(STATIC / "expert.html", headers={"Cache-Control": "no-store"})


@app.get("/health")
def health():
    return {"service": "console", "status": "ok"}
