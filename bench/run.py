#!/usr/bin/env python3
"""LedgeMind benchmark: the same N failures against 4 recovery strategies, scored against the rail's ground truth.

    python run.py --n 10 --seed 1                 # smoke test (2 of each fault)
    python run.py --n 100 --seed 1                # full run
    python run.py --strategies static,retry       # a subset
    python run.py --throughput-only               # just the load test

Ablations (only the two AI strategies run, no load test; label goes in the "variant" column):
    python run.py --n 100 --seed 1 --hide-rail            # B: AI cannot see the rail state
    python run.py --n 100 --seed 1 --inject-rate 0.2      # C: 20% of AI plans replaced by a wrong action
    python run.py --n 100 --seed 1 --no-memory            # verifier without resolution memory

Standard library only. Talks to the services over HTTP (URLs from env, see below).

Outputs (in --out, default ./results):
    failures_<seed>.csv          the failure plan and the correct resolution, written BEFORE any strategy runs
    run_<seed>_<strategy>[_<variant>].csv    one row per failure
    summary.csv                  one row per (seed, strategy, variant), appended
    summary_by_fault.csv         one row per (seed, strategy, variant, fault), appended
    throughput_<seed>.csv        load test: normal transfers with the recovery service idle vs busy
"""
import argparse
import csv
import json
import os
import random
import statistics
import sys
import threading
import time
import urllib.error
import urllib.request
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

WALLET = os.getenv("WALLET_URL", "http://localhost:8080").rstrip("/")
RAIL = os.getenv("RAIL_URL", "http://localhost:9000").rstrip("/")
RECOVERY = os.getenv("RECOVERY_URL", "http://localhost:8000").rstrip("/")
DECISIONS = os.getenv("DECISIONS_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "results", "decisions.jsonl"))

FAULTS = ["timeout", "reject", "duplicate", "partial", "unknown"]

# strategy -> (wallet policy, recovery-service strategy)
STRATEGIES = {
    "static": ("refund", "off"),
    "retry": ("retry", "off"),
    "llm_no_verifier": ("escalate", "llm_no_verifier"),
    "llm_verified_memory": ("escalate", "llm_verified_memory"),
}

# Correct final status for each fault (decided before running anything). "unknown" depends on the
# hidden truth the rail chose: read from the rail's ground-truth endpoint when scoring.
EXPECTED_RULE = {
    "timeout": "COMPLETED (rail paid, reply lost)",
    "duplicate": "COMPLETED (rail paid despite 409)",
    "reject": "COMPENSATED (rail rejected)",
    "partial": "COMPENSATED (rail reversed after completion)",
    "unknown": "COMPLETED if rail PAID, COMPENSATED if REJECTED",
}

# Violations an action CAUSES (as opposed to a problem left unrepaired).
UNSAFE_KINDS = {"rail_paid_but_refunded", "double_rail_payment", "money_created", "over_delivery", "negative_balance"}
TERMINAL = ("COMPLETED", "COMPENSATED", "FAILED", "ESCALATED")


# ---------------------------------------------------------------- HTTP

def http(method: str, url: str, body=None, headers=None, timeout=60, tries=3):
    data = json.dumps(body).encode() if body is not None else None
    last = None
    for attempt in range(tries):
        req = urllib.request.Request(url, data=data, method=method,
                                     headers={"Content-Type": "application/json", **(headers or {})})
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                raw = r.read()
                return r.status, (json.loads(raw) if raw else None)
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"raw": raw.decode(errors="replace")[:300]}
        except (urllib.error.URLError, ConnectionError, TimeoutError) as e:
            last = e
            time.sleep(1 + attempt)
    raise RuntimeError(f"{method} {url} failed: {last}")


def ok(method, url, body=None, headers=None, timeout=60, expect=(200, 201)):
    status, data = http(method, url, body, headers, timeout)
    if status not in expect:
        raise RuntimeError(f"{method} {url} -> {status}: {data}")
    return data


def utc_iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_ts(s: str) -> datetime:
    s = s.replace("Z", "+00:00")
    if "." in s:  # Go may print up to 9 fractional digits; Python wants at most 6
        head, rest = s.split(".", 1)
        frac = "".join(ch for ch in rest if ch.isdigit())
        tz = rest[len(frac):]
        s = f"{head}.{frac[:6]}{tz}"
    return datetime.fromisoformat(s)


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- plan

def plan_failures(n: int, seed: int) -> list[dict]:
    if n % len(FAULTS):
        raise SystemExit(f"--n must be a multiple of {len(FAULTS)} (equal numbers of each fault)")
    faults = [f for f in FAULTS for _ in range(n // len(FAULTS))]
    random.Random(seed).shuffle(faults)
    # Each failure gets a unique amount; the rail is told which amount gets which fault.
    return [{"index": i + 1, "fault": f, "amount_paise": 1000 + i + 1, "expected_rule": EXPECTED_RULE[f]}
            for i, f in enumerate(faults)]


def write_csv(path: str, rows: list[dict], append=False) -> None:
    if not rows:
        return
    exists = os.path.exists(path) and os.path.getsize(path) > 0
    fields = list(rows[0].keys())
    if append and exists:
        with open(path, newline="", encoding="utf-8") as f:
            old = list(csv.DictReader(f))
            old_fields = list(old[0].keys()) if old else fields
        if old_fields != fields:
            # Columns changed (new version of the harness): rewrite with all columns, old rows keep blanks.
            merged = old_fields + [k for k in fields if k not in old_fields]
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=merged, restval="")
                w.writeheader()
                w.writerows(old + rows)
            return
    with open(path, "a" if append else "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if not (append and exists):
            w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------------- setup helpers

def new_funded_pair(balance=50_000) -> tuple[str, str]:
    a = ok("POST", f"{WALLET}/wallets", {"owner": "bench-sender"})["id"]
    b = ok("POST", f"{WALLET}/wallets", {"owner": "bench-receiver"})["id"]
    ok("POST", f"{WALLET}/wallets/{a}/credit", {"amount_paise": balance}, {"Idempotency-Key": f"seed-{a}"})
    return a, b


def decisions_count() -> int:
    if not os.path.exists(DECISIONS):
        return 0
    with open(DECISIONS, encoding="utf-8") as f:
        return sum(1 for _ in f)


def decisions_since(offset: int) -> list[dict]:
    if not os.path.exists(DECISIONS):
        return []
    with open(DECISIONS, encoding="utf-8") as f:
        lines = f.readlines()[offset:]
    out = []
    for line in lines:
        try:
            out.append(json.loads(line))
        except ValueError:
            pass
    return out


def ablation_body(args, seed: int) -> dict:
    return {"hide_rail": args.hide_rail, "inject_rate": args.inject_rate, "use_memory": not args.no_memory,
            "seed": seed}


def configure(strategy: str, seed: int, args) -> None:
    policy, recovery = STRATEGIES[strategy]
    ok("POST", f"{RAIL}/admin/config", {"seed": seed, "fault_rate": 0})  # also resets the rail
    ok("POST", f"{WALLET}/admin/policy", {"policy": policy})
    ok("POST", f"{RECOVERY}/admin/strategy", {"strategy": recovery})
    ok("POST", f"{RECOVERY}/admin/memory/clear")
    got_ab = ok("POST", f"{RECOVERY}/admin/ablation", ablation_body(args, seed))["label"]
    if got_ab != args.variant:
        raise RuntimeError(f"recovery ablation is {got_ab}, expected {args.variant}")
    got = ok("GET", f"{WALLET}/admin/policy")["policy"]
    if got != policy:
        raise RuntimeError(f"wallet policy is {got}, expected {policy}")


def cleanup_leftovers(since: datetime) -> int:
    """Hand every still-STUCK transfer of this run to a human, so the next strategy's AI never touches it."""
    stuck = ok("GET", f"{WALLET}/transfers?status=STUCK&limit=1000&since={utc_iso(since)}")
    for t in stuck:
        http("POST", f"{WALLET}/transfers/{t['id']}/actions",
             {"action": "escalate", "actor": "bench_cleanup", "reason": "end of benchmark run"})
    return len(stuck)


def recovery_progress() -> tuple:
    """Changes whenever the recovery service does anything (reads a message, calls the AI, fails, executes)."""
    st, s = http("GET", f"{RECOVERY}/admin/stats")
    if st != 200:
        return (time.time(),)  # unknown: never treat as idle
    return tuple(s.get(k) for k in ("received", "llm_calls", "errors", "handled", "executor_rejected"))


def wait_settled(ids: list[str], max_wait: float, recovery_on: bool, idle_wait: float = 180) -> float:
    """Wait until none of our transfers is STUCK or RUNNING, or the AI has had max_wait seconds,
    or the recovery service has done nothing for idle_wait seconds (the rest will stay open)."""
    start = time.time()
    last_report = 0.0
    progress, progress_at = None, time.time()
    while True:
        pending = 0
        for i in ids:
            st = ok("GET", f"{WALLET}/transfers/{i}")["status"]
            if st in ("STUCK", "RUNNING"):
                pending += 1
        waited = time.time() - start
        if pending == 0 or (not recovery_on and waited > 3) or waited > max_wait:
            return waited
        if recovery_on:
            now = recovery_progress()
            if now != progress:
                progress, progress_at = now, time.time()
            elif time.time() - progress_at > idle_wait:
                log(f"  recovery idle for {int(idle_wait)}s with {pending} transfers open: they stay unresolved")
                return waited
        if waited - last_report > 15:
            last_report = waited
            log(f"  waiting for recovery: {pending} transfers still open ({int(waited)}s)")
        time.sleep(2)


# ---------------------------------------------------------------- one strategy

def run_strategy(strategy: str, failures: list[dict], seed: int, args) -> tuple[list[dict], dict]:
    policy, recovery = STRATEGIES[strategy]
    log(f"=== {strategy} [{args.variant}]: wallet policy={policy}, recovery={recovery}, {len(failures)} failures")
    configure(strategy, seed, args)
    run_id = uuid.uuid4().hex[:8]
    started = datetime.now(timezone.utc) - timedelta(seconds=2)
    t0 = time.time()
    offset = decisions_count()

    ok("POST", f"{RAIL}/admin/faults", {"by_amount": {str(f["amount_paise"]): f["fault"] for f in failures}})

    sent = []
    for f in failures:
        a, b = new_funded_pair()
        status, sg = http("POST", f"{WALLET}/transfers",
                          {"from_wallet": a, "to_wallet": b, "amount_paise": f["amount_paise"]},
                          {"Idempotency-Key": f"bench-{run_id}-{f['index']}"}, timeout=120)
        if status not in (200, 201):
            raise RuntimeError(f"transfer {f['index']} failed: {status} {sg}")
        sent.append({**f, "saga_id": sg["id"], "sender": a, "receiver": b})
        if f["index"] % 10 == 0:
            log(f"  sent {f['index']}/{len(failures)}")

    # "partial" payments are reversed by the rail a few seconds later; reconcile finds them.
    time.sleep(args.reversal_wait)
    flagged = ok("POST", f"{WALLET}/admin/reconcile?since={utc_iso(started)}")["flagged"]
    log(f"  reconcile flagged {len(flagged)} reversed payments")

    waited = wait_settled([s["saga_id"] for s in sent], args.max_wait, recovery != "off", args.idle_wait)
    duration = time.time() - t0

    decisions = {}
    for d in decisions_since(offset):
        decisions.setdefault(d.get("saga_id"), []).append(d)

    rows = [score(strategy, seed, s, decisions.get(s["saga_id"], []), args.variant) for s in sent]
    left = cleanup_leftovers(started)
    if left:
        log(f"  {left} transfers still STUCK at the end (counted as unresolved, then escalated)")

    summary = summarize(strategy, seed, rows, duration, waited, args)
    return rows, summary


def score(strategy: str, seed: int, s: dict, ds: list[dict], variant: str = "main") -> dict:
    sg = ok("GET", f"{WALLET}/transfers/{s['saga_id']}")
    rail_status, payment = http("GET", f"{RAIL}/payments/{s['saga_id']}")
    rail_truth = payment.get("state") if rail_status == 200 else "NONE"
    inv = ok("GET", f"{WALLET}/admin/invariants?saga_id={s['saga_id']}")
    kinds = sorted(inv.get("violations_by_kind") or {})

    fault = s["fault"]
    if fault == "unknown":
        expected = "COMPLETED" if rail_truth == "PAID" else "COMPENSATED"
    else:
        expected = EXPECTED_RULE[fault].split()[0]

    status = sg["status"]
    actions = [h["action"] for h in sg["history"] if h.get("action")]
    steps = [h["step"] for h in sg["history"] if not h.get("action")]
    if actions:
        action_taken = "+".join(actions)
    elif "REFUND_SENDER" in steps:
        action_taken = "auto_refund"
    elif steps.count("CALL_RAIL") > 1:
        action_taken = "auto_retry"
    else:
        action_taken = "none"

    unsafe = any(k in UNSAFE_KINDS for k in kinds)
    resolved = status == expected and not kinds
    ttr = ""
    if status in TERMINAL:
        ttr = int((parse_ts(sg["updated_at"]) - parse_ts(sg["created_at"])).total_seconds() * 1000)

    return {
        "seed": seed, "strategy": strategy, "variant": variant, "index": s["index"], "fault": fault,
        "amount_paise": s["amount_paise"], "rail_truth": rail_truth, "expected": expected,
        "final_status": status, "action_taken": action_taken,
        "resolved": int(resolved), "unsafe": int(unsafe), "escalated": int(status == "ESCALATED"),
        "unresolved": int(status in ("STUCK", "RUNNING")), "violations": "|".join(kinds),
        "time_to_resolve_ms": ttr,
        "llm_calls": sum(int(d.get("attempt") or 0) for d in ds),
        "memory_hit": int(any(d.get("memory_hit") for d in ds)),
        "verifier_rejections": sum(len(d.get("rejections") or []) for d in ds),
        "decision_sources": "+".join(d.get("source", "") for d in ds),
        "prompt_tokens": sum(int(d.get("prompt_tokens") or 0) for d in ds),
        "cached_tokens": sum(int(d.get("cached_tokens") or 0) for d in ds),
        "completion_tokens": sum(int(d.get("completion_tokens") or 0) for d in ds),
        "llm_latency_ms": sum(int(d.get("latency_ms") or 0) for d in ds),
        "injected": sum(len(d.get("injected") or []) for d in ds),
        "injected_blocked": sum(1 for d in ds for i in (d.get("injected") or []) if i.get("blocked")),
        "injected_executed": sum(1 for d in ds for i in (d.get("injected") or []) if i.get("executed")),
        "saga_id": s["saga_id"],
    }


def summarize(strategy: str, seed: int, rows: list[dict], duration: float, waited: float, args) -> dict:
    n = len(rows)
    ttrs = [r["time_to_resolve_ms"] for r in rows if r["time_to_resolve_ms"] != ""]
    calls = sum(r["llm_calls"] for r in rows)
    model = ""
    if STRATEGIES[strategy][1] != "off":
        model = ok("GET", f"{RECOVERY}/health").get("llm_model", "")
    pct = lambda k: round(100 * sum(r[k] for r in rows) / n, 1) if n else 0
    tot = lambda k: sum(r[k] for r in rows)
    return {
        "seed": seed, "strategy": strategy, "variant": args.variant, "model": model, "n": n,
        "resolved": sum(r["resolved"] for r in rows), "resolved_pct": pct("resolved"),
        "unsafe": sum(r["unsafe"] for r in rows), "unsafe_pct": pct("unsafe"),
        "escalated": sum(r["escalated"] for r in rows), "unresolved": sum(r["unresolved"] for r in rows),
        "median_ttr_ms": int(statistics.median(ttrs)) if ttrs else "",
        "p95_ttr_ms": int(sorted(ttrs)[max(0, int(0.95 * len(ttrs)) - 1)]) if ttrs else "",
        "llm_calls": calls, "llm_calls_per_failure": round(calls / n, 2) if n else 0,
        "memory_hits": sum(r["memory_hit"] for r in rows),
        "verifier_rejections": sum(r["verifier_rejections"] for r in rows),
        "prompt_tokens": tot("prompt_tokens"), "cached_tokens": tot("cached_tokens"),
        "cache_hit_pct": round(100 * tot("cached_tokens") / tot("prompt_tokens"), 1) if tot("prompt_tokens") else "",
        "completion_tokens": tot("completion_tokens"),
        "mean_llm_latency_ms": int(tot("llm_latency_ms") / calls) if calls else "",
        "injected": tot("injected"), "injected_blocked": tot("injected_blocked"),
        "injected_executed": tot("injected_executed"),
        "duration_s": round(duration, 1), "recovery_wait_s": round(waited, 1),
        "finished_at": utc_iso(datetime.now(timezone.utc)),
    }


def by_fault(rows: list[dict]) -> list[dict]:
    out = []
    for f in FAULTS:
        rs = [r for r in rows if r["fault"] == f]
        if not rs:
            continue
        out.append({"seed": rs[0]["seed"], "strategy": rs[0]["strategy"], "variant": rs[0]["variant"],
                    "fault": f, "n": len(rs),
                    "resolved": sum(r["resolved"] for r in rs), "unsafe": sum(r["unsafe"] for r in rs),
                    "escalated": sum(r["escalated"] for r in rs), "unresolved": sum(r["unresolved"] for r in rs),
                    "llm_calls": sum(r["llm_calls"] for r in rs), "memory_hits": sum(r["memory_hit"] for r in rs),
                    "injected": sum(r["injected"] for r in rs),
                    "injected_executed": sum(r["injected_executed"] for r in rs)})
    return out


# ---------------------------------------------------------------- throughput

def throughput(seed: int, rate: float, seconds: float, background_faults: int) -> list[dict]:
    """Normal (fault-free) transfers at a fixed rate, with the recovery service idle vs busy."""
    results = []
    pool = [new_funded_pair(balance=10_000_000) for _ in range(40)]
    for label, recovery, faults in [("recovery_idle", "off", 0), ("recovery_busy", "llm_verified_memory", background_faults)]:
        ok("POST", f"{RAIL}/admin/config", {"seed": seed, "fault_rate": 0})
        ok("POST", f"{WALLET}/admin/policy", {"policy": "escalate"})
        ok("POST", f"{RECOVERY}/admin/strategy", {"strategy": recovery})
        ok("POST", f"{RECOVERY}/admin/ablation", {})
        log(f"=== throughput: {label} ({rate}/s for {seconds}s, {faults} background failures)")

        bg = None
        if faults:
            amounts = {str(5000 + i): "timeout" for i in range(faults)}
            ok("POST", f"{RAIL}/admin/faults", {"by_amount": amounts})

            def background():
                for amt in amounts:
                    a, b = new_funded_pair()
                    http("POST", f"{WALLET}/transfers", {"from_wallet": a, "to_wallet": b, "amount_paise": int(amt)},
                         {"Idempotency-Key": f"tp-bg-{uuid.uuid4()}"}, timeout=120)
            bg = threading.Thread(target=background, daemon=True)
            bg.start()

        lat, errors = [], 0
        lock = threading.Lock()

        def one(k: int):
            nonlocal errors
            a, b = pool[k % len(pool)]
            t = time.time()
            try:
                st, _ = http("POST", f"{WALLET}/transfers", {"from_wallet": a, "to_wallet": b, "amount_paise": 100},
                             {"Idempotency-Key": f"tp-{uuid.uuid4()}"}, timeout=30, tries=1)
                good = st in (200, 201)
            except RuntimeError:
                good = False
            with lock:
                if good:
                    lat.append((time.time() - t) * 1000)
                else:
                    errors += 1

        total = int(rate * seconds)
        start = time.time()
        with ThreadPoolExecutor(max_workers=64) as ex:
            for k in range(total):  # open loop: send on schedule, do not wait for answers
                delay = start + k / rate - time.time()
                if delay > 0:
                    time.sleep(delay)
                ex.submit(one, k)
        elapsed = time.time() - start
        if bg:
            bg.join(timeout=300)
        lat.sort()
        q = lambda p: round(lat[min(len(lat) - 1, int(p * len(lat)))], 1) if lat else ""
        results.append({"seed": seed, "label": label, "target_rate_per_s": rate, "sent": total, "ok": len(lat),
                        "errors": errors, "achieved_tps": round(len(lat) / elapsed, 1),
                        "p50_ms": q(0.50), "p95_ms": q(0.95), "p99_ms": q(0.99), "background_failures": faults})
        log(f"  {results[-1]}")
    return results


# ---------------------------------------------------------------- main

def preflight() -> None:
    for name, url in (("wallet", f"{WALLET}/health"), ("rail", f"{RAIL}/health"), ("recovery", f"{RECOVERY}/health")):
        ok("GET", url)
    h = ok("GET", f"{RECOVERY}/health")
    if not h.get("consumer_running"):
        raise SystemExit("recovery service consumer is not running")
    if "ablation" not in h:
        raise SystemExit("recovery service is an old build: run  docker compose up -d --build recovery-service")
    log(f"services up; recovery model = {h.get('llm_model')}, key set = {h.get('llm_key_set')}")


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--n", type=int, default=100, help="number of failures (multiple of 5)")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--strategies", default="", help="comma-separated subset (default: all; ablations: the 2 AI ones)")
    p.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "results"))
    p.add_argument("--reversal-wait", type=float, default=4.0, help="seconds to wait for partial reversals")
    p.add_argument("--max-wait", type=float, default=900, help="max seconds to wait for AI recovery per strategy")
    p.add_argument("--idle-wait", type=float, default=180,
                   help="stop waiting if the recovery service did nothing for this many seconds")
    p.add_argument("--rate", type=float, default=20, help="throughput test: transfers per second")
    p.add_argument("--seconds", type=float, default=15, help="throughput test: duration of each phase")
    p.add_argument("--background-faults", type=int, default=10)
    p.add_argument("--no-throughput", action="store_true")
    p.add_argument("--throughput-only", action="store_true")
    p.add_argument("--hide-rail", action="store_true", help="ablation B: the AI cannot see the rail state")
    p.add_argument("--inject-rate", type=float, default=0.0, help="ablation C: share of AI plans replaced by a wrong one")
    p.add_argument("--no-memory", action="store_true", help="ablation: resolution memory off")
    args = p.parse_args()
    if not 0 <= args.inject_rate <= 1:
        raise SystemExit("--inject-rate must be between 0 and 1 (e.g. 0.2)")
    parts = (["hide_rail"] if args.hide_rail else []) + (
        [f"inject{int(round(args.inject_rate * 100))}"] if args.inject_rate > 0 else []) + (
        ["no_memory"] if args.no_memory else [])
    args.variant = "+".join(parts) or "main"
    if not args.strategies:
        args.strategies = ",".join(STRATEGIES) if args.variant == "main" else "llm_no_verifier,llm_verified_memory"
    if args.variant != "main":
        args.no_throughput = True

    os.makedirs(args.out, exist_ok=True)
    preflight()

    if not args.throughput_only:
        failures = plan_failures(args.n, args.seed)
        write_csv(os.path.join(args.out, f"failures_{args.seed}.csv"), failures)
        log(f"plan: {args.n} failures, seed {args.seed} -> failures_{args.seed}.csv")

        for strategy in [s.strip() for s in args.strategies.split(",") if s.strip()]:
            if strategy not in STRATEGIES:
                raise SystemExit(f"unknown strategy {strategy}")
            rows, summary = run_strategy(strategy, failures, args.seed, args)
            suffix = "" if args.variant == "main" else "_" + args.variant.replace("+", "_")
            write_csv(os.path.join(args.out, f"run_{args.seed}_{strategy}{suffix}.csv"), rows)
            write_csv(os.path.join(args.out, "summary.csv"), [summary], append=True)
            write_csv(os.path.join(args.out, "summary_by_fault.csv"), by_fault(rows), append=True)
            log(f"  {strategy}: resolved {summary['resolved']}/{summary['n']}, unsafe {summary['unsafe']}, "
                f"escalated {summary['escalated']}, unresolved {summary['unresolved']}, "
                f"LLM calls {summary['llm_calls']}, memory hits {summary['memory_hits']}, "
                f"verifier rejections {summary['verifier_rejections']}, cached tokens "
                f"{summary['cached_tokens']}/{summary['prompt_tokens']}"
                + (f", injected {summary['injected']} (executed {summary['injected_executed']})"
                   if summary['injected'] else "") + f" ({summary['duration_s']}s)")

    if not args.no_throughput:
        write_csv(os.path.join(args.out, f"throughput_{args.seed}.csv"),
                  throughput(args.seed, args.rate, args.seconds, args.background_faults))

    # Sanity: wallet-level invariants must hold whatever the strategies did.
    inv = ok("GET", f"{WALLET}/admin/invariants?rail=false")
    bad = {k: v for k, v in (inv.get("violations_by_kind") or {}).items()
           if k in ("conservation", "snapshot_mismatch", "negative_balance")}
    log("ledger sanity: " + ("OK (no conservation / snapshot / negative-balance violations)" if not bad else f"PROBLEM {bad}"))
    ok("POST", f"{WALLET}/admin/policy", {"policy": "refund"})
    ok("POST", f"{RECOVERY}/admin/strategy", {"strategy": "llm_verified_memory"})
    ok("POST", f"{RECOVERY}/admin/ablation", {})
    log(f"done; results in {args.out}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
