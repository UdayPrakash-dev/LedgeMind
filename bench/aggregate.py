#!/usr/bin/env python3
"""Turn benchmark results into the paper's tables and figures.

    python aggregate.py                       # reads ./results, writes ./results/paper
    python aggregate.py --results path --out path

Uses only runs with n=100 that are valid (valid column 1 or empty). When a (seed, strategy, variant)
was run more than once, the latest valid run counts. Needs matplotlib for figures (tables work without it).

Outputs (in --out):
    table_main.csv / .tex         mean +- std over seeds, per strategy (variant "main")
    table_by_fault.csv / .tex     resolved and unsafe per fault, summed over seeds
    table_ablation.csv / .tex     seed-1 experiments: hide_rail, injected errors, memory off
    table_throughput.csv / .tex   normal transfers with recovery idle vs busy (mean over seeds)
    fig_outcomes.pdf / .png       what happened to every failure, per strategy
    fig_injection.pdf / .png      unsafe outcomes vs injected error rate (seed 1)
    report.md                     all tables in one readable file
"""
import argparse
import csv
import glob
import os
import re
import statistics
from collections import defaultdict

STRATEGY_ORDER = ["static", "retry", "llm_no_verifier", "llm_verified_memory"]
NAMES = {
    "static": "Static refund",
    "retry": "Retry with backoff",
    "llm_no_verifier": "AI only",
    "llm_verified_memory": "AI + verifier + memory",
}
FAULTS = ["timeout", "reject", "duplicate", "partial", "unknown"]
OUTCOMES = ["Resolved", "Wrong but safe", "Escalated", "Unresolved", "Unsafe"]


# ------------------------------------------------------------------ loading

def read_csv(path):
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def num(v, default=0.0):
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def valid_runs(results):
    """Latest valid n=100 summary row per (seed, strategy, variant)."""
    rows = read_csv(os.path.join(results, "summary.csv"))
    best = {}
    for r in rows:
        r["variant"] = r.get("variant") or "main"
        if int(num(r["n"])) != 100 or r.get("valid", "1") == "0":
            continue
        key = (int(num(r["seed"])), r["strategy"], r["variant"])
        if key not in best or r["finished_at"] >= best[key]["finished_at"]:
            best[key] = r
    return best


def run_file(results, seed, strategy, variant):
    suffix = "" if variant == "main" else "_" + variant.replace("+", "_")
    return os.path.join(results, f"run_{seed}_{strategy}{suffix}.csv")


def run_rows(results, seed, strategy, variant, n=100):
    """Rows of one run file, or [] if it is missing or was overwritten by a run of another size."""
    path = run_file(results, seed, strategy, variant)
    if not os.path.exists(path):
        return []
    rows = read_csv(path)
    return rows if len(rows) == n else []


def outcome(row):
    if row["resolved"] == "1":
        return "Resolved"
    if row["unsafe"] == "1":
        return "Unsafe"
    if row["final_status"] == "ESCALATED":
        return "Escalated"
    if row["unresolved"] == "1" or row["final_status"] in ("STUCK", "RUNNING"):
        return "Unresolved"
    return "Wrong but safe"  # finished in a different state than expected, nothing violated


def mean_sd(values):
    values = [v for v in values if v is not None]
    if not values:
        return None, None
    return statistics.mean(values), (statistics.stdev(values) if len(values) > 1 else 0.0)


def fmt(m, s, digits=1):
    if m is None:
        return "-"
    return f"{m:.{digits}f}" if not s else f"{m:.{digits}f} ± {s:.{digits}f}"


# ------------------------------------------------------------------ tables

def table_main(runs):
    out = []
    for st in STRATEGY_ORDER:
        rs = [r for (seed, s, v), r in sorted(runs.items()) if s == st and v == "main"]
        if not rs:
            continue
        col = lambda k: mean_sd([num(r[k]) for r in rs])
        ttr = mean_sd([num(r["median_ttr_ms"]) / 1000 for r in rs if r["median_ttr_ms"] != ""])
        out.append({
            "strategy": NAMES[st], "seeds": len(rs),
            "resolved_%": fmt(*col("resolved_pct")), "unsafe_%": fmt(*col("unsafe_pct")),
            "escalated": fmt(*col("escalated")), "unresolved": fmt(*col("unresolved")),
            "ai_calls_per_failure": fmt(*col("llm_calls_per_failure"), digits=2),
            "memory_hits": fmt(*col("memory_hits")),
            "median_time_to_resolve_s": fmt(*ttr),
            "model": rs[0].get("model") or "-",
        })
    return out


def table_by_fault(results, runs):
    out = []
    for st in STRATEGY_ORDER:
        counts = defaultdict(lambda: defaultdict(int))
        seeds = 0
        for (seed, s, v), _ in sorted(runs.items()):
            rows = run_rows(results, seed, s, v) if s == st and v == "main" else []
            if not rows:
                continue
            seeds += 1
            for row in rows:
                c = counts[row["fault"]]
                c["n"] += 1
                c["resolved"] += row["resolved"] == "1"
                c["unsafe"] += row["unsafe"] == "1"
        if not seeds:
            continue
        rec = {"strategy": NAMES[st], "seeds": seeds}
        for f in FAULTS:
            c = counts[f]
            rec[f] = f"{c['resolved']}/{c['n']}" + (f" ({c['unsafe']} unsafe)" if c["unsafe"] else "")
        out.append(rec)
    return out


def variant_sort(v):
    rate = int(m.group(1)) if (m := re.search(r"inject(\d+)", v)) else 0
    return ("hide_rail" in v, "no_memory" in v, rate)


def table_ablation(runs, seed=1):
    out = []
    keys = [(s, v) for (sd, s, v) in runs if sd == seed and s.startswith("llm_")]
    for s, v in sorted(set(keys), key=lambda k: (variant_sort(k[1]), STRATEGY_ORDER.index(k[0]))):
        r = runs[(seed, s, v)]
        name = NAMES[s] if "no_memory" not in v else "AI + verifier (no memory)"
        out.append({
            "variant": v, "strategy": name,
            "resolved": int(num(r["resolved"])), "unsafe": int(num(r["unsafe"])),
            "escalated": int(num(r["escalated"])), "unresolved": int(num(r["unresolved"])),
            "ai_calls": int(num(r["llm_calls"])), "memory_hits": int(num(r["memory_hits"])),
            "verifier_rejections": int(num(r["verifier_rejections"])),
            "injected": int(num(r.get("injected"))), "injected_executed": int(num(r.get("injected_executed"))),
        })
    return out


def table_throughput(results):
    by = defaultdict(list)
    for path in sorted(glob.glob(os.path.join(results, "throughput_*.csv"))):
        for r in read_csv(path):
            by[r["label"]].append(r)
    out = []
    for label in ("recovery_idle", "recovery_busy"):
        rs = by.get(label)
        if not rs:
            continue
        col = lambda k: mean_sd([num(r[k]) for r in rs])
        out.append({"recovery_service": label.split("_")[1], "seeds": len(rs),
                    "rate_per_s": rs[0]["target_rate_per_s"], "errors": sum(int(num(r["errors"])) for r in rs),
                    "achieved_tps": fmt(*col("achieved_tps")), "p50_ms": fmt(*col("p50_ms")),
                    "p95_ms": fmt(*col("p95_ms")), "p99_ms": fmt(*col("p99_ms")),
                    "background_failures": rs[0]["background_failures"]})
    return out


def write_table(rows, out, name):
    if not rows:
        return ""
    cols = list(rows[0].keys())
    with open(os.path.join(out, name + ".csv"), "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)
    esc = lambda x: str(x).replace("_", r"\_").replace("%", r"\%").replace("±", r"$\pm$").replace("+", "+")
    tex = [r"\begin{tabular}{" + "l" + "r" * (len(cols) - 1) + "}", r"\toprule",
           " & ".join(esc(c) for c in cols) + r" \\", r"\midrule"]
    tex += [" & ".join(esc(r[c]) for c in cols) + r" \\" for r in rows]
    tex += [r"\bottomrule", r"\end{tabular}"]
    with open(os.path.join(out, name + ".tex"), "w", encoding="utf-8") as f:
        f.write("\n".join(tex) + "\n")
    md = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    md += ["| " + " | ".join(str(r[c]) for c in cols) + " |" for r in rows]
    return "\n".join(md)


# ------------------------------------------------------------------ figures
# Print-first figures (light surface). Outcome colours are status colours (good/serious/critical)
# plus neutral greys, always with a legend and a hatch on "Unsafe", so they read in greyscale too.

INK, INK2, MUTED, GRID, BASE, SURFACE = "#0b0b0b", "#52514e", "#898781", "#e1e0d9", "#c3c2b7", "#ffffff"
OUTCOME_COLOR = {"Resolved": "#0ca30c", "Wrong but safe": "#ec835a", "Escalated": "#b7b5ab",
                 "Unresolved": "#e1e0d9", "Unsafe": "#d03b3b"}
SERIES = {"AI only": "#eb6834", "AI + verifier + memory": "#2a78d6", "AI + verifier (no memory)": "#1baf7a"}


def style(ax):
    ax.set_facecolor(SURFACE)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(BASE)
    ax.tick_params(colors=INK2, labelsize=8, length=0)
    ax.yaxis.grid(True, color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)


def fig_outcomes(results, runs, out, plt):
    data = {}
    for st in STRATEGY_ORDER:
        counts, total = defaultdict(int), 0
        for (seed, s, v), _ in runs.items():
            if s == st and v == "main":
                for row in run_rows(results, seed, s, v):
                    counts[outcome(row)] += 1
                    total += 1
        if total:
            data[st] = {o: 100 * counts[o] / total for o in OUTCOMES}
    if not data:
        return
    fig, ax = plt.subplots(figsize=(6.4, 2.6), dpi=200)
    style(ax)
    ax.yaxis.grid(False)
    ax.xaxis.grid(True, color=GRID, linewidth=0.6)
    labels = [NAMES[s] for s in data]
    ys = list(range(len(labels)))[::-1]
    left = [0.0] * len(labels)
    for o in OUTCOMES:
        vals = [data[s][o] for s in data]
        if not any(vals):
            continue  # no legend entry for an outcome that never happened
        ax.barh(ys, vals, left=left, height=0.62, color=OUTCOME_COLOR[o], edgecolor=SURFACE, linewidth=1.2,
                hatch="////" if o == "Unsafe" else None, label=o)
        for y, l, v in zip(ys, left, vals):
            if v >= 8:
                ax.text(l + v / 2, y, f"{v:.0f}%", ha="center", va="center", fontsize=7,
                        color="#ffffff" if o in ("Resolved", "Unsafe") else INK,
                        bbox=dict(facecolor=OUTCOME_COLOR[o], edgecolor="none", pad=1.5) if o == "Unsafe" else None)
        left = [a + b for a, b in zip(left, vals)]
    ax.set_yticks(ys, labels, color=INK, fontsize=8)
    ax.set_xlim(0, 100)
    ax.set_xlabel("share of injected failures (%), 3 seeds x 100", color=INK2, fontsize=8)
    ax.legend(ncol=len(OUTCOMES), fontsize=7, frameon=False, loc="upper center", bbox_to_anchor=(0.45, 1.2),
              handlelength=1.2, columnspacing=1.0, labelcolor=INK2)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out, f"fig_outcomes.{ext}"), facecolor=SURFACE)
    plt.close(fig)


def fig_injection(runs, out, plt, seed=1):
    """Unsafe outcomes vs injected error rate. Counts of wrong actions executed are in table_ablation."""
    series = defaultdict(dict)  # name -> rate -> unsafe
    for (sd, s, v), r in runs.items():
        if sd != seed or not s.startswith("llm_") or "hide_rail" in v:
            continue
        m = re.search(r"inject(\d+)", v)
        rate = int(m.group(1)) if m else 0
        name = "AI + verifier (no memory)" if "no_memory" in v else NAMES[s]
        series[name][rate] = num(r["unsafe"])
    if not any(len(p) > 1 for p in series.values()):
        return
    fig, ax = plt.subplots(figsize=(4.2, 2.6), dpi=200)
    style(ax)
    order = [n for n in ("AI only", "AI + verifier + memory", "AI + verifier (no memory)") if n in series]
    for name in order:
        pts = series[name]
        xs = sorted(pts)
        ax.plot(xs, [pts[x] for x in xs], color=SERIES[name], linewidth=2, marker="o", markersize=5,
                markeredgecolor=SURFACE, markeredgewidth=1, label=name, clip_on=False, zorder=3)
    ax.set_xlabel("injected error rate (% of AI plans replaced by a wrong action)", fontsize=7, color=INK2)
    ax.set_ylabel("unsafe outcomes per 100 failures", fontsize=7, color=INK2)
    ax.set_xticks([0, 10, 20, 30])
    ax.set_xlim(-1, 31)
    ax.set_ylim(0, max(5, ax.get_ylim()[1]))
    ax.legend(fontsize=7, frameon=False, loc="upper left", labelcolor=INK2)
    fig.tight_layout()
    for ext in ("pdf", "png"):
        fig.savefig(os.path.join(out, f"fig_injection.{ext}"), facecolor=SURFACE)
    plt.close(fig)


# ------------------------------------------------------------------ main

def main():
    here = os.path.dirname(os.path.abspath(__file__))
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--results", default=os.path.join(here, "results"))
    p.add_argument("--out", default=None)
    args = p.parse_args()
    out = args.out or os.path.join(args.results, "paper")
    os.makedirs(out, exist_ok=True)

    runs = valid_runs(args.results)
    skipped = [r for r in read_csv(os.path.join(args.results, "summary.csv")) if r.get("valid") == "0"]
    report = ["# LedgeMind results", "",
              f"Runs used: {len(runs)} (n=100, valid, latest per seed/strategy/variant). "
              f"Invalid runs skipped: {len(skipped)}.", ""]
    for title, name, rows in [
        ("Main results (mean ± std over seeds)", "table_main", table_main(runs)),
        ("Resolved per fault (summed over seeds)", "table_by_fault", table_by_fault(args.results, runs)),
        ("Experiments (seed 1)", "table_ablation", table_ablation(runs)),
        ("Throughput: normal transfers, recovery idle vs busy", "table_throughput", table_throughput(args.results)),
    ]:
        md = write_table(rows, out, name)
        if md:
            report += [f"## {title}", "", md, ""]

    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        plt.rcParams.update({"font.family": "DejaVu Sans", "hatch.color": "#ffffff", "hatch.linewidth": 0.8})
        fig_outcomes(args.results, runs, out, plt)
        fig_injection(runs, out, plt)
        report += ["Figures: fig_outcomes.pdf, fig_injection.pdf (PNG copies for previews).", ""]
    except ImportError:
        report += ["(matplotlib not installed: figures skipped)", ""]

    with open(os.path.join(out, "report.md"), "w", encoding="utf-8") as f:
        f.write("\n".join(report))
    print("\n".join(report))
    print(f"written to {out}")


if __name__ == "__main__":
    main()
