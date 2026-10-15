"""Markdown tables for the WT-9074 Jira comment from run.sh results: one row per thread count and
number of low-priority threads, one column per build, median over rounds.

Usage: python3 -I jira_tables.py RESULTS METRIC [METRIC...]
Metrics ending in _p50/_p99/_p999/_p9999/_max are printed in microseconds, ops_per_sec as is.
"""
import collections
import statistics
import sys

BUILDS = [("base", "develop"), ("s1000", "PR (1000)"), ("s100", "PR, 100"), ("s0", "PR, 0")]
LABELS = {
    "ops_per_sec": "Committed transactions per second",
    "begin_r_p999": "p99.9 of begin_transaction in searches, us",
    "begin_w_p999": "p99.9 of begin_transaction in updates, us",
    "begin_r_max": "Maximum of begin_transaction in searches, us",
    "begin_w_max": "Maximum of begin_transaction in updates, us",
}


def parse(path):
    runs = []
    with open(path) as f:
        for line in f:
            kv = dict(t.split("=", 1) for t in line.split() if "=" in t)
            if "ops_per_sec" in kv:
                runs.append(kv)
    return runs


def fmt(metric, v):
    if metric == "ops_per_sec":
        return f"{v:,.0f}".replace(",", " ")
    us = v / 1000.0
    if us >= 100:
        return f"{us:,.0f}".replace(",", " ")
    return f"{us:.1f}"


def main():
    path, metrics = sys.argv[1], sys.argv[2:]
    runs = parse(path)
    groups = collections.OrderedDict()
    for r in runs:
        key = (int(r["threads"]), int(r["nice"]))
        groups.setdefault(key, collections.defaultdict(list))[r["build"]].append(r)
    for m in metrics:
        print(f"\n{LABELS.get(m, m)}\n")
        print("| threads | low priority | " + " | ".join(name for _, name in BUILDS) + " |")
        print("|---|---|" + "---|" * len(BUILDS))
        for (t, nc), by in sorted(groups.items()):
            cells = []
            for b, _ in BUILDS:
                vals = [float(r[m]) for r in by.get(b, []) if m in r]
                cells.append(fmt(m, statistics.median(vals)) if vals else "-")
            print(f"| {t} | {nc} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
