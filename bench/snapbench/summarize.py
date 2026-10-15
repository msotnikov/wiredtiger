"""Summarize run.sh output: median over rounds per build and workload, values per round in brackets.

Usage: python3 -I summarize.py RESULTS [METRIC...]
Default metrics: ops_per_sec, begin p50/p99/p99.9/max for readers and writers, stat3 (ID
allocation retries).
"""
import collections
import statistics
import sys

DEFAULT = ["ops_per_sec", "begin_r_p50", "begin_r_p99", "begin_r_p999", "begin_r_max",
           "begin_w_p99", "begin_w_max", "txn_w_p99", "txn_w_max", "stat2", "stat1", "stat3"]


def parse(path):
    runs = []
    with open(path) as f:
        for line in f:
            kv = {}
            for tok in line.split():
                if "=" in tok:
                    k, v = tok.split("=", 1)
                    kv[k] = v
            if "ops_per_sec" in kv:
                runs.append(kv)
    return runs


def num(v):
    try:
        return float(v)
    except ValueError:
        return None


def fmt(x):
    if x is None:
        return "-"
    if abs(x) >= 100:
        return f"{x:,.0f}".replace(",", " ")
    return f"{x:.2f}"


def main():
    path = sys.argv[1]
    metrics = sys.argv[2:] or DEFAULT
    runs = parse(path)
    groups = collections.OrderedDict()
    builds = []
    for r in runs:
        key = (int(r["threads"]), int(r["read_pct"]), int(r["nice"]), int(r["ts"]))
        groups.setdefault(key, collections.defaultdict(list))[r["build"]].append(r)
        if r["build"] not in builds:
            builds.append(r["build"])
    for key, by_build in groups.items():
        t, rp, nc, ts = key
        print(f"\n### threads={t} read_pct={rp} low_priority={nc} timestamps={ts}\n")
        print("| metric | " + " | ".join(builds) + " |")
        print("|---|" + "---|" * len(builds))
        for m in metrics:
            cells = []
            for b in builds:
                vals = [num(r.get(m, "")) for r in by_build.get(b, [])]
                vals = [v for v in vals if v is not None]
                if not vals:
                    cells.append("-")
                    continue
                med = statistics.median(vals)
                cells.append(f"{fmt(med)} ({', '.join(fmt(v) for v in vals)})")
            print(f"| {m} | " + " | ".join(cells) + " |")


if __name__ == "__main__":
    main()
