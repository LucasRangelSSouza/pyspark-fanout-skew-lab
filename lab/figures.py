"""Figures for the article, from a benchmark run's results.jsonl.

  python -m lab.figures results/run-37086764815/results.jsonl figures
"""
from __future__ import annotations

import json
import os
import statistics
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

BLUE, ORANGE, GREEN, GREY = "#006699", "#E8871E", "#2E8B57", "#8A8A8A"
LABELS = {"duplicate_key": "Duplicate key\n(header at item grain)", "hot_key": "Hot key\n(1% of keys, 40% of rows)",
          "weak_window_key": "Window over a\nweak key", "legitimate_fanout": "Legitimate fan-out\n(20 users per account)"}
VARIANTS = [("naive", "as first written", GREY), ("wrong", "more shuffle partitions", ORANGE), ("right", "fix the cause", GREEN)]


def main(path: str, out: str) -> None:
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False,
                         "axes.titleweight": "bold", "axes.grid": True, "grid.color": "#E5E7EB", "axes.axisbelow": True})
    os.makedirs(out, exist_ok=True)
    runs = defaultdict(list)
    for line in open(path, encoding="utf-8"):
        if line.strip():
            r = json.loads(line)
            runs[(r["scenario"], r["variant"])].append(r)
    med = lambda s, v, k: statistics.median((x[k] or 0) for x in runs[(s, v)])
    scenarios = list(LABELS)

    fig, ax = plt.subplots(figsize=(10, 3.9))
    for j, (v, label, c) in enumerate(VARIANTS):
        xs = [i + (j - 1) * 0.26 for i in range(len(scenarios))]
        vals = [med(s, v, "seconds") for s in scenarios]
        ax.bar(xs, vals, 0.26, color=c, label=label)
        for x, val in zip(xs, vals):
            ax.text(x, val + 0.8, f"{val:.1f}", ha="center", fontsize=8)
    ax.set_xticks(range(len(scenarios)), [LABELS[s] for s in scenarios])
    ax.set_ylabel("Seconds (median of 3)")
    ax.set_title("20 million rows on a 4-core GitHub runner")
    ax.legend(fontsize=8, frameon=False)
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig1_seconds.png"), dpi=200, facecolor="white", bbox_inches="tight"); plt.close(fig)

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 3.6))
    for j, (v, label, c) in enumerate(VARIANTS):
        a1.bar(j, med("duplicate_key", v, "spill_memory_mb") / 1024 + med("duplicate_key", v, "spill_disk_mb") / 1024, color=c)
        a2.bar(j, med("hot_key", v, "max_to_median_task"), color=c)
    for ax in (a1, a2):
        ax.set_xticks(range(3), [l for _, l, _ in VARIANTS], fontsize=8.5)
        for p in ax.patches:
            ax.text(p.get_x() + p.get_width() / 2, p.get_height() * 1.02 + 0.1, f"{p.get_height():.1f}", ha="center", fontsize=8)
    a1.set_ylabel("Spill (GB, memory + disk)"); a1.set_title("Duplicate key: spill")
    a2.set_ylabel("Slowest task / median task"); a2.set_title("Hot key: how uneven the tasks are")
    fig.tight_layout(); fig.savefig(os.path.join(out, "fig2_spill_and_skew.png"), dpi=200, facecolor="white", bbox_inches="tight"); plt.close(fig)
    print("ok")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
