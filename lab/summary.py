"""Prints a Markdown table of the median of each scenario and variant, and checks that right matches naive."""
from __future__ import annotations

import json
import statistics
import sys
from collections import defaultdict


def main(path: str) -> None:
    runs = [json.loads(line) for line in open(path, encoding="utf-8") if line.strip()]
    groups = defaultdict(list)
    for r in runs:
        groups[(r["scenario"], r["variant"])].append(r)
    print("| scenario | variant | runs | seconds (median) | shuffle read MB | spill MB | slowest / median task | rows | matches naive |")
    print("|---|---|---:|---:|---:|---:|---:|---:|---|")
    for (scenario, variant), rs in sorted(groups.items()):
        naive = groups.get((scenario, "naive"), [{}])[0]
        same = "n/a (key changed)" if scenario == "weak_window_key" else ("yes" if rs[0]["hash"] == naive.get("hash") and rs[0]["rows"] == naive.get("rows") else "NO")
        print(f"| {scenario} | {variant} | {len(rs)} | {statistics.median(r['seconds'] for r in rs):.1f} | "
              f"{statistics.median(r['shuffle_read_mb'] for r in rs):.0f} | "
              f"{statistics.median(r['spill_memory_mb'] + r['spill_disk_mb'] for r in rs):.0f} | "
              f"{statistics.median(r['max_to_median_task'] or 0 for r in rs):.1f} | {rs[0]['rows']} | {same} |")


if __name__ == "__main__":
    main(sys.argv[1])
