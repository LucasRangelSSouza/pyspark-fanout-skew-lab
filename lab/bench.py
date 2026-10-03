"""Runs one scenario variant in its own Spark application and records time, shuffle, spill, task skew and a checksum.

  python -m lab.bench generate --rows 20000000 --data /tmp/lab-data
  python -m lab.bench run --scenario duplicate_key --variant naive --data /tmp/lab-data --out results.jsonl

Metrics come from the Spark event log of the application (SparkListenerTaskEnd), so they are the same numbers the
Spark UI shows, collected without a UI.
"""
from __future__ import annotations

import argparse
import glob
import json
import os
import platform
import statistics
import tempfile
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from lab.scenarios import SCENARIOS, generate


def session(name: str, event_dir: str) -> SparkSession:
    return (
        SparkSession.builder.appName(name)
        .master(os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("DRIVER_MEMORY", "6g"))
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", event_dir)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def task_metrics(event_dir: str) -> dict:
    """Sum shuffle and spill over every task, and compare the slowest task with the median one (the skew signal)."""
    log = sorted(glob.glob(os.path.join(event_dir, "*")), key=os.path.getmtime)[-1]
    shuffle_read = shuffle_write = mem_spill = disk_spill = 0
    run_times = []
    with open(log, encoding="utf-8") as fh:
        for line in fh:
            event = json.loads(line)
            if event.get("Event") != "SparkListenerTaskEnd" or "Task Metrics" not in event:
                continue
            m = event["Task Metrics"]
            read = m.get("Shuffle Read Metrics", {})
            shuffle_read += read.get("Remote Bytes Read", 0) + read.get("Local Bytes Read", 0)
            shuffle_write += m.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)
            mem_spill += m.get("Memory Bytes Spilled", 0)
            disk_spill += m.get("Disk Bytes Spilled", 0)
            run_times.append(m.get("Executor Run Time", 0))
    run_times = [t for t in run_times if t > 0] or [0]
    median = statistics.median(run_times)
    return {
        "shuffle_read_mb": round(shuffle_read / 2**20, 1),
        "shuffle_write_mb": round(shuffle_write / 2**20, 1),
        "spill_memory_mb": round(mem_spill / 2**20, 1),
        "spill_disk_mb": round(disk_spill / 2**20, 1),
        "tasks": len(run_times),
        "max_task_ms": max(run_times),
        "median_task_ms": median,
        "max_to_median_task": round(max(run_times) / median, 1) if median else None,
    }


def checksum(df) -> dict:
    """Row count and an order-independent hash of every row, with doubles rounded so float noise doesn't count."""
    cols = [F.round(F.col(c), 4) if t == "double" else F.col(c) for c, t in df.dtypes]
    row = df.select(F.count(F.lit(1)).alias("n"), F.sum(F.xxhash64(*cols) % 1_000_000_007).alias("h")).first()
    return {"rows": row["n"], "hash": str(row["h"])}


def run(scenario: str, variant: str, data: str, out: str, repeat: int) -> None:
    event_dir = tempfile.mkdtemp(prefix="events-")
    spark = session(f"{scenario}-{variant}", "file://" + event_dir.replace("\\", "/") if os.name == "nt" else event_dir)
    df = SCENARIOS[scenario](spark, data, variant)
    start = time.perf_counter()
    df.write.format("noop").mode("overwrite").save()
    seconds = round(time.perf_counter() - start, 2)
    metrics = task_metrics(event_dir)
    extra = {}
    if scenario == "duplicate_key" and variant == "right":
        # The dedup is only safe if the header columns are constant per key; record the proof with the result.
        headers = spark.read.parquet(f"{data}/headers_dup")
        varying = headers.groupBy("order_id").agg(F.countDistinct("channel", "order_date").alias("k")).filter("k > 1").count()
        extra["keys_with_varying_header_columns"] = varying
    result = {"scenario": scenario, "variant": variant, "repeat": repeat, "seconds": seconds, **metrics,
              **checksum(df), **extra, "spark": spark.version, "python": platform.python_version(), "cpus": os.cpu_count()}
    spark.stop()
    with open(out, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(result) + "\n")
    print(json.dumps(result))


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)
    g = sub.add_parser("generate")
    g.add_argument("--rows", type=int, default=20_000_000)
    g.add_argument("--data", required=True)
    r = sub.add_parser("run")
    r.add_argument("--scenario", choices=sorted(SCENARIOS), required=True)
    r.add_argument("--variant", choices=["naive", "wrong", "right"], required=True)
    r.add_argument("--data", required=True)
    r.add_argument("--out", required=True)
    r.add_argument("--repeat", type=int, default=1)
    args = parser.parse_args()
    if args.cmd == "generate":
        spark = session("generate", tempfile.mkdtemp(prefix="events-"))
        generate(spark, args.rows, args.data)
        spark.stop()
    else:
        run(args.scenario, args.variant, args.data, args.out, args.repeat)


if __name__ == "__main__":
    main()
