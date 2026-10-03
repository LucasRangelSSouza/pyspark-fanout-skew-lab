"""Four join problems seen in production lakehouses, rebuilt on synthetic data, each with three variants:

  naive  the query as first written
  wrong  the fix people try first: more shuffle partitions (or AQE switched off, for the hot key)
  right  the fix that addresses the cause

Every scenario returns a DataFrame; bench.py times writing it to a no-op sink and checks that "right" returns the
same rows as "naive" (except the window scenario, where the naive key is the bug and the result is meant to change).
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def generate(spark: SparkSession, rows: int, path: str) -> None:
    """Writes the synthetic tables as Parquet. Fixed seeds, so every run reads the same data."""
    keys = max(1, rows // 50)
    # 1. Fact table and a header table whose key repeats 12 times (the header came from an item-grain source).
    spark.range(rows).select(
        F.col("id").alias("line_id"),
        (F.col("id") % keys).alias("order_id"),
        (F.rand(1) * 100).alias("amount"),
    ).write.mode("overwrite").parquet(f"{path}/lines")
    spark.range(keys * 12).select(
        (F.col("id") % keys).alias("order_id"),
        (F.col("id") % keys % 7).alias("channel"),
        F.lit("2026-01-01").alias("order_date"),
    ).write.mode("overwrite").parquet(f"{path}/headers_dup")

    # 2. A fact table where 1% of the keys carry 40% of the rows, against a dimension too large to broadcast.
    hot = max(1, keys // 100)
    spark.range(rows).select(
        F.col("id").alias("event_id"),
        F.when(F.rand(2) < 0.4, (F.rand(3) * hot).cast("long")).otherwise((F.rand(4) * keys).cast("long")).alias("customer_id"),
        (F.rand(5) * 50).alias("value"),
    ).write.mode("overwrite").parquet(f"{path}/events")
    spark.range(keys).select(
        F.col("id").alias("customer_id"),
        F.sha2(F.col("id").cast("string"), 256).alias("profile"),
        F.sha2((F.col("id") * 7).cast("string"), 256).alias("segment"),
    ).write.mode("overwrite").parquet(f"{path}/customers")

    # 3. Rows to number by document, where 20% have no document and share the same empty value.
    spark.range(rows).select(
        F.col("id").alias("entry_id"),
        F.when(F.rand(6) < 0.2, F.lit("")).otherwise((F.rand(7) * keys).cast("long").cast("string")).alias("document"),
        (F.col("id") % 3).alias("source"),
        (F.rand(8) * 1000).alias("amount"),
    ).write.mode("overwrite").parquet(f"{path}/entries")

    # 4. Legitimate fan-out: every card has 20 plates, and every transaction belongs to a card.
    cards = keys
    spark.range(cards * 20).select(
        (F.col("id") % cards).alias("card_id"),
        F.concat(F.lit("P"), F.col("id").cast("string")).alias("plate"),
    ).write.mode("overwrite").parquet(f"{path}/plates")
    spark.range(rows // 4).select(
        F.col("id").alias("tx_id"),
        (F.rand(9) * cards).cast("long").alias("card_id"),
        (F.rand(10) * 300).alias("amount"),
    ).write.mode("overwrite").parquet(f"{path}/transactions")


def duplicate_key(spark: SparkSession, path: str, variant: str) -> DataFrame:
    lines, headers = spark.read.parquet(f"{path}/lines"), spark.read.parquet(f"{path}/headers_dup")
    if variant == "right":
        # Prove the kept columns are constant per key (checked in bench.py), then deduplicate before the join.
        return lines.join(F.broadcast(headers.dropDuplicates(["order_id"])), "order_id")
    if variant == "wrong":
        spark.conf.set("spark.sql.shuffle.partitions", "800")
    return lines.join(headers, "order_id").dropDuplicates(["line_id"])


def hot_key(spark: SparkSession, path: str, variant: str) -> DataFrame:
    events, customers = spark.read.parquet(f"{path}/events"), spark.read.parquet(f"{path}/customers")
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")  # force a sort-merge join, as with two big tables
    if variant == "naive":
        spark.conf.set("spark.sql.adaptive.enabled", "false")
    elif variant == "wrong":
        spark.conf.set("spark.sql.adaptive.enabled", "false")
        spark.conf.set("spark.sql.shuffle.partitions", "800")
    else:
        spark.conf.set("spark.sql.adaptive.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "3")
        spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "16m")
    return events.join(customers, "customer_id")


def weak_window_key(spark: SparkSession, path: str, variant: str) -> DataFrame:
    entries = spark.read.parquet(f"{path}/entries")
    if variant == "right":
        # Rows with no document are unrelated to each other: give each one its own key instead of the shared "".
        # Rows with a document keep exactly the grouping of the naive query.
        key = F.when(F.col("document") == "", F.concat(F.lit("no-doc:"), F.col("entry_id").cast("string")))                .otherwise(F.col("document"))
        window = Window.partitionBy(key).orderBy("amount")
    else:
        if variant == "wrong":
            spark.conf.set("spark.sql.shuffle.partitions", "800")
        window = Window.partitionBy("document").orderBy("amount")
    return entries.withColumn("rn", F.row_number().over(window))


def legitimate_fanout(spark: SparkSession, path: str, variant: str) -> DataFrame:
    plates, tx = spark.read.parquet(f"{path}/plates"), spark.read.parquet(f"{path}/transactions")
    if variant == "right":
        # Aggregate each side to one row per card, then join: no row multiplication.
        spend = tx.groupBy("card_id").agg(F.sum("amount").alias("spend"), F.count("*").alias("transactions"))
        fleet = plates.groupBy("card_id").agg(F.count("*").alias("plates"))
        return spend.join(fleet, "card_id")
    if variant == "wrong":
        spark.conf.set("spark.sql.shuffle.partitions", "800")
    joined = tx.join(plates, "card_id")  # every transaction repeated once per plate
    return joined.groupBy("card_id").agg(
        (F.sum("amount") / F.count("plate") * F.countDistinct("tx_id")).alias("spend"),
        F.countDistinct("tx_id").alias("transactions"),
        F.countDistinct("plate").alias("plates"),
    )


SCENARIOS = {
    "duplicate_key": duplicate_key,
    "hot_key": hot_key,
    "weak_window_key": weak_window_key,
    "legitimate_fanout": legitimate_fanout,
}
