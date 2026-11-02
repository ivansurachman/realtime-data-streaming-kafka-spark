"""
Streaming for Data Pipeline

Query A: Kafka(transactions) -> validasi -> transactions_valid / transactions_dlq
                                         -> console (timestamp, running_total)
Query B: Kafka(transactions_valid) -> watermark + tumbling window 1 menit -> console
"""
from __future__ import annotations

import logging
import os
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from config import Settings
from validation import (TS_FORMAT, PipelineState, Reason, flag_late_and_duplicates,
                        validate_stateless)

log = logging.getLogger("streaming")
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3"


@dataclass
class BatchStats:
    total_in: int
    amount: float
    count: int
    max_event_time: Optional[int]
    valid_keys: Dict[str, int]
    reasons: Dict[str, int]
    late_by_window: Dict[str, int]


# ---------------------------------------------------------------- Kafka I/O
def read_kafka(spark: SparkSession, cfg: Settings, topic: str) -> DataFrame:
    return (spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "latest")
            .load())


def write_kafka(df: DataFrame, topic: str, bootstrap: str) -> None:
    """df harus punya kolom `value` (string), opsional `key`."""
    (df.write.format("kafka")
       .option("kafka.bootstrap.servers", bootstrap)
       .option("topic", topic)
       .save())


# ---------------------------------------------------------------- per batch
def publish(valid: DataFrame, invalid: DataFrame, cfg: Settings) -> None:
    """Routing: valid -> transactions_valid, invalid -> transactions_dlq."""
    valid_msg = valid.select(
        F.col("user_id").alias("key"),
        F.to_json(F.struct("user_id", F.col("amount_num").cast("long").alias("amount"),
                           "timestamp", "source")).alias("value"))
    dlq_msg = invalid.select(F.to_json(F.struct("raw", "error_reason")).alias("value"))
    write_kafka(valid_msg, cfg.topic_valid, cfg.bootstrap)
    write_kafka(dlq_msg, cfg.topic_dlq, cfg.bootstrap)


def collect_stats(valid: DataFrame, invalid: DataFrame, cfg: Settings) -> BatchStats:
    event_epoch = F.col("event_time").cast("long")
    amount, count, max_et = valid.agg(
        F.coalesce(F.sum("amount_num"), F.lit(0.0)), F.count("*"), F.max(event_epoch)).first()
    valid_keys = {r["key"]: r["t"] for r in valid.select("key", event_epoch.alias("t")).collect()}
    reasons = {r["error_reason"]: r["count"]
               for r in invalid.groupBy("error_reason").count().collect()}

    late = invalid.filter(F.col("error_reason") == Reason.LATE_EVENT.value)
    window_start = F.window("event_time", cfg.window_duration)["start"]
    late_by_window = {
        r["w"]: r["count"]
        for r in late.select(F.date_format(window_start, "HH:mm").alias("w")).groupBy("w").count().collect()
    }
    return BatchStats(count + invalid.count(), float(amount), count, max_et,
                      valid_keys, reasons, late_by_window)


def show_running_total(spark: SparkSession, state: PipelineState, late_in_batch: int) -> None:
    """Output console: timestamp (waktu Spark) + running_total (kumulatif valid)."""
    (spark.createDataFrame(
        [(state.total_amount, state.valid_count, late_in_batch, state.late_count)],
        ["running_total", "running_count", "late_in_batch", "late_total"])
     .select(F.current_timestamp().alias("timestamp"),
             F.col("running_total").cast("decimal(20,2)").alias("running_total"),
             "running_count", "late_in_batch", "late_total")
     .show(truncate=False))
    sys.stdout.flush()  # show() = print Python; tanpa flush tertahan buffer saat tanpa TTY


def _process(df: DataFrame, batch_id: int, cfg: Settings, state: PipelineState) -> None:
    started = time.time()
    batch = flag_late_and_duplicates(df, state).cache()
    try:
        valid, invalid = batch.filter("is_valid"), batch.filter(~F.col("is_valid"))
        publish(valid, invalid, cfg)
        stats = collect_stats(valid, invalid, cfg)

        late_in_batch = stats.reasons.get(Reason.LATE_EVENT.value, 0)
        state.record_batch(stats.valid_keys, stats.amount, stats.count,
                           late_in_batch, stats.max_event_time)
        show_running_total(df.sparkSession, state, late_in_batch)
        log.info(
            "batch=%s in=%d valid=%d invalid=%d reasons=%s late_by_window=%s wm_epoch=%s "
            "seen=%d running_total=%.0f dur=%.1fs",
            batch_id, stats.total_in, stats.count, sum(stats.reasons.values()), stats.reasons,
            stats.late_by_window, state.watermark, len(state.seen), state.total_amount,
            time.time() - started)
    finally:
        batch.unpersist()


def process_batch(df: DataFrame, batch_id: int, cfg: Settings, state: PipelineState) -> None:
    try:
        _process(df, batch_id, cfg, state)
    except Exception:
        log.exception("batch=%s GAGAL diproses", batch_id)
        raise


# ---------------------------------------------------------------- queries
def build_validation_query(spark: SparkSession, cfg: Settings, state: PipelineState,
                           checkpoint: str) -> StreamingQuery:
    raw = read_kafka(spark, cfg, cfg.topic_raw).select(
        F.col("value").cast("string").alias("raw"), F.col("offset").alias("kafka_offset"))
    # Default: .withWatermark("event_time", "3 minutes"). Watermark Spark hanya aktif di
    # operasi stateful (Query B); late > watermark ditegakkan di process_batch dgn formula sama.
    stream = validate_stateless(raw, cfg).withWatermark("event_time", cfg.watermark_delay)
    return (stream.writeStream
            .foreachBatch(lambda df, batch_id: process_batch(df, batch_id, cfg, state))
            .option("checkpointLocation", f"{checkpoint}/validation")
            .start())


def build_window_query(spark: SparkSession, cfg: Settings, checkpoint: str) -> StreamingQuery:
    valid = (read_kafka(spark, cfg, cfg.topic_valid)
             .select(F.from_json(F.col("value").cast("string"),
                                 "user_id string, amount long, timestamp string, source string").alias("j"))
             .select("j.*")
             .withColumn("event_time", F.to_timestamp("timestamp", TS_FORMAT)))
    per_window = (valid.withWatermark("event_time", cfg.watermark_delay)
                  .groupBy(F.window("event_time", cfg.window_duration))  # tumbling
                  .agg(F.count("*").alias("total_transactions"), F.sum("amount").alias("total_amount")))
    return (per_window.writeStream.outputMode("update").format("console")
            .option("truncate", "false")
            .option("checkpointLocation", f"{checkpoint}/window")
            .start())


# ---------------------------------------------------------------- entry point
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder.appName("streaming-for-data-pipeline")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")   # 'Z' di timestamp = UTC
        .config("spark.sql.ansi.enabled", "false")     # cast/parse invalid -> null, bukan exception
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main() -> None:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)

    cfg = Settings.from_env()
    log.info("config %s", cfg)
    state = PipelineState(watermark_sec=cfg.watermark_sec)
    checkpoint = tempfile.mkdtemp(prefix="ckpt_")  # fresh tiap run: state driver tidak persisten

    spark = create_spark()
    build_validation_query(spark, cfg, state, checkpoint)
    build_window_query(spark, cfg, checkpoint)
    log.info(">>> STREAMING STARTED — sekarang jalankan producer")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
