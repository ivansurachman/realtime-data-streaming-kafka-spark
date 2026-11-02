"""Aturan validasi transaksi (Spark column expressions) dan state antar-batch."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional

from pyspark.sql import Column, DataFrame, Window, functions as F
from pyspark.sql.types import StringType, StructField, StructType

from config import Settings

TS_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"

# Semua field dibaca sebagai string supaya type validation bisa dilakukan eksplisit
# (kalau langsung LongType, "abc" diam-diam jadi null tanpa jejak).
SCHEMA = StructType([StructField(n, StringType()) for n in ("user_id", "amount", "timestamp", "source")])


class Reason(str, Enum):
    """Alasan penolakan; nilainya masuk ke kolom `error_reason` dan DLQ."""

    MISSING_FIELD = "missing_field"
    INVALID_TYPE_AMOUNT = "invalid_type_amount"
    INVALID_TYPE_TIMESTAMP = "invalid_type_timestamp"
    AMOUNT_OUT_OF_RANGE = "amount_out_of_range"
    INVALID_SOURCE = "invalid_source"
    LATE_EVENT = "late_event"
    DUPLICATE_EVENT = "duplicate_event"


@dataclass
class PipelineState:
    """State lintas-batch (di driver): watermark, key duplikat, dan total berjalan."""

    watermark_sec: int
    max_event_time: Optional[int] = None  # epoch detik, dari event valid
    seen: Dict[str, int] = field(default_factory=dict)  # key user_id|timestamp -> epoch
    total_amount: float = 0.0
    valid_count: int = 0
    late_count: int = 0

    @property
    def watermark(self) -> Optional[int]:
        """Event dengan event_time < watermark dianggap terlambat (None sebelum ada data)."""
        if self.max_event_time is None:
            return None
        return self.max_event_time - self.watermark_sec

    def record_batch(self, valid_keys: Dict[str, int], amount: float, count: int,
                     late: int, max_event_time: Optional[int]) -> None:
        self.seen.update(valid_keys)
        if max_event_time is not None:
            self.max_event_time = max(self.max_event_time or 0, max_event_time)
        self.total_amount += amount
        self.valid_count += count
        self.late_count += late
        self._prune()

    def _prune(self) -> None:
        """Key lebih tua dari watermark tidak perlu disimpan: event serupa sudah pasti late."""
        cutoff = self.watermark
        if cutoff is not None:
            self.seen = {k: t for k, t in self.seen.items() if t >= cutoff}


def _stateless_reason(cfg: Settings) -> Column:
    """Validasi 1-4 (mandatory, type, range, source); urutan = prioritas alasan."""
    missing = (
        (F.coalesce(F.col("user_id"), F.lit("")) == "")
        | F.col("amount").isNull()
        | F.col("timestamp").isNull()
    )
    source_ok = F.coalesce(F.col("source").isin(list(cfg.valid_sources)), F.lit(False))
    in_range = F.col("amount_num").between(cfg.amount_min, cfg.amount_max)
    return F.coalesce(
        F.when(missing, Reason.MISSING_FIELD.value),
        F.when(F.col("amount_num").isNull(), Reason.INVALID_TYPE_AMOUNT.value),
        F.when(F.col("event_time").isNull(), Reason.INVALID_TYPE_TIMESTAMP.value),
        F.when(~in_range, Reason.AMOUNT_OUT_OF_RANGE.value),
        F.when(~source_ok, Reason.INVALID_SOURCE.value),
    )


def validate_stateless(raw_df: DataFrame, cfg: Settings) -> DataFrame:
    """Input: kolom `raw` (JSON string) + `kafka_offset`. Output: + event_time, amount_num, error_reason."""
    parsed = (
        raw_df.withColumn("j", F.from_json("raw", SCHEMA))
        .select("raw", "kafka_offset", "j.*")
        .withColumn("amount_num", F.col("amount").cast("double"))
        .withColumn("event_time", F.to_timestamp("timestamp", TS_FORMAT))
    )
    return parsed.withColumn("error_reason", _stateless_reason(cfg))


def flag_late_and_duplicates(df: DataFrame, state: PipelineState) -> DataFrame:
    """Validasi 5 (duplikat user_id+timestamp) dan aturan late > watermark.

    Urutan: stateless -> late -> duplikat. Mengembalikan df dengan `error_reason`
    final, `is_valid`, dan `key`.
    """
    watermark = state.watermark
    is_late = (
        F.col("event_time").cast("long") < F.lit(watermark) if watermark is not None else F.lit(False)
    )
    ok_so_far = F.col("after_late").isNull()
    # duplikat dalam batch: urutan pertama lolos; antar batch: cek `state.seen`
    first_rank = F.row_number().over(
        Window.partitionBy("key", ok_so_far).orderBy("kafka_offset"))
    is_duplicate = ok_so_far & ((first_rank > 1) | F.col("key").isin(list(state.seen)))

    return (
        df.withColumn("key", F.concat_ws("|", "user_id", "timestamp"))
        .withColumn("after_late", F.coalesce("error_reason", F.when(is_late, Reason.LATE_EVENT.value)))
        .withColumn("error_reason",
                    F.coalesce("after_late", F.when(is_duplicate, Reason.DUPLICATE_EVENT.value)))
        .withColumn("is_valid", F.col("error_reason").isNull())
        .drop("after_late")
    )
