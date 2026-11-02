"""Konfigurasi streaming job, dibaca dari environment (.env)."""
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap: str = "localhost:9092"
    topic_raw: str = "transactions"
    topic_valid: str = "transactions_valid"
    topic_dlq: str = "transactions_dlq"
    watermark_min: int = 3
    window_min: int = 1
    amount_min: float = 1
    amount_max: float = 10_000_000
    valid_sources: tuple = ("mobile", "web", "pos")

    @property
    def watermark_delay(self) -> str:
        return f"{self.watermark_min} minutes"

    @property
    def watermark_sec(self) -> int:
        return self.watermark_min * 60

    @property
    def window_duration(self) -> str:
        return f"{self.window_min} minutes"

    @classmethod
    def from_env(cls) -> "Settings":
        env = os.getenv
        d = cls()
        return cls(
            bootstrap=env("KAFKA_BOOTSTRAP_INTERNAL", d.bootstrap),
            topic_raw=env("TOPIC_RAW", d.topic_raw),
            topic_valid=env("TOPIC_VALID", d.topic_valid),
            topic_dlq=env("TOPIC_DLQ", d.topic_dlq),
            watermark_min=int(env("WATERMARK_MINUTES", d.watermark_min)),
            window_min=int(env("WINDOW_MINUTES", d.window_min)),
            amount_min=float(env("AMOUNT_MIN", d.amount_min)),
            amount_max=float(env("AMOUNT_MAX", d.amount_max)),
            valid_sources=tuple(env("VALID_SOURCES", ",".join(d.valid_sources)).split(",")),
        )
