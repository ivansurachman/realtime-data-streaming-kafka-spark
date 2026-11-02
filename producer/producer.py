"""Event producer transaksi -> Kafka topic `transactions`.

1-2 detik per event; dari 60 event: 7 invalid + 4 late (lihat SCHEDULE).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from collections import Counter
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Callable, Dict, Optional

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()  # cari .env ke atas dari folder ini

TOPIC = os.getenv("TOPIC_RAW", "transactions")
WATERMARK_MIN = int(os.getenv("WATERMARK_MINUTES", "3"))
AMOUNT_MAX = int(os.getenv("AMOUNT_MAX", "10000000"))
TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
SOURCES = ("mobile", "web", "pos")

log = logging.getLogger("producer")


class Kind(str, Enum):
    VALID = "valid"
    NEGATIVE_AMOUNT = "negative_amount"
    HUGE_AMOUNT = "huge_amount"
    BAD_TIMESTAMP = "bad_timestamp"
    UNKNOWN_SOURCE = "unknown_source"
    MISSING_FIELD = "missing_field"
    BAD_TYPE = "bad_type"
    DUPLICATE = "duplicate"
    LATE = "late"


# nomor event (1-based) -> jenis; selain ini = VALID
SCHEDULE: Dict[int, Kind] = {
    8: Kind.NEGATIVE_AMOUNT,
    12: Kind.HUGE_AMOUNT,
    14: Kind.LATE,
    16: Kind.BAD_TIMESTAMP,
    20: Kind.UNKNOWN_SOURCE,
    22: Kind.LATE,
    24: Kind.DUPLICATE,
    28: Kind.MISSING_FIELD,
    30: Kind.LATE,
    32: Kind.BAD_TYPE,
    36: Kind.LATE,
}

Event = dict


def utc_timestamp(seconds_ago: int = 0) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds_ago)).strftime(TS_FORMAT)


def new_valid_event() -> Event:
    return {
        "user_id": f"U{random.randint(10000, 99999)}",
        "amount": random.randrange(10_000, 500_001, 1_000),
        "timestamp": utc_timestamp(),
        "source": random.choice(SOURCES),
    }


# Cara merusak event valid untuk tiap jenis (DUPLICATE ditangani di make_event)
MUTATORS: Dict[Kind, Callable[[Event], object]] = {
    Kind.NEGATIVE_AMOUNT: lambda e: e.update(amount=-random.randint(1_000, 50_000)),
    Kind.HUGE_AMOUNT: lambda e: e.update(amount=AMOUNT_MAX * 5),
    Kind.BAD_TIMESTAMP: lambda e: e.update(timestamp="2025-13-45T99:99:99Z"),
    Kind.UNKNOWN_SOURCE: lambda e: e.update(source="atm"),
    Kind.MISSING_FIELD: lambda e: e.pop("user_id"),
    Kind.BAD_TYPE: lambda e: e.update(amount="seratus ribu"),
    # 2-7 menit melewati watermark
    Kind.LATE: lambda e: e.update(timestamp=utc_timestamp(WATERMARK_MIN * 60 + random.randint(120, 420))),
}


def make_event(kind: Kind, last_valid: Optional[Event] = None) -> Event:
    if kind is Kind.DUPLICATE:
        return dict(last_valid)  # user_id + timestamp sama persis
    event = new_valid_event()
    mutate = MUTATORS.get(kind)
    if mutate:
        mutate(event)
    return event


@dataclass
class Stats:
    sent: Counter = field(default_factory=Counter)  # per jenis event
    delivered: int = 0
    failed: int = 0


def _random_pause() -> None:
    time.sleep(random.uniform(1, 2))


def send_events(producer: Producer, count: int, stats: Stats,
                pause: Callable[[], None] = _random_pause) -> None:
    def on_delivery(err, msg) -> None:
        if err:
            stats.failed += 1
            log.error("delivery gagal: %s", err)
        else:
            stats.delivered += 1
            log.debug("delivered partition=%s offset=%s", msg.partition(), msg.offset())

    last_valid: Optional[Event] = None
    for number in range(1, count + 1):
        kind = SCHEDULE.get(number, Kind.VALID)
        event = make_event(kind, last_valid)
        if kind is Kind.VALID:
            last_valid = event
        producer.produce(TOPIC, key=event.get("user_id"), value=json.dumps(event), callback=on_delivery)
        producer.poll(0)
        stats.sent[kind.value] += 1
        log.info("[%03d] %-15s %s", number, kind.value, json.dumps(event))
        pause()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_HOST", "localhost:9092"))
    parser.add_argument("--count", type=int, default=int(os.getenv("PRODUCER_COUNT", "60")))
    args = parser.parse_args()

    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s | %(levelname)-5s | %(name)s | %(message)s")
    log.info("start bootstrap=%s topic=%s count=%d", args.bootstrap, TOPIC, args.count)

    producer = Producer({"bootstrap.servers": args.bootstrap})
    stats = Stats()
    try:
        send_events(producer, args.count, stats)
    except KeyboardInterrupt:
        log.warning("dihentikan manual")
    finally:
        pending = producer.flush(10)
        if pending:
            log.error("%d pesan belum terkirim (broker tidak terjangkau?)", pending)
        log.info("selesai delivered=%d failed=%d jenis=%s",
                 stats.delivered, stats.failed, dict(stats.sent))


if __name__ == "__main__":
    main()
