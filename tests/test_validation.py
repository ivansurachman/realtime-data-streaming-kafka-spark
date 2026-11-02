"""Tes logika validasi tanpa Kafka: DataFrame statis + write_kafka di-mock."""
import json

import pytest

import spark_streaming_job as job
from config import Settings
from validation import PipelineState, Reason, flag_late_and_duplicates, validate_stateless

CFG = Settings()
BASE = 1_765_702_800  # 2025-12-14T09:00:00Z


def iso(offset_sec: int = 0) -> str:
    from datetime import datetime, timezone
    return datetime.fromtimestamp(BASE + offset_sec, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def event(**overrides) -> str:
    base = {"user_id": "U1", "amount": 150_000, "timestamp": iso(), "source": "mobile"}
    base.update(overrides)
    return json.dumps({k: v for k, v in base.items() if v is not None})


def to_df(spark, payloads):
    return spark.createDataFrame([(p, i) for i, p in enumerate(payloads)], ["raw", "kafka_offset"])


def reasons_of(df):
    return [r["error_reason"] for r in df.orderBy("kafka_offset").collect()]


@pytest.mark.parametrize("payload, expected", [
    (event(), None),
    (event(user_id=None), Reason.MISSING_FIELD),
    (event(amount=None), Reason.MISSING_FIELD),
    (event(timestamp=None), Reason.MISSING_FIELD),
    (event(amount="seratus ribu"), Reason.INVALID_TYPE_AMOUNT),
    (event(timestamp="2025-13-45T99:99:99Z"), Reason.INVALID_TYPE_TIMESTAMP),
    (event(amount=-5_000), Reason.AMOUNT_OUT_OF_RANGE),
    (event(amount=0), Reason.AMOUNT_OUT_OF_RANGE),
    (event(amount=10_000_001), Reason.AMOUNT_OUT_OF_RANGE),
    (event(amount=10_000_000), None),
    (event(amount=1), None),
    (event(source="atm"), Reason.INVALID_SOURCE),
    (event(source=None), Reason.INVALID_SOURCE),  # source bukan mandatory field, tapi harus dikenal
    ("not json", Reason.MISSING_FIELD),
])
def test_stateless_rules(spark, payload, expected):
    [reason] = reasons_of(validate_stateless(to_df(spark, [payload]), CFG))
    assert reason == (expected.value if expected else None)


def test_duplicate_within_batch_flags_second_only(spark):
    df = validate_stateless(to_df(spark, [event(), event(), event(user_id="U2")]), CFG)
    out = flag_late_and_duplicates(df, PipelineState(CFG.watermark_sec))
    assert reasons_of(out) == [None, Reason.DUPLICATE_EVENT.value, None]


def test_duplicate_across_batches_uses_state(spark):
    state = PipelineState(CFG.watermark_sec)
    state.record_batch({f"U1|{iso()}": BASE}, 150_000, 1, 0, BASE)
    df = validate_stateless(to_df(spark, [event()]), CFG)
    assert reasons_of(flag_late_and_duplicates(df, state)) == [Reason.DUPLICATE_EVENT.value]


def test_invalid_event_does_not_mark_valid_twin_as_duplicate(spark):
    df = validate_stateless(to_df(spark, [event(source="atm"), event()]), CFG)
    out = flag_late_and_duplicates(df, PipelineState(CFG.watermark_sec))
    assert reasons_of(out) == [Reason.INVALID_SOURCE.value, None]


@pytest.mark.parametrize("age_sec, late", [(179, False), (181, True), (600, True)])
def test_late_event_relative_to_watermark(spark, age_sec, late):
    state = PipelineState(CFG.watermark_sec)
    state.record_batch({}, 0, 0, 0, BASE)  # event terbaru = BASE
    df = validate_stateless(to_df(spark, [event(timestamp=iso(-age_sec))]), CFG)
    expected = Reason.LATE_EVENT.value if late else None
    assert reasons_of(flag_late_and_duplicates(df, state)) == [expected]


def test_no_late_detection_before_first_valid_event(spark):
    df = validate_stateless(to_df(spark, [event(timestamp=iso(-3600))]), CFG)
    assert reasons_of(flag_late_and_duplicates(df, PipelineState(CFG.watermark_sec))) == [None]


def test_state_prunes_keys_older_than_watermark():
    state = PipelineState(180)
    state.record_batch({"old": BASE - 1000, "recent": BASE - 10}, 0, 0, 0, BASE)
    assert set(state.seen) == {"recent"}


def test_process_batch_routes_valid_and_dlq(spark, monkeypatch):
    sent = {}
    monkeypatch.setattr(job, "write_kafka",
                        lambda df, topic, bootstrap: sent.setdefault(topic, []).extend(
                            json.loads(r.value) for r in df.collect()))
    state = PipelineState(CFG.watermark_sec)
    job.process_batch(validate_stateless(to_df(spark, [
        event(user_id="U1", amount=1_000), event(user_id="U2", amount=2_000),
        event(user_id="U3", amount=-1), event(user_id="U1", amount=1_000),
    ]), CFG), 0, CFG, state)
    job.process_batch(validate_stateless(to_df(spark, [
        event(user_id="U9", timestamp=iso(-600)),
    ]), CFG), 1, CFG, state)

    assert sorted(m["user_id"] for m in sent[CFG.topic_valid]) == ["U1", "U2"]
    assert sorted(m["error_reason"] for m in sent[CFG.topic_dlq]) == sorted([
        Reason.AMOUNT_OUT_OF_RANGE.value, Reason.DUPLICATE_EVENT.value, Reason.LATE_EVENT.value])
    assert (state.total_amount, state.valid_count, state.late_count) == (3_000, 2, 1)
