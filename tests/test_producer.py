from datetime import datetime, timezone

import producer as p


class FakeProducer:
    def __init__(self):
        self.messages = []

    def produce(self, topic, key, value, callback):
        self.messages.append((topic, key, value))
        callback(None, type("Msg", (), {"partition": lambda s: 0, "offset": lambda s: 0})())

    def poll(self, _):
        pass


def test_schedule_meets_assignment_minimums():
    kinds = list(p.SCHEDULE.values())
    late = kinds.count(p.Kind.LATE)
    invalid = len([k for k in kinds if k is not p.Kind.LATE])
    assert late >= 3 and invalid >= 3
    assert p.Kind.DUPLICATE in kinds


def test_valid_event_has_required_format():
    e = p.new_valid_event()
    assert set(e) == {"user_id", "amount", "timestamp", "source"}
    assert e["source"] in p.SOURCES and 1 <= e["amount"] <= p.AMOUNT_MAX
    datetime.strptime(e["timestamp"], p.TS_FORMAT)


def test_each_invalid_kind_is_actually_broken():
    assert p.make_event(p.Kind.NEGATIVE_AMOUNT)["amount"] < 0
    assert p.make_event(p.Kind.HUGE_AMOUNT)["amount"] > p.AMOUNT_MAX
    assert p.make_event(p.Kind.UNKNOWN_SOURCE)["source"] not in p.SOURCES
    assert "user_id" not in p.make_event(p.Kind.MISSING_FIELD)
    assert isinstance(p.make_event(p.Kind.BAD_TYPE)["amount"], str)


def test_late_event_is_older_than_watermark():
    ts = datetime.strptime(p.make_event(p.Kind.LATE)["timestamp"], p.TS_FORMAT).replace(tzinfo=timezone.utc)
    assert (datetime.now(timezone.utc) - ts).total_seconds() > p.WATERMARK_MIN * 60


def test_duplicate_repeats_last_valid_event():
    last = p.new_valid_event()
    assert p.make_event(p.Kind.DUPLICATE, last) == last


def test_send_events_publishes_everything_to_topic():
    fake, stats = FakeProducer(), p.Stats()
    p.send_events(fake, 40, stats, pause=lambda: None)
    assert len(fake.messages) == 40 and {m[0] for m in fake.messages} == {p.TOPIC}
    assert stats.delivered == 40 and stats.sent["late"] == 4
