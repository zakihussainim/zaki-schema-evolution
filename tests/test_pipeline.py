"""Decoding, schema resolution and validation, with an in-memory schema registry."""

import random

from helpers import standard_resolver

from schema_evolution.avro import encode_datum
from schema_evolution.events import make_event, make_invalid, make_poison
from schema_evolution.pipeline import (
    RawRecord,
    archive_body,
    batch_id,
    curated_key,
    group_by_event_date,
    load_reader_schema,
    parse_archive,
    process_records,
    raw_key,
    validate_reading,
)
from schema_evolution.wire import frame

EVENT_TIME = 1_790_000_000_000  # 2026-09-21
NOW = "2026-10-08T10:00:00.000Z"


def record(data, number=1):
    return RawRecord(str(number), 1_790_000_000_000 + number, "dev-001", data)


def message(writer, event):
    return frame(writer.version_id, encode_datum(writer.schema, event))


def process(records, reader_version):
    resolver, _, _ = standard_resolver()
    return process_records(records, resolver, load_reader_schema(reader_version), reader_version, NOW)


def test_valid_v1_event_becomes_a_curated_row():
    _, v1, _ = standard_resolver()
    event = make_event(random.Random(1), EVENT_TIME)
    result = process([record(message(v1, event))], 1)
    assert not result.rejects
    (row,) = result.rows
    assert row["event_id"] == event["event_id"]
    assert row["writer_schema_version"] == 1
    assert row["reader_schema_version"] == 1
    assert row["battery_pct"] is None and row["firmware"] is None
    assert row["event_time"].endswith("Z") and row["event_time"].startswith("2026-09-")
    assert row["ingested_at"] == NOW


def test_old_consumer_still_reads_new_events_and_drops_the_new_fields():
    _, _, v2 = standard_resolver()
    event = make_event(random.Random(2), EVENT_TIME)
    (row,) = process([record(message(v2, event))], 1).rows
    assert row["writer_schema_version"] == 2
    assert row["reader_schema_version"] == 1
    assert row["battery_pct"] is None  # this consumer does not know the field yet


def test_upgraded_consumer_gets_defaults_for_old_events_and_values_for_new_ones():
    _, v1, v2 = standard_resolver()
    event = make_event(random.Random(3), EVENT_TIME)
    old_row, new_row = process([record(message(v1, event), 1), record(message(v2, event), 2)], 2).rows
    assert old_row["battery_pct"] is None and old_row["firmware"] == "unknown"
    assert new_row["battery_pct"] == event["battery_pct"] and new_row["firmware"] == event["firmware"]
    assert old_row["reader_schema_version"] == new_row["reader_schema_version"] == 2


def test_business_rule_violations_are_rejected_with_a_reason():
    _, v1, _ = standard_resolver()
    rng = random.Random(4)
    cases = {
        "humidity_high": "humidity_pct",
        "temperature_extreme": "temperature_c",
        "empty_device": "device_id",
    }
    for kind, field in cases.items():
        broken = make_invalid(make_event(rng, EVENT_TIME), kind)
        result = process([record(message(v1, broken))], 1)
        assert not result.rows
        (reject,) = result.rejects
        assert reject.stage == "validation" and field in reject.reason


def test_undecodable_messages_are_rejected_at_the_right_stage():
    _, v1, _ = standard_resolver()
    rng = random.Random(5)
    valid = message(v1, make_event(rng, EVENT_TIME))
    stages = {}
    for kind in ("garbage", "unknown_schema", "truncated"):
        result = process([record(make_poison(rng, kind, valid))], 1)
        assert not result.rows and len(result.rejects) == 1
        stages[kind] = result.rejects[0].stage
    assert stages == {"garbage": "wire", "unknown_schema": "schema", "truncated": "decode"}


def test_a_bad_record_never_blocks_the_good_ones_around_it():
    _, v1, _ = standard_resolver()
    rng = random.Random(6)
    good = [message(v1, make_event(rng, EVENT_TIME + i)) for i in range(3)]
    batch = [record(good[0], 1), record(b"\xff" * 30, 2), record(good[1], 3), record(good[2], 4)]
    result = process(batch, 1)
    assert len(result.rows) == 3 and len(result.rejects) == 1
    assert result.rejects[0].sequence_number == "2"


def test_validate_reading_boundaries():
    base = {"event_id": "e", "device_id": "d", "event_time": EVENT_TIME, "temperature_c": 20.0, "humidity_pct": 50.0}
    assert validate_reading(base) == []
    assert validate_reading({**base, "humidity_pct": 0.0}) == []
    assert validate_reading({**base, "humidity_pct": 100.0}) == []
    assert validate_reading({**base, "humidity_pct": 100.1})
    assert validate_reading({**base, "temperature_c": float("nan")})
    assert validate_reading({**base, "battery_pct": 120.0})
    assert validate_reading({**base, "event_time": 5})


def test_archive_roundtrip_keeps_bytes_exactly():
    records = [record(b"\x00\x01\xff binary", 1), record(b"second", 2)]
    assert parse_archive(archive_body(records)) == records


def test_keys_are_deterministic_and_safe():
    batch = batch_id("shardId-000000000000", "4959123")
    assert batch == "shardId-000000000000-4959123"
    assert raw_key("2026-10-08", batch) == "raw/dt=2026-10-08/shardId-000000000000-4959123.jsonl"
    assert curated_key("2026-10-07", batch) == "curated/dt=2026-10-07/shardId-000000000000-4959123.jsonl"
    assert batch_id("shard/../x", "1") == "shard_.._x-1"


def test_group_by_event_date_splits_a_batch_that_crosses_midnight():
    rows = [{"event_time": "2026-10-07T23:59:59.900Z"}, {"event_time": "2026-10-08T00:00:00.100Z"}]
    assert sorted(group_by_event_date(rows)) == ["2026-10-07", "2026-10-08"]
