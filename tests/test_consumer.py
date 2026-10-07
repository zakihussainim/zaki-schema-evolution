"""The Lambda handler and the replay tool, end to end with in-memory S3 and SQS."""

import json
from datetime import datetime, timezone

from helpers import FakeS3, FakeSQS, kinesis_event, standard_resolver

from schema_evolution.consumer import handle_batch
from schema_evolution.pipeline import load_reader_schema
from schema_evolution.producer import build_payloads
from schema_evolution.replay import dates_between, replay

BUCKET = "test-bucket"
QUEUE = "https://sqs.example/queue"
START_MS = 1_790_000_000_000  # 2026-09-21T12:13:20Z
ARRIVAL = 1_790_000_000.0
NOW = datetime(2026, 9, 21, 12, 30, tzinfo=timezone.utc)


def run_batch(s3, sqs, resolver, payloads, reader_version, first_sequence=1000, shard="shardId-000000000000"):
    event = kinesis_event(payloads, shard=shard, first_sequence=first_sequence, arrival_seconds=ARRIVAL)
    return handle_batch(
        event["Records"],
        s3=s3,
        sqs=sqs,
        resolver=resolver,
        bucket=BUCKET,
        dlq_url=QUEUE,
        reader_schema=load_reader_schema(reader_version),
        reader_version=reader_version,
        now=NOW,
    )


def curated_rows(s3):
    rows = []
    for key in s3.keys("curated/"):
        for line in s3.objects[(BUCKET, key)].decode().splitlines():
            rows.append(json.loads(line))
    return rows


def test_batch_archives_curates_and_dead_letters():
    resolver, v1, _ = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()
    payloads = build_payloads(v1, 120, seed=5, bad_rate=0.05, poison_rate=0.03, start_ms=START_MS)
    outcome = run_batch(s3, sqs, resolver, payloads, 1)

    good = sum(1 for p in payloads if p.kind == "valid")
    assert outcome == {"records": 120, "curated": good, "rejected": 120 - good}
    assert s3.keys("raw/") == ["raw/dt=2026-09-21/shardId-000000000000-1000.jsonl"]
    assert len(curated_rows(s3)) == good
    assert all(k.startswith("curated/dt=2026-09-21/") for k in s3.keys("curated/"))
    assert len(sqs.messages) == 120 - good
    body = json.loads(sqs.messages[0][1]["MessageBody"])
    assert {"batch_id", "sequence_number", "stage", "reason", "payload_b64"} <= set(body)
    assert sqs.messages[0][1]["MessageAttributes"]["stage"]["StringValue"] == body["stage"]


def test_dead_letter_messages_are_sent_in_groups_of_ten():
    resolver, v1, _ = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()
    payloads = build_payloads(v1, 60, seed=1, bad_rate=0.0, poison_rate=1.0, start_ms=START_MS)
    run_batch(s3, sqs, resolver, payloads, 1)
    assert len(sqs.messages) == 60
    ids = [entry["Id"] for _, entry in sqs.messages]
    assert max(int(i) for i in ids) <= 9


def test_retrying_a_batch_overwrites_instead_of_duplicating():
    resolver, v1, _ = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()
    payloads = build_payloads(v1, 50, seed=2, bad_rate=0, poison_rate=0, start_ms=START_MS)
    run_batch(s3, sqs, resolver, payloads, 1)
    first = dict(s3.objects)
    run_batch(s3, sqs, resolver, payloads, 1)
    assert s3.objects == first
    assert len(curated_rows(s3)) == 50


def test_empty_batch_does_nothing():
    resolver, _, _ = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()
    outcome = handle_batch(
        [], s3=s3, sqs=sqs, resolver=resolver, bucket=BUCKET, dlq_url=QUEUE,
        reader_schema=load_reader_schema(1), reader_version=1,
    )
    assert outcome["records"] == 0 and not s3.objects


def test_replay_rebuilds_curated_with_the_upgraded_reader_schema():
    resolver, v1, v2 = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()

    # Live: the consumer is still on reader schema 1 while producers move from v1 to v2.
    run_batch(s3, sqs, resolver, build_payloads(v1, 40, seed=10, bad_rate=0, poison_rate=0, start_ms=START_MS), 1, 1000)
    run_batch(s3, sqs, resolver, build_payloads(v2, 40, seed=11, bad_rate=0, poison_rate=0, start_ms=START_MS), 1, 2000)
    live = curated_rows(s3)
    assert len(live) == 80
    assert {r["writer_schema_version"] for r in live} == {1, 2}
    assert all(r["reader_schema_version"] == 1 and r["battery_pct"] is None for r in live)

    # The consumer is upgraded to reader schema 2. Replay the archive.
    summary = replay(s3, BUCKET, ["2026-09-21"], resolver, load_reader_schema(2), 2, now=NOW)
    assert summary.batches == 2 and summary.records == 80 and summary.curated == 80 and summary.rejected == 0
    assert dict(summary.by_writer_version) == {1: 40, 2: 40}

    replayed = curated_rows(s3)
    assert len(replayed) == 80  # same keys, overwritten: no duplicates
    assert all(r["reader_schema_version"] == 2 for r in replayed)
    from_v1 = [r for r in replayed if r["writer_schema_version"] == 1]
    from_v2 = [r for r in replayed if r["writer_schema_version"] == 2]
    assert all(r["battery_pct"] is None and r["firmware"] == "unknown" for r in from_v1)
    assert all(r["battery_pct"] is not None and r["firmware"] != "unknown" for r in from_v2)


def test_replay_dry_run_writes_nothing_and_reports_rejects():
    resolver, v1, _ = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()
    run_batch(s3, sqs, resolver, build_payloads(v1, 100, seed=4, bad_rate=0.1, poison_rate=0.05, start_ms=START_MS), 1)
    before = dict(s3.objects)
    summary = replay(s3, BUCKET, ["2026-09-21"], resolver, load_reader_schema(1), 1, dry_run=True, now=NOW)
    assert s3.objects == before
    assert summary.rejected > 0 and sum(summary.by_reject_stage.values()) == summary.rejected


def test_replay_writes_rejects_to_their_own_prefix():
    resolver, v1, _ = standard_resolver()
    s3, sqs = FakeS3(), FakeSQS()
    run_batch(s3, sqs, resolver, build_payloads(v1, 100, seed=4, bad_rate=0.1, poison_rate=0.05, start_ms=START_MS), 1)
    replay(s3, BUCKET, ["2026-09-21"], resolver, load_reader_schema(1), 1, now=NOW)
    reject_keys = s3.keys("replay-rejects/")
    assert reject_keys == ["replay-rejects/dt=2026-09-21/shardId-000000000000-1000.jsonl"]


def test_replay_ignores_days_with_no_archive():
    resolver, _, _ = standard_resolver()
    summary = replay(FakeS3(), BUCKET, ["2026-01-01"], resolver, load_reader_schema(1), 1, now=NOW)
    assert summary.batches == 0


def test_dates_between_is_inclusive():
    from datetime import date

    assert list(dates_between(date(2026, 10, 7), date(2026, 10, 9))) == ["2026-10-07", "2026-10-08", "2026-10-09"]
