"""The producer's message building, without AWS."""

from collections import Counter

from helpers import standard_resolver

from schema_evolution.pipeline import RawRecord, load_reader_schema, process_records
from schema_evolution.producer import Payload, build_payloads, put_all

START_MS = 1_790_000_000_000


def test_payloads_are_deterministic_for_a_seed():
    _, v1, _ = standard_resolver()
    first = build_payloads(v1, 200, seed=11, start_ms=START_MS)
    second = build_payloads(v1, 200, seed=11, start_ms=START_MS)
    assert first == second
    assert first != build_payloads(v1, 200, seed=12, start_ms=START_MS)


def test_mix_of_good_and_bad_messages_matches_the_rates():
    _, v1, _ = standard_resolver()
    payloads = build_payloads(v1, 2000, seed=3, bad_rate=0.05, poison_rate=0.02, start_ms=START_MS)
    kinds = Counter(p.kind.split(":")[0] for p in payloads)
    assert 60 <= kinds["invalid"] <= 140
    assert 15 <= kinds["poison"] <= 65
    assert kinds["valid"] > 1700


def test_consumer_outcome_matches_what_the_producer_intended():
    resolver, v1, v2 = standard_resolver()
    for writer in (v1, v2):
        payloads = build_payloads(writer, 300, seed=21, start_ms=START_MS)
        records = [RawRecord(str(i), START_MS, p.partition_key, p.data) for i, p in enumerate(payloads)]
        result = process_records(records, resolver, load_reader_schema(2), 2, "2026-10-08T00:00:00.000Z")
        expected_good = sum(1 for p in payloads if p.kind == "valid")
        assert len(result.rows) == expected_good
        assert len(result.rejects) == len(payloads) - expected_good
        stages = Counter(r.stage for r in result.rejects)
        assert stages["validation"] == sum(1 for p in payloads if p.kind.startswith("invalid"))
        assert stages["wire"] + stages["schema"] + stages["decode"] == sum(1 for p in payloads if p.kind.startswith("poison"))


def test_event_ids_are_unique_within_a_run():
    _, v1, _ = standard_resolver()
    payloads = build_payloads(v1, 500, seed=8, bad_rate=0, poison_rate=0, start_ms=START_MS)
    resolver, _, _ = standard_resolver()
    records = [RawRecord(str(i), START_MS, p.partition_key, p.data) for i, p in enumerate(payloads)]
    rows = process_records(records, resolver, load_reader_schema(1), 1, "x").rows
    assert len({r["event_id"] for r in rows}) == 500


class FakeKinesis:
    """Fails the first attempt for every other record, then succeeds."""

    def __init__(self):
        self.calls = []

    def put_records(self, StreamName, Records):
        self.calls.append([r["Data"] for r in Records])
        if len(self.calls) == 1:
            statuses = [{"ErrorCode": "ProvisionedThroughputExceededException"} if i % 2 else {"SequenceNumber": "1"} for i in range(len(Records))]
            return {"FailedRecordCount": sum("ErrorCode" in s for s in statuses), "Records": statuses}
        return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "1"} for _ in Records]}


def test_put_all_retries_only_the_failed_records():
    payloads = [Payload("k", bytes([i]), "valid") for i in range(6)]
    kinesis = FakeKinesis()
    put_all(kinesis, "stream", payloads)
    assert len(kinesis.calls) == 2
    assert kinesis.calls[1] == [bytes([1]), bytes([3]), bytes([5])]


def test_put_all_splits_into_chunks_of_500():
    class Always:
        def __init__(self):
            self.sizes = []

        def put_records(self, StreamName, Records):
            self.sizes.append(len(Records))
            return {"FailedRecordCount": 0, "Records": []}

    sender = Always()
    put_all(sender, "stream", [Payload("k", b"x", "valid")] * 1100)
    assert sender.sizes == [500, 500, 100]
