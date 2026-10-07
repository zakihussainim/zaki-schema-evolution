"""Send simulated sensor events to the Kinesis stream.

Usage (from the src folder):
    python -m schema_evolution.producer --env dev --writer-version 1 --count 300
    python -m schema_evolution.producer --env dev --writer-version 2 --count 300

--writer-version chooses which registered schema version the events are written with, which is how the
project simulates an upstream team changing its schema. About 2% of events break a business rule and
about 1% are not decodable at all, so the dead-letter queue has something to catch.
"""

import argparse
import random
import sys
import time
from collections import Counter
from dataclasses import dataclass

from . import names
from .avro import encode_datum, parse_schema
from .events import INVALID_KINDS, POISON_KINDS, make_event, make_invalid, make_poison
from .pipeline import ResolvedSchema
from .registry import SchemaRegistry
from .wire import frame


@dataclass(frozen=True)
class Payload:
    partition_key: str
    data: bytes
    kind: str


def build_payloads(writer, count, *, seed, bad_rate=0.02, poison_rate=0.01, start_ms, step_ms=100):
    """The messages a run would send. Deterministic for a given seed."""
    rng = random.Random(seed)
    payloads = []
    for index in range(count):
        event = make_event(rng, start_ms + index * step_ms)
        roll = rng.random()
        if roll < poison_rate:
            kind = rng.choice(POISON_KINDS)
            valid = frame(writer.version_id, encode_datum(writer.schema, event))
            data = make_poison(rng, kind, valid)
            payloads.append(Payload(event["device_id"], data, f"poison:{kind}"))
        elif roll < poison_rate + bad_rate:
            kind = rng.choice(INVALID_KINDS)
            broken = make_invalid(event, kind)
            data = frame(writer.version_id, encode_datum(writer.schema, broken))
            payloads.append(Payload(event["device_id"] or "unknown", data, f"invalid:{kind}"))
        else:
            data = frame(writer.version_id, encode_datum(writer.schema, event))
            payloads.append(Payload(event["device_id"], data, "valid"))
    return payloads


def put_all(kinesis, stream, payloads, max_attempts=5):
    """Send in chunks of 500 (the API limit), retrying only the records Kinesis reports as failed."""
    for start in range(0, len(payloads), 500):
        pending = payloads[start : start + 500]
        for attempt in range(max_attempts):
            response = kinesis.put_records(
                StreamName=stream,
                Records=[{"Data": p.data, "PartitionKey": p.partition_key} for p in pending],
            )
            if response["FailedRecordCount"] == 0:
                pending = []
                break
            pending = [p for p, r in zip(pending, response["Records"]) if "ErrorCode" in r]
            time.sleep(min(0.2 * 2**attempt, 3))
        if pending:
            raise RuntimeError(f"{len(pending)} records still failing after {max_attempts} attempts")


def main(argv=None):
    parser = argparse.ArgumentParser(description="Send simulated sensor events to Kinesis.")
    parser.add_argument("--env", required=True, choices=names.ENVIRONMENTS)
    parser.add_argument("--writer-version", type=int, default=1, help="schema version to write with (default 1)")
    parser.add_argument("--count", type=int, default=200)
    parser.add_argument("--bad-rate", type=float, default=0.02, help="share of events that break a business rule")
    parser.add_argument("--poison-rate", type=float, default=0.01, help="share of messages that cannot be decoded")
    parser.add_argument("--seed", type=int, default=None, help="random seed (default: current time)")
    args = parser.parse_args(argv)

    import boto3

    glue = boto3.client("glue", region_name=names.REGION)
    kinesis = boto3.client("kinesis", region_name=names.REGION)
    registry = SchemaRegistry(glue, names.registry_name(args.env))

    try:
        version = registry.get_version_by_number(args.writer_version)
    except Exception as exc:
        print(f"Schema version {args.writer_version} is not available: {exc}")
        print("Register it first with: python -m schema_evolution.registry register <file> --env " + args.env)
        return 1
    writer = ResolvedSchema(version.version_id, version.version_number, parse_schema(version.definition))

    seed = args.seed if args.seed is not None else int(time.time())
    start_ms = int(time.time() * 1000)
    payloads = build_payloads(
        writer,
        args.count,
        seed=seed,
        bad_rate=args.bad_rate,
        poison_rate=args.poison_rate,
        start_ms=start_ms,
    )
    put_all(kinesis, names.stream_name(args.env), payloads)

    kinds = Counter(p.kind for p in payloads)
    print(f"Sent {len(payloads)} messages to {names.stream_name(args.env)} written with schema version {writer.version_number}.")
    print(f"Seed {seed}. Breakdown:")
    for kind, number in sorted(kinds.items()):
        print(f"  {kind}: {number}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
