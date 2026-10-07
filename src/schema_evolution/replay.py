"""Reprocess archived events through the same logic the consumer uses.

Why: the raw archive keeps every message exactly as it arrived. If the consumer's reader schema changes, a
validation rule is fixed, or a bug is found, replay rebuilds the curated files from that archive.

Usage (from the src folder):
    python -m schema_evolution.replay --env dev --from 2026-10-08 --to 2026-10-08 --reader-version 2
    python -m schema_evolution.replay --env dev --dry-run          (today only, writes nothing)

Safe to run repeatedly: curated files are written to the same keys the live consumer uses, so a replay
overwrites them. Records that are rejected are written to replay-rejects/ instead of the queue.
"""

import argparse
import sys
from collections import Counter
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from . import names
from .consumer import RegistryResolver
from .pipeline import (
    group_by_event_date,
    curated_key,
    load_reader_schema,
    parse_archive,
    process_records,
    reject_body,
    reject_key,
    to_jsonl,
)
from .registry import SchemaRegistry


@dataclass
class ReplaySummary:
    batches: int = 0
    records: int = 0
    curated: int = 0
    rejected: int = 0
    by_writer_version: Counter = field(default_factory=Counter)
    by_reject_stage: Counter = field(default_factory=Counter)


def list_keys(s3, bucket, prefix):
    keys = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        response = s3.list_objects_v2(**kwargs)
        keys.extend(item["Key"] for item in response.get("Contents", []))
        if not response.get("IsTruncated"):
            return keys
        token = response["NextContinuationToken"]


def dates_between(start, end):
    current = start
    while current <= end:
        yield current.isoformat()
        current += timedelta(days=1)


def replay(s3, bucket, dates, resolver, reader_schema, reader_version, *, dry_run=False, now=None):
    summary = ReplaySummary()
    ingested_at = (now or datetime.now(timezone.utc)).isoformat(timespec="milliseconds").replace("+00:00", "Z")
    for day in dates:
        for key in list_keys(s3, bucket, f"raw/dt={day}/"):
            batch = key.rsplit("/", 1)[-1].removesuffix(".jsonl")
            records = parse_archive(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
            result = process_records(records, resolver, reader_schema, reader_version, ingested_at)

            summary.batches += 1
            summary.records += len(records)
            summary.curated += len(result.rows)
            summary.rejected += len(result.rejects)
            for row in result.rows:
                summary.by_writer_version[row["writer_schema_version"]] += 1
            for reject in result.rejects:
                summary.by_reject_stage[reject.stage] += 1

            if dry_run:
                continue
            for event_date, rows in group_by_event_date(result.rows).items():
                s3.put_object(
                    Bucket=bucket,
                    Key=curated_key(event_date, batch),
                    Body=to_jsonl(rows),
                    ContentType="application/x-ndjson",
                )
            if result.rejects:
                s3.put_object(
                    Bucket=bucket,
                    Key=reject_key(day, batch),
                    Body=reject_body(result.rejects, batch),
                    ContentType="application/x-ndjson",
                )
    return summary


def main(argv=None):
    today = datetime.now(timezone.utc).date()
    parser = argparse.ArgumentParser(description="Replay archived events through the consumer logic.")
    parser.add_argument("--env", required=True, choices=names.ENVIRONMENTS)
    parser.add_argument("--from", dest="start", type=date.fromisoformat, default=today, help="first arrival date, YYYY-MM-DD")
    parser.add_argument("--to", dest="end", type=date.fromisoformat, default=today, help="last arrival date, YYYY-MM-DD")
    parser.add_argument("--reader-version", type=int, default=1, help="reader schema version to shape records to")
    parser.add_argument("--dry-run", action="store_true", help="count what would happen, write nothing")
    args = parser.parse_args(argv)
    if args.end < args.start:
        parser.error("--to must not be before --from")

    import boto3

    s3 = boto3.client("s3", region_name=names.REGION)
    registry = SchemaRegistry(boto3.client("glue", region_name=names.REGION), names.registry_name(args.env))
    bucket = names.bucket_name(args.env, names.account_id())

    summary = replay(
        s3,
        bucket,
        dates_between(args.start, args.end),
        RegistryResolver(registry),
        load_reader_schema(args.reader_version),
        args.reader_version,
        dry_run=args.dry_run,
    )
    mode = "DRY RUN, nothing written" if args.dry_run else "curated files rewritten"
    print(f"Replay of {args.start} to {args.end} with reader schema version {args.reader_version} ({mode})")
    print(f"  archive batches read: {summary.batches}")
    print(f"  records read:         {summary.records}")
    print(f"  curated:              {summary.curated}")
    print(f"  rejected:             {summary.rejected}")
    for version, number in sorted(summary.by_writer_version.items()):
        print(f"  written with schema v{version}: {number}")
    for stage, number in sorted(summary.by_reject_stage.items()):
        print(f"  rejected at {stage}: {number}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
