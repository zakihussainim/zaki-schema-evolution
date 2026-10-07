"""The Lambda function that reads events from Kinesis.

For every batch it:
  1. archives the exact bytes received to S3 (raw/), so history can be replayed;
  2. decodes each message with the schema version named in its header and shapes it to this
     consumer's own reader schema;
  3. writes valid readings to S3 (curated/), partitioned by event date;
  4. sends everything it cannot use to the SQS dead-letter queue, with the reason.

All S3 keys are derived from the Kinesis shard and first sequence number of the batch, so if Lambda
retries a batch it overwrites the same files instead of duplicating data.
"""

import base64
import json
import os
import time
from datetime import datetime, timezone

from . import names
from .pipeline import (
    RawRecord,
    ResolvedSchema,
    SchemaNotFound,
    archive_body,
    arrival_date,
    batch_id,
    curated_key,
    group_by_event_date,
    load_reader_schema,
    process_records,
    raw_key,
    to_jsonl,
)
from .avro import parse_schema
from .registry import RegistryNotFound, SchemaRegistry

# Environment variables set by Terraform (tests/test_contracts.py compares both sides).
ENV_ENVIRONMENT = "ENVIRONMENT"
ENV_BUCKET = "EVENTS_BUCKET"
ENV_DLQ_URL = "DLQ_URL"
ENV_REGISTRY = "REGISTRY_NAME"
ENV_READER_VERSION = "READER_SCHEMA_VERSION"

_STATE = {}


class RegistryResolver:
    """Looks schema versions up in the registry once, then serves them from memory."""

    def __init__(self, registry):
        self._registry = registry
        self._found = {}
        self._missing = {}

    def resolve(self, version_id):
        if version_id in self._missing:
            raise SchemaNotFound(self._missing[version_id])
        if version_id not in self._found:
            try:
                version = self._registry.get_version(version_id)
            except RegistryNotFound as exc:
                self._missing[version_id] = str(exc)
                raise SchemaNotFound(str(exc)) from exc
            self._found[version_id] = ResolvedSchema(
                version_id, version.version_number, parse_schema(version.definition)
            )
        return self._found[version_id]


def from_kinesis_record(item):
    detail = item["kinesis"]
    return RawRecord(
        sequence_number=detail["sequenceNumber"],
        arrival_ms=int(float(detail["approximateArrivalTimestamp"]) * 1000),
        partition_key=detail["partitionKey"],
        data=base64.b64decode(detail["data"]),
    )


def shard_of(item):
    """eventID looks like 'shardId-000000000000:4959...'."""
    return item["eventID"].split(":")[0]


def send_rejects(sqs, queue_url, rejects, batch):
    for start in range(0, len(rejects), 10):
        entries = []
        for offset, reject in enumerate(rejects[start : start + 10]):
            body = {
                "batch_id": batch,
                "sequence_number": reject.sequence_number,
                "stage": reject.stage,
                "reason": reject.reason,
                "payload_b64": reject.payload_b64,
            }
            entries.append(
                {
                    "Id": str(offset),
                    "MessageBody": json.dumps(body),
                    "MessageAttributes": {"stage": {"DataType": "String", "StringValue": reject.stage}},
                }
            )
        response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
        if response.get("Failed"):
            raise RuntimeError(f"SQS rejected {len(response['Failed'])} dead-letter messages")


def emit_metrics(environment, curated, rejected):
    """CloudWatch Embedded Metric Format: a log line that CloudWatch turns into metrics for free."""
    print(
        json.dumps(
            {
                "_aws": {
                    "Timestamp": int(time.time() * 1000),
                    "CloudWatchMetrics": [
                        {
                            "Namespace": names.PROJECT,
                            "Dimensions": [["Environment"]],
                            "Metrics": [
                                {"Name": "RecordsCurated", "Unit": "Count"},
                                {"Name": "RecordsRejected", "Unit": "Count"},
                            ],
                        }
                    ],
                },
                "Environment": environment,
                "RecordsCurated": curated,
                "RecordsRejected": rejected,
            }
        )
    )


def handle_batch(
    kinesis_records,
    *,
    s3,
    sqs,
    resolver,
    bucket,
    dlq_url,
    reader_schema,
    reader_version,
    environment="dev",
    now=None,
):
    """Process one Kinesis batch. Every AWS client is passed in so tests can use fakes."""
    if not kinesis_records:
        return {"records": 0, "curated": 0, "rejected": 0}
    records = [from_kinesis_record(item) for item in kinesis_records]
    batch = batch_id(shard_of(kinesis_records[0]), records[0].sequence_number)
    ingested_at = (now or datetime.now(timezone.utc)).isoformat(timespec="milliseconds").replace("+00:00", "Z")

    s3.put_object(
        Bucket=bucket,
        Key=raw_key(arrival_date(records[0]), batch),
        Body=archive_body(records),
        ContentType="application/x-ndjson",
    )

    result = process_records(records, resolver, reader_schema, reader_version, ingested_at)

    for event_date, rows in group_by_event_date(result.rows).items():
        s3.put_object(
            Bucket=bucket,
            Key=curated_key(event_date, batch),
            Body=to_jsonl(rows),
            ContentType="application/x-ndjson",
        )

    if result.rejects:
        send_rejects(sqs, dlq_url, result.rejects, batch)

    emit_metrics(environment, len(result.rows), len(result.rejects))
    return {"records": len(records), "curated": len(result.rows), "rejected": len(result.rejects)}


def _dependencies():
    if "deps" not in _STATE:
        import boto3

        region = os.environ.get("AWS_REGION", names.REGION)
        registry = SchemaRegistry(boto3.client("glue", region_name=region), os.environ[ENV_REGISTRY])
        reader_version = int(os.environ.get(ENV_READER_VERSION, "1"))
        _STATE["deps"] = {
            "s3": boto3.client("s3", region_name=region),
            "sqs": boto3.client("sqs", region_name=region),
            "resolver": RegistryResolver(registry),
            "reader_schema": load_reader_schema(reader_version),
            "reader_version": reader_version,
        }
    return _STATE["deps"]


def handler(event, context):
    deps = _dependencies()
    return handle_batch(
        event.get("Records", []),
        s3=deps["s3"],
        sqs=deps["sqs"],
        resolver=deps["resolver"],
        bucket=os.environ[ENV_BUCKET],
        dlq_url=os.environ[ENV_DLQ_URL],
        reader_schema=deps["reader_schema"],
        reader_version=deps["reader_version"],
        environment=os.environ.get(ENV_ENVIRONMENT, "dev"),
    )
