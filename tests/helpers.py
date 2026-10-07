"""Small fakes and helpers shared by the tests (no AWS, no third-party packages)."""

import base64
import io
from contextlib import contextmanager
from pathlib import Path

from schema_evolution.avro import parse_schema
from schema_evolution.pipeline import ResolvedSchema, SchemaNotFound

SCHEMA_DIR = Path(__file__).resolve().parents[1] / "src" / "schema_evolution" / "schemas"


@contextmanager
def expect_error(error_type, contains=None):
    """pytest.raises without needing pytest, so the same tests run in any environment."""
    try:
        yield
    except error_type as exc:
        if contains is not None:
            assert contains in str(exc), f"{contains!r} not found in {str(exc)!r}"
    else:
        raise AssertionError(f"{error_type.__name__} was not raised")


def schema_text(name):
    return (SCHEMA_DIR / name).read_text(encoding="utf-8")


def parsed(name):
    return parse_schema(schema_text(name))


def fixed_uuid(number):
    return f"00000000-0000-4000-8000-{number:012d}"


class FakeResolver:
    """Maps schema version IDs to parsed schemas, like the registry would."""

    def __init__(self):
        self._versions = {}

    def add(self, version_number, schema_file):
        version_id = fixed_uuid(version_number)
        self._versions[version_id] = ResolvedSchema(version_id, version_number, parsed(schema_file))
        return self._versions[version_id]

    def resolve(self, version_id):
        if version_id not in self._versions:
            raise SchemaNotFound(f"schema version {version_id} is not in the registry")
        return self._versions[version_id]


def standard_resolver():
    resolver = FakeResolver()
    v1 = resolver.add(1, "sensor_reading_v1.avsc")
    v2 = resolver.add(2, "sensor_reading_v2.avsc")
    return resolver, v1, v2


class FakeS3:
    def __init__(self):
        self.objects = {}

    def put_object(self, Bucket, Key, Body, ContentType=None):
        self.objects[(Bucket, Key)] = bytes(Body)

    def get_object(self, Bucket, Key):
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def list_objects_v2(self, Bucket, Prefix="", ContinuationToken=None):
        keys = sorted(k for (b, k) in self.objects if b == Bucket and k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in keys], "IsTruncated": False}

    def keys(self, prefix=""):
        return sorted(k for (_, k) in self.objects if k.startswith(prefix))


class FakeSQS:
    def __init__(self):
        self.messages = []

    def send_message_batch(self, QueueUrl, Entries):
        for entry in Entries:
            self.messages.append((QueueUrl, entry))
        return {"Successful": [{"Id": e["Id"]} for e in Entries], "Failed": []}


def kinesis_event(payloads, shard="shardId-000000000000", first_sequence=1000, arrival_seconds=1_790_000_000.0):
    """Build the dictionary AWS Lambda passes to a Kinesis consumer."""
    records = []
    for offset, payload in enumerate(payloads):
        sequence = str(first_sequence + offset)
        records.append(
            {
                "eventID": f"{shard}:{sequence}",
                "eventSource": "aws:kinesis",
                "kinesis": {
                    "sequenceNumber": sequence,
                    "partitionKey": payload.partition_key,
                    "approximateArrivalTimestamp": arrival_seconds + offset,
                    "data": base64.b64encode(payload.data).decode("ascii"),
                },
            }
        )
    return {"Records": records}
