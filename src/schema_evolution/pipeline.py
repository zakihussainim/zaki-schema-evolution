"""The processing logic shared by the Lambda consumer and the replay tool.

Both call `process_records`, so replaying history runs exactly the code that handled it live.
Nothing in this module talks to AWS: the callers pass in the schema lookup and do the reading and writing.
"""

import base64
import json
import re
import struct
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from .avro import AvroError, decode_datum, parse_schema
from .wire import WireFormatError, unframe

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"

# The curated table. terraform/modules/catalog must define exactly these columns (tests/test_contracts.py).
CURATED_COLUMNS = (
    ("event_id", "string"),
    ("device_id", "string"),
    ("event_time", "string"),
    ("temperature_c", "double"),
    ("humidity_pct", "double"),
    ("battery_pct", "double"),
    ("firmware", "string"),
    ("writer_schema_version", "int"),
    ("reader_schema_version", "int"),
    ("sequence_number", "string"),
    ("ingested_at", "string"),
)

MIN_EVENT_MS = int(datetime(2020, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
MAX_EVENT_MS = int(datetime(2100, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
MAX_PAYLOAD_CHARS = 200_000  # keeps a reject message under the SQS size limit


class SchemaNotFound(Exception):
    """The schema version a message was written with is not in the registry."""


@dataclass(frozen=True)
class RawRecord:
    sequence_number: str
    arrival_ms: int
    partition_key: str
    data: bytes


@dataclass(frozen=True)
class ResolvedSchema:
    version_id: str
    version_number: int
    schema: dict


@dataclass(frozen=True)
class Reject:
    sequence_number: str
    stage: str
    reason: str
    payload_b64: str


@dataclass
class BatchResult:
    rows: list = field(default_factory=list)
    rejects: list = field(default_factory=list)


# ---------------------------------------------------------------------------
# Reader schema
# ---------------------------------------------------------------------------


def schema_file(version):
    return SCHEMA_DIR / f"sensor_reading_v{int(version)}.avsc"


def load_reader_schema(version):
    """The schema this consumer reads with. Newer writers are narrowed to it; older ones get its defaults."""
    path = schema_file(version)
    if not path.exists():
        raise ValueError(f"no reader schema for version {version} ({path.name} does not exist)")
    return parse_schema(path.read_text(encoding="utf-8"))


# ---------------------------------------------------------------------------
# Validation and row building
# ---------------------------------------------------------------------------


def validate_reading(value):
    """Business rules. Returns a list of problems; empty means the reading is acceptable."""
    problems = []
    if not value.get("event_id"):
        problems.append("event_id is empty")
    if not str(value.get("device_id") or "").strip():
        problems.append("device_id is empty")
    temperature = value.get("temperature_c")
    if not (-50.0 <= temperature <= 100.0):
        problems.append(f"temperature_c {temperature} is outside -50..100")
    humidity = value.get("humidity_pct")
    if not (0.0 <= humidity <= 100.0):
        problems.append(f"humidity_pct {humidity} is outside 0..100")
    battery = value.get("battery_pct")
    if battery is not None and not (0.0 <= battery <= 100.0):
        problems.append(f"battery_pct {battery} is outside 0..100")
    event_time = value.get("event_time")
    if not (MIN_EVENT_MS <= event_time <= MAX_EVENT_MS):
        problems.append(f"event_time {event_time} is not a plausible timestamp")
    return problems


def iso_from_ms(milliseconds):
    moment = datetime.fromtimestamp(milliseconds / 1000, tz=timezone.utc)
    return moment.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def build_row(value, writer_version, reader_version, record, ingested_at):
    return {
        "event_id": value["event_id"],
        "device_id": value["device_id"],
        "event_time": iso_from_ms(value["event_time"]),
        "temperature_c": value["temperature_c"],
        "humidity_pct": value["humidity_pct"],
        "battery_pct": value.get("battery_pct"),
        "firmware": value.get("firmware"),
        "writer_schema_version": writer_version,
        "reader_schema_version": reader_version,
        "sequence_number": record.sequence_number,
        "ingested_at": ingested_at,
    }


def _reject(record, stage, reason):
    encoded = base64.b64encode(record.data).decode("ascii")
    return Reject(record.sequence_number, stage, reason, encoded[:MAX_PAYLOAD_CHARS])


def process_records(records, resolver, reader_schema, reader_version, ingested_at):
    """Decode, resolve to the reader schema, validate. Bad records are returned as rejects, never dropped.

    `resolver.resolve(schema_version_id)` returns a ResolvedSchema or raises SchemaNotFound.
    """
    result = BatchResult()
    for record in records:
        try:
            version_id, payload = unframe(record.data)
        except WireFormatError as exc:
            result.rejects.append(_reject(record, "wire", str(exc)))
            continue
        try:
            writer = resolver.resolve(version_id)
        except SchemaNotFound as exc:
            result.rejects.append(_reject(record, "schema", str(exc)))
            continue
        try:
            value = decode_datum(writer.schema, payload, reader_schema)
        except (AvroError, ValueError, struct.error) as exc:
            result.rejects.append(_reject(record, "decode", str(exc)))
            continue
        problems = validate_reading(value)
        if problems:
            result.rejects.append(_reject(record, "validation", "; ".join(problems)))
            continue
        result.rows.append(build_row(value, writer.version_number, reader_version, record, ingested_at))
    return result


# ---------------------------------------------------------------------------
# S3 layout and file formats (idempotent: the same batch always maps to the same keys)
# ---------------------------------------------------------------------------

_SAFE = re.compile(r"[^A-Za-z0-9_.=-]")


def batch_id(shard_id, first_sequence_number):
    return _SAFE.sub("_", f"{shard_id}-{first_sequence_number}")


def raw_key(arrival_date, batch):
    return f"raw/dt={arrival_date}/{batch}.jsonl"


def curated_key(event_date, batch):
    return f"curated/dt={event_date}/{batch}.jsonl"


def reject_key(arrival_date, batch):
    return f"replay-rejects/dt={arrival_date}/{batch}.jsonl"


def group_by_event_date(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[row["event_time"][:10]].append(row)
    return dict(groups)


def to_jsonl(items):
    return "".join(json.dumps(item, sort_keys=True) + "\n" for item in items).encode("utf-8")


def archive_body(records):
    """The exact bytes received, so any batch can be reprocessed later."""
    return to_jsonl(
        {
            "sequence_number": r.sequence_number,
            "arrival_ms": r.arrival_ms,
            "partition_key": r.partition_key,
            "data_b64": base64.b64encode(r.data).decode("ascii"),
        }
        for r in records
    )


def parse_archive(body):
    records = []
    for line in bytes(body).decode("utf-8").splitlines():
        if not line.strip():
            continue
        item = json.loads(line)
        records.append(
            RawRecord(
                sequence_number=item["sequence_number"],
                arrival_ms=int(item["arrival_ms"]),
                partition_key=item["partition_key"],
                data=base64.b64decode(item["data_b64"]),
            )
        )
    return records


def reject_body(rejects, batch):
    return to_jsonl(
        {
            "batch_id": batch,
            "sequence_number": r.sequence_number,
            "stage": r.stage,
            "reason": r.reason,
            "payload_b64": r.payload_b64,
        }
        for r in rejects
    )


def arrival_date(record):
    return datetime.fromtimestamp(record.arrival_ms / 1000, tz=timezone.utc).date().isoformat()
