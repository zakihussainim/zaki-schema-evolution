"""The message framing used on the stream (the AWS Glue Schema Registry wire format).

    byte 0      header version, always 3
    byte 1      compression: 0 = none, 5 = zlib
    bytes 2-17  the schema version ID (a UUID) the payload was written with
    bytes 18-   the Avro-encoded payload (compressed when byte 1 says so)

Putting the schema version ID in every message is what lets a consumer decode data written with
any past version of the schema.
"""

import uuid
import zlib

HEADER_VERSION = 3
COMPRESSION_NONE = 0
COMPRESSION_ZLIB = 5
HEADER_LENGTH = 18


class WireFormatError(Exception):
    """The message does not follow the framing."""


def frame(schema_version_id, payload, compress=False):
    """Prefix an Avro payload with the registry header."""
    try:
        version_bytes = uuid.UUID(schema_version_id).bytes
    except (ValueError, AttributeError, TypeError) as exc:
        raise WireFormatError(f"invalid schema version ID: {schema_version_id!r}") from exc
    flag = COMPRESSION_NONE
    if compress:
        payload = zlib.compress(payload)
        flag = COMPRESSION_ZLIB
    return bytes([HEADER_VERSION, flag]) + version_bytes + bytes(payload)


def unframe(data):
    """Split a message into (schema version ID, Avro payload)."""
    data = bytes(data)
    if len(data) < HEADER_LENGTH:
        raise WireFormatError(f"message is only {len(data)} bytes, shorter than the {HEADER_LENGTH}-byte header")
    if data[0] != HEADER_VERSION:
        raise WireFormatError(f"unexpected header version {data[0]} (expected {HEADER_VERSION})")
    flag = data[1]
    version_id = str(uuid.UUID(bytes=data[2:HEADER_LENGTH]))
    payload = data[HEADER_LENGTH:]
    if flag == COMPRESSION_NONE:
        return version_id, payload
    if flag == COMPRESSION_ZLIB:
        try:
            return version_id, zlib.decompress(payload)
        except zlib.error as exc:
            raise WireFormatError(f"payload is not valid zlib data: {exc}") from exc
    raise WireFormatError(f"unknown compression byte {flag}")
