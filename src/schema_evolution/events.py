"""Deterministic sensor events, and the ways they can go wrong on purpose.

Everything here is pure (no AWS), so the tests can check exactly what the producer would send.
"""

import random
import uuid

from .wire import HEADER_LENGTH, frame

DEVICE_COUNT = 20
FIRMWARES = ["1.0.3", "1.1.0", "1.2.4"]

# Events that are valid Avro but break a business rule (the consumer rejects them at validation).
INVALID_KINDS = ("humidity_high", "temperature_extreme", "empty_device")
# Messages that are not valid events at all.
POISON_KINDS = ("garbage", "unknown_schema", "truncated")


def device_id(number):
    return f"dev-{number:03d}"


def make_event(rng, event_time_ms):
    """One valid reading carrying every field of the newest schema. Older writers simply ignore the extras."""
    return {
        "event_id": str(uuid.UUID(int=rng.getrandbits(128), version=4)),
        "device_id": device_id(rng.randint(1, DEVICE_COUNT)),
        "event_time": event_time_ms,
        "temperature_c": round(rng.uniform(15.0, 35.0), 2),
        "humidity_pct": round(rng.uniform(25.0, 80.0), 1),
        "battery_pct": round(rng.uniform(20.0, 100.0), 1),
        "firmware": rng.choice(FIRMWARES),
    }


def make_invalid(event, kind):
    broken = dict(event)
    if kind == "humidity_high":
        broken["humidity_pct"] = 140.0
    elif kind == "temperature_extreme":
        broken["temperature_c"] = 999.0
    elif kind == "empty_device":
        broken["device_id"] = ""
    else:
        raise ValueError(f"unknown invalid kind {kind}")
    return broken


def make_poison(rng, kind, valid_message):
    """A message the consumer cannot decode. `valid_message` is a correctly framed event."""
    if kind == "garbage":
        data = bytearray(rng.getrandbits(8) for _ in range(40))
        data[0] = 0xFF  # never a valid header version
        return bytes(data)
    if kind == "unknown_schema":
        unknown_id = str(uuid.UUID(int=rng.getrandbits(128), version=4))
        return frame(unknown_id, valid_message[HEADER_LENGTH:])
    if kind == "truncated":
        return valid_message[: HEADER_LENGTH + 2]
    raise ValueError(f"unknown poison kind {kind}")
