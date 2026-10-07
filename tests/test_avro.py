"""The Avro codec, checked against the encodings written in the Avro specification."""

import struct

from helpers import expect_error, parsed, schema_text

from schema_evolution.avro import (
    DecodeError,
    EncodeError,
    ResolutionError,
    SchemaError,
    check_compatibility,
    decode_datum,
    encode_datum,
    parse_schema,
    resolution_problems,
)


def roundtrip(schema_json, value):
    schema = parse_schema(schema_json)
    return decode_datum(schema, encode_datum(schema, value))


def test_long_uses_zigzag_varint_from_the_spec():
    long_schema = parse_schema('"long"')
    expected = {0: b"\x00", -1: b"\x01", 1: b"\x02", -2: b"\x03", 2: b"\x04", 64: b"\x80\x01", -64: b"\x7f", -65: b"\x81\x01"}
    for value, encoded in expected.items():
        assert encode_datum(long_schema, value) == encoded, value
        assert decode_datum(long_schema, encoded) == value, value


def test_long_extremes_roundtrip():
    for value in (2**63 - 1, -(2**63), 2**31, -(2**31)):
        assert roundtrip('"long"', value) == value


def test_spec_example_record_a_27_b_foo():
    # The Avro spec: record {long a; string b} with a=27, b="foo" is 36 06 66 6f 6f.
    schema = parse_schema(
        '{"type":"record","name":"test","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}'
    )
    encoded = encode_datum(schema, {"a": 27, "b": "foo"})
    assert encoded == bytes([0x36, 0x06, 0x66, 0x6F, 0x6F])
    assert decode_datum(schema, encoded) == {"a": 27, "b": "foo"}


def test_spec_example_union_null_string():
    # The Avro spec: union ["null","string"] holding "a" is 02 02 61; null is 00.
    schema = parse_schema('["null","string"]')
    assert encode_datum(schema, "a") == bytes([0x02, 0x02, 0x61])
    assert encode_datum(schema, None) == bytes([0x00])
    assert decode_datum(schema, bytes([0x02, 0x02, 0x61])) == "a"
    assert decode_datum(schema, bytes([0x00])) is None


def test_primitives_roundtrip():
    assert roundtrip('"boolean"', True) is True
    assert roundtrip('"boolean"', False) is False
    assert roundtrip('"string"', "héllo ✓") == "héllo ✓"
    assert roundtrip('"bytes"', b"\x00\xff\x10") == b"\x00\xff\x10"
    assert roundtrip('"double"', 1.5) == 1.5
    assert roundtrip('"double"', 3) == 3.0
    assert abs(roundtrip('"float"', 0.1) - 0.1) < 1e-6
    assert roundtrip('"null"', None) is None


def test_double_is_little_endian_ieee():
    assert encode_datum(parse_schema('"double"'), 1.0) == struct.pack("<d", 1.0)


def test_collections_roundtrip():
    assert roundtrip('{"type":"array","items":"long"}', [1, 2, 3]) == [1, 2, 3]
    assert roundtrip('{"type":"array","items":"long"}', []) == []
    assert roundtrip('{"type":"map","values":"string"}', {"a": "x", "b": "y"}) == {"a": "x", "b": "y"}
    enum = '{"type":"enum","name":"Colour","symbols":["RED","GREEN"]}'
    assert roundtrip(enum, "GREEN") == "GREEN"
    fixed = '{"type":"fixed","name":"Id","size":4}'
    assert roundtrip(fixed, b"abcd") == b"abcd"


def test_array_encoding_matches_spec_block_layout():
    # [3, 27] as array<long>: block count 2 (04), items 03 36, end marker 00.
    schema = parse_schema('{"type":"array","items":"long"}')
    assert encode_datum(schema, [3, 27]) == bytes([0x04, 0x06, 0x36, 0x00])


def test_decoder_accepts_negative_block_counts():
    # Count -2 means two items follow after a byte-size long.
    schema = parse_schema('{"type":"array","items":"long"}')
    data = bytes([0x03, 0x04, 0x06, 0x36, 0x00])
    assert decode_datum(schema, data) == [3, 27]


def test_nested_record_with_named_type_reuse():
    schema_json = """{
      "type": "record", "name": "Order", "namespace": "shop",
      "fields": [
        {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["NEW", "SHIPPED"]}},
        {"name": "previous", "type": ["null", "shop.Status"], "default": null},
        {"name": "tags", "type": {"type": "array", "items": "string"}}
      ]
    }"""
    value = {"status": "NEW", "previous": "SHIPPED", "tags": ["a", "b"]}
    assert roundtrip(schema_json, value) == value
    assert roundtrip(schema_json, {"status": "SHIPPED", "previous": None, "tags": []})["previous"] is None


def test_encode_ignores_unknown_keys_and_uses_defaults():
    schema = parsed("sensor_reading_v2.avsc")
    event = {
        "event_id": "e1",
        "device_id": "dev-001",
        "event_time": 1_700_000_000_000,
        "temperature_c": 20.5,
        "humidity_pct": 40.0,
        "not_in_schema": "ignored",
    }
    decoded = decode_datum(schema, encode_datum(schema, event))
    assert decoded["battery_pct"] is None
    assert decoded["firmware"] == "unknown"
    assert "not_in_schema" not in decoded


def test_encode_rejects_wrong_types_and_missing_fields():
    schema = parsed("sensor_reading_v1.avsc")
    good = {"event_id": "e", "device_id": "d", "event_time": 1, "temperature_c": 1.0, "humidity_pct": 1.0}
    encode_datum(schema, good)
    with expect_error(EncodeError, "temperature_c"):
        encode_datum(schema, {**good, "temperature_c": "hot"})
    broken = dict(good)
    del broken["device_id"]
    with expect_error(EncodeError, "device_id"):
        encode_datum(schema, broken)
    with expect_error(EncodeError):
        encode_datum(parse_schema('"int"'), 2**31)
    with expect_error(EncodeError):
        encode_datum(parse_schema('"long"'), True)


def test_decode_rejects_truncated_and_trailing_data():
    schema = parsed("sensor_reading_v1.avsc")
    good = encode_datum(
        schema,
        {"event_id": "e", "device_id": "d", "event_time": 1, "temperature_c": 1.0, "humidity_pct": 1.0},
    )
    with expect_error(DecodeError):
        decode_datum(schema, good[:-3])
    with expect_error(DecodeError, "trailing"):
        decode_datum(schema, good + b"\x00")
    with expect_error(DecodeError):
        decode_datum(parse_schema('"string"'), b"\x0aabc")  # claims 5 bytes, has 3
    with expect_error(DecodeError):
        decode_datum(parse_schema('"boolean"'), b"\x07")
    with expect_error(DecodeError):
        decode_datum(parse_schema('"long"'), b"\xff" * 11)
    with expect_error(DecodeError):
        decode_datum(parse_schema('"string"'), b"\x04\xff\xfe")  # not UTF-8


def test_resolution_new_reader_reads_old_data_using_defaults():
    v1, v2 = parsed("sensor_reading_v1.avsc"), parsed("sensor_reading_v2.avsc")
    old = encode_datum(
        v1, {"event_id": "e", "device_id": "d", "event_time": 5, "temperature_c": 21.0, "humidity_pct": 30.0}
    )
    decoded = decode_datum(v1, old, v2)
    assert decoded["battery_pct"] is None
    assert decoded["firmware"] == "unknown"
    assert decoded["temperature_c"] == 21.0
    assert list(decoded) == ["event_id", "device_id", "event_time", "temperature_c", "humidity_pct", "battery_pct", "firmware"]


def test_resolution_old_reader_skips_fields_it_does_not_know():
    v1, v2 = parsed("sensor_reading_v1.avsc"), parsed("sensor_reading_v2.avsc")
    new = encode_datum(
        v2,
        {
            "event_id": "e",
            "device_id": "d",
            "event_time": 5,
            "temperature_c": 21.0,
            "humidity_pct": 30.0,
            "battery_pct": 77.0,
            "firmware": "1.2.4",
        },
    )
    decoded = decode_datum(v2, new, v1)
    assert decoded == {"event_id": "e", "device_id": "d", "event_time": 5, "temperature_c": 21.0, "humidity_pct": 30.0}


def test_resolution_skips_every_kind_of_unknown_field_in_the_middle():
    writer = parse_schema(
        """{"type":"record","name":"R","fields":[
          {"name":"a","type":"long"},
          {"name":"skip_arr","type":{"type":"array","items":"string"}},
          {"name":"skip_map","type":{"type":"map","values":"double"}},
          {"name":"skip_union","type":["null","string"]},
          {"name":"skip_enum","type":{"type":"enum","name":"E","symbols":["X","Y"]}},
          {"name":"skip_fixed","type":{"type":"fixed","name":"F","size":3}},
          {"name":"b","type":"string"}]}"""
    )
    reader = parse_schema('{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"b","type":"string"}]}')
    value = {
        "a": 7,
        "skip_arr": ["p", "q"],
        "skip_map": {"k": 1.5},
        "skip_union": "u",
        "skip_enum": "Y",
        "skip_fixed": b"xyz",
        "b": "end",
    }
    assert decode_datum(writer, encode_datum(writer, value), reader) == {"a": 7, "b": "end"}


def test_resolution_type_promotion_and_failure():
    assert decode_datum(parse_schema('"int"'), encode_datum(parse_schema('"int"'), 5), parse_schema('"long"')) == 5
    promoted = decode_datum(parse_schema('"long"'), encode_datum(parse_schema('"long"'), 5), parse_schema('"double"'))
    assert promoted == 5.0 and isinstance(promoted, float)
    assert decode_datum(parse_schema('"string"'), encode_datum(parse_schema('"string"'), "x"), parse_schema('"bytes"')) == b"x"
    with expect_error(ResolutionError):
        decode_datum(parse_schema('"double"'), encode_datum(parse_schema('"double"'), 1.0), parse_schema('"long"'))
    with expect_error(ResolutionError):
        decode_datum(parse_schema('"string"'), encode_datum(parse_schema('"string"'), "x"), parse_schema('"long"'))


def test_resolution_fails_when_reader_field_has_no_default():
    v1 = parsed("sensor_reading_v1.avsc")
    v3 = parsed("sensor_reading_v3_breaking.avsc")
    data = encode_datum(
        v1, {"event_id": "e", "device_id": "d", "event_time": 5, "temperature_c": 21.0, "humidity_pct": 30.0}
    )
    with expect_error(ResolutionError, "temperature_celsius"):
        decode_datum(v1, data, v3)


def test_resolution_with_union_reader_and_writer():
    writer = parse_schema('["null","long"]')
    reader = parse_schema('["null","double"]')
    assert decode_datum(writer, encode_datum(writer, 4), reader) == 4.0
    assert decode_datum(writer, encode_datum(writer, None), reader) is None
    plain_reader = parse_schema('"long"')
    assert decode_datum(writer, encode_datum(writer, 9), plain_reader) == 9
    with expect_error(ResolutionError):
        decode_datum(writer, encode_datum(writer, None), plain_reader)


def test_enum_resolution_uses_default_for_unknown_symbol():
    writer = parse_schema('{"type":"enum","name":"E","symbols":["A","B","C"]}')
    reader = parse_schema('{"type":"enum","name":"E","symbols":["A","B"],"default":"A"}')
    assert decode_datum(writer, encode_datum(writer, "C"), reader) == "A"
    strict = parse_schema('{"type":"enum","name":"E","symbols":["A","B"]}')
    with expect_error(ResolutionError):
        decode_datum(writer, encode_datum(writer, "C"), strict)


def test_field_alias_lets_a_renamed_field_resolve():
    writer = parse_schema('{"type":"record","name":"R","fields":[{"name":"temp","type":"double"}]}')
    reader = parse_schema(
        '{"type":"record","name":"R","fields":[{"name":"temperature","type":"double","aliases":["temp"]}]}'
    )
    assert decode_datum(writer, encode_datum(writer, {"temp": 3.5}), reader) == {"temperature": 3.5}


def test_compatibility_v2_is_full_compatible_with_v1():
    v1, v2 = parsed("sensor_reading_v1.avsc"), parsed("sensor_reading_v2.avsc")
    for mode in ("BACKWARD", "FORWARD", "FULL", "FULL_ALL", "NONE"):
        assert check_compatibility(v2, [("version 1", v1)], mode) == [], mode


def test_compatibility_breaking_schema_is_rejected_in_both_directions():
    v1, v2, v3 = parsed("sensor_reading_v1.avsc"), parsed("sensor_reading_v2.avsc"), parsed("sensor_reading_v3_breaking.avsc")
    existing = [("version 1", v1), ("version 2", v2)]
    backward = check_compatibility(v3, existing, "BACKWARD")
    forward = check_compatibility(v3, existing, "FORWARD")
    full = check_compatibility(v3, existing, "FULL")
    assert backward and forward
    assert len(full) == len(backward) + len(forward)
    assert any("temperature_celsius" in p for p in backward)
    assert any("temperature_c" in p for p in forward)
    assert any("cannot read" in p and "humidity_pct" in p for p in full)
    assert check_compatibility(v3, existing, "NONE") == []


def test_compatibility_all_modes_check_every_version():
    v1 = parse_schema('{"type":"record","name":"R","fields":[{"name":"a","type":"long"}]}')
    v2 = parse_schema('{"type":"record","name":"R","fields":[{"name":"a","type":"long","default":0}]}')
    new = parse_schema('{"type":"record","name":"R","fields":[{"name":"c","type":"string","default":""}]}')
    versions = [("version 1", v1), ("version 2", v2)]
    # Only the latest version (v2) is checked: v2 has a default for 'a', so it can read data without it.
    assert check_compatibility(new, versions, "FORWARD") == []
    # Every version is checked: v1 requires 'a' and has no default, so it cannot read the new data.
    problems = check_compatibility(new, versions, "FORWARD_ALL")
    assert problems and all("version 1" in p for p in problems)


def test_adding_a_required_field_without_default_breaks_backward():
    v1 = parsed("sensor_reading_v1.avsc")
    text = schema_text("sensor_reading_v1.avsc").replace(
        '"humidity_pct", "type": "double"}', '"humidity_pct", "type": "double"}, {"name": "site", "type": "string"}'
    )
    new = parse_schema(text)
    assert check_compatibility(new, [("version 1", v1)], "BACKWARD")
    assert check_compatibility(new, [("version 1", v1)], "FORWARD") == []


def test_resolution_problems_is_empty_for_identical_schemas():
    schema = parsed("sensor_reading_v2.avsc")
    assert resolution_problems(schema, schema) == []


def test_schema_errors():
    with expect_error(SchemaError):
        parse_schema("{not json")
    with expect_error(SchemaError):
        parse_schema('{"type":"record","name":"R","fields":[{"name":"a","type":"nope"}]}')
    with expect_error(SchemaError):
        parse_schema('{"type":"record","name":"R","fields":[{"name":"a","type":["null","string"],"default":"x"}]}')
    with expect_error(SchemaError):
        parse_schema('{"type":"record","name":"R","fields":[{"name":"a","type":"long"},{"name":"a","type":"long"}]}')
    with expect_error(SchemaError):
        parse_schema('["string","string"]')
    with expect_error(SchemaError):
        parse_schema('{"type":"enum","name":"E","symbols":[]}')
