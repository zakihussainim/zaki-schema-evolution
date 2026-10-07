"""A small, dependency-free implementation of Apache Avro.

It covers what this project needs and nothing more:

* parsing schemas (primitives, records, enums, arrays, maps, unions, fixed),
* binary encoding and decoding,
* schema resolution (reading data written with one schema using another),
* compatibility checking (BACKWARD, FORWARD and FULL, as the Glue Schema Registry defines them).

It exists so the Lambda consumer needs no third-party packages. A production system would normally use
fastavro; the unit tests check this code against the encodings written down in the Avro specification.

Decoded values are plain Python: null -> None, boolean -> bool, int/long -> int, float/double -> float,
bytes -> bytes, string -> str, record -> dict, enum -> str, array -> list, map -> dict. A union decodes to
the value of whichever branch was written.
"""

from __future__ import annotations

import copy
import json
import struct

PRIMITIVES = ("null", "boolean", "int", "long", "float", "double", "bytes", "string")
NAMED_TYPES = ("record", "enum", "fixed")

# (writer type, reader type) pairs where the reader may read the writer's data.
PROMOTIONS = {
    ("int", "long"),
    ("int", "float"),
    ("int", "double"),
    ("long", "float"),
    ("long", "double"),
    ("float", "double"),
    ("string", "bytes"),
    ("bytes", "string"),
}

INT_MIN, INT_MAX = -(2**31), 2**31 - 1
LONG_MIN, LONG_MAX = -(2**63), 2**63 - 1
MAX_COLLECTION_ITEMS = 1_000_000


class AvroError(Exception):
    """Base class for every error raised by this module."""


class SchemaError(AvroError):
    """The schema itself is invalid."""


class EncodeError(AvroError):
    """A value does not fit the schema it is being written with."""


class DecodeError(AvroError):
    """The bytes are not valid for the writer schema."""


class ResolutionError(AvroError):
    """The reader schema cannot read data written with the writer schema."""


# ---------------------------------------------------------------------------
# Schema parsing
# ---------------------------------------------------------------------------


def parse_schema(schema):
    """Turn schema JSON (text, dict or list) into the normalised form used by the rest of this module."""
    if isinstance(schema, (bytes, bytearray)):
        schema = bytes(schema).decode("utf-8")
    if isinstance(schema, str):
        try:
            schema = json.loads(schema)
        except json.JSONDecodeError as exc:
            raise SchemaError(f"schema is not valid JSON: {exc}") from exc
    return _parse(schema, {}, "")


def _fullname(schema, namespace):
    name = schema.get("name")
    if not isinstance(name, str) or not name:
        raise SchemaError("a named type is missing its name")
    if "." in name:
        return name, name.rsplit(".", 1)[0]
    space = schema.get("namespace", namespace) or ""
    return (f"{space}.{name}" if space else name), space


def _label(node):
    if node["type"] in NAMED_TYPES:
        return node["name"]
    return node["type"]


def _parse(schema, names, namespace):
    if isinstance(schema, str):
        if schema in PRIMITIVES:
            return {"type": schema}
        candidates = [schema] if "." in schema else ([f"{namespace}.{schema}"] if namespace else []) + [schema]
        for candidate in candidates:
            if candidate in names:
                return names[candidate]
        raise SchemaError(f"unknown type name {schema!r}")

    if isinstance(schema, list):
        if not schema:
            raise SchemaError("a union needs at least one branch")
        branches = [_parse(branch, names, namespace) for branch in schema]
        seen = set()
        for branch in branches:
            if branch["type"] == "union":
                raise SchemaError("a union may not directly contain another union")
            key = branch["name"] if branch["type"] in NAMED_TYPES else branch["type"]
            if key in seen:
                raise SchemaError(f"duplicate branch in union: {key}")
            seen.add(key)
        return {"type": "union", "branches": branches}

    if not isinstance(schema, dict):
        raise SchemaError(f"unsupported schema: {schema!r}")

    kind = schema.get("type")
    if isinstance(kind, (dict, list)):
        return _parse(kind, names, namespace)
    if kind in PRIMITIVES:
        node = {"type": kind}
        if "logicalType" in schema:
            node["logicalType"] = schema["logicalType"]
        return node

    if kind == "record":
        full, inner = _fullname(schema, namespace)
        if full in names:
            raise SchemaError(f"{full} is defined twice")
        node = {"type": "record", "name": full, "aliases": list(schema.get("aliases", [])), "fields": []}
        names[full] = node
        seen_fields = set()
        for field in schema.get("fields", []):
            fname = field.get("name")
            if not isinstance(fname, str) or not fname:
                raise SchemaError(f"a field of {full} has no name")
            if fname in seen_fields:
                raise SchemaError(f"{full} has two fields called {fname}")
            seen_fields.add(fname)
            if "type" not in field:
                raise SchemaError(f"{full}.{fname} has no type")
            ftype = _parse(field["type"], names, inner)
            entry = {
                "name": fname,
                "type": ftype,
                "aliases": list(field.get("aliases", [])),
                "has_default": "default" in field,
            }
            if entry["has_default"]:
                entry["default"] = _convert_default(ftype, field["default"], f"{full}.{fname}")
            node["fields"].append(entry)
        return node

    if kind == "enum":
        full, _ = _fullname(schema, namespace)
        symbols = schema.get("symbols")
        if not isinstance(symbols, list) or not symbols or len(set(symbols)) != len(symbols):
            raise SchemaError(f"enum {full} needs a non-empty list of unique symbols")
        node = {
            "type": "enum",
            "name": full,
            "symbols": list(symbols),
            "aliases": list(schema.get("aliases", [])),
            "default": schema.get("default"),
        }
        names[full] = node
        return node

    if kind == "fixed":
        full, _ = _fullname(schema, namespace)
        size = schema.get("size")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            raise SchemaError(f"fixed {full} needs a non-negative integer size")
        node = {"type": "fixed", "name": full, "size": size, "aliases": list(schema.get("aliases", []))}
        names[full] = node
        return node

    if kind == "array":
        return {"type": "array", "items": _parse(schema.get("items"), names, namespace)}

    if kind == "map":
        return {"type": "map", "values": _parse(schema.get("values"), names, namespace)}

    raise SchemaError(f"unsupported schema type: {kind!r}")


def _convert_default(node, raw, where):
    """Validate a JSON default value and convert it to the Python value a reader would produce."""

    def bad():
        return SchemaError(f"invalid default {raw!r} for {where}")

    kind = node["type"]
    if kind == "null":
        if raw is not None:
            raise bad()
        return None
    if kind == "boolean":
        if not isinstance(raw, bool):
            raise bad()
        return raw
    if kind in ("int", "long"):
        if not isinstance(raw, int) or isinstance(raw, bool):
            raise bad()
        return raw
    if kind in ("float", "double"):
        if not isinstance(raw, (int, float)) or isinstance(raw, bool):
            raise bad()
        return float(raw)
    if kind == "string":
        if not isinstance(raw, str):
            raise bad()
        return raw
    if kind in ("bytes", "fixed"):
        if not isinstance(raw, str):
            raise bad()
        return raw.encode("latin-1")
    if kind == "enum":
        if raw not in node["symbols"]:
            raise bad()
        return raw
    if kind == "array":
        if not isinstance(raw, list):
            raise bad()
        return [_convert_default(node["items"], item, where) for item in raw]
    if kind == "map":
        if not isinstance(raw, dict):
            raise bad()
        return {key: _convert_default(node["values"], item, where) for key, item in raw.items()}
    if kind == "record":
        if not isinstance(raw, dict):
            raise bad()
        out = {}
        for field in node["fields"]:
            if field["name"] in raw:
                out[field["name"]] = _convert_default(field["type"], raw[field["name"]], where)
            elif field["has_default"]:
                out[field["name"]] = copy.deepcopy(field["default"])
            else:
                raise bad()
        return out
    if kind == "union":
        return _convert_default(node["branches"][0], raw, where)
    raise bad()


def field_names(schema):
    """Names of the fields of a parsed record schema, in order."""
    return [field["name"] for field in schema["fields"]]


# ---------------------------------------------------------------------------
# Encoding
# ---------------------------------------------------------------------------


def _write_long(out, value):
    if not LONG_MIN <= value <= LONG_MAX:
        raise EncodeError(f"{value} does not fit in 64 bits")
    zigzag = (value << 1) ^ (value >> 63)
    while zigzag > 0x7F:
        out.append((zigzag & 0x7F) | 0x80)
        zigzag >>= 7
    out.append(zigzag)


def _is_int(value):
    return isinstance(value, int) and not isinstance(value, bool)


def _is_number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _accepts(node, value):
    kind = node["type"]
    if kind == "null":
        return value is None
    if kind == "boolean":
        return isinstance(value, bool)
    if kind in ("int", "long"):
        return _is_int(value)
    if kind in ("float", "double"):
        return _is_number(value)
    if kind == "string":
        return isinstance(value, str)
    if kind == "bytes":
        return isinstance(value, (bytes, bytearray))
    if kind in ("record", "map"):
        return isinstance(value, dict)
    if kind == "array":
        return isinstance(value, (list, tuple))
    if kind == "enum":
        return isinstance(value, str) and value in node["symbols"]
    if kind == "fixed":
        return isinstance(value, (bytes, bytearray)) and len(value) == node["size"]
    return False


def _write(out, node, value, path):
    kind = node["type"]
    if kind == "union":
        for index, branch in enumerate(node["branches"]):
            if _accepts(branch, value):
                _write_long(out, index)
                _write(out, branch, value, path)
                return
        raise EncodeError(f"{path}: {value!r} matches no branch of the union")
    if kind == "null":
        if value is not None:
            raise EncodeError(f"{path}: expected null, got {value!r}")
    elif kind == "boolean":
        if not isinstance(value, bool):
            raise EncodeError(f"{path}: expected a boolean, got {value!r}")
        out.append(1 if value else 0)
    elif kind in ("int", "long"):
        if not _is_int(value):
            raise EncodeError(f"{path}: expected an integer, got {value!r}")
        if kind == "int" and not INT_MIN <= value <= INT_MAX:
            raise EncodeError(f"{path}: {value} does not fit in an int")
        _write_long(out, value)
    elif kind in ("float", "double"):
        if not _is_number(value):
            raise EncodeError(f"{path}: expected a number, got {value!r}")
        out.extend(struct.pack("<f" if kind == "float" else "<d", float(value)))
    elif kind == "string":
        if not isinstance(value, str):
            raise EncodeError(f"{path}: expected a string, got {value!r}")
        data = value.encode("utf-8")
        _write_long(out, len(data))
        out.extend(data)
    elif kind == "bytes":
        if not isinstance(value, (bytes, bytearray)):
            raise EncodeError(f"{path}: expected bytes, got {value!r}")
        _write_long(out, len(value))
        out.extend(value)
    elif kind == "record":
        if not isinstance(value, dict):
            raise EncodeError(f"{path}: expected a mapping for record {node['name']}, got {value!r}")
        for field in node["fields"]:
            if field["name"] in value:
                item = value[field["name"]]
            elif field["has_default"]:
                item = copy.deepcopy(field["default"])
            else:
                raise EncodeError(f"{path}.{field['name']}: missing and has no default")
            _write(out, field["type"], item, f"{path}.{field['name']}")
    elif kind == "enum":
        if value not in node["symbols"]:
            raise EncodeError(f"{path}: {value!r} is not a symbol of {node['name']}")
        _write_long(out, node["symbols"].index(value))
    elif kind == "array":
        if not isinstance(value, (list, tuple)):
            raise EncodeError(f"{path}: expected a list, got {value!r}")
        if value:
            _write_long(out, len(value))
            for item in value:
                _write(out, node["items"], item, f"{path}[]")
        _write_long(out, 0)
    elif kind == "map":
        if not isinstance(value, dict):
            raise EncodeError(f"{path}: expected a mapping, got {value!r}")
        if value:
            _write_long(out, len(value))
            for key, item in value.items():
                _write(out, {"type": "string"}, key, f"{path}{{key}}")
                _write(out, node["values"], item, f"{path}{{}}")
        _write_long(out, 0)
    elif kind == "fixed":
        if not isinstance(value, (bytes, bytearray)) or len(value) != node["size"]:
            raise EncodeError(f"{path}: expected {node['size']} bytes")
        out.extend(value)
    else:
        raise EncodeError(f"{path}: unsupported type {kind}")


def encode_datum(schema, value):
    """Encode one value with a parsed schema. Keys in a record dict that the schema does not know are ignored."""
    out = bytearray()
    _write(out, schema, value, "$")
    return bytes(out)


# ---------------------------------------------------------------------------
# Decoding with schema resolution
# ---------------------------------------------------------------------------


class _Reader:
    def __init__(self, data):
        self.data = bytes(data)
        self.pos = 0

    def read(self, count):
        end = self.pos + count
        if count < 0 or end > len(self.data):
            raise DecodeError("unexpected end of data")
        chunk = self.data[self.pos : end]
        self.pos = end
        return chunk

    def read_long(self):
        shift = 0
        result = 0
        while True:
            byte = self.read(1)[0]
            result |= (byte & 0x7F) << shift
            if not byte & 0x80:
                break
            shift += 7
            if shift > 63:
                raise DecodeError("variable-length integer is too long")
        value = (result >> 1) ^ -(result & 1)
        if not LONG_MIN <= value <= LONG_MAX:
            raise DecodeError("integer does not fit in 64 bits")
        return value


def _read_primitive(reader, kind, path):
    if kind == "null":
        return None
    if kind == "boolean":
        byte = reader.read(1)[0]
        if byte not in (0, 1):
            raise DecodeError(f"{path}: invalid boolean byte {byte}")
        return byte == 1
    if kind in ("int", "long"):
        value = reader.read_long()
        if kind == "int" and not INT_MIN <= value <= INT_MAX:
            raise DecodeError(f"{path}: {value} does not fit in an int")
        return value
    if kind == "float":
        return struct.unpack("<f", reader.read(4))[0]
    if kind == "double":
        return struct.unpack("<d", reader.read(8))[0]
    if kind in ("bytes", "string"):
        length = reader.read_long()
        if length < 0:
            raise DecodeError(f"{path}: negative length")
        raw = reader.read(length)
        if kind == "bytes":
            return raw
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise DecodeError(f"{path}: string is not valid UTF-8") from exc
    raise DecodeError(f"{path}: unsupported primitive {kind}")


def _blocks(reader, path):
    total = 0
    while True:
        count = reader.read_long()
        if count == 0:
            return
        if count < 0:
            count = -count
            reader.read_long()  # byte size of the block, not needed
        total += count
        if total > MAX_COLLECTION_ITEMS:
            raise DecodeError(f"{path}: more than {MAX_COLLECTION_ITEMS} items")
        yield count


def _short_name(name):
    return name.rsplit(".", 1)[-1]


def _names_match(writer_name, reader_node):
    short = _short_name(writer_name)
    if short == _short_name(reader_node["name"]):
        return True
    aliases = reader_node.get("aliases", [])
    return writer_name in aliases or short in [_short_name(a) for a in aliases]


def _find_writer_field(writer_fields, reader_field):
    for candidate in [reader_field["name"], *reader_field.get("aliases", [])]:
        if candidate in writer_fields:
            return writer_fields[candidate]
    return None


def _read(reader, writer, target, path):
    if writer["type"] == "union":
        index = reader.read_long()
        branches = writer["branches"]
        if not 0 <= index < len(branches):
            raise DecodeError(f"{path}: union index {index} is out of range")
        return _read(reader, branches[index], target, path)

    if target["type"] == "union":
        for branch in target["branches"]:
            if not resolution_problems(writer, branch):
                return _read(reader, writer, branch, path)
        raise ResolutionError(f"{path}: {_label(writer)} matches no branch of the reader's union")

    wt = writer["type"]
    rt = target["type"]

    if wt in PRIMITIVES:
        if wt != rt and (wt, rt) not in PROMOTIONS:
            raise ResolutionError(f"{path}: cannot read {wt} as {rt}")
        value = _read_primitive(reader, wt, path)
        if rt in ("float", "double") and wt in ("int", "long", "float"):
            return float(value)
        if wt == "string" and rt == "bytes":
            return value.encode("utf-8")
        if wt == "bytes" and rt == "string":
            try:
                return value.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise DecodeError(f"{path}: bytes are not valid UTF-8") from exc
        return value

    if wt == "record":
        if rt != "record" or not _names_match(writer["name"], target):
            raise ResolutionError(f"{path}: cannot read record {writer['name']} as {_label(target)}")
        writer_fields = {field["name"]: field for field in writer["fields"]}
        mapping = {}
        defaults = {}
        for rfield in target["fields"]:
            wfield = _find_writer_field(writer_fields, rfield)
            if wfield is not None:
                mapping[wfield["name"]] = rfield
            elif rfield["has_default"]:
                defaults[rfield["name"]] = rfield["default"]
            else:
                raise ResolutionError(
                    f"{path}.{rfield['name']}: the reader requires this field, it has no default, "
                    "and the writer did not write it"
                )
        values = {}
        for wfield in writer["fields"]:
            rfield = mapping.get(wfield["name"])
            where = f"{path}.{wfield['name']}"
            if rfield is None:
                _read(reader, wfield["type"], wfield["type"], where)  # present in the data, unknown to the reader: skip
            else:
                values[rfield["name"]] = _read(reader, wfield["type"], rfield["type"], where)
        return {
            rfield["name"]: values[rfield["name"]]
            if rfield["name"] in values
            else copy.deepcopy(defaults[rfield["name"]])
            for rfield in target["fields"]
        }

    if wt == "enum":
        index = reader.read_long()
        if not 0 <= index < len(writer["symbols"]):
            raise DecodeError(f"{path}: enum index {index} is out of range")
        symbol = writer["symbols"][index]
        if rt != "enum" or not _names_match(writer["name"], target):
            raise ResolutionError(f"{path}: cannot read enum {writer['name']} as {_label(target)}")
        if symbol in target["symbols"]:
            return symbol
        if target.get("default") is not None:
            return target["default"]
        raise ResolutionError(f"{path}: symbol {symbol!r} is not in the reader's enum")

    if wt == "array":
        if rt != "array":
            raise ResolutionError(f"{path}: cannot read array as {_label(target)}")
        items = []
        for count in _blocks(reader, path):
            for _ in range(count):
                items.append(_read(reader, writer["items"], target["items"], f"{path}[]"))
        return items

    if wt == "map":
        if rt != "map":
            raise ResolutionError(f"{path}: cannot read map as {_label(target)}")
        result = {}
        for count in _blocks(reader, path):
            for _ in range(count):
                key = _read_primitive(reader, "string", path)
                result[key] = _read(reader, writer["values"], target["values"], f"{path}{{}}")
        return result

    if wt == "fixed":
        if rt != "fixed" or writer["size"] != target["size"] or not _names_match(writer["name"], target):
            raise ResolutionError(f"{path}: cannot read fixed {writer['name']} as {_label(target)}")
        return reader.read(writer["size"])

    raise ResolutionError(f"{path}: unsupported type {wt}")


def decode_datum(writer, data, reader=None):
    """Decode bytes written with `writer`, shaped to `reader` (the same schema when omitted)."""
    stream = _Reader(data)
    value = _read(stream, writer, reader if reader is not None else writer, "$")
    if stream.pos != len(stream.data):
        raise DecodeError(f"{len(stream.data) - stream.pos} unexpected trailing bytes")
    return value


# ---------------------------------------------------------------------------
# Resolution and compatibility checks (no data involved)
# ---------------------------------------------------------------------------


def resolution_problems(writer, reader, path="$", _seen=None):
    """Why `reader` cannot read data written with `writer`. An empty list means it can."""
    seen = _seen if _seen is not None else set()
    key = (id(writer), id(reader))
    if key in seen:
        return []
    seen.add(key)

    if writer["type"] == "union":
        problems = []
        for branch in writer["branches"]:
            problems.extend(resolution_problems(branch, reader, f"{path}|{_label(branch)}", seen))
        return problems

    if reader["type"] == "union":
        for branch in reader["branches"]:
            if not resolution_problems(writer, branch, path, set(seen)):
                return []
        return [f"{path}: {_label(writer)} matches no branch of the reader's union"]

    wt = writer["type"]
    rt = reader["type"]
    if wt in PRIMITIVES and rt in PRIMITIVES:
        if wt == rt or (wt, rt) in PROMOTIONS:
            return []
        return [f"{path}: cannot read {wt} as {rt}"]
    if wt != rt:
        return [f"{path}: cannot read {_label(writer)} as {_label(reader)}"]

    if wt == "record":
        if not _names_match(writer["name"], reader):
            return [f"{path}: record {writer['name']} cannot be read as {reader['name']}"]
        writer_fields = {field["name"]: field for field in writer["fields"]}
        problems = []
        for rfield in reader["fields"]:
            where = f"{path}.{rfield['name']}"
            wfield = _find_writer_field(writer_fields, rfield)
            if wfield is None:
                if not rfield["has_default"]:
                    problems.append(f"{where}: no default, and the writer does not provide this field")
            else:
                problems.extend(resolution_problems(wfield["type"], rfield["type"], where, seen))
        return problems

    if wt == "enum":
        if not _names_match(writer["name"], reader):
            return [f"{path}: enum {writer['name']} cannot be read as {reader['name']}"]
        if reader.get("default") is not None:
            return []
        missing = [symbol for symbol in writer["symbols"] if symbol not in reader["symbols"]]
        return [f"{path}: symbols {missing} are missing from the reader's enum"] if missing else []

    if wt == "array":
        return resolution_problems(writer["items"], reader["items"], f"{path}[]", seen)

    if wt == "map":
        return resolution_problems(writer["values"], reader["values"], f"{path}{{}}", seen)

    if wt == "fixed":
        if writer["size"] != reader["size"] or not _names_match(writer["name"], reader):
            return [f"{path}: fixed types differ"]
        return []

    return [f"{path}: unsupported type {wt}"]


COMPATIBILITY_MODES = (
    "NONE",
    "DISABLED",
    "BACKWARD",
    "BACKWARD_ALL",
    "FORWARD",
    "FORWARD_ALL",
    "FULL",
    "FULL_ALL",
)


def check_compatibility(new_schema, existing, mode):
    """Problems that stop `new_schema` from being registered under `mode`.

    `existing` is a list of (label, parsed schema) from oldest to newest.

    BACKWARD: consumers on the NEW schema can read data written with the old one.
    FORWARD:  consumers still on the OLD schema can read data written with the new one.
    FULL:     both. The *_ALL modes check every earlier version instead of only the latest.
    """
    mode = mode.upper()
    if mode not in COMPATIBILITY_MODES:
        raise ValueError(f"unknown compatibility mode {mode}")
    if mode in ("NONE", "DISABLED") or not existing:
        return []
    targets = existing if mode.endswith("_ALL") else existing[-1:]
    base = mode.removesuffix("_ALL")
    problems = []
    for label, old in targets:
        if base in ("BACKWARD", "FULL"):
            for problem in resolution_problems(old, new_schema):
                problems.append(f"BACKWARD against {label} (new readers, old data): {problem}")
        if base in ("FORWARD", "FULL"):
            for problem in resolution_problems(new_schema, old):
                problems.append(f"FORWARD against {label} (old readers, new data): {problem}")
    return problems
