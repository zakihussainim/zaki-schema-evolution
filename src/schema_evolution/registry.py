"""Client and command-line tool for the AWS Glue Schema Registry.

Terraform creates the registry and the schema (with version 1). New versions are registered with this tool,
so that evolving a schema is a visible, repeatable action and a breaking change is rejected where it counts.

Usage (from the src folder):
    python -m schema_evolution.registry list     --env dev
    python -m schema_evolution.registry check    schema_evolution/schemas/sensor_reading_v2.avsc --env dev
    python -m schema_evolution.registry register schema_evolution/schemas/sensor_reading_v2.avsc --env dev
"""

import argparse
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from . import names
from .avro import SchemaError, check_compatibility, parse_schema

SCHEMA_DIR = Path(__file__).resolve().parent / "schemas"
NOT_FOUND_CODES = ("EntityNotFoundException", "InvalidInputException")


class RegistryError(Exception):
    """Base class for registry problems."""


class RegistryNotFound(RegistryError):
    """The schema version does not exist."""


class RegistryRejected(RegistryError):
    """The registry refused to accept a new schema version."""


@dataclass(frozen=True)
class SchemaVersion:
    version_id: str
    version_number: int
    definition: str
    status: str = "AVAILABLE"


def error_code(exc):
    """The AWS error code of a botocore ClientError, without importing botocore."""
    response = getattr(exc, "response", None)
    if isinstance(response, dict):
        return response.get("Error", {}).get("Code")
    return None


class SchemaRegistry:
    def __init__(self, glue_client, registry_name, schema_name=names.SCHEMA_NAME):
        self._glue = glue_client
        self.registry_name = registry_name
        self.schema_name = schema_name
        self._by_id = {}

    def _schema_id(self):
        return {"RegistryName": self.registry_name, "SchemaName": self.schema_name}

    @staticmethod
    def _to_version(response):
        return SchemaVersion(
            version_id=response["SchemaVersionId"],
            version_number=int(response["VersionNumber"]),
            definition=response["SchemaDefinition"],
            status=response.get("Status", "AVAILABLE"),
        )

    def compatibility(self):
        return self._glue.get_schema(SchemaId=self._schema_id())["Compatibility"]

    def get_version(self, version_id):
        """Look up a version by its ID (what every message carries). Results are cached."""
        if version_id not in self._by_id:
            try:
                response = self._glue.get_schema_version(SchemaVersionId=version_id)
            except Exception as exc:
                if error_code(exc) in NOT_FOUND_CODES:
                    raise RegistryNotFound(f"schema version {version_id} is not in the registry") from exc
                raise
            self._by_id[version_id] = self._to_version(response)
        return self._by_id[version_id]

    def get_version_by_number(self, number):
        try:
            response = self._glue.get_schema_version(
                SchemaId=self._schema_id(), SchemaVersionNumber={"VersionNumber": number}
            )
        except Exception as exc:
            if error_code(exc) in NOT_FOUND_CODES:
                raise RegistryNotFound(f"version {number} of {self.schema_name} is not registered") from exc
            raise
        version = self._to_version(response)
        if version.status != "AVAILABLE":
            raise RegistryNotFound(f"version {number} exists but its status is {version.status}")
        return version

    def list_versions(self):
        """Every version, oldest first, as dictionaries with VersionNumber, SchemaVersionId and Status."""
        versions = []
        token = None
        while True:
            kwargs = {"SchemaId": self._schema_id(), "MaxResults": 100}
            if token:
                kwargs["NextToken"] = token
            response = self._glue.list_schema_versions(**kwargs)
            versions.extend(response.get("Schemas", []))
            token = response.get("NextToken")
            if not token:
                break
        return sorted(versions, key=lambda v: v["VersionNumber"])

    def available_versions(self):
        """Parsed definitions of the versions a producer could be using, oldest first."""
        found = []
        for entry in self.list_versions():
            if entry.get("Status") != "AVAILABLE":
                continue
            version = self.get_version(entry["SchemaVersionId"])
            found.append((f"version {version.version_number}", parse_schema(version.definition)))
        return found

    def check(self, definition):
        """Compatibility problems found locally with the same rules the registry applies."""
        return check_compatibility(parse_schema(definition), self.available_versions(), self.compatibility())

    def register(self, definition, wait_seconds=60):
        """Register a new version. Raises RegistryRejected if the registry refuses it."""
        try:
            response = self._glue.register_schema_version(SchemaId=self._schema_id(), SchemaDefinition=definition)
        except Exception as exc:
            code = error_code(exc)
            if code == "AlreadyExistsException":
                found = self._glue.get_schema_by_definition(SchemaId=self._schema_id(), SchemaDefinition=definition)
                return self.get_version(found["SchemaVersionId"])
            if code == "InvalidInputException":
                raise RegistryRejected(str(exc)) from exc
            raise
        version_id = response["SchemaVersionId"]
        deadline = time.time() + wait_seconds
        status = response.get("Status")
        while status == "PENDING" and time.time() < deadline:
            time.sleep(2)
            status = self._glue.get_schema_version(SchemaVersionId=version_id).get("Status")
        if status == "FAILURE":
            raise RegistryRejected(
                f"the registry marked the new version {response.get('VersionNumber')} as FAILURE "
                f"(compatibility mode: {self.compatibility()})"
            )
        if status != "AVAILABLE":
            raise RegistryError(f"the new version is still {status} after {wait_seconds} seconds")
        self._by_id.pop(version_id, None)
        return self.get_version(version_id)


def _make_registry(env):
    import boto3

    glue = boto3.client("glue", region_name=names.REGION)
    return SchemaRegistry(glue, names.registry_name(env))


def _read_definition(path):
    file = Path(path)
    if not file.exists():
        candidate = SCHEMA_DIR / file.name
        if candidate.exists():
            file = candidate
        else:
            raise SystemExit(f"File not found: {path}")
    text = file.read_text(encoding="utf-8")
    try:
        parse_schema(text)
    except SchemaError as exc:
        raise SystemExit(f"{file.name} is not a valid Avro schema: {exc}") from exc
    return text


def main(argv=None):
    parser = argparse.ArgumentParser(description="Work with the Glue Schema Registry for this project.")
    sub = parser.add_subparsers(dest="command", required=True)
    for command, help_text in (
        ("list", "show every registered version and the compatibility mode"),
        ("check", "check a schema file against the registered versions without registering it"),
        ("register", "register a schema file as a new version"),
    ):
        p = sub.add_parser(command, help=help_text)
        p.add_argument("--env", required=True, choices=names.ENVIRONMENTS)
        if command != "list":
            p.add_argument("file", help="path to an .avsc file")
        if command == "register":
            p.add_argument(
                "--no-local-check",
                action="store_true",
                help="skip the local compatibility check and let the registry decide (to see its own rejection)",
            )
    args = parser.parse_args(argv)
    registry = _make_registry(args.env)

    if args.command == "list":
        print(f"Registry {registry.registry_name}, schema {registry.schema_name}")
        print(f"Compatibility mode: {registry.compatibility()}")
        for entry in registry.list_versions():
            print(f"  version {entry['VersionNumber']}: {entry['Status']}  {entry['SchemaVersionId']}")
        return 0

    definition = _read_definition(args.file)
    problems = registry.check(definition)

    if args.command == "check" or (args.command == "register" and not args.no_local_check):
        if problems:
            print(f"REJECTED locally. This schema is not {registry.compatibility()} compatible:")
            for problem in problems:
                print(f"  - {problem}")
            return 2
        print(f"Compatible with every earlier version under {registry.compatibility()}.")
        if args.command == "check":
            return 0

    if problems:
        print(f"Skipping the local check; the registry should reject this ({len(problems)} problems found locally).")
    try:
        version = registry.register(definition)
    except RegistryRejected as exc:
        print(f"REJECTED by the registry: {exc}")
        return 2
    print(f"Registered version {version.version_number} ({version.version_id}).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
