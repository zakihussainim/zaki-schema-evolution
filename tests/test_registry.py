"""The registry client, against a fake Glue API."""

from helpers import expect_error, fixed_uuid, schema_text

from schema_evolution.registry import RegistryNotFound, RegistryRejected, SchemaRegistry


class FakeClientError(Exception):
    def __init__(self, code, message="boom"):
        super().__init__(f"{code}: {message}")
        self.response = {"Error": {"Code": code, "Message": message}}


class FakeGlue:
    """Behaves like the parts of the Glue API the client uses, with FULL compatibility enforced by the caller."""

    def __init__(self, reject_with=None, final_status="AVAILABLE"):
        self.versions = {}
        self.order = []
        self.reject_with = reject_with
        self.final_status = final_status
        self.calls = []

    def seed(self, number, text):
        version_id = fixed_uuid(number)
        self.versions[version_id] = {
            "SchemaVersionId": version_id,
            "VersionNumber": number,
            "SchemaDefinition": text,
            "Status": "AVAILABLE",
        }
        self.order.append(version_id)

    def get_schema(self, SchemaId):
        return {"Compatibility": "FULL"}

    def get_schema_version(self, SchemaVersionId=None, SchemaId=None, SchemaVersionNumber=None):
        self.calls.append(SchemaVersionId or SchemaVersionNumber)
        if SchemaVersionId:
            if SchemaVersionId not in self.versions:
                raise FakeClientError("EntityNotFoundException")
            return self.versions[SchemaVersionId]
        number = SchemaVersionNumber["VersionNumber"]
        for version in self.versions.values():
            if version["VersionNumber"] == number:
                return version
        raise FakeClientError("EntityNotFoundException")

    def list_schema_versions(self, SchemaId, MaxResults, NextToken=None):
        return {
            "Schemas": [
                {"SchemaVersionId": v["SchemaVersionId"], "VersionNumber": v["VersionNumber"], "Status": v["Status"]}
                for v in self.versions.values()
            ]
        }

    def register_schema_version(self, SchemaId, SchemaDefinition):
        if self.reject_with:
            raise FakeClientError(self.reject_with)
        number = len(self.versions) + 1
        self.seed(number, SchemaDefinition)
        self.versions[fixed_uuid(number)]["Status"] = self.final_status
        return {"SchemaVersionId": fixed_uuid(number), "VersionNumber": number, "Status": self.final_status}


def registry_with_v1():
    glue = FakeGlue()
    glue.seed(1, schema_text("sensor_reading_v1.avsc"))
    return glue, SchemaRegistry(glue, "test-registry")


def test_get_version_is_cached():
    glue, registry = registry_with_v1()
    first = registry.get_version(fixed_uuid(1))
    registry.get_version(fixed_uuid(1))
    registry.get_version(fixed_uuid(1))
    assert first.version_number == 1
    assert glue.calls.count(fixed_uuid(1)) == 1


def test_unknown_version_raises_not_found():
    _, registry = registry_with_v1()
    with expect_error(RegistryNotFound):
        registry.get_version(fixed_uuid(99))
    with expect_error(RegistryNotFound):
        registry.get_version_by_number(5)


def test_local_check_accepts_v2_and_rejects_the_breaking_schema():
    _, registry = registry_with_v1()
    assert registry.check(schema_text("sensor_reading_v2.avsc")) == []
    problems = registry.check(schema_text("sensor_reading_v3_breaking.avsc"))
    assert problems and any("temperature_celsius" in p for p in problems)


def test_register_returns_the_new_version():
    glue, registry = registry_with_v1()
    version = registry.register(schema_text("sensor_reading_v2.avsc"))
    assert version.version_number == 2
    assert registry.get_version_by_number(2).version_id == version.version_id


def test_register_reports_a_registry_rejection():
    glue, registry = registry_with_v1()
    glue.reject_with = "InvalidInputException"
    with expect_error(RegistryRejected):
        registry.register(schema_text("sensor_reading_v3_breaking.avsc"))


def test_register_reports_a_failed_version_status():
    glue, registry = registry_with_v1()
    glue.final_status = "FAILURE"
    with expect_error(RegistryRejected, "FAILURE"):
        registry.register(schema_text("sensor_reading_v3_breaking.avsc"))


def test_a_version_that_is_not_available_cannot_be_used_by_a_producer():
    glue, registry = registry_with_v1()
    glue.final_status = "FAILURE"
    try:
        registry.register(schema_text("sensor_reading_v3_breaking.avsc"))
    except RegistryRejected:
        pass
    with expect_error(RegistryNotFound, "FAILURE"):
        registry.get_version_by_number(2)
