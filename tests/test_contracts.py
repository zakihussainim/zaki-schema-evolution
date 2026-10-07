"""Keeps the Python code and the Terraform in step. If you rename something on one side, a test here fails."""

import re
from pathlib import Path

from schema_evolution import consumer, names, pipeline

ROOT = Path(__file__).resolve().parents[1]
TF = ROOT / "terraform"
MODULES = ("storage", "registry", "streaming", "catalog")


def tf(relative):
    return (TF / relative).read_text(encoding="utf-8")


def all_module_text():
    return "\n".join(tf(f"modules/{m}/main.tf") for m in MODULES)


def test_project_name_is_the_same_in_code_and_bootstrap():
    match = re.search(r'project\s*=\s*"([^"]+)"', tf("bootstrap/main.tf"))
    assert match and match.group(1) == names.PROJECT
    assert f'"{names.REGION}"' in tf("bootstrap/main.tf")


def test_every_module_builds_names_from_the_project_prefix():
    prefix = f"{names.PROJECT}-${{var.environment}}"
    for module in ("storage", "registry", "streaming", "catalog"):
        assert prefix in tf(f"modules/{module}/main.tf"), module


def test_bucket_stream_queue_and_function_names_match():
    assert "${local.name_prefix}-events-${local.account_id}" in tf("modules/storage/main.tf")
    assert names.bucket_name("dev", "123") == "zaki-schema-evolution-dev-events-123"
    streaming = tf("modules/streaming/main.tf")
    assert 'name             = "${local.prefix}-events"' in streaming
    assert names.stream_name("dev") == "zaki-schema-evolution-dev-events"
    assert 'name                      = "${local.prefix}-dlq"' in streaming
    assert names.queue_name("dev") == "zaki-schema-evolution-dev-dlq"
    assert 'function_name = "${local.prefix}-consumer"' in streaming
    assert names.function_name("dev") == "zaki-schema-evolution-dev-consumer"


def test_registry_database_and_workgroup_names_match():
    registry = tf("modules/registry/main.tf")
    assert 'registry_name = "zaki-schema-evolution-${var.environment}"' in registry
    assert names.registry_name("prod") == "zaki-schema-evolution-prod"
    default = re.search(r'variable "schema_name" \{.*?default\s*=\s*"([^"]+)"', tf("modules/registry/variables.tf"), re.S)
    assert default and default.group(1) == names.SCHEMA_NAME
    catalog = tf("modules/catalog/main.tf")
    assert 'name = "schema_evolution_${var.environment}"' in catalog
    assert names.database_name("dev") == "schema_evolution_dev"
    assert 'name          = "zaki-schema-evolution-${var.environment}"' in catalog
    assert names.workgroup_name("dev") == "zaki-schema-evolution-dev"


def test_lambda_handler_path_exists():
    assert 'handler          = "schema_evolution.consumer.handler"' in tf("modules/streaming/main.tf")
    assert callable(consumer.handler)


def test_lambda_environment_variables_match_what_the_consumer_reads():
    streaming = tf("modules/streaming/main.tf")
    block = streaming[streaming.index("variables = {") :]
    block = block[: block.index("}")]
    declared = set(re.findall(r"^\s+([A-Z][A-Z_]+)\s*=", block, re.M))
    used = {
        consumer.ENV_ENVIRONMENT,
        consumer.ENV_BUCKET,
        consumer.ENV_DLQ_URL,
        consumer.ENV_REGISTRY,
        consumer.ENV_READER_VERSION,
    }
    assert declared == used


def test_catalog_columns_match_the_curated_rows():
    catalog = tf("modules/catalog/main.tf")
    declared = re.findall(r'\{ name = "(\w+)", type = "(\w+)" \}', catalog)
    assert declared == list(pipeline.CURATED_COLUMNS)


def test_row_builder_produces_exactly_the_curated_columns():
    row = pipeline.build_row(
        {"event_id": "e", "device_id": "d", "event_time": 1_790_000_000_000, "temperature_c": 1.0, "humidity_pct": 2.0},
        1,
        1,
        pipeline.RawRecord("1", 0, "k", b""),
        "now",
    )
    assert list(row) == [name for name, _ in pipeline.CURATED_COLUMNS]


def test_environments_are_wired_the_same_way_and_prod_streaming_is_off():
    dev, prod = tf("envs/dev/main.tf"), tf("envs/prod/main.tf")
    assert 'environment = "dev"' in dev and 'environment = "prod"' in prod
    assert "enable_streaming = true" in dev
    assert "enable_streaming = false" in prod
    for text in (dev, prod):
        for module in MODULES:
            assert f'source' in text and f"../../modules/{module}" in text
        assert "sensor_reading_v1.avsc" in text
    assert (ROOT / "src/schema_evolution/schemas/sensor_reading_v1.avsc").exists()


def test_consumer_reader_version_in_each_environment_has_a_schema_file():
    for env in ("dev", "prod"):
        match = re.search(r'consumer_reader_schema_version\s*=\s*"(\d+)"', tf(f"envs/{env}/main.tf"))
        assert match, env
        assert pipeline.schema_file(match.group(1)).exists()


def test_each_environment_has_a_backend_with_its_own_state_key():
    keys = []
    for env in ("dev", "prod"):
        text = tf(f"envs/{env}/backend.tf")
        keys.append(re.search(r'key\s*=\s*"([^"]+)"', text).group(1))
        assert "use_lockfile = true" in text
        assert "bucket" not in text  # the bucket name comes from backend.hcl or the CI variable
    assert keys[0] != keys[1]


def test_ci_role_covers_every_service_the_modules_create():
    needed = {
        "aws_s3_": "s3:",
        "aws_glue_": "glue:",
        "aws_athena_": "athena:",
        "aws_kinesis_": "kinesis:",
        "aws_lambda_": "lambda:",
        "aws_sqs_": "sqs:",
        "aws_cloudwatch_log_group": "logs:",
        "aws_cloudwatch_metric_alarm": "cloudwatch:",
        "aws_iam_": "iam:",
    }
    bootstrap = tf("bootstrap/main.tf")
    resource_types = set(re.findall(r'^resource "(aws_\w+)"', all_module_text(), re.M))
    assert resource_types
    for resource_type in resource_types:
        prefix = next((action for key, action in needed.items() if resource_type.startswith(key)), None)
        assert prefix, f"{resource_type} has no entry in this test: add the permission to the CI role, then here"
        assert f'"{prefix}' in bootstrap, f"CI role has no {prefix} permission for {resource_type}"


def test_ci_role_cannot_touch_the_state_bucket_or_itself_through_wildcards():
    bootstrap = tf("bootstrap/main.tf")
    assert 'arn:aws:s3:::${local.project}-dev-*' in bootstrap
    assert 'role/${local.project}-dev-*' in bootstrap
    assert "-github-actions-deploy" in bootstrap
    # tfstate and the CI role name must not start with <project>-dev- or <project>-prod-
    assert not "zaki-schema-evolution-tfstate".startswith("zaki-schema-evolution-dev-")
    assert not "zaki-schema-evolution-github-actions-deploy".startswith("zaki-schema-evolution-prod-")


def test_lambda_role_permissions_match_the_calls_the_code_makes():
    streaming = tf("modules/streaming/main.tf")
    for needed in ("s3:PutObject", "sqs:SendMessage", "glue:GetSchemaVersion", "kinesis:GetRecords"):
        assert f'"{needed}"' in streaming, needed
    source = (ROOT / "src/schema_evolution/consumer.py").read_text()
    assert "put_object" in source and "send_message_batch" in source
    assert "get_schema_version" in (ROOT / "src/schema_evolution/registry.py").read_text()


def test_terraform_workflow_reruns_when_the_lambda_source_changes():
    workflow = (ROOT / ".github/workflows/terraform.yml").read_text()
    assert workflow.count('"src/**"') == 2  # pull_request and push


def test_lambda_package_contains_the_schema_files():
    source_dir = ROOT / "src"
    assert (source_dir / "schema_evolution" / "consumer.py").exists()
    for version in ("1", "2"):
        assert pipeline.schema_file(version).exists()
