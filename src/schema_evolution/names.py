"""Names shared by the code and the Terraform (tests/test_contracts.py keeps them in step)."""

PROJECT = "zaki-schema-evolution"
REGION = "eu-west-2"
SCHEMA_NAME = "sensor-reading"
ENVIRONMENTS = ("dev", "prod")


def prefix(env):
    return f"{PROJECT}-{env}"


def registry_name(env):
    return prefix(env)


def stream_name(env):
    return f"{prefix(env)}-events"


def queue_name(env):
    return f"{prefix(env)}-dlq"


def function_name(env):
    return f"{prefix(env)}-consumer"


def bucket_name(env, account_id):
    return f"{prefix(env)}-events-{account_id}"


def workgroup_name(env):
    return prefix(env)


def database_name(env):
    return f"schema_evolution_{env}"


def account_id():
    """The AWS account of the current login. Read at run time so no account number is stored in the repo."""
    import boto3

    return boto3.client("sts", region_name=REGION).get_caller_identity()["Account"]
