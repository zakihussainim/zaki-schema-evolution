locals {
  registry_name = "zaki-schema-evolution-${var.environment}"
}

resource "aws_glue_registry" "this" {
  registry_name = local.registry_name
  description   = "Schemas for the ${var.environment} sensor event stream"
}

# FULL compatibility: every new version must be readable by consumers on the old schema (FORWARD)
# AND able to read data written with the old schema (BACKWARD). Adding an optional field passes;
# renaming a field or changing its type is rejected by the registry.
resource "aws_glue_schema" "sensor_reading" {
  schema_name       = var.schema_name
  registry_arn      = aws_glue_registry.this.arn
  data_format       = "AVRO"
  compatibility     = "FULL"
  schema_definition = var.schema_definition
  description       = "One reading from an environmental sensor"

  # Versions after the first are registered with: python -m schema_evolution.registry register
  # Without this, Terraform would see the newer registered version as drift and try to revert it.
  lifecycle {
    ignore_changes = [schema_definition]
  }
}
