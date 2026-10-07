locals {
  # Mirrored in src/schema_evolution/pipeline.py (tests/test_contracts.py fails if they drift).
  curated_columns = [
    { name = "event_id", type = "string" },
    { name = "device_id", type = "string" },
    { name = "event_time", type = "string" },
    { name = "temperature_c", type = "double" },
    { name = "humidity_pct", type = "double" },
    { name = "battery_pct", type = "double" },
    { name = "firmware", type = "string" },
    { name = "writer_schema_version", type = "int" },
    { name = "reader_schema_version", type = "int" },
    { name = "sequence_number", type = "string" },
    { name = "ingested_at", type = "string" },
  ]
}

resource "aws_glue_catalog_database" "events" {
  name = "schema_evolution_${var.environment}"
}

# JSON lines under curated/dt=YYYY-MM-DD/. Partition projection means Athena finds new days by itself,
# so no crawler or MSCK REPAIR is needed.
resource "aws_glue_catalog_table" "sensor_readings" {
  name          = "sensor_readings"
  database_name = aws_glue_catalog_database.events.name
  table_type    = "EXTERNAL_TABLE"

  parameters = {
    EXTERNAL                      = "TRUE"
    classification                = "json"
    "projection.enabled"          = "true"
    "projection.dt.type"          = "date"
    "projection.dt.format"        = "yyyy-MM-dd"
    "projection.dt.range"         = "2026-01-01,NOW"
    "projection.dt.interval"      = "1"
    "projection.dt.interval.unit" = "DAYS"
    "storage.location.template"   = "s3://${var.events_bucket_name}/curated/dt=$${dt}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${var.events_bucket_name}/curated/"
    input_format  = "org.apache.hadoop.mapred.TextInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"

    ser_de_info {
      serialization_library = "org.openx.data.jsonserde.JsonSerDe"
    }

    dynamic "columns" {
      for_each = local.curated_columns

      content {
        name = columns.value.name
        type = columns.value.type
      }
    }
  }
}

resource "aws_athena_workgroup" "events" {
  name          = "zaki-schema-evolution-${var.environment}"
  force_destroy = true

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = false
    bytes_scanned_cutoff_per_query     = 1073741824

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${var.events_bucket_name}/athena-results/"

      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}
