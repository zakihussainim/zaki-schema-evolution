locals {
  environment = "dev"

  # Flip to false and merge to destroy the stream, the consumer and the alarms (they cost money while they
  # exist: one Kinesis shard is roughly 11 pounds a month). Buckets, the registry and the catalog are free when idle.
  enable_streaming = false

  # Which schema version the consumer shapes records to. Change "1" to "2" and merge to upgrade the consumer.
  consumer_reader_schema_version = "2"
}

module "storage" {
  source      = "../../modules/storage"
  environment = local.environment
}

module "registry" {
  source            = "../../modules/registry"
  environment       = local.environment
  schema_definition = file("${path.module}/../../../src/schema_evolution/schemas/sensor_reading_v1.avsc")
}

module "catalog" {
  source             = "../../modules/catalog"
  environment        = local.environment
  events_bucket_name = module.storage.events_bucket_name
}

module "streaming" {
  count                 = local.enable_streaming ? 1 : 0
  source                = "../../modules/streaming"
  environment           = local.environment
  events_bucket_name    = module.storage.events_bucket_name
  events_bucket_arn     = module.storage.events_bucket_arn
  registry_name         = module.registry.registry_name
  registry_arn          = module.registry.registry_arn
  schema_arn            = module.registry.schema_arn
  reader_schema_version = local.consumer_reader_schema_version
  lambda_source_dir     = "${path.module}/../../../src"
}
