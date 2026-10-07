output "events_bucket_name" {
  value = module.storage.events_bucket_name
}

output "registry_name" {
  value = module.registry.registry_name
}

output "database_name" {
  value = module.catalog.database_name
}

output "athena_workgroup_name" {
  value = module.catalog.athena_workgroup_name
}

output "stream_name" {
  value = one(module.streaming[*].stream_name)
}

output "dlq_url" {
  value = one(module.streaming[*].dlq_url)
}

output "consumer_function_name" {
  value = one(module.streaming[*].consumer_function_name)
}
