output "registry_name" {
  description = "Name of the Glue schema registry"
  value       = aws_glue_registry.this.registry_name
}

output "registry_arn" {
  description = "ARN of the Glue schema registry"
  value       = aws_glue_registry.this.arn
}

output "schema_name" {
  description = "Name of the sensor reading schema"
  value       = aws_glue_schema.sensor_reading.schema_name
}

output "schema_arn" {
  description = "ARN of the sensor reading schema"
  value       = aws_glue_schema.sensor_reading.arn
}
