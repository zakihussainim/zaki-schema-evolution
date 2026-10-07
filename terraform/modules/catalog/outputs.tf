output "database_name" {
  description = "Glue database holding the curated table"
  value       = aws_glue_catalog_database.events.name
}

output "table_name" {
  description = "Curated sensor readings table"
  value       = aws_glue_catalog_table.sensor_readings.name
}

output "athena_workgroup_name" {
  description = "Athena workgroup to run queries in"
  value       = aws_athena_workgroup.events.name
}
