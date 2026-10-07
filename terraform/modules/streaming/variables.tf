variable "environment" {
  description = "Deployment environment, for example dev or prod"
  type        = string
}

variable "events_bucket_name" {
  description = "Bucket the consumer writes raw archives and curated files to"
  type        = string
}

variable "events_bucket_arn" {
  description = "ARN of that bucket"
  type        = string
}

variable "registry_name" {
  description = "Glue schema registry the consumer reads schema versions from"
  type        = string
}

variable "registry_arn" {
  description = "ARN of the schema registry"
  type        = string
}

variable "schema_arn" {
  description = "ARN of the sensor reading schema"
  type        = string
}

variable "reader_schema_version" {
  description = "Schema version the consumer shapes every record to. Change it to upgrade the consumer."
  type        = string
  default     = "1"
}

variable "lambda_source_dir" {
  description = "Folder that is zipped as the Lambda package (it must contain the schema_evolution package)"
  type        = string
}

variable "log_retention_days" {
  description = "How long the consumer's logs are kept"
  type        = number
  default     = 14
}
