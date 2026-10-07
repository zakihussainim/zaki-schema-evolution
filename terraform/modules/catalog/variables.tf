variable "environment" {
  description = "Deployment environment, for example dev or prod"
  type        = string
}

variable "events_bucket_name" {
  description = "Bucket that holds the curated files and Athena results"
  type        = string
}
