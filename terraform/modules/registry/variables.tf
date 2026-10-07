variable "environment" {
  description = "Deployment environment, for example dev or prod"
  type        = string
}

variable "schema_definition" {
  description = "Avro definition of version 1 of the schema. Later versions are registered with the registry command-line tool, not Terraform."
  type        = string
}

variable "schema_name" {
  description = "Name of the schema inside the registry"
  type        = string
  default     = "sensor-reading"
}
