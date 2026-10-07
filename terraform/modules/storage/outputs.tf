output "events_bucket_name" {
  description = "Bucket holding raw, curated and Athena result files"
  value       = aws_s3_bucket.events.id
}

output "events_bucket_arn" {
  description = "ARN of the events bucket"
  value       = aws_s3_bucket.events.arn
}
