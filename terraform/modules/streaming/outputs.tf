output "stream_name" {
  description = "Kinesis stream the producer writes to"
  value       = aws_kinesis_stream.events.name
}

output "dlq_url" {
  description = "Dead-letter queue URL"
  value       = aws_sqs_queue.dlq.url
}

output "consumer_function_name" {
  description = "Lambda consumer"
  value       = aws_lambda_function.consumer.function_name
}

output "consumer_log_group" {
  description = "CloudWatch log group of the consumer"
  value       = aws_cloudwatch_log_group.consumer.name
}
