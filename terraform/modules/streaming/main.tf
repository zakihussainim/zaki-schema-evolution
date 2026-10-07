data "aws_region" "current" {}

locals {
  prefix        = "zaki-schema-evolution-${var.environment}"
  function_name = "${local.prefix}-consumer"
}

# ---------------------------------------------------------------------------
# Stream and dead-letter queue
# ---------------------------------------------------------------------------

# One provisioned shard (1 MB/s in, 2 MB/s out) is plenty for the demo and is cheaper than on-demand mode,
# which has a higher hourly floor.
resource "aws_kinesis_stream" "events" {
  name             = "${local.prefix}-events"
  shard_count      = 1
  retention_period = 24

  stream_mode_details {
    stream_mode = "PROVISIONED"
  }
}

# Receives (1) records the consumer cannot use, with the reason, and (2) whole batches Lambda gave up on.
resource "aws_sqs_queue" "dlq" {
  name                      = "${local.prefix}-dlq"
  message_retention_seconds = 1209600
  sqs_managed_sse_enabled   = true
}

# ---------------------------------------------------------------------------
# Lambda consumer
# ---------------------------------------------------------------------------

data "archive_file" "consumer" {
  type        = "zip"
  source_dir  = var.lambda_source_dir
  output_path = "${path.module}/consumer.zip"
  excludes    = ["**/__pycache__/**", "**/*.pyc"]
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

data "aws_iam_policy_document" "consumer_assume_role" {
  statement {
    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "consumer" {
  name               = local.function_name
  assume_role_policy = data.aws_iam_policy_document.consumer_assume_role.json
}

data "aws_iam_policy_document" "consumer" {
  statement {
    sid       = "WriteLogs"
    actions   = ["logs:CreateLogStream", "logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.consumer.arn}:*"]
  }

  statement {
    sid = "ReadStream"
    actions = [
      "kinesis:DescribeStream",
      "kinesis:DescribeStreamSummary",
      "kinesis:GetRecords",
      "kinesis:GetShardIterator",
      "kinesis:ListShards",
    ]
    resources = [aws_kinesis_stream.events.arn]
  }

  statement {
    sid       = "ListStreams"
    actions   = ["kinesis:ListStreams"]
    resources = ["*"]
  }

  statement {
    sid     = "WriteArchiveAndCuratedFiles"
    actions = ["s3:PutObject"]
    resources = [
      "${var.events_bucket_arn}/raw/*",
      "${var.events_bucket_arn}/curated/*",
    ]
  }

  statement {
    sid       = "SendToDeadLetterQueue"
    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.dlq.arn]
  }

  statement {
    sid       = "ReadSchemaVersions"
    actions   = ["glue:GetSchemaVersion"]
    resources = [var.registry_arn, var.schema_arn]
  }
}

resource "aws_iam_role_policy" "consumer" {
  name   = "${local.function_name}-permissions"
  role   = aws_iam_role.consumer.id
  policy = data.aws_iam_policy_document.consumer.json
}

resource "aws_lambda_function" "consumer" {
  function_name    = local.function_name
  role             = aws_iam_role.consumer.arn
  handler          = "schema_evolution.consumer.handler"
  runtime          = "python3.12"
  architectures    = ["arm64"]
  filename         = data.archive_file.consumer.output_path
  source_code_hash = data.archive_file.consumer.output_base64sha256
  timeout          = 60
  memory_size      = 256

  environment {
    variables = {
      ENVIRONMENT           = var.environment
      EVENTS_BUCKET         = var.events_bucket_name
      DLQ_URL               = aws_sqs_queue.dlq.url
      REGISTRY_NAME         = var.registry_name
      READER_SCHEMA_VERSION = var.reader_schema_version
    }
  }

  depends_on = [
    aws_cloudwatch_log_group.consumer,
    aws_iam_role_policy.consumer,
  ]
}

resource "aws_lambda_event_source_mapping" "consumer" {
  event_source_arn                   = aws_kinesis_stream.events.arn
  function_name                      = aws_lambda_function.consumer.arn
  starting_position                  = "TRIM_HORIZON"
  batch_size                         = 100
  maximum_batching_window_in_seconds = 5
  maximum_retry_attempts             = 2
  maximum_record_age_in_seconds      = 3600
  bisect_batch_on_function_error     = true

  destination_config {
    on_failure {
      destination_arn = aws_sqs_queue.dlq.arn
    }
  }

  depends_on = [aws_iam_role_policy.consumer]
}

# ---------------------------------------------------------------------------
# Alarms (they only change colour in the console; no notification is wired up)
# ---------------------------------------------------------------------------

# The consumer is falling behind: its oldest unread record is more than a minute old.
resource "aws_cloudwatch_metric_alarm" "iterator_age" {
  alarm_name          = "${local.prefix}-consumer-lag"
  alarm_description   = "Kinesis iterator age above 60 seconds"
  namespace           = "AWS/Kinesis"
  metric_name         = "GetRecords.IteratorAgeMilliseconds"
  statistic           = "Maximum"
  period              = 60
  evaluation_periods  = 3
  threshold           = 60000
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    StreamName = aws_kinesis_stream.events.name
  }
}

resource "aws_cloudwatch_metric_alarm" "consumer_errors" {
  alarm_name          = "${local.prefix}-consumer-errors"
  alarm_description   = "The Lambda consumer threw an error"
  namespace           = "AWS/Lambda"
  metric_name         = "Errors"
  statistic           = "Sum"
  period              = 60
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    FunctionName = aws_lambda_function.consumer.function_name
  }
}

resource "aws_cloudwatch_metric_alarm" "dlq_not_empty" {
  alarm_name          = "${local.prefix}-dlq-not-empty"
  alarm_description   = "Messages are waiting in the dead-letter queue"
  namespace           = "AWS/SQS"
  metric_name         = "ApproximateNumberOfMessagesVisible"
  statistic           = "Maximum"
  period              = 300
  evaluation_periods  = 1
  threshold           = 0
  comparison_operator = "GreaterThanThreshold"
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.dlq.name
  }
}
