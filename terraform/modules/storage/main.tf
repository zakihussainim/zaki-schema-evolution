data "aws_caller_identity" "current" {}

locals {
  name_prefix = "zaki-schema-evolution-${var.environment}"
  account_id  = data.aws_caller_identity.current.account_id
}

# One bucket per environment. Prefixes inside it:
#   raw/             every message exactly as received (the replay source)
#   curated/         decoded, validated readings, partitioned by event date
#   replay-rejects/  records a replay could not process
#   athena-results/  query output (expires after 7 days)
resource "aws_s3_bucket" "events" {
  bucket        = "${local.name_prefix}-events-${local.account_id}"
  force_destroy = true
}

resource "aws_s3_bucket_versioning" "events" {
  bucket = aws_s3_bucket.events.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "events" {
  bucket = aws_s3_bucket.events.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "events" {
  bucket = aws_s3_bucket.events.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "events" {
  bucket = aws_s3_bucket.events.id

  rule {
    id     = "expire-query-results"
    status = "Enabled"

    filter {
      prefix = "athena-results/"
    }

    expiration {
      days = 7
    }
  }

  rule {
    id     = "expire-old-object-versions"
    status = "Enabled"

    filter {}

    noncurrent_version_expiration {
      noncurrent_days = 30
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }

  depends_on = [aws_s3_bucket_versioning.events]
}
