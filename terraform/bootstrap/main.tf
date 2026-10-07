terraform {
  required_version = ">= 1.10.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = "eu-west-2"
}

data "aws_caller_identity" "current" {}

locals {
  account_id = data.aws_caller_identity.current.account_id
  region     = "eu-west-2"
  project    = "zaki-schema-evolution"

  # The token GitHub issues for a workflow run names the owner and repository by name AND numeric ID.
  github_subject = "repo:${var.github_owner}@${var.github_owner_id}/${local.project}@${var.github_repo_id}:*"

  oidc_provider_arn = var.create_oidc_provider ? aws_iam_openid_connect_provider.github[0].arn : data.aws_iam_openid_connect_provider.github[0].arn

  # Data buckets are named <project>-<env>-events-<account>. The pattern deliberately
  # does NOT match the state bucket (<project>-tfstate-<account>).
  env_bucket_arns = [
    "arn:aws:s3:::${local.project}-dev-*",
    "arn:aws:s3:::${local.project}-dev-*/*",
    "arn:aws:s3:::${local.project}-prod-*",
    "arn:aws:s3:::${local.project}-prod-*/*",
  ]

  # Roles created by the environments. Does NOT match the CI role itself
  # (<project>-github-actions-deploy), so CI cannot edit its own permissions.
  env_role_arns = [
    "arn:aws:iam::${local.account_id}:role/${local.project}-dev-*",
    "arn:aws:iam::${local.account_id}:role/${local.project}-prod-*",
  ]

  glue_arns = [
    "arn:aws:glue:${local.region}:${local.account_id}:catalog",
    "arn:aws:glue:${local.region}:${local.account_id}:database/default",
    "arn:aws:glue:${local.region}:${local.account_id}:database/schema_evolution_*",
    "arn:aws:glue:${local.region}:${local.account_id}:table/schema_evolution_*/*",
    "arn:aws:glue:${local.region}:${local.account_id}:userDefinedFunction/schema_evolution_*/*",
    "arn:aws:glue:${local.region}:${local.account_id}:registry/${local.project}-*",
    "arn:aws:glue:${local.region}:${local.account_id}:schema/${local.project}-*/*",
  ]

  athena_arns = [
    "arn:aws:athena:${local.region}:${local.account_id}:workgroup/${local.project}-*",
    "arn:aws:athena:${local.region}:${local.account_id}:datacatalog/AwsDataCatalog",
  ]

  log_group_arns = [
    "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.project}-*",
    "arn:aws:logs:${local.region}:${local.account_id}:log-group:/aws/lambda/${local.project}-*:*",
  ]
}

# ---------------------------------------------------------------------------
# S3 bucket that holds Terraform state
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "tf_state" {
  bucket = "${local.project}-tfstate-${local.account_id}"
}

resource "aws_s3_bucket_versioning" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "tf_state" {
  bucket = aws_s3_bucket.tf_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# GitHub Actions -> AWS (OIDC)
# ---------------------------------------------------------------------------

# There is only one GitHub OIDC provider per AWS account. If another project already created it,
# this project looks it up. Set create_oidc_provider = true in a fresh account.
data "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 0 : 1
  url   = "https://token.actions.githubusercontent.com"
}

resource "aws_iam_openid_connect_provider" "github" {
  count = var.create_oidc_provider ? 1 : 0

  url            = "https://token.actions.githubusercontent.com"
  client_id_list = ["sts.amazonaws.com"]
}

data "aws_iam_policy_document" "github_actions_assume_role" {
  statement {
    actions = ["sts:AssumeRoleWithWebIdentity"]

    principals {
      type        = "Federated"
      identifiers = [local.oidc_provider_arn]
    }

    condition {
      test     = "StringEquals"
      variable = "token.actions.githubusercontent.com:aud"
      values   = ["sts.amazonaws.com"]
    }

    condition {
      test     = "StringLike"
      variable = "token.actions.githubusercontent.com:sub"
      values   = [local.github_subject]
    }
  }
}

resource "aws_iam_role" "github_actions_deploy" {
  name               = "${local.project}-github-actions-deploy"
  assume_role_policy = data.aws_iam_policy_document.github_actions_assume_role.json
}

# ---------------------------------------------------------------------------
# What the CI role may do
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "github_actions_permissions" {
  statement {
    sid = "ReadWriteTerraformState"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:ListBucket",
    ]
    resources = [
      aws_s3_bucket.tf_state.arn,
      "${aws_s3_bucket.tf_state.arn}/*",
    ]
  }

  statement {
    sid       = "ManageEnvironmentBuckets"
    actions   = ["s3:*"]
    resources = local.env_bucket_arns
  }

  statement {
    sid       = "ManageGlueCatalogAndRegistry"
    actions   = ["glue:*"]
    resources = local.glue_arns
  }

  statement {
    sid       = "ListGlueRegistries"
    actions   = ["glue:ListRegistries", "glue:ListSchemas"]
    resources = ["*"]
  }

  statement {
    sid       = "ManageAthena"
    actions   = ["athena:*"]
    resources = local.athena_arns
  }

  statement {
    sid = "AthenaCatalogRead"
    actions = [
      "athena:GetDataCatalog",
      "athena:ListDataCatalogs",
      "athena:GetDatabase",
      "athena:ListDatabases",
      "athena:GetTableMetadata",
      "athena:ListTableMetadata",
      "athena:ListWorkGroups",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "ManageKinesisStreams"
    actions   = ["kinesis:*"]
    resources = ["arn:aws:kinesis:${local.region}:${local.account_id}:stream/${local.project}-*"]
  }

  statement {
    sid       = "ManageLambdaFunctions"
    actions   = ["lambda:*"]
    resources = ["arn:aws:lambda:${local.region}:${local.account_id}:function:${local.project}-*"]
  }

  statement {
    sid = "ManageEventSourceMappings"
    actions = [
      "lambda:CreateEventSourceMapping",
      "lambda:DeleteEventSourceMapping",
      "lambda:GetEventSourceMapping",
      "lambda:UpdateEventSourceMapping",
      "lambda:ListEventSourceMappings",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "ManageQueues"
    actions   = ["sqs:*"]
    resources = ["arn:aws:sqs:${local.region}:${local.account_id}:${local.project}-*"]
  }

  statement {
    sid       = "ManageLambdaLogGroups"
    actions   = ["logs:*"]
    resources = local.log_group_arns
  }

  statement {
    sid = "ReadLogGroups"
    actions = [
      "logs:DescribeLogGroups",
      "logs:ListTagsForResource",
      "logs:ListTagsLogGroup",
    ]
    resources = ["*"]
  }

  statement {
    sid = "ManageAlarms"
    actions = [
      "cloudwatch:PutMetricAlarm",
      "cloudwatch:DeleteAlarms",
      "cloudwatch:TagResource",
      "cloudwatch:UntagResource",
      "cloudwatch:ListTagsForResource",
    ]
    resources = ["arn:aws:cloudwatch:${local.region}:${local.account_id}:alarm:${local.project}-*"]
  }

  statement {
    sid       = "ReadAlarms"
    actions   = ["cloudwatch:DescribeAlarms"]
    resources = ["*"]
  }

  statement {
    sid = "ManageEnvironmentRoles"
    actions = [
      "iam:CreateRole",
      "iam:DeleteRole",
      "iam:GetRole",
      "iam:UpdateRole",
      "iam:UpdateAssumeRolePolicy",
      "iam:TagRole",
      "iam:UntagRole",
      "iam:ListRoleTags",
      "iam:PutRolePolicy",
      "iam:GetRolePolicy",
      "iam:DeleteRolePolicy",
      "iam:ListRolePolicies",
      "iam:ListAttachedRolePolicies",
      "iam:ListInstanceProfilesForRole",
    ]
    resources = local.env_role_arns
  }

  statement {
    sid       = "PassEnvironmentRolesToLambda"
    actions   = ["iam:PassRole"]
    resources = local.env_role_arns

    condition {
      test     = "StringEquals"
      variable = "iam:PassedToService"
      values   = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role_policy" "github_actions_permissions" {
  name   = "${local.project}-github-actions-permissions"
  role   = aws_iam_role.github_actions_deploy.id
  policy = data.aws_iam_policy_document.github_actions_permissions.json
}
