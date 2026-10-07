output "state_bucket_name" {
  description = "Put this in the GitHub Variable TF_STATE_BUCKET and in each environment's backend.hcl"
  value       = aws_s3_bucket.tf_state.id
}

output "github_actions_role_arn" {
  description = "Put this in the GitHub Variable DEPLOY_ROLE_ARN"
  value       = aws_iam_role.github_actions_deploy.arn
}
