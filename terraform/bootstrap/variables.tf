variable "github_owner" {
  description = "GitHub account that owns the repository"
  type        = string
  default     = "zakihussainim"
}

variable "github_owner_id" {
  description = "Numeric ID of that GitHub account (shown by https://api.github.com/users/<owner>)"
  type        = string
  default     = "290070594"
}

variable "github_repo_id" {
  description = "Numeric ID of the repository (shown by https://api.github.com/repos/<owner>/zaki-schema-evolution). Set it in terraform.tfvars, which is not committed."
  type        = string
}

variable "create_oidc_provider" {
  description = "Create the GitHub OIDC provider. Leave false when another project in this AWS account already created it (there can only be one per account)."
  type        = bool
  default     = false
}
