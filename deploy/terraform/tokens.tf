# OPT-IN token management. Creating a Cloudflare token via Terraform has real
# tradeoffs (see README "Managing tokens") — so this is disabled by default and
# self-contained in one file. Set `manage_ci_purge_token = true` only after you
# accept both of:
#   1. the token Terraform runs with must gain Account > Account API Tokens >
#      Edit — i.e. it can now mint arbitrary tokens (a privilege escalation), and
#   2. the created token's secret is written to terraform state in plaintext
#      (local state = a live credential on disk).
#
# Scope of what this manages: the narrow CI cache-purge token that the ETL's
# purge-on-publish uses (GitHub Actions secret CLOUDFLARE_API_TOKEN). It is NOT
# the terraform-admin token (Terraform can't create the token it authenticates
# with) and NOT the R2/Cache-Rules token you run plan/apply with.

variable "manage_ci_purge_token" {
  type        = bool
  default     = false
  description = "Opt-in: create the CI cache-purge account token. Read the README's 'Managing tokens' tradeoffs first."
}

# Resolve the global "Cache Purge" permission group id (no magic GUID).
data "cloudflare_account_api_token_permission_groups_list" "cache_purge" {
  count      = var.manage_ci_purge_token ? 1 : 0
  account_id = var.cloudflare_account_id
  name       = "Cache Purge"
}

# Account-owned (not user-owned) so it survives any individual leaving — the
# right shape for a durable CI credential. Narrow: Cache Purge on the one zone.
resource "cloudflare_account_token" "ci_purge" {
  count      = var.manage_ci_purge_token ? 1 : 0
  account_id = var.cloudflare_account_id
  name       = "spicy-regs-ci-cache-purge"

  policies = [{
    effect            = "allow"
    permission_groups = [{ id = data.cloudflare_account_api_token_permission_groups_list.cache_purge[0].result[0].id }]
    resources = jsonencode({
      "com.cloudflare.api.account.zone.${var.cloudflare_zone_id}" = "*"
    })
  }]
}

# Retrieve with `terraform output -raw ci_purge_token_value`, then set it as the
# GitHub Actions secret CLOUDFLARE_API_TOKEN. Rotating = taint + apply (a NEW
# secret each time — Cloudflare only reveals it at creation).
output "ci_purge_token_value" {
  value       = var.manage_ci_purge_token ? cloudflare_account_token.ci_purge[0].value : null
  sensitive   = true
  description = "Secret for the CI cache-purge token. Put it in the GitHub secret CLOUDFLARE_API_TOKEN."
}
