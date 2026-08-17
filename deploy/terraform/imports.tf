# Import-first: adopt the EXISTING production resources into state. Only these
# three support `terraform import` (the custom domain + CORS do not — see the note
# in main.tf). Fill the ids from terraform.tfvars, then `terraform plan` (expect
# "3 to import, 0 to change") and `terraform apply`. Delete or keep these blocks
# afterward — they are no-ops once the resources are in state.

# R2 bucket. NB the id has THREE parts: <account_id>/<bucket>/<jurisdiction>.
# "default" is the standard (non-jurisdiction-restricted) location.
import {
  to = cloudflare_r2_bucket.corpus
  id = "${var.cloudflare_account_id}/${var.bucket_name}/default"
}

# Iceberg data catalog: <account_id>/<bucket_name>.
import {
  to = cloudflare_r2_data_catalog.corpus
  id = "${var.cloudflare_account_id}/${var.bucket_name}"
}

# Cache-settings ruleset: zones/<zone_id>/<ruleset_id>. The ruleset id is the
# existing http_request_cache_settings entrypoint (created 2026-08-15):
# 63af84667b3e4f3da0ecafc1094d51a2 — confirm with
#   GET /zones/{zone}/rulesets/phases/http_request_cache_settings/entrypoint
import {
  to = cloudflare_ruleset.r2_cache
  id = "zones/${var.cloudflare_zone_id}/63af84667b3e4f3da0ecafc1094d51a2"
}
