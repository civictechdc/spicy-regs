# Import-first: these resources ALREADY EXIST in production. Uncomment the block
# for each and run `terraform plan` — Terraform adopts the live resource into
# state instead of trying to create (and colliding with) it. A clean plan after
# import (no changes) is the proof this config matches reality. Remove/keep these
# blocks after the first successful apply; they are no-ops once in state.
#
# Import IDs follow the Cloudflare provider's documented format for each resource
# (see registry.terraform.io/providers/cloudflare/cloudflare/latest/docs). Fill in
# <ACCOUNT_ID> / <ZONE_ID> from terraform.tfvars, and the ruleset id from
# `deploy/terraform` outputs of the existing rule (already created 2026-08-15).

# import {
#   to = cloudflare_r2_bucket.corpus
#   id = "<ACCOUNT_ID>/spicy-regs"
# }

# import {
#   to = cloudflare_r2_custom_domain.data
#   id = "<ACCOUNT_ID>/spicy-regs/data.spicy-regs.dev"
# }

# import {
#   to = cloudflare_r2_bucket_cors.corpus
#   id = "<ACCOUNT_ID>/spicy-regs"
# }

# import {
#   to = cloudflare_r2_data_catalog.corpus
#   id = "<ACCOUNT_ID>/spicy-regs"
# }

# import {
#   to = cloudflare_ruleset.r2_cache
#   id = "zones/<ZONE_ID>/<RULESET_ID>"
# }
