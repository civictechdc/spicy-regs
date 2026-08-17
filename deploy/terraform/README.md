# R2 infrastructure (Terraform)

Codifies the public data corpus's Cloudflare setup: the R2 bucket, its public
custom domain (`data.spicy-regs.dev`), CORS for browser reads, the Iceberg data
catalog, and the edge cache rule.

## Import-first — this adopts EXISTING production resources

All of these already exist in production. This config is **not** a from-scratch
provisioner; running a naive `apply` against a fresh state would try to *create*
them and collide. The correct workflow adopts the live resources into Terraform
state, then verifies the code matches reality:

```bash
cd deploy/terraform
cp terraform.tfvars.example terraform.tfvars   # fill in account_id + zone_id
export CLOUDFLARE_API_TOKEN=...                 # account R2 edit + zone Cache Rules edit

terraform init
# Uncomment the blocks in imports.tf, filling in the account/zone/ruleset ids, then:
terraform plan     # should show "will import" + NO resource changes
terraform apply    # adopts them into state; no-op on the actual infra
```

A clean plan after import (imports only, zero changes) is the proof the code
matches production. Once imported, delete or leave the `imports.tf` blocks — they
are no-ops thereafter.

Finding the import ids: `<ACCOUNT_ID>` and `<ZONE_ID>` are in `terraform.tfvars`;
the cache `<RULESET_ID>` is the id of the existing `http_request_cache_settings`
ruleset (`GET /zones/{zone}/rulesets/phases/http_request_cache_settings/entrypoint`).

## What it manages

- `cloudflare_r2_bucket.corpus` — the `spicy-regs` bucket.
- `cloudflare_r2_custom_domain.data` — public read at `data.spicy-regs.dev`.
- `cloudflare_r2_bucket_cors.corpus` — GET/HEAD from the app origins (DuckDB-WASM).
- `cloudflare_r2_data_catalog.corpus` — the Iceberg catalog (comments system of record).
- `cloudflare_ruleset.r2_cache` — the edge cache rule (respect-origin; the ETL's
  purge-on-publish handles invalidation). Keep in sync with `sources/r2.py` headers.

## Guardrails

- `terraform.tfvars` and all `*.tfvars` are gitignored (they hold ids); only
  `*.tfvars.example` is committed. The API token comes from the environment.
- Validated with `terraform validate` against the Cloudflare v5 provider. `plan`
  and `apply` need a token and are intentionally left to a human — this repo has
  never had credentials, by design.
