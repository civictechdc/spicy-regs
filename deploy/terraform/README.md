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

A clean plan after import ("3 to import, 0 to add/change/destroy") is the proof
the code matches production. Once imported, delete or leave the `imports.tf`
blocks — they are no-ops thereafter.

`imports.tf` already carries the concrete ids (they interpolate `account_id` /
`zone_id` from your tfvars; the cache ruleset id is hard-coded from the existing
entrypoint). The bucket id needs a third `/default` jurisdiction component — a
Cloudflare quirk.

## What it manages (importable)

- `cloudflare_r2_bucket.corpus` — the `spicy-regs` bucket.
- `cloudflare_r2_data_catalog.corpus` — the Iceberg catalog (comments system of record).
- `cloudflare_ruleset.r2_cache` — the edge cache rule, **kept `enabled = false`**.
  Edge-caching parquet corrupts DuckDB's concurrent byte-range reads (see
  `sources/r2.py`, which serves parquet `no-cache`); the resource stays so
  Terraform owns the disabled state and an `apply` can't re-enable the corruption.

## Not importable (provider limitation)

`cloudflare_r2_custom_domain` (data.spicy-regs.dev) and `cloudflare_r2_bucket_cors`
have **no `terraform import` support** in the provider. They exist in production and
stay dashboard/API-managed here; adopting them into state isn't possible. For a
**fresh** environment, rename `fresh-environment.tf.example` → `.tf` to have
Terraform create them. Don't apply it against the existing bucket — it would try
to re-create/overwrite live config.

## Guardrails

- `terraform.tfvars` and all `*.tfvars` are gitignored (they hold ids); only
  `*.tfvars.example` is committed. The API token comes from the environment.
- Validated with `terraform validate` against the Cloudflare v5 provider. `plan`
  and `apply` need a token and are intentionally left to a human — this repo has
  never had credentials, by design.

## Remote state in R2

State is stored in Cloudflare R2 via the Terraform `s3` backend (`backend.tf`) —
in a **separate, private** bucket (`spicy-regs-tfstate`), never the public data
bucket. R2 credentials come from the environment, never the committed config.

### One-time setup / migrating from local state

```bash
# 1. Create the PRIVATE state bucket (do NOT give it a public domain).
npx wrangler r2 bucket create spicy-regs-tfstate

# 2. Point the s3 backend at R2 using your R2 S3 API credentials
#    (the same access-key/secret the ETL uses; from .env).
export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"

# 3. Re-init; Terraform detects the new backend and offers to copy local state up.
cd deploy/terraform
terraform init -migrate-state    # answer "yes" to copy terraform.tfstate to R2

# 4. Confirm, then the local state files are no longer authoritative.
terraform plan                   # "No changes" — now reading state from R2
rm -f terraform.tfstate terraform.tfstate.backup
```

Thereafter every `plan`/`apply` reads and locks state in R2 (the `AWS_*` env vars
must be set). `use_lockfile` puts a lock object in the bucket for the duration of
a run, so two people can't apply at once.

Notes:
- The state bucket **must stay private**. State can contain sensitive attributes;
  a public state bucket would leak them.
- The `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY` names are just how the s3 backend
  reads credentials — the values are your **R2** access key + secret, not AWS.
- `*.tfstate*` is gitignored regardless, so state never lands in git even locally.
