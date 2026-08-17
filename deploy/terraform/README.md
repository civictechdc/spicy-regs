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
- `cloudflare_ruleset.r2_cache` — the edge cache rule (respect-origin; the ETL's
  purge-on-publish handles invalidation). Keep in sync with `sources/r2.py` headers.

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

## Managing tokens (opt-in, off by default)

`tokens.tf` can create the **CI cache-purge token** (the one the ETL's
purge-on-publish uses, stored as the GitHub Actions secret
`CLOUDFLARE_API_TOKEN`). It is **disabled by default** — `manage_ci_purge_token`
defaults to `false`, so nothing is created unless you opt in. Read these
tradeoffs first; token creation is not like the other resources:

- **Bootstrap.** Terraform cannot create the token it authenticates with, so
  this only manages *other* service tokens — never the R2/Cache-Rules token you
  run `plan`/`apply` with (that stays hand-made).
- **Privilege escalation.** To create tokens, the token Terraform runs with must
  gain **Account · Account API Tokens · Edit** — the power to mint arbitrary
  tokens. Add that permission deliberately; it's a big step up from the scoped
  R2 + Cache Rules token.
- **Secret in state.** The new token's value is a sensitive computed attribute
  written to `terraform.tfstate`. With **local state that's a live credential in
  a plaintext file on disk** — treat state as a secret, or move to an encrypted
  remote backend before enabling this.
- **No import / rotation = new secret.** Cloudflare reveals a token's secret only
  at creation, so an existing token can't be imported with a usable value.
  Enabling this creates a *fresh* token; you then migrate CI to it and revoke the
  old one. Rotating later (`terraform taint` + `apply`) likewise mints a new secret.

Given all that — especially on **local state** — this is genuinely optional and
arguably not worth it until there's an encrypted remote backend. It's here so the
option is codified and reviewed, not because you must use it.

### If you do enable it

```bash
# 1. Grant the terraform token Account > Account API Tokens > Edit.
# 2. Opt in and apply:
terraform apply -var manage_ci_purge_token=true
# 3. Read the secret and set it as the GitHub Actions secret:
terraform output -raw ci_purge_token_value | gh secret set CLOUDFLARE_API_TOKEN
# 4. Revoke the old hand-made purge token in the dashboard.
```
