terraform {
  required_version = ">= 1.9"
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 5"
    }
  }
}

# CLOUDFLARE_API_TOKEN in the environment. Needs account-level R2 edit + zone
# Cache Rules edit on spicy-regs.dev (broader than the cache-purge token used by
# the ETL). See README for the import-first workflow — these resources already
# exist in production, so this config ADOPTS them; a clean `plan` shows no change.
provider "cloudflare" {}

# The R2 bucket that holds the public corpus (parquet + the Iceberg mirror).
resource "cloudflare_r2_bucket" "corpus" {
  account_id = var.cloudflare_account_id
  name       = var.bucket_name
  location   = var.bucket_location
}

# NOTE — the public custom domain (data.spicy-regs.dev) and the bucket CORS config
# also exist in production, but the Cloudflare provider does NOT support
# `terraform import` for `cloudflare_r2_custom_domain` or `cloudflare_r2_bucket_cors`
# (confirmed against the provider docs). Adopting the existing ones into state is
# therefore impossible, so they stay dashboard/API-managed here. For a FRESH
# environment, `fresh-environment.tf.example` has Terraform create them.

# R2 Data Catalog (Apache Iceberg) on the bucket — the system of record for the
# comments table (R2_CATALOG_*). Enabling it is idempotent; the REST endpoint and
# credentials are managed in the Cloudflare dashboard / via R2 API tokens.
resource "cloudflare_r2_data_catalog" "corpus" {
  account_id  = var.cloudflare_account_id
  bucket_name = cloudflare_r2_bucket.corpus.name
}

# Edge cache rule for the corpus. Codifies what is currently a hand-created rule:
# scoped to the data host only (so docs./mcp./app. subdomains are untouched),
# eligible for cache, respecting the origin Cache-Control (purge-on-publish in the
# ETL handles invalidation). Keep this in sync with sources/r2.py's headers.
resource "cloudflare_ruleset" "r2_cache" {
  zone_id = var.cloudflare_zone_id
  name    = "default"
  kind    = "zone"
  phase   = "http_request_cache_settings"

  rules = [{
    ref         = "cache_public_corpus"
    description = "Cache public data corpus at edge; respect origin Cache-Control (purge-on-publish invalidates)"
    expression  = "(http.host eq \"${var.custom_domain}\")"
    action      = "set_cache_settings"
    enabled     = true
    action_parameters = {
      cache = true
      edge_ttl = {
        mode = "respect_origin"
      }
      browser_ttl = {
        mode = "respect_origin"
      }
    }
  }]
}
