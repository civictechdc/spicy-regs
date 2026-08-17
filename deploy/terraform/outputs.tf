output "bucket_name" {
  value       = cloudflare_r2_bucket.corpus.name
  description = "R2 bucket name."
}

output "public_url" {
  value       = "https://${var.custom_domain}"
  description = "Public base URL for the corpus (R2_PUBLIC_URL)."
}

output "cache_ruleset_id" {
  value       = cloudflare_ruleset.r2_cache.id
  description = "ID of the http_request_cache_settings ruleset."
}
