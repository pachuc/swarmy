terraform {
  required_version = ">= 1.8.0"
  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "1.70.1"
    }
    minio = {
      source  = "aminueza/minio"
      version = "3.45.0"
    }
  }
}

# Credentials come only from HCLOUD_TOKEN, MINIO_USER and MINIO_PASSWORD.
# No secret is a variable, so none reaches tfvars, the plan or state.
provider "hcloud" {}

# Configure a valid endpoint even for groups that do not create a bucket.
provider "minio" {
  minio_server        = "${local.provider_region}.your-objectstorage.com"
  minio_region        = local.provider_region
  minio_ssl           = true
  s3_compat_mode      = true
  skip_bucket_tagging = true
}
