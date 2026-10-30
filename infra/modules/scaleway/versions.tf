terraform {
  required_version = ">= 1.8.0"
  required_providers {
    scaleway = {
      source  = "scaleway/scaleway"
      version = "2.89.0"
    }
  }
}

# Credentials come only from SCW_ACCESS_KEY, SCW_SECRET_KEY and
# SCW_DEFAULT_PROJECT_ID in the environment of each run. No secret is a
# variable, so none reaches tfvars, the plan or state. The zone and region
# always come from location, so a default zone in the environment never moves
# a group.
provider "scaleway" {
  zone   = var.location
  region = local.region
}
