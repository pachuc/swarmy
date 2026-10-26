terraform {
  required_version = ">= 1.8.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "6.70.0"
    }
  }
}

# Credentials come only from the environment of each run: AWS_ACCESS_KEY_ID
# and AWS_SECRET_ACCESS_KEY (plus AWS_SESSION_TOKEN for temporary keys). No
# secret is a variable, so none reaches a variables file, the plan, or the
# state. The region is the group's location, so a default region in the
# environment never moves a group.
provider "aws" {
  region = var.location
}
