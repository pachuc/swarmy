# Mock provider: no AWS calls. Data sources get fixed values so the image
# lookup and joined-subnet paths are deterministic.
mock_provider "aws" {
  mock_resource "aws_vpc" {
    defaults = { id = "vpc-mock" }
  }
  mock_resource "aws_subnet" {
    defaults = { id = "subnet-mock" }
  }
  mock_resource "aws_internet_gateway" {
    defaults = { id = "igw-mock" }
  }
  mock_resource "aws_route_table" {
    defaults = { id = "rtb-mock" }
  }
  mock_resource "aws_security_group" {
    defaults = { id = "sg-mock" }
  }
  mock_resource "aws_instance" {
    defaults = {
      id         = "i-mock"
      public_ip  = "203.0.113.10"
      private_ip = "10.0.0.10"
    }
  }
  mock_data "aws_ec2_instance_type" {
    defaults = { supported_architectures = ["x86_64"] }
  }
  mock_data "aws_ssm_parameter" {
    defaults = { insecure_value = "ami-0123456789abcdef0" }
  }
  mock_data "aws_subnet" {
    defaults = {
      cidr_block = "10.1.0.0/20"
      vpc_id     = "vpc-shared"
    }
  }
}

variables {
  swarm          = "fleet"
  group          = "workers"
  machine_count  = 2
  size           = "m6i.large"
  location       = "eu-central-1"
  ssh_public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBswarmyTestKeyNotARealKey0000000000000 swarmy-test"
}

run "bucket_when_named" {
  variables { bucket_name = "fleet-data-7f3a" }
  assert {
    condition     = length(aws_s3_bucket.this) == 1 && aws_s3_bucket.this[0].bucket == "fleet-data-7f3a" && aws_s3_bucket.this[0].force_destroy == false && aws_s3_bucket.this[0].region == "eu-central-1"
    error_message = "Create the bucket in the group's region."
  }
  assert {
    condition     = aws_s3_bucket_public_access_block.this[0].block_public_acls && aws_s3_bucket_public_access_block.this[0].block_public_policy && aws_s3_bucket_public_access_block.this[0].ignore_public_acls && aws_s3_bucket_public_access_block.this[0].restrict_public_buckets
    error_message = "Block all public access."
  }
  assert {
    condition     = output.bucket == { endpoint = "https://s3.eu-central-1.amazonaws.com", name = "fleet-data-7f3a", region = "eu-central-1" }
    error_message = "Output bucket endpoint and region."
  }
}
run "bucket_location_overrides_group_location" {
  variables {
    location             = "us-east-1"
    bucket_name          = "fleet-data-7f3a"
    bucket_location      = "eu-west-1"
    bucket_force_destroy = true
  }
  assert {
    condition     = aws_s3_bucket.this[0].region == "eu-west-1" && output.bucket.endpoint == "https://s3.eu-west-1.amazonaws.com" && output.bucket.region == "eu-west-1"
    error_message = "Bucket location chooses bucket region."
  }
  assert {
    condition     = aws_s3_bucket.this[0].force_destroy == true
    error_message = "Force destroy reaches bucket."
  }
}
run "no_bucket_without_name" {
  assert {
    condition     = length(aws_s3_bucket.this) == 0 && length(aws_s3_bucket_public_access_block.this) == 0 && output.bucket == null
    error_message = "Empty bucket_name creates no bucket."
  }
}
