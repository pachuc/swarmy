# Mock provider: no AWS calls. Data sources get fixed values so the image
# lookup and joined-subnet paths are deterministic.
mock_provider "aws" {
  mock_resource "aws_vpc" { defaults = { id = "vpc-mock" } }
  mock_resource "aws_subnet" { defaults = { id = "subnet-mock" } }
  mock_resource "aws_internet_gateway" { defaults = { id = "igw-mock" } }
  mock_resource "aws_route_table" { defaults = { id = "rtb-mock" } }
  mock_resource "aws_security_group" { defaults = { id = "sg-mock" } }
  mock_resource "aws_instance" {
    defaults = {
      id         = "i-mock"
      public_ip  = "203.0.113.10"
      private_ip = "10.0.0.10"
    }
  }
  mock_data "aws_ec2_instance_type" { defaults = { supported_architectures = ["x86_64"] } }
  mock_data "aws_ssm_parameter" { defaults = { insecure_value = "ami-0123456789abcdef0" } }
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
run "rejects_bad_swarm_name" {
  command = plan
  variables { swarm = "Fleet_1" }
  expect_failures = [var.swarm]
}
run "rejects_group_ending_in_hyphen" {
  command = plan
  variables { group = "workers-" }
  expect_failures = [var.group]
}
run "rejects_unknown_location" {
  command = plan
  variables { location = "mars" }
  expect_failures = [var.location]
}
run "rejects_bad_size" {
  command = plan
  variables { size = "large" }
  expect_failures = [var.size]
}
run "rejects_unknown_image" {
  command = plan
  variables { image = "debian-12" }
  expect_failures = [var.image]
}
run "rejects_malformed_cidr" {
  command = plan
  variables { ssh_allowed_cidrs = ["anywhere"] }
  expect_failures = [var.ssh_allowed_cidrs]
}
run "rejects_bad_bucket_name" {
  command = plan
  variables { bucket_name = "Fleet_Data" }
  expect_failures = [var.bucket_name]
}
run "rejects_bad_bucket_location" {
  command = plan
  variables { bucket_location = "Europe" }
  expect_failures = [var.bucket_location]
}
run "rejects_ecdsa_keys" {
  command = plan
  variables { ssh_public_key = "ecdsa-sha2-nistp256 AAAAE2VjZHNh swarmy-test" }
  expect_failures = [var.ssh_public_key]
}
run "rejects_bad_ssh_key" {
  command = plan
  variables { ssh_public_key = "not a key" }
  expect_failures = [var.ssh_public_key]
}
run "rejects_bad_network_id" {
  command = plan
  variables { network_id = "vpc-shared" }
  expect_failures = [var.network_id]
}
