# Mock providers: no cloud calls. IDs are numeric strings for integer arguments.
mock_provider "hcloud" {
  mock_resource "hcloud_ssh_key" { defaults = { id = "5005" } }
  mock_resource "hcloud_firewall" { defaults = { id = "3003" } }
  mock_resource "hcloud_network" { defaults = { id = "1001" } }
  mock_resource "hcloud_network_subnet" { defaults = { id = "1001-10.0.0.0/20" } }
  mock_resource "hcloud_server" {
    defaults = {
      id           = "2002"
      ipv4_address = "203.0.113.10"
      ipv6_address = "2001:db8::1"
    }
  }
  mock_resource "hcloud_server_network" {
    defaults = { id = "2002-1001", ip = "10.0.0.2" }
  }
}
mock_provider "minio" {}
variables {
  swarm          = "fleet"
  group          = "workers"
  machine_count  = 2
  size           = "cx23"
  location       = "fsn1"
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
run "rejects_bucket_location_without_object_storage" {
  command = plan
  variables { bucket_location = "ash" }
  expect_failures = [var.bucket_location]
}
run "rejects_bad_ssh_key" {
  command = plan
  variables { ssh_public_key = "not a key" }
  expect_failures = [var.ssh_public_key]
}
