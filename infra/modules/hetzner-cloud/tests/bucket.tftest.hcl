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

run "bucket_when_named" {
  variables { bucket_name = "fleet-data-7f3a" }
  assert {
    condition     = length(minio_s3_bucket.this) == 1 && minio_s3_bucket.this[0].bucket == "fleet-data-7f3a" && minio_s3_bucket.this[0].acl == "private" && minio_s3_bucket.this[0].force_destroy == false
    error_message = "The bucket must be private and retained by default."
  }
  assert {
    condition     = output.bucket == { endpoint = "https://fsn1.your-objectstorage.com", name = "fleet-data-7f3a", region = "fsn1" }
    error_message = "Bucket output must name endpoint, bucket and region."
  }
}
run "bucket_location_overrides_group_location" {
  variables {
    location             = "ash"
    bucket_name          = "fleet-data-7f3a"
    bucket_location      = "nbg1"
    bucket_force_destroy = true
  }
  assert {
    condition     = output.bucket.endpoint == "https://nbg1.your-objectstorage.com" && output.bucket.region == "nbg1" && minio_s3_bucket.this[0].force_destroy == true && hcloud_network_subnet.this[0].network_zone == "us-east"
    error_message = "Bucket and machine locations must remain independent."
  }
}
run "bucket_needs_an_object_storage_location" {
  command = plan
  variables {
    location    = "ash"
    bucket_name = "fleet-data-7f3a"
  }
  expect_failures = [minio_s3_bucket.this]
}
run "no_bucket_without_name" {
  assert {
    condition     = length(minio_s3_bucket.this) == 0 && output.bucket == null
    error_message = "No name must yield no bucket."
  }
}
