# Mock provider: no Scaleway calls. Ids follow Scaleway's zoned form.
mock_provider "scaleway" {
  mock_resource "scaleway_instance_security_group" {
    defaults = { id = "fr-par-2/22222222-2222-2222-2222-222222222222" }
  }
  mock_resource "scaleway_instance_ip" {
    defaults = {
      id      = "fr-par-2/11111111-1111-1111-1111-111111111111"
      address = "203.0.113.20"
    }
  }
  mock_resource "scaleway_instance_server" {
    defaults = { id = "fr-par-2/33333333-3333-3333-3333-333333333333" }
  }
}
variables {
  swarm          = "fleet"
  group          = "gpu"
  machine_count  = 2
  size           = "L4-1-24G"
  location       = "fr-par-2"
  ssh_public_key = "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIBswarmyTestKeyNotARealKey0000000000000 swarmy-test"
}
run "rejects_swarm" {
  command = plan
  variables { swarm = "Fleet_1" }
  expect_failures = [var.swarm]
}

run "rejects_group" {
  command = plan
  variables { group = "gpu-" }
  expect_failures = [var.group]
}

run "rejects_machine_count" {
  command = plan
  variables { machine_count = 51 }
  expect_failures = [var.machine_count]
}

run "rejects_location" {
  command = plan
  variables { location = "fsn1" }
  expect_failures = [var.location]
}

run "rejects_size" {
  command = plan
  variables { size = "l4-1-24g" }
  expect_failures = [var.size]
}

run "rejects_image" {
  command = plan
  variables { image = "Ubuntu 24.04" }
  expect_failures = [var.image]
}

run "rejects_ssh_public_key" {
  command = plan
  variables { ssh_public_key = "not a key" }
  expect_failures = [var.ssh_public_key]
}

run "rejects_ssh_allowed_cidrs" {
  command = plan
  variables { ssh_allowed_cidrs = ["anywhere"] }
  expect_failures = [var.ssh_allowed_cidrs]
}

run "rejects_labels" {
  command = plan
  variables { labels = { "a=b" = "x" } }
  expect_failures = [var.labels]
}

run "rejects_bucket_name" {
  command = plan
  variables { bucket_name = "fleet-data" }
  expect_failures = [var.bucket_name]
}

run "rejects_bucket_location" {
  command = plan
  variables { bucket_location = "fr-par" }
  expect_failures = [var.bucket_location]
}

run "rejects_network_id" {
  command = plan
  variables { network_id = "pn-1" }
  expect_failures = [var.network_id]
}
