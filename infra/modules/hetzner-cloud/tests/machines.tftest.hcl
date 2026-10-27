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

run "creates_the_group" {
  assert {
    condition     = [for server in hcloud_server.this : server.name] == ["fleet-workers-1", "fleet-workers-2"]
    error_message = "Machines must have stable numbered names."
  }
  assert {
    condition     = alltrue([for server in hcloud_server.this : server.server_type == "cx23" && server.image == "ubuntu-24.04" && server.location == "fsn1"])
    error_message = "Machines must use the requested size, image and location."
  }
  assert {
    condition     = alltrue([for server in hcloud_server.this : server.labels == tomap({ "swarmy-swarm" = "fleet", "swarmy-group" = "workers", "swarmy-managed" = "true" })])
    error_message = "Machines must carry the protected labels."
  }
  assert {
    condition     = hcloud_ssh_key.this.name == "fleet-workers" && hcloud_ssh_key.this.public_key == var.ssh_public_key && alltrue([for server in hcloud_server.this : contains(server.ssh_keys, "5005")])
    error_message = "Each machine must use the group's key."
  }
  assert {
    condition     = length(hcloud_network.this) == 1 && hcloud_network.this[0].ip_range == "10.0.0.0/16" && hcloud_network_subnet.this[0].network_zone == "eu-central" && hcloud_network_subnet.this[0].type == "cloud" && hcloud_network_subnet.this[0].ip_range == "10.0.0.0/20"
    error_message = "The group must create a private cloud subnet."
  }
  assert {
    condition     = length(hcloud_server_network.this) == 2 && alltrue([for attachment in hcloud_server_network.this : attachment.subnet_id == hcloud_network_subnet.this[0].id])
    error_message = "Each machine must attach to the subnet."
  }
  assert {
    condition     = output.machines[1] == { name = "fleet-workers-2", public_ipv4 = "203.0.113.10", public_ipv6 = "2001:db8::1", private_ip = "10.0.0.2", ssh_user = "root" }
    error_message = "Machine output must contain exactly the five contract fields."
  }
  assert {
    condition     = length(output.machines) == 2 && output.network_id == "1001" && output.bucket == null && length(minio_s3_bucket.this) == 0
    error_message = "Outputs must describe machines and network, without an unwanted bucket."
  }
}
run "ssh_is_open_to_anywhere_by_default" {
  assert {
    condition     = length(hcloud_firewall.this.rule) == 1 && toset(one(hcloud_firewall.this.rule).source_ips) == toset(["0.0.0.0/0", "::/0"])
    error_message = "SSH defaults to every IPv4 and IPv6 source."
  }
}
run "firewall_allows_only_ssh_from_allowed_cidrs" {
  variables { ssh_allowed_cidrs = ["203.0.113.0/24", "2001:db8::/32"] }
  assert {
    condition     = length(hcloud_firewall.this.rule) == 1 && one(hcloud_firewall.this.rule).direction == "in" && one(hcloud_firewall.this.rule).protocol == "tcp" && one(hcloud_firewall.this.rule).port == "22"
    error_message = "The only firewall rule must be inbound TCP 22."
  }
  assert {
    condition     = toset(one(hcloud_firewall.this.rule).source_ips) == toset(["203.0.113.0/24", "2001:db8::/32"]) && alltrue([for server in hcloud_server.this : contains(server.firewall_ids, 3003)])
    error_message = "Every machine must use the restricted firewall."
  }
}
run "caller_labels_cannot_replace_swarmy_labels" {
  variables { labels = { "swarmy-swarm" = "other", team = "infra" } }
  assert {
    condition     = hcloud_server.this[0].labels == tomap({ "swarmy-swarm" = "fleet", "swarmy-group" = "workers", "swarmy-managed" = "true", team = "infra" }) && hcloud_firewall.this.labels["team"] == "infra" && hcloud_ssh_key.this.labels["swarmy-group"] == "workers"
    error_message = "Caller labels must not override swarmy labels."
  }
}
run "joins_an_existing_network" {
  variables { network_id = "4242" }
  assert {
    condition     = length(hcloud_network.this) == 0 && length(hcloud_network_subnet.this) == 0 && alltrue([for attachment in hcloud_server_network.this : attachment.network_id == 4242 && attachment.subnet_id == null]) && output.network_id == "4242"
    error_message = "Joining must not create a new network."
  }
}
run "scales_to_zero" {
  variables { machine_count = 0 }
  assert {
    condition     = length(hcloud_server.this) == 0 && length(hcloud_server_network.this) == 0 && length(output.machines) == 0
    error_message = "Zero machines must yield an empty list."
  }
}
