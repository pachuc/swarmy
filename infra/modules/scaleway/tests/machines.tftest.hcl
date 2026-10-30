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
# Check changed image and labels before creating state: image changes are ignored after launch.
run "caller_labels_cannot_replace_swarmy_labels" {
  command = plan
  variables { labels = { "swarmy-swarm" = "other", team = "infra" } }
  assert {
    condition     = scaleway_instance_server.this[0].tags == tolist(["swarmy-group=gpu", "swarmy-managed=true", "swarmy-swarm=fleet", "team=infra"]) && scaleway_instance_security_group.this.tags == scaleway_instance_server.this[0].tags && scaleway_instance_ip.this[0].tags == scaleway_instance_server.this[0].tags
    error_message = "Caller labels cannot replace swarmy labels; all three resources must have the same sorted tags."
  }
}

run "explicit_image_is_used_as_given" {
  command = plan
  variables { image = "ubuntu_noble_gpu_os_13_nvidia" }
  assert {
    condition     = alltrue([for s in scaleway_instance_server.this : s.image == "ubuntu_noble_gpu_os_13_nvidia"])
    error_message = "explicit_image_is_used_as_given failed"
  }
}

run "creates_the_group" {
  assert {
    condition     = [for s in scaleway_instance_server.this : s.name] == ["fleet-gpu-1", "fleet-gpu-2"] && alltrue([for i, s in scaleway_instance_server.this : s.type == "L4-1-24G" && s.image == "ubuntu_noble" && s.ip_id == scaleway_instance_ip.this[i].id && s.security_group_id == scaleway_instance_security_group.this.id && one(s.root_volume).size_in_gb == 200 && one(s.root_volume).volume_type == "sbs_volume" && one(s.root_volume).delete_on_termination]) && alltrue([for ip in scaleway_instance_ip.this : ip.type == "routed_ipv4"]) && output.machines[1] == { name = "fleet-gpu-2", public_ipv4 = "203.0.113.20", public_ipv6 = null, private_ip = null, ssh_user = "root" } && output.bucket == null && output.network_id == null
    error_message = "creates_the_group failed"
  }
}

run "cloud_init_installs_the_group_key_for_root" {
  assert {
    condition     = alltrue([for s in scaleway_instance_server.this : startswith(s.user_data["cloud-init"], "#cloud-config\n")]) && alltrue([for s in scaleway_instance_server.this : yamldecode(s.user_data["cloud-init"]).disable_root == false]) && alltrue([for s in scaleway_instance_server.this : yamldecode(s.user_data["cloud-init"]).ssh_authorized_keys == [var.ssh_public_key]])
    error_message = "cloud_init_installs_the_group_key_for_root failed"
  }
}

run "ssh_is_open_to_anywhere_by_default" {
  assert {
    condition     = length(scaleway_instance_security_group.this.inbound_rule) == 2 && toset([for r in scaleway_instance_security_group.this.inbound_rule : r.ip_range]) == toset(["0.0.0.0/0", "::/0"])
    error_message = "ssh_is_open_to_anywhere_by_default failed"
  }
}

run "security_group_allows_only_ssh_from_allowed_cidrs" {
  variables { ssh_allowed_cidrs = ["203.0.113.0/24"] }
  assert {
    condition     = length(scaleway_instance_security_group.this.inbound_rule) == 1 && one(scaleway_instance_security_group.this.inbound_rule).action == "accept" && one(scaleway_instance_security_group.this.inbound_rule).protocol == "TCP" && one(scaleway_instance_security_group.this.inbound_rule).port == 22 && one(scaleway_instance_security_group.this.inbound_rule).ip_range == "203.0.113.0/24" && scaleway_instance_security_group.this.inbound_default_policy == "drop" && scaleway_instance_security_group.this.outbound_default_policy == "accept"
    error_message = "security_group_allows_only_ssh_from_allowed_cidrs failed"
  }
}

run "scales_to_zero" {
  variables { machine_count = 0 }
  assert {
    condition     = length(scaleway_instance_server.this) == 0 && length(scaleway_instance_ip.this) == 0 && length(output.machines) == 0
    error_message = "scales_to_zero failed"
  }
}
