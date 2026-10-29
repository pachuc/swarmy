locals {
  name = "${var.swarm}-${var.group}"
  # Swarmy's own labels are merged last so a caller label cannot replace them.
  labels = merge(var.labels, {
    "swarmy-swarm"   = var.swarm
    "swarmy-group"   = var.group
    "swarmy-managed" = "true"
  })
  # Scaleway tags are plain strings.
  tags           = [for key in sort(keys(local.labels)) : "${key}=${local.labels[key]}"]
  region         = join("-", slice(split("-", var.location), 0, 2))
  root_volume_gb = 200
  # cloud-init installs the group key for root. A Scaleway IAM SSH key would
  # need IAM permissions and would reach every instance of the project.
  cloud_init = "#cloud-config\n${yamlencode({ disable_root = false, ssh_authorized_keys = [var.ssh_public_key] })}"
}
# From the internet only SSH is open. Nodes reach the primary through their own outbound SSH tunnel.
resource "scaleway_instance_security_group" "this" {
  name                    = local.name
  inbound_default_policy  = "drop"
  outbound_default_policy = "accept"
  stateful                = true
  tags                    = local.tags
  dynamic "inbound_rule" {
    for_each = var.ssh_allowed_cidrs
    content {
      action   = "accept"
      protocol = "TCP"
      port     = 22
      ip_range = inbound_rule.value
    }
  }
}
resource "scaleway_instance_ip" "this" {
  count = var.machine_count
  type  = "routed_ipv4"
  tags  = local.tags
}
resource "scaleway_instance_server" "this" {
  count             = var.machine_count
  name              = format("%s-%d", local.name, count.index + 1)
  type              = var.size
  image             = var.image
  ip_id             = scaleway_instance_ip.this[count.index].id
  security_group_id = scaleway_instance_security_group.this.id
  tags              = local.tags
  user_data         = { "cloud-init" = local.cloud_init }
  root_volume {
    size_in_gb            = local.root_volume_gb
    volume_type           = "sbs_volume"
    delete_on_termination = true
  }
  lifecycle {
    # A new image label or group key must not replace running machines.
    ignore_changes = [image, user_data]
  }
}
