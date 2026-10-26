locals {
  name = "${var.swarm}-${var.group}"
  labels = merge(var.labels, {
    "swarmy-swarm"   = var.swarm
    "swarmy-group"   = var.group
    "swarmy-managed" = "true"
  })
  network_zones = {
    fsn1 = "eu-central"
    nbg1 = "eu-central"
    hel1 = "eu-central"
    ash  = "us-east"
    hil  = "us-west"
    sin  = "ap-southeast"
  }
  create_network           = var.network_id == ""
  network_ip_range         = "10.0.0.0/16"
  subnet_ip_range          = "10.0.0.0/20"
  object_storage_locations = ["fsn1", "nbg1", "hel1"]
  bucket_region            = var.bucket_location != "" ? var.bucket_location : var.location
  provider_region          = contains(local.object_storage_locations, local.bucket_region) ? local.bucket_region : "fsn1"
  bucket_endpoint          = "https://${local.bucket_region}.your-objectstorage.com"
}
resource "hcloud_ssh_key" "this" {
  name       = local.name
  public_key = var.ssh_public_key
  labels     = local.labels
}
# Hetzner firewalls filter public interfaces only. Private traffic needs no rule.
resource "hcloud_firewall" "this" {
  name   = local.name
  labels = local.labels
  rule {
    description = "SSH"
    direction   = "in"
    protocol    = "tcp"
    port        = "22"
    source_ips  = var.ssh_allowed_cidrs
  }
}
resource "hcloud_network" "this" {
  count    = local.create_network ? 1 : 0
  name     = local.name
  ip_range = local.network_ip_range
  labels   = local.labels
}
resource "hcloud_network_subnet" "this" {
  count        = local.create_network ? 1 : 0
  network_id   = hcloud_network.this[0].id
  type         = "cloud"
  network_zone = lookup(local.network_zones, var.location, "eu-central")
  ip_range     = local.subnet_ip_range
}
resource "hcloud_server" "this" {
  count        = var.machine_count
  name         = format("%s-%d", local.name, count.index + 1)
  server_type  = var.size
  image        = var.image
  location     = var.location
  ssh_keys     = [hcloud_ssh_key.this.id]
  firewall_ids = [hcloud_firewall.this.id]
  labels       = local.labels
  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
  lifecycle {
    # Changing the group key must not replace running machines.
    ignore_changes = [ssh_keys]
  }
}
resource "hcloud_server_network" "this" {
  count      = var.machine_count
  server_id  = hcloud_server.this[count.index].id
  subnet_id  = local.create_network ? hcloud_network_subnet.this[0].id : null
  network_id = local.create_network ? null : var.network_id
}
resource "minio_s3_bucket" "this" {
  count         = var.bucket_name == "" ? 0 : 1
  bucket        = var.bucket_name
  acl           = "private"
  force_destroy = var.bucket_force_destroy
  lifecycle {
    precondition {
      condition     = contains(local.object_storage_locations, local.bucket_region)
      error_message = "Hetzner object storage exists only in fsn1, nbg1, and hel1; set bucket_location for a group in ${var.location}."
    }
  }
}
