locals {
  name = "${var.swarm}-${var.group}"
  # Swarmy's own tags are applied last so a caller label cannot replace them.
  tags = merge(var.labels, {
    "swarmy-swarm"   = var.swarm
    "swarmy-group"   = var.group
    "swarmy-managed" = "true"
  })
  create_network   = var.network_id == ""
  network_ip_range = "10.0.0.0/16"
  subnet_ip_range  = "10.0.0.0/20"
  subnet_id        = local.create_network ? aws_subnet.this[0].id : var.network_id
  vpc_id           = local.create_network ? aws_vpc.this[0].id : data.aws_subnet.joined[0].vpc_id
  # Every machine of the swarm sits in this one subnet.
  private_range = local.create_network ? aws_subnet.this[0].cidr_block : data.aws_subnet.joined[0].cidr_block

  architecture    = contains(data.aws_ec2_instance_type.this.supported_architectures, "arm64") ? "arm64" : "amd64"
  lookup_image    = var.image == "ubuntu-24.04"
  image_id        = local.lookup_image ? data.aws_ssm_parameter.ubuntu[0].insecure_value : var.image
  root_volume_gb  = 100
  bucket_region   = var.bucket_location != "" ? var.bucket_location : var.location
  bucket_endpoint = "https://s3.${local.bucket_region}.amazonaws.com"
}

data "aws_ec2_instance_type" "this" {
  instance_type = var.size
}
data "aws_ssm_parameter" "ubuntu" {
  count = local.lookup_image ? 1 : 0
  name  = "/aws/service/canonical/ubuntu/server/24.04/stable/current/${local.architecture}/hvm/ebs-gp3/ami-id"
}
data "aws_subnet" "joined" {
  count = local.create_network ? 0 : 1
  id    = var.network_id
}
resource "aws_vpc" "this" {
  count                = local.create_network ? 1 : 0
  cidr_block           = local.network_ip_range
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags                 = merge(local.tags, { Name = local.name })
}
resource "aws_internet_gateway" "this" {
  count  = local.create_network ? 1 : 0
  vpc_id = aws_vpc.this[0].id
  tags   = merge(local.tags, { Name = local.name })
}
resource "aws_subnet" "this" {
  count                   = local.create_network ? 1 : 0
  vpc_id                  = aws_vpc.this[0].id
  cidr_block              = local.subnet_ip_range
  map_public_ip_on_launch = true
  tags                    = merge(local.tags, { Name = local.name })
}
resource "aws_route_table" "this" {
  count  = local.create_network ? 1 : 0
  vpc_id = aws_vpc.this[0].id
  tags   = merge(local.tags, { Name = local.name })
  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.this[0].id
  }
}
resource "aws_route_table_association" "this" {
  count          = local.create_network ? 1 : 0
  subnet_id      = aws_subnet.this[0].id
  route_table_id = aws_route_table.this[0].id
}
# From the internet only SSH is open; the API is reached through an SSH tunnel.
resource "aws_security_group" "this" {
  name        = "swarmy-${local.name}"
  description = "swarmy ${local.name} machines"
  vpc_id      = local.vpc_id
  tags        = merge(local.tags, { Name = local.name })
}
resource "aws_vpc_security_group_ingress_rule" "ssh" {
  for_each          = toset(var.ssh_allowed_cidrs)
  security_group_id = aws_security_group.this.id
  description       = "SSH"
  ip_protocol       = "tcp"
  from_port         = 22
  to_port           = 22
  cidr_ipv4         = strcontains(each.value, ":") ? null : each.value
  cidr_ipv6         = strcontains(each.value, ":") ? each.value : null
  tags              = local.tags
}
resource "aws_vpc_security_group_ingress_rule" "private" {
  security_group_id = aws_security_group.this.id
  description       = "Swarm private range"
  ip_protocol       = "-1"
  cidr_ipv4         = local.private_range
  tags              = local.tags
}
resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.this.id
  description       = "Outbound"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
  tags              = local.tags
}
resource "aws_key_pair" "this" {
  key_name   = local.name
  public_key = var.ssh_public_key
  tags       = local.tags
}
resource "aws_instance" "this" {
  count                       = var.machine_count
  ami                         = local.image_id
  instance_type               = var.size
  subnet_id                   = local.subnet_id
  vpc_security_group_ids      = [aws_security_group.this.id]
  key_name                    = aws_key_pair.this.key_name
  associate_public_ip_address = true
  tags                        = merge(local.tags, { Name = format("%s-%d", local.name, count.index + 1) })
  metadata_options {
    http_endpoint = "enabled"
    http_tokens   = "required"
  }
  root_block_device {
    volume_size           = local.root_volume_gb
    volume_type           = "gp3"
    encrypted             = true
    delete_on_termination = true
  }
  lifecycle {
    # The Ubuntu parameter moves and a key pair is fixed at launch.
    ignore_changes = [ami, key_name]
  }
  # The internet route must exist before first boot.
  depends_on = [aws_route_table_association.this]
}
resource "aws_s3_bucket" "this" {
  count         = var.bucket_name == "" ? 0 : 1
  region        = local.bucket_region
  bucket        = var.bucket_name
  force_destroy = var.bucket_force_destroy
  tags          = local.tags
}
resource "aws_s3_bucket_public_access_block" "this" {
  count                   = var.bucket_name == "" ? 0 : 1
  region                  = local.bucket_region
  bucket                  = aws_s3_bucket.this[0].id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
