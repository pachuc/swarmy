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
run "creates_the_group" {
  assert {
    condition     = [for machine in output.machines : machine.name] == ["fleet-workers-1", "fleet-workers-2"]
    error_message = "Machines must be named swarm-group-N from 1."
  }
  assert {
    condition     = alltrue([for instance in aws_instance.this : instance.instance_type == "m6i.large" && instance.ami == "ami-0123456789abcdef0" && instance.subnet_id == aws_subnet.this[0].id && instance.key_name == "fleet-workers"])
    error_message = "Machines must use the size, image, subnet, and group's key pair."
  }
  assert {
    condition     = data.aws_ssm_parameter.ubuntu[0].name == "/aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id"
    error_message = "x86 instance types must get the amd64 image."
  }
  assert {
    condition     = aws_instance.this[1].tags == tomap({ "swarmy-swarm" = "fleet", "swarmy-group" = "workers", "swarmy-managed" = "true", Name = "fleet-workers-2" })
    error_message = "Machines must carry the swarm and group tags and their name."
  }
  assert {
    condition     = alltrue([for instance in aws_instance.this : one(instance.root_block_device).volume_size == 100 && one(instance.root_block_device).encrypted && one(instance.metadata_options).http_tokens == "required"])
    error_message = "Machines need an encrypted 100 GiB root disk and IMDSv2."
  }
  assert {
    condition     = aws_key_pair.this.key_name == "fleet-workers" && aws_key_pair.this.public_key == var.ssh_public_key
    error_message = "The group's key pair must use ssh_public_key."
  }
  assert {
    condition     = length(aws_vpc.this) == 1 && aws_vpc.this[0].cidr_block == "10.0.0.0/16" && aws_subnet.this[0].cidr_block == "10.0.0.0/20" && aws_subnet.this[0].map_public_ip_on_launch
    error_message = "The group must create its VPC and public subnet."
  }
  assert {
    condition     = one(aws_route_table.this[0].route).cidr_block == "0.0.0.0/0" && one(aws_route_table.this[0].route).gateway_id == aws_internet_gateway.this[0].id && aws_route_table_association.this[0].subnet_id == aws_subnet.this[0].id
    error_message = "The subnet must route to the internet gateway."
  }
  assert {
    condition     = output.machines[1].name == "fleet-workers-2" && output.machines[1].public_ipv4 == "203.0.113.10" && output.machines[1].public_ipv6 == null && output.machines[1].private_ip == "10.0.0.10" && output.machines[1].ssh_user == "ubuntu"
    error_message = "Each machine output must carry addresses and login."
  }
  assert {
    condition     = toset(keys(output.machines[0])) == toset(["name", "public_ipv4", "public_ipv6", "private_ip", "ssh_user"])
    error_message = "Each machine output must have exactly the five contract keys."
  }
  assert {
    condition     = output.network_id == "subnet-mock" && output.bucket == null && length(aws_s3_bucket.this) == 0
    error_message = "The output must name the created subnet and no bucket."
  }
}
run "ssh_is_open_to_anywhere_by_default" {
  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.ssh) == 2 && aws_vpc_security_group_ingress_rule.ssh["0.0.0.0/0"].cidr_ipv4 == "0.0.0.0/0" && aws_vpc_security_group_ingress_rule.ssh["::/0"].cidr_ipv6 == "::/0" && aws_vpc_security_group_ingress_rule.ssh["::/0"].cidr_ipv4 == null
    error_message = "SSH must default to every IPv4 and IPv6 source."
  }
}
run "firewall_allows_only_ssh_from_allowed_cidrs" {
  variables { ssh_allowed_cidrs = ["203.0.113.0/24"] }
  assert {
    condition     = length(aws_vpc_security_group_ingress_rule.ssh) == 1 && aws_vpc_security_group_ingress_rule.ssh["203.0.113.0/24"].ip_protocol == "tcp" && aws_vpc_security_group_ingress_rule.ssh["203.0.113.0/24"].from_port == 22 && aws_vpc_security_group_ingress_rule.ssh["203.0.113.0/24"].to_port == 22
    error_message = "The only public rule must be TCP 22."
  }
  assert {
    condition     = aws_vpc_security_group_ingress_rule.private.ip_protocol == "-1" && aws_vpc_security_group_ingress_rule.private.cidr_ipv4 == "10.0.0.0/20"
    error_message = "Allow the private range."
  }
  assert {
    condition     = aws_vpc_security_group_egress_rule.all.ip_protocol == "-1" && aws_vpc_security_group_egress_rule.all.cidr_ipv4 == "0.0.0.0/0"
    error_message = "Machines must reach the internet."
  }
  assert {
    condition     = alltrue([for instance in aws_instance.this : contains(instance.vpc_security_group_ids, aws_security_group.this.id)]) && aws_security_group.this.vpc_id == aws_vpc.this[0].id
    error_message = "Every machine must use the group's security group."
  }
}
run "caller_labels_cannot_replace_swarmy_labels" {
  variables { labels = { "swarmy-swarm" = "other", team = "infra" } }
  assert {
    condition     = aws_key_pair.this.tags == tomap({ "swarmy-swarm" = "fleet", "swarmy-group" = "workers", "swarmy-managed" = "true", team = "infra" })
    error_message = "Swarmy's tags must win."
  }
  assert {
    condition     = aws_security_group.this.tags["team"] == "infra" && aws_instance.this[0].tags["swarmy-group"] == "workers"
    error_message = "Labels apply to tagged resources."
  }
}
run "joins_an_existing_network" {
  variables { network_id = "subnet-shared" }
  assert {
    condition     = length(aws_vpc.this) == 0 && length(aws_subnet.this) == 0 && length(aws_internet_gateway.this) == 0 && length(aws_route_table.this) == 0
    error_message = "Joining must not create a network."
  }
  assert {
    condition     = alltrue([for instance in aws_instance.this : instance.subnet_id == "subnet-shared"]) && aws_security_group.this.vpc_id == "vpc-shared"
    error_message = "Machines and security group use joined VPC."
  }
  assert {
    condition     = aws_vpc_security_group_ingress_rule.private.cidr_ipv4 == "10.1.0.0/20" && output.network_id == "subnet-shared"
    error_message = "Private range follows joined subnet."
  }
}
run "arm_instance_types_get_the_arm_image" {
  variables { size = "m7g.large" }
  override_data {
    target = data.aws_ec2_instance_type.this
    values = { supported_architectures = ["arm64"] }
  }
  assert {
    condition     = data.aws_ssm_parameter.ubuntu[0].name == "/aws/service/canonical/ubuntu/server/24.04/stable/current/arm64/hvm/ebs-gp3/ami-id"
    error_message = "arm64 instance types must get arm64 image."
  }
}
run "explicit_ami_skips_the_lookup" {
  # This must start from a fresh state: running instances deliberately ignore AMI changes.
  state_key = "explicit-ami"
  variables { image = "ami-0fedcba9876543210" }
  assert {
    condition     = length(data.aws_ssm_parameter.ubuntu) == 0 && alltrue([for instance in aws_instance.this : instance.ami == "ami-0fedcba9876543210"])
    error_message = "An AMI id is used as given."
  }
}
run "scales_to_zero" {
  variables { machine_count = 0 }
  assert {
    condition     = length(aws_instance.this) == 0 && length(output.machines) == 0
    error_message = "A zero count leaves no machines."
  }
}
