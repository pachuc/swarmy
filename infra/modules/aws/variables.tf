variable "swarm" {
  description = "Swarm name, used for resource names and labels."
  type        = string
  validation {
    condition     = can(regex("^[a-z]([a-z0-9-]{0,18}[a-z0-9])?$", var.swarm))
    error_message = "swarm must be 1-20 lowercase letters, digits, and hyphens, starting with a letter and not ending with a hyphen."
  }
}
variable "group" {
  description = "Group name, used for resource names and labels."
  type        = string
  validation {
    condition     = can(regex("^[a-z]([a-z0-9-]{0,18}[a-z0-9])?$", var.group))
    error_message = "group must be 1-20 lowercase letters, digits, and hyphens, starting with a letter and not ending with a hyphen."
  }
}
variable "machine_count" {
  description = "Number of machines. Lowering it removes the highest-numbered machines."
  type        = number
  validation {
    condition     = var.machine_count >= 0 && var.machine_count <= 50 && floor(var.machine_count) == var.machine_count
    error_message = "machine_count must be a whole number from 0 to 50."
  }
}
variable "size" {
  description = "EC2 instance type, for example m6i.large."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9-]+\\.[a-z0-9-]+$", var.size))
    error_message = "size must be an EC2 instance type such as m6i.large."
  }
}
variable "location" {
  description = "AWS region for the machines, for example eu-central-1."
  type        = string
  validation {
    condition     = can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.location))
    error_message = "location must be an AWS region such as eu-central-1."
  }
}
variable "image" {
  description = "ubuntu-24.04 for Canonical's current Ubuntu 24.04 image in the region, an AMI id, or a public SSM parameter path under /aws/service/ that holds an AMI id. Swarmy provisioning expects Ubuntu."
  type        = string
  default     = "ubuntu-24.04"
  validation {
    condition     = var.image == "ubuntu-24.04" || can(regex("^ami-[0-9a-f]{8}([0-9a-f]{9})?$", var.image)) || can(regex("^/aws/service/[A-Za-z0-9_./-]+$", var.image))
    error_message = "image must be ubuntu-24.04, an AMI id such as ami-0123456789abcdef0, or an SSM parameter path under /aws/service/."
  }
}
variable "ssh_public_key" {
  description = "OpenSSH public key installed for the ubuntu login. EC2 key pairs accept ed25519 and RSA keys."
  type        = string
  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa) [A-Za-z0-9+/=]+( .*)?$", var.ssh_public_key))
    error_message = "ssh_public_key must be one OpenSSH ssh-ed25519 or ssh-rsa public key line."
  }
}
variable "labels" {
  description = "Extra tags for every resource. The swarmy-swarm, swarmy-group, and swarmy-managed tags always win."
  type        = map(string)
  default     = {}
  validation {
    condition = alltrue([
      for key, value in var.labels :
      can(regex("^[A-Za-z0-9 _.:/=+@-]{1,128}$", key)) && !startswith(lower(key), "aws:")
      && can(regex("^[A-Za-z0-9 _.:/=+@-]{0,256}$", value))
    ])
    error_message = "labels must follow EC2 tag rules: keys up to 128 and values up to 256 letters, digits, spaces, and _.:/=+@- characters, and keys must not start with aws:."
  }
}
variable "ssh_allowed_cidrs" {
  description = "IPv4 and IPv6 CIDRs allowed to reach SSH. Nothing else is open publicly."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
  validation {
    condition     = length(var.ssh_allowed_cidrs) > 0 && alltrue([for cidr in var.ssh_allowed_cidrs : can(cidrhost(cidr, 0))])
    error_message = "ssh_allowed_cidrs must be a non-empty list of CIDRs such as 203.0.113.0/24 or ::/0."
  }
}
variable "bucket_name" {
  description = "S3 bucket to create for the swarm, or empty for none. Bucket names are global."
  type        = string
  default     = ""
  validation {
    condition     = var.bucket_name == "" || can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be empty or 3-63 lowercase letters, digits, and hyphens."
  }
}
variable "bucket_location" {
  description = "AWS region for the bucket; empty means the group's location."
  type        = string
  default     = ""
  validation {
    condition     = var.bucket_location == "" || can(regex("^[a-z]{2}(-[a-z]+)+-[0-9]+$", var.bucket_location))
    error_message = "bucket_location must be empty or an AWS region such as eu-west-1."
  }
}
variable "bucket_force_destroy" {
  description = "Delete a non-empty bucket only when the user requested data destruction."
  type        = bool
  default     = false
}
variable "network_id" {
  description = "ID of an existing subnet of the swarm to join, or empty to create a VPC and subnet."
  type        = string
  default     = ""
  validation {
    condition     = var.network_id == "" || can(regex("^subnet-[0-9a-z]+$", var.network_id))
    error_message = "network_id must be empty or an EC2 subnet id."
  }
}
