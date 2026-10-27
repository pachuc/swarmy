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
  description = "Hetzner Cloud server type."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9]+$", var.size))
    error_message = "size must be a Hetzner Cloud server type name such as cx23."
  }
}
variable "location" {
  description = "Hetzner Cloud location for the machines."
  type        = string
  validation {
    condition     = contains(["fsn1", "nbg1", "hel1", "ash", "hil", "sin"], var.location)
    error_message = "location must be one of fsn1, nbg1, hel1, ash, hil, or sin."
  }
}
variable "image" {
  description = "Operating system image. Swarmy provisioning expects Ubuntu."
  type        = string
  default     = "ubuntu-24.04"
}
variable "ssh_public_key" {
  description = "OpenSSH public key for root. Each group needs a unique key."
  type        = string
  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp(256|384|521)) [A-Za-z0-9+/=]+( .*)?$", var.ssh_public_key))
    error_message = "ssh_public_key must be one OpenSSH public key line."
  }
}
variable "labels" {
  description = "Extra labels; swarmy's labels always win."
  type        = map(string)
  default     = {}
  validation {
    condition = alltrue([
      for key, value in var.labels :
      can(regex("^([a-z0-9]([a-z0-9.-]*[a-z0-9])?/)?[A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?$", key))
      && can(regex("^([A-Za-z0-9]([A-Za-z0-9._-]{0,61}[A-Za-z0-9])?)?$", value))
    ])
    error_message = "labels must follow Hetzner label rules."
  }
}
variable "ssh_allowed_cidrs" {
  description = "Sources allowed to reach SSH on public interfaces."
  type        = list(string)
  default     = ["0.0.0.0/0", "::/0"]
  validation {
    condition     = length(var.ssh_allowed_cidrs) > 0 && alltrue([for cidr in var.ssh_allowed_cidrs : can(cidrhost(cidr, 0))])
    error_message = "ssh_allowed_cidrs must be a non-empty list of CIDRs."
  }
}
variable "bucket_name" {
  description = "Object storage bucket, or empty for none."
  type        = string
  default     = ""
  validation {
    condition     = var.bucket_name == "" || can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.bucket_name))
    error_message = "bucket_name must be empty or 3-63 lowercase letters, digits, and hyphens."
  }
}
variable "bucket_location" {
  description = "Object storage location; empty means the group's location."
  type        = string
  default     = ""
  validation {
    condition     = contains(["", "fsn1", "nbg1", "hel1"], var.bucket_location)
    error_message = "bucket_location must be empty or fsn1, nbg1, or hel1."
  }
}
variable "bucket_force_destroy" {
  description = "Delete a non-empty bucket only when the user requested data destruction."
  type        = bool
  default     = false
}
variable "network_id" {
  description = "Existing swarm network to join, or empty to create one."
  type        = string
  default     = ""
  validation {
    condition     = can(regex("^[0-9]*$", var.network_id))
    error_message = "network_id must be empty or a numeric Hetzner Cloud network ID."
  }
}
