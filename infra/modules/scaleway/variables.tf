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
  description = "Scaleway Instance commercial type, for example L4-1-24G."
  type        = string
  validation {
    condition     = can(regex("^[A-Z][A-Z0-9]*(-[A-Z0-9]+)*$", var.size))
    error_message = "size must be a Scaleway commercial type such as L4-1-24G or POP2-2C-8G."
  }
}
variable "location" {
  description = "Scaleway zone for the machines."
  type        = string
  validation {
    condition     = contains(["fr-par-1", "fr-par-2", "fr-par-3", "nl-ams-1", "nl-ams-2", "nl-ams-3", "pl-waw-1", "pl-waw-2", "pl-waw-3"], var.location)
    error_message = "location must be one of fr-par-1, fr-par-2, fr-par-3, nl-ams-1, nl-ams-2, nl-ams-3, pl-waw-1, pl-waw-2, or pl-waw-3."
  }
}
variable "image" {
  description = "Scaleway image label, such as ubuntu_noble (Ubuntu 24.04), or an image UUID. Swarmy provisioning expects Ubuntu."
  type        = string
  default     = "ubuntu_noble"
  validation {
    condition     = can(regex("^[a-z0-9_]+$", var.image)) || can(regex("^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", var.image))
    error_message = "image must be a lowercase Scaleway image label or UUID."
  }
}
variable "ssh_public_key" {
  description = "OpenSSH public key installed for root by cloud-init. Each group needs a unique key."
  type        = string
  validation {
    condition     = can(regex("^(ssh-ed25519|ssh-rsa|ecdsa-sha2-nistp(256|384|521)) [A-Za-z0-9+/=]+( .*)?$", var.ssh_public_key))
    error_message = "ssh_public_key must be one OpenSSH public key line."
  }
}
variable "labels" {
  description = "Extra instance tags; swarmy's labels always win."
  type        = map(string)
  default     = {}
  validation {
    condition = alltrue([for key, value in var.labels :
      can(regex("^[A-Za-z0-9._/-]{1,63}$", key)) && can(regex("^[A-Za-z0-9._/-]{0,63}$", value))
    ])
    error_message = "labels must be up to 63 letters, digits, and ._/- characters; they become key=value instance tags."
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
  description = "Unused: this module creates no bucket."
  type        = string
  default     = ""
  validation {
    condition     = var.bucket_name == ""
    error_message = "the scaleway module creates no bucket; bucket_name must be empty. Give the swarm a [bucket] in its spec instead."
  }
}
variable "bucket_location" {
  description = "Unused: this module creates no bucket."
  type        = string
  default     = ""
  validation {
    condition     = var.bucket_location == ""
    error_message = "the scaleway module creates no bucket; bucket_location must be empty. Give the swarm a [bucket] in its spec instead."
  }
}
variable "bucket_force_destroy" {
  description = "Unused: this module creates no bucket."
  type        = bool
  default     = false
}
variable "network_id" {
  description = "Unused: this module creates no private network."
  type        = string
  default     = ""
  validation {
    condition     = var.network_id == ""
    error_message = "the scaleway module creates no private network; network_id must be empty"
  }
}
