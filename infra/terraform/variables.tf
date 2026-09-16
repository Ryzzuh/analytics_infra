variable "hcloud_token" {
  description = "Hetzner Cloud API token. Passed from the environment (TF_VAR_hcloud_token), never committed."
  type        = string
  sensitive   = true
}

variable "server_type" {
  description = <<-EOT
    Hetzner server type. CX42 is 8 vCPU / 16 GB / 160 GB, ~EUR 17/month.

    The sizing is not arbitrary: the full stack needs ~13 GB (SPEC.md §2), so 16 GB leaves
    about 3 GB of headroom. CX32 (8 GB) cannot run it; CX52 (32 GB) doubles the cost for
    headroom nothing uses.
  EOT
  type        = string
  default     = "cx42"
}

variable "location" {
  description = "Hetzner location. fsn1 (Falkenstein) is the cheapest EU region."
  type        = string
  default     = "fsn1"
}

variable "volume_size_gb" {
  description = <<-EOT
    Attached volume for all docker volumes, in GB.

    100 GB holds the ~21 GB history, ~0.9 GB/day of live events, the golden archive, and
    Redpanda's retention with room to spare (SPEC.md §6.2 sizing). Data lives here rather than
    on the server's own disk so the box can be rebuilt without losing the warehouse.
  EOT
  type        = number
  default     = 100

  validation {
    condition     = var.volume_size_gb >= 60
    error_message = "Below ~60 GB the 12-month history plus a golden archive will not fit."
  }
}

variable "ssh_public_keys" {
  description = "SSH public keys allowed to reach the box. At least one, or the server is unreachable."
  type        = list(string)

  validation {
    condition     = length(var.ssh_public_keys) > 0
    error_message = "Provide at least one SSH key: password auth is disabled on the server."
  }
}

variable "admin_ipv4_cidrs" {
  description = <<-EOT
    Source addresses allowed to reach SSH and the internal ports.

    Deliberately NOT defaulted to 0.0.0.0/0. The public reaches the platform through Caddy on
    80/443 only; everything else is administrative.
  EOT
  type        = list(string)
}

variable "domain" {
  description = "Hostname the Caddy config serves, used for TLS. Empty disables the DNS record."
  type        = string
  default     = ""
}

variable "environment" {
  description = "Environment name, used in resource names and labels."
  type        = string
  default     = "prod"
}
