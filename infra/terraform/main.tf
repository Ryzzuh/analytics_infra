# The whole of the running platform's infrastructure (SPEC.md §2).
#
# Everything here is created by Terraform and destroyed by `terraform destroy`. Nothing is
# clicked into existence in the Hetzner console, because a resource Terraform does not know
# about is a resource that survives teardown and keeps billing — which is the failure this
# project's cost discipline exists to prevent.

locals {
  name = "analytics-infra-${var.environment}"

  labels = {
    project     = "analytics-infra"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "hcloud_ssh_key" "admin" {
  count      = length(var.ssh_public_keys)
  name       = "${local.name}-${count.index}"
  public_key = var.ssh_public_keys[count.index]
  labels     = local.labels
}

resource "hcloud_firewall" "platform" {
  name   = local.name
  labels = local.labels

  # Public: HTTP and HTTPS only. Caddy terminates TLS and decides what the public may see
  # (SPEC.md §11) — read-only dashboards, passcode-protected actions.
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "80"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "443"
    source_ips = ["0.0.0.0/0", "::/0"]
  }

  # Administrative: SSH from known addresses only. The deploy pipeline connects from GitHub
  # Actions, whose ranges change, so CI deploys go through a tunnel or a self-hosted runner
  # rather than by widening this rule (see docs/deployment.md).
  rule {
    direction  = "in"
    protocol   = "tcp"
    port       = "22"
    source_ips = var.admin_ipv4_cidrs
  }
}

resource "hcloud_volume" "data" {
  name     = "${local.name}-data"
  size     = var.volume_size_gb
  location = var.location
  format   = "ext4"
  labels   = local.labels

  # The warehouse, the source database, Redpanda's log and the golden archives live here. The
  # server is disposable; this is not.
  lifecycle {
    prevent_destroy = false # flip to true once it holds anything worth keeping
  }
}

resource "hcloud_server" "platform" {
  name         = local.name
  server_type  = var.server_type
  location     = var.location
  image        = "debian-12"
  ssh_keys     = hcloud_ssh_key.admin[*].id
  firewall_ids = [hcloud_firewall.platform.id]
  labels       = local.labels

  # Mounts the volume, installs Docker, and creates the deploy user. Everything beyond that is
  # the deploy pipeline's job, so a rebuild of this box is a few minutes rather than a
  # reconstruction from memory.
  user_data = templatefile("${path.module}/cloud-init.yaml", {
    volume_device = hcloud_volume.data.linux_device
  })

  public_net {
    ipv4_enabled = true
    ipv6_enabled = true
  }
}

resource "hcloud_volume_attachment" "data" {
  volume_id = hcloud_volume.data.id
  server_id = hcloud_server.platform.id
  automount = false # cloud-init mounts it: automount races with the first boot
}

resource "hcloud_rdns" "platform" {
  count      = var.domain == "" ? 0 : 1
  server_id  = hcloud_server.platform.id
  ip_address = hcloud_server.platform.ipv4_address
  dns_ptr    = var.domain
}
