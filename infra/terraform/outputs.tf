output "server_ipv4" {
  description = "Public IPv4 of the platform host."
  value       = hcloud_server.platform.ipv4_address
}

output "ssh_command" {
  description = "Ready-to-paste SSH command for the deploy user."
  value       = "ssh deploy@${hcloud_server.platform.ipv4_address}"
}

output "volume_device" {
  description = "Block device the data volume appears as; cloud-init mounts it at /data."
  value       = hcloud_volume.data.linux_device
}

output "monthly_cost_estimate_eur" {
  description = <<-EOT
    Rough monthly cost, so the bill is a decision rather than a surprise.

    Hetzner prices at time of writing: CX42 ~EUR 17/month, volumes ~EUR 0.044/GB/month.
  EOT
  value       = format("~EUR %.2f (server %s + %d GB volume)", 17 + var.volume_size_gb * 0.044, var.server_type, var.volume_size_gb)
}
