output "machines" {
  description = "Ordered machines to provision over SSH."
  value = [
    for server in hcloud_server.this : {
      name        = server.name
      public_ipv4 = server.ipv4_address == "" ? null : server.ipv4_address
      public_ipv6 = server.ipv6_address == "" ? null : server.ipv6_address
      private_ip  = try(one([for n in server.network : n.ip if n.ip != ""]), null)
      ssh_user    = "root"
    }
  ]
}
output "bucket" {
  description = "The group's bucket, or null when none was requested."
  value = var.bucket_name == "" ? null : {
    endpoint = local.bucket_endpoint
    name     = minio_s3_bucket.this[0].bucket
    region   = local.bucket_region
  }
}
output "network_id" {
  description = "Decimal ID of the created or joined swarm network."
  value       = local.create_network ? tostring(hcloud_network.this[0].id) : var.network_id
}
