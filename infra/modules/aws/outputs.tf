output "machines" {
  description = "Machines to provision over SSH, ordered by number. EC2 instances here have no IPv6 address."
  value = [for index, instance in aws_instance.this : {
    name        = format("%s-%d", local.name, index + 1)
    public_ipv4 = instance.public_ip != "" ? instance.public_ip : null
    public_ipv6 = null
    private_ip  = instance.private_ip != "" ? instance.private_ip : null
    ssh_user    = "ubuntu"
  }]
}
output "bucket" {
  description = "The swarm's bucket, or null when bucket_name is empty."
  value = var.bucket_name == "" ? null : {
    endpoint = local.bucket_endpoint
    name     = aws_s3_bucket.this[0].bucket
    region   = local.bucket_region
  }
}
output "network_id" {
  description = "The subnet the machines are in: created here, or the one joined through network_id."
  value       = local.subnet_id
}
