output "machines" {
  description = "Machines to provision over SSH, ordered by number. Scaleway machines here have a routed public IPv4 address, no IPv6 and no private network."
  value = [for index, server in scaleway_instance_server.this : {
    name        = server.name
    public_ipv4 = scaleway_instance_ip.this[index].address != "" ? scaleway_instance_ip.this[index].address : null
    public_ipv6 = null
    private_ip  = null
    ssh_user    = "root"
  }]
}
output "bucket" {
  description = "Always null: this module creates no bucket."
  value       = null
}
output "network_id" {
  description = "Always null: this module creates no private network, so there is nothing for later groups to join."
  value       = null
}
