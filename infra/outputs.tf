# The "machines" output is shaped exactly like a runner inventory's machines
# map (gen-inventory.sh wraps it and injects the key path): host = private IP
# (what other machines — and {{ip "name"}} in suite templates — use),
# ssh = user@public-IP (what the runner connects to).
#
# A suite pins a role to one member of the client group with an indexed name
# ("clients[0]"), so no alias entry is needed for it.

output "machines" {
  description = "Runner inventory machines: host = private IP, ssh = user@public IP."
  value = merge(
    {
      for name, inst in aws_instance.server : name => {
        host = inst.private_ip
        ssh  = "ubuntu@${inst.public_ip}"
      }
    },
    {
      "clients" = [
        for i in aws_instance.client : {
          host = i.private_ip
          ssh  = "ubuntu@${i.public_ip}"
        }
      ]
    }
  )
}

output "public_ips" {
  description = "Public IPs by machine, for hand ssh-ing."
  value = merge(
    { for name, inst in aws_instance.server : name => inst.public_ip },
    { for i, inst in aws_instance.client : "client-${i}" => inst.public_ip }
  )
}
