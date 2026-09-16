# Every variable has a default, so `terraform apply` and `terraform destroy`
# both run with no arguments and nothing prompts. Change a default here and
# commit it; `-var name=value` covers a one-off run (avoid a terraform.tfvars
# file — terraform auto-loads it, silently overriding these defaults).

variable "region" {
  description = "AWS region to deploy into."
  type        = string
  default     = "us-east-1"
}

variable "name" {
  description = "Name prefix for every resource (tags, security group, placement group)."
  type        = string
  default     = "rig"
}

variable "allowed_cidr" {
  description = <<-EOT
    CIDR allowed to reach port 22. Defaults to the whole internet: the
    instances accept keys only (Ubuntu's cloud images disable password auth),
    the clusters are short-lived, and pinning your own address means
    re-applying every time it changes. Narrow it to "203.0.113.7/32" if a run
    is long-lived or the account holds anything else.
  EOT
  type        = string
  default     = "0.0.0.0/0"
}

variable "key_name" {
  description = "Name of an existing EC2 key pair in this region; its private key path is what you pass to gen-inventory.sh."
  type        = string
  default     = "rig"
}

variable "client_count" {
  description = "Number of client (workload) machines — the inventory's \"clients\" group. Must be >= 1 (suites that need one client machine pin themselves to \"clients[0]\")."
  type        = number
  default     = 10
}

variable "nofile_limit" {
  description = <<-EOT
    Max open files (fd limit) for every process the runner launches over SSH.
    The default suits client shards, which hold ~2 sockets per simulated
    client. It is deliberately NOT raised by default: a SERVER that aggregates
    one inbound connection per active client needs more, so a run above roughly
    1M concurrent active clients must OPT IN by raising this (e.g. 4194304) —
    otherwise the server brushes the ceiling, Accept returns EMFILE, and it
    dies. Applied three ways so it actually takes on Ubuntu cloud images:
    fs.nr_open (the kernel ceiling), systemd DefaultLimitNOFILE (the real
    governor of SSH-session limits here — pam_limits/limits.d alone does not
    apply to the runner's non-login sessions), and limits.d as a backstop.
  EOT
  type        = number
  default     = 1048576
}

variable "server_instance_type" {
  description = "Instance type for every server machine."
  type        = string
  default     = "r8in.24xlarge"
}

variable "client_instance_type" {
  description = "Instance type for each client machine."
  type        = string
  default     = "c8i.8xlarge"
}

variable "server_names" {
  description = <<-EOT
    Logical names for the server machines, one instance each. These are the
    names your suites and inventories speak: a role saying `"machine":
    "server"` and a command saying `{{ip "server"}}` both resolve through
    this. Two servers that talk to each other would be ["server-a",
    "server-b"].
  EOT
  type        = list(string)
  default     = ["server"]
}

variable "server_volume_gb" {
  description = "Root volume size for each server machine, in GB."
  type        = number
  default     = 32
}

variable "client_volume_gb" {
  description = "Root volume size for each client machine, in GB."
  type        = number
  default     = 16
}
