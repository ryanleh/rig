# Terraform environment for distributed experiment runs: the server machines
# named by var.server_names and a fleet of client machines, all in one cluster
# placement group with an open intra-group network, reachable from outside
# only over SSH. `terraform output -json | ./gen-inventory.sh` turns the result
# into a rig inventory (private IPs as "host", public as "ssh").

terraform {
  required_version = ">= 1.5"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.region
}

# Latest Ubuntu 24.04 LTS amd64 image from Canonical. If you switch to a
# Graviton instance type, change amd64 -> arm64 here and cross-build the
# binaries with GOARCH=arm64.
data "aws_ami" "ubuntu" {
  most_recent = true
  owners      = ["099720109477"] # Canonical

  filter {
    name   = "name"
    values = ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]
  }
  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

# Default VPC, one fixed subnet: a cluster placement group needs every
# instance in the same AZ.
data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

locals {
  subnet_id = sort(data.aws_subnets.default.ids)[0]

  # Cloud-init: the kernel limits from ../sysctl.sh, made persistent —
  # sysctls via sysctl.d, and the fd ceiling for SSH sessions via pam_limits
  # (sysctl.sh's `ulimit -n` is per-process; limits.d is the machine-wide
  # equivalent for the processes the runner launches over SSH).
  user_data = <<-EOF
    #cloud-config
    write_files:
      - path: /etc/sysctl.d/99-rig.conf
        content: |
          # ../sysctl.sh equivalents
          fs.nr_open = 1048576
          net.ipv4.ip_local_port_range = 1024 65535
          net.core.somaxconn = 4096
          net.ipv4.tcp_max_syn_backlog = 8192
          net.ipv4.tcp_slow_start_after_idle = 0
      - path: /etc/security/limits.d/99-rig.conf
        content: |
          * soft nofile 1048576
          * hard nofile 1048576
          root soft nofile 1048576
          root hard nofile 1048576
    runcmd:
      - sysctl --system
  EOF

  common_tags = {
    Project   = var.name
    ManagedBy = "terraform"
  }
}

resource "aws_placement_group" "cluster" {
  name     = "${var.name}-cluster"
  strategy = "cluster"
  tags     = local.common_tags
}

resource "aws_security_group" "eval" {
  name        = var.name
  description = "Experiment cluster: ssh from the operator, everything within the group"
  vpc_id      = data.aws_vpc.default.id

  ingress {
    description = "ssh from allowed_cidr (the internet by default; key auth only)"
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = [var.allowed_cidr]
  }

  ingress {
    description = "all traffic between cluster machines"
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    self        = true
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = local.common_tags
}

# One instance per logical server name. The names are what suites and
# inventories use — {{ip "server"}} in a command resolves through the
# inventory to this machine's private IP — so changing var.server_names
# changes the vocabulary of your suites and nothing else.
resource "aws_instance" "server" {
  for_each = toset(var.server_names)

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.server_instance_type
  key_name                    = var.key_name
  subnet_id                   = local.subnet_id
  placement_group             = aws_placement_group.cluster.id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  user_data                   = local.user_data

  root_block_device {
    volume_size = var.server_volume_gb
    volume_type = "gp3"
  }

  tags = merge(local.common_tags, {
    Name = "${var.name}-${each.key}"
    Role = each.key
  })
}
}

resource "aws_instance" "client" {
  count = var.client_count

  ami                         = data.aws_ami.ubuntu.id
  instance_type               = var.client_instance_type
  key_name                    = var.key_name
  subnet_id                   = local.subnet_id
  placement_group             = aws_placement_group.cluster.id
  vpc_security_group_ids      = [aws_security_group.eval.id]
  associate_public_ip_address = true
  user_data                   = local.user_data

  root_block_device {
    volume_size = var.client_volume_gb
    volume_type = "gp3"
  }

  tags = merge(local.common_tags, {
    Name = "${var.name}-client-${count.index}"
    Role = "client"
  })
}
