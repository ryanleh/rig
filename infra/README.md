# Cluster (AWS)

**A starting point to copy into your own repo, not a dependency.** Real
deployments grow application facts this config cannot know — your services'
port ranges in the security groups, instance shapes, extra machines — so take
it, rename it, and shape it. The only contract rig's runner has with any of it
is the inventory JSON it emits; a cluster provisioned any other way works the
same.

Terraform for a distributed experiment cluster: one instance per name in
`var.server_names` plus `var.client_count` client machines, all Ubuntu 24.04 in
a cluster placement group, SSH open to `allowed_cidr` and everything open
within the group. Cloud-init applies the kernel limits from `sysctl.sh`
(sysctl.d + limits.d, so SSH-launched processes get the high fd ceiling — the
one that actually bites when a client machine opens tens of thousands of
connections).

Nothing here is required to use rig: an inventory of machines you already have
works just as well. This exists because standing the cluster up by hand, twice,
is how you end up with two clusters that differ in ways you find out about from
the numbers.

## Bring-up

```sh
cd infra
./up.sh ~/.ssh/rig.pem ./driver/example/echoserver ./driver/example/echodriver
```

That runs `terraform apply` (interactive — instances bill while up), writes
`inventory-aws.json`, and cross-builds the packages you name into `bin/` for
linux/amd64. Then:

```sh
./bin/rig run -suite <suite>.json -inventory inventory-aws.json -results results -bin bin
```

Tear down with `cd infra && terraform destroy`. **Instances bill until you
do.** Nothing in this repo destroys them on a timer, and nothing on the
instances can terminate them from the inside.

## Variables

`variables.tf` declares every knob with a default, so nothing is required on
the command line and `destroy` never prompts for a value `apply` was given. To
change the cluster — instance types, counts, region — edit the default there
and commit it: the settings are worth versioning alongside the experiments they
produced. There is deliberately no tfvars file; note that terraform auto-loads
`terraform.tfvars` if one exists, which would silently override these defaults.
`-var name=value` covers a one-off.

`server_names` is the one worth understanding. Its entries are the names your
suites speak: `"machine": "server-a"` in a role, `{{ip "server-a"}}` in a
command, `server-a` in the inventory. Change it and the suites change with it.

SSH is open to `0.0.0.0/0` by default. The instances take keys only (Ubuntu's
cloud images ship with password auth off) and the alternative — pinning your
own address — means re-applying whenever it changes. Narrow `allowed_cidr` if
a cluster is long-lived or the account holds anything else.

## Cross-building

The fleet is x86 by default, and your laptop may not be. `up.sh` builds
`GOOS=linux GOARCH=amd64`, stripped. If your service is not Go, build it on one
of the instances and pull the binary back — `rig` stages whatever is in `-bin`,
it does not care what produced it.

Graviton instance types need `GOARCH=arm64` **and** the arm64 AMI filter in
`main.tf`; changing one without the other produces a cluster that fails at the
first exec, several minutes after you stopped watching.
