# Deploying to Oracle Cloud (Always Free)

Runs the whole stack (Redpanda, producer, indexer, alerter, Postgres, Prometheus, Grafana)
24/7 on one **Ampere A1 VM inside OCI's Always Free tier**. Terraform provisions the VM and
cloud-init sets it up, so the deployment is reproducible and costs nothing.

```
 your laptop ──ssh :22 (your IP only)──►  VCN 10.20.0.0/16 · public subnet
     │                                      └─ VM.Standard.A1.Flex  1 OCPU / 6 GB / 50 GB
     └─ ssh -L 3000/9090/8080 tunnel           Ubuntu 22.04 arm64 · Docker
                                               docker compose (all ports bound to 127.0.0.1)
```

| Choice | Why |
|---|---|
| 1 OCPU / 6 GB / 50 GB | The smallest size that runs the stack reliably (about 3 GB in use). It leaves 3 OCPU, 18 GB and 150 GB of the Always Free allowance for other projects. Redpanda runs on one core (`--smp=1`, 1 GB), and Postgres uses 256 MB of shared buffers with a 1 GB WAL cap. |
| Only port 22, only from `admin_cidr` | The UIs are reached through an SSH tunnel. Compose ports are re-bound to `127.0.0.1` (`docker-compose.oci.yml`), because Docker-published ports bypass the host firewall. |
| `PRODUCER_RATE=20`, 1 indexer worker | The local default of 5,000/s would fill the 50 GB disk within hours. Postgres keeps 7 daily partitions, and Redpanda keeps 24 h of topic data. For more load, raise `producer_rate` together with `boot_volume_gb`. |
| Disk guard cron | If root disk use reaches 85%, the producer is stopped before Postgres or Redpanda run out of space. |
| Secrets generated on the VM | The Postgres and Grafana passwords are created by `bootstrap.sh` into `/opt/log-pipeline/.env` (mode 600). They never appear in git or Terraform state. |
| `restart: unless-stopped` everywhere | The stack comes back by itself after a VM reboot. |
| `ignore_changes` on image/metadata | A new Ubuntu image release can't silently replace the VM and wipe the Postgres volume. |

## One-time setup

1. **OCI account:** sign up at https://signup.cloud.oracle.com. The home region is permanent,
   and Always Free A1 capacity exists only there.
2. **API signing key for Terraform:** Console → profile menu → **My profile** → **API keys** →
   **Add API key** → *Generate API key pair* → download the private key → **Add**.
   - Save the key as `~/.oci/oci_api_key.pem`.
   - Paste the shown *Configuration file preview* into `~/.oci/config`.
   - Set `key_file=~/.oci/oci_api_key.pem` in that file.
3. **Terraform ≥ 1.5:** `winget install Hashicorp.Terraform` (or brew/apt).
4. **Variables:** `cp terraform.tfvars.example terraform.tfvars` and fill in the `region` and
   `tenancy` values from `~/.oci/config`, plus your IPv4 address
   (`curl -4 ifconfig.me`, written as `/32`).

## Deploy

```bash
cd deploy/oci/terraform
terraform init
terraform apply
# first boot takes ~8–12 min on 1 OCPU (Docker install + arm64 image build)
$(terraform output -raw follow_bootstrap)        # watch cloud-init; Ctrl-C when done
$(terraform output -raw tunnel)                  # keep open, then browse:
#   http://localhost:3000  Grafana   (admin / `ssh ubuntu@<ip> sudo grep GRAFANA /opt/log-pipeline/.env`)
#   http://localhost:9090  Prometheus
#   http://localhost:8080  Redpanda Console
```

If the apply fails with **"Out of host capacity"**, the region has no free A1 hosts right
now. Retry with `-var availability_domain_index=1` (or 2), or retry later. Upgrading the
account to Pay-As-You-Go keeps Always Free resources free and usually avoids this error.

## Operate

```bash
ssh ubuntu@<ip>
cd /opt/log-pipeline
C="docker compose -f docker-compose.yml -f /opt/deploy/docker-compose.oci.yml"
$C ps                                   # health
$C logs -f --tail=50 indexer            # follow a service
df -h /                                 # disk budget
sudo git pull && sudo /opt/deploy/bootstrap.sh   # deploy a new app version (keeps .env + data)
```

When your public IP changes, update `admin_cidr` and run `terraform apply` again. Only the
security list changes.

**Tear down:** `terraform destroy` deletes the VM, its boot volume (with all data) and the
network.
