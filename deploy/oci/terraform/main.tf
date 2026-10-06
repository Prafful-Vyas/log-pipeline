data "oci_identity_availability_domains" "ads" {
  compartment_id = var.compartment_ocid
}

# Latest Canonical Ubuntu 22.04 image built for arm64 (Ampere A1).
data "oci_core_images" "ubuntu_arm" {
  compartment_id           = var.compartment_ocid
  operating_system         = "Canonical Ubuntu"
  operating_system_version = "22.04"
  shape                    = "VM.Standard.A1.Flex"
  sort_by                  = "TIMECREATED"
  sort_order               = "DESC"
}

locals {
  name = "log-pipeline"
  ad   = data.oci_identity_availability_domains.ads.availability_domains[var.availability_domain_index].name
}

# ------------------------------------------------------------------ network
resource "oci_core_vcn" "this" {
  compartment_id = var.compartment_ocid
  display_name   = "${local.name}-vcn"
  cidr_blocks    = ["10.20.0.0/16"]
  dns_label      = "logpipe"
}

resource "oci_core_internet_gateway" "this" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${local.name}-igw"
}

resource "oci_core_route_table" "public" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${local.name}-public-rt"

  route_rules {
    destination       = "0.0.0.0/0"
    network_entity_id = oci_core_internet_gateway.this.id
  }
}

# Only SSH from your IP. Grafana, Prometheus, Redpanda Console and Postgres are reached
# through an SSH tunnel, never exposed to the internet.
resource "oci_core_security_list" "public" {
  compartment_id = var.compartment_ocid
  vcn_id         = oci_core_vcn.this.id
  display_name   = "${local.name}-sl"

  egress_security_rules {
    destination = "0.0.0.0/0"
    protocol    = "all"
  }

  ingress_security_rules {
    source   = var.admin_cidr
    protocol = "6" # TCP
    tcp_options {
      min = 22
      max = 22
    }
  }

  # Path-MTU discovery
  ingress_security_rules {
    source   = "0.0.0.0/0"
    protocol = "1" # ICMP
    icmp_options {
      type = 3
      code = 4
    }
  }
}

resource "oci_core_subnet" "public" {
  compartment_id    = var.compartment_ocid
  vcn_id            = oci_core_vcn.this.id
  display_name      = "${local.name}-public"
  cidr_block        = "10.20.1.0/24"
  dns_label         = "public"
  route_table_id    = oci_core_route_table.public.id
  security_list_ids = [oci_core_security_list.public.id]
}

# ------------------------------------------------------------------ compute
resource "oci_core_instance" "this" {
  compartment_id      = var.compartment_ocid
  availability_domain = local.ad
  fault_domain        = var.fault_domain
  display_name        = local.name
  shape               = "VM.Standard.A1.Flex"

  shape_config {
    ocpus         = var.ocpus
    memory_in_gbs = var.memory_gb
  }

  source_details {
    source_type             = "image"
    source_id               = data.oci_core_images.ubuntu_arm.images[0].id
    boot_volume_size_in_gbs = var.boot_volume_gb
  }

  create_vnic_details {
    subnet_id        = oci_core_subnet.public.id
    assign_public_ip = true
    hostname_label   = local.name
  }

  metadata = {
    ssh_authorized_keys = trimspace(file(pathexpand(var.ssh_public_key_path)))
    user_data = base64encode(templatefile("${path.module}/cloud-init.yaml.tftpl", {
      repo_url      = var.repo_url
      repo_ref      = var.repo_ref
      producer_rate = var.producer_rate
      compose_oci   = file("${path.module}/../docker-compose.oci.yml")
      bootstrap_sh  = file("${path.module}/../bootstrap.sh")
    }))
  }

  # A new image release or cloud-init edit must not silently rebuild the VM (and wipe the
  # Postgres volume); replace it deliberately with `terraform apply -replace=...`.
  lifecycle {
    ignore_changes = [source_details[0].source_id, metadata, fault_domain]
  }
}
