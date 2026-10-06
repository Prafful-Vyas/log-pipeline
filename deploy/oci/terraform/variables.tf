variable "oci_profile" {
  description = "Profile name in ~/.oci/config"
  type        = string
  default     = "DEFAULT"
}

variable "region" {
  description = "Your tenancy's home region (Always Free A1 capacity lives there), e.g. ap-hyderabad-1"
  type        = string
}

variable "compartment_ocid" {
  description = "Compartment for all resources; the tenancy OCID (root compartment) is fine"
  type        = string
}

variable "admin_cidr" {
  description = "Only source allowed to SSH in, e.g. \"203.0.113.7/32\" (your public IP)"
  type        = string

  validation {
    condition     = can(cidrhost(var.admin_cidr, 0)) && var.admin_cidr != "0.0.0.0/0"
    error_message = "admin_cidr must be a valid CIDR and not 0.0.0.0/0."
  }
}

variable "ssh_public_key_path" {
  description = "Public key installed for the 'ubuntu' user"
  type        = string
  default     = "~/.ssh/id_ed25519.pub"
}

# Always Free Ampere allowance is 4 OCPU / 24 GB / 200 GB storage in total. This stack is
# sized to the minimum (1 / 6 / 50) so the rest stays free for other projects.
variable "ocpus" {
  description = "1 OCPU = 1 Ampere core; Redpanda is pinned to --smp=1 to match"
  type        = number
  default     = 1
}

variable "memory_gb" {
  description = "Stack uses ~3 GB at the default producer rate; 6 GB leaves headroom for builds"
  type        = number
  default     = 6
}

variable "boot_volume_gb" {
  description = "50 GB is the OCI minimum; Always Free includes 200 GB of block + boot volume in total"
  type        = number
  default     = 50
}

variable "fault_domain" {
  description = "Optional, e.g. FAULT-DOMAIN-2. Rotating it can get past 'Out of host capacity'"
  type        = string
  default     = null
}

variable "availability_domain_index" {
  description = "Try 1 or 2 if launch fails with 'Out of host capacity'"
  type        = number
  default     = 0
}

variable "repo_url" {
  type    = string
  default = "https://github.com/Prafful-Vyas/log-pipeline.git"
}

variable "repo_ref" {
  description = "Branch, tag or commit of the app code to deploy"
  type        = string
  default     = "main"
}

variable "producer_rate" {
  description = "Synthetic events/sec. ~20/s keeps 7 days of Postgres partitions inside a 50 GB boot volume"
  type        = number
  default     = 20
}
