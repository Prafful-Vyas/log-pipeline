terraform {
  required_version = ">= 1.5"
  required_providers {
    oci = {
      source  = "oracle/oci"
      version = ">= 6.0"
    }
  }
}

# Credentials come from ~/.oci/config (API signing key), never from tfvars.
provider "oci" {
  config_file_profile = var.oci_profile
  region              = var.region
}
