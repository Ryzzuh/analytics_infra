# Provider and backend pinning (SPEC.md §2, §13).
terraform {
  required_version = ">= 1.9"

  required_providers {
    hcloud = {
      source  = "hetznercloud/hcloud"
      version = "~> 1.51"
    }
  }

  # State lives in Hetzner Object Storage (S3-compatible), not on a laptop: a local state file
  # is how infrastructure becomes un-destroyable when the laptop is reimaged, and `make destroy`
  # is the whole point of the teardown discipline.
  #
  # Configured via -backend-config so the bucket name and credentials are not committed:
  #   terraform init -backend-config=backend.hcl
  backend "s3" {
    key = "analytics-infra/terraform.tfstate"

    # Hetzner Object Storage is S3-compatible but is not AWS, so the AWS-specific preflight
    # calls have to be skipped or init fails against a perfectly good bucket.
    skip_credentials_validation = true
    skip_metadata_api_check     = true
    skip_region_validation      = true
    skip_requesting_account_id  = true
    use_path_style              = true
  }
}

provider "hcloud" {
  token = var.hcloud_token
}
