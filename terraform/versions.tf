
terraform {
  backend "s3" {
    bucket       = "telecom-data-pipeline-jvlia17"
    key          = "terraform/terraform.tfstate"
    region       = "ca-central-1"
    use_lockfile = true
  }

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.0"
    }

    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.7"
    }
  }
}
